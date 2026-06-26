package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"redis-go/internal/db"
	"redis-go/internal/metrics"
)

// NextKeyValue contains the response for GetNextKeyForReplication.
type NextKeyValue struct {
	ID      uint64 `json:"id"`
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
	Present bool   `json:"present"`
	Error   string `json:"error,omitempty"`
}

type client struct {
	db         *db.Database
	leaderAddr string
}

const internalHTTPTimeout = 3 * time.Second

var httpClient = &http.Client{Timeout: internalHTTPTimeout}
var internalAuthToken string

func SetInternalAuthToken(token string) {
	internalAuthToken = token
}

// ClientLoop continuously downloads new keys from the master and applies them.
func ClientLoop(db *db.Database, leaderAddr string) {
	ClientLoopWithContext(context.Background(), db, leaderAddr)
}

func ClientLoopWithContext(ctx context.Context, db *db.Database, leaderAddr string) {
	c := &client{db: db, leaderAddr: leaderAddr}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		present, err := c.loop(ctx)
		if err != nil {
			metrics.ReplicationErrorsTotal.Add(1)
			log.Printf("Loop error: %v", err)
			sleep(ctx, time.Second)
			continue
		}

		if !present {
			sleep(ctx, time.Millisecond*100)
		}
	}
}

func (c *client) loop(ctx context.Context) (present bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.leaderAddr+"/next-replication-key", nil)
	if err != nil {
		return false, err
	}
	addAuthHeader(req)

	resp, err := httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, errors.New(resp.Status)
	}

	var res NextKeyValue
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}

	if res.Error != "" {
		return false, errors.New(res.Error)
	}

	if !res.Present {
		return false, nil
	}

	if res.Deleted {
		if err := c.db.DelKeyOnReplica(res.Key); err != nil {
			return false, err
		}
	} else {
		if err := c.db.SetKeyOnReplica(res.Key, []byte(res.Value)); err != nil {
			return false, err
		}
	}
	metrics.ReplicationEventsTotal.Add(1)

	if err := c.deleteFromReplicationQueue(ctx, res.ID, res.Key, res.Value, res.Deleted); err != nil {
		metrics.ReplicationErrorsTotal.Add(1)
		log.Printf("DeleteKeyFromReplication failed: %v", err)
	}

	return true, nil
}

func (c *client) deleteFromReplicationQueue(ctx context.Context, id uint64, key, value string, deleted bool) error {
	u := url.Values{}
	u.Set("id", strconv.FormatUint(id, 10))
	u.Set("key", key)
	u.Set("value", value)
	if deleted {
		u.Set("deleted", "true")
	}

	log.Printf("Deleting key=%q, value=%q from replication queue on %q", key, value, c.leaderAddr)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+c.leaderAddr+"/delete-replication-key?"+u.Encode(), nil)
	if err != nil {
		return err
	}
	addAuthHeader(req)

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !bytes.Equal(result, []byte("ok")) {
		return errors.New(string(result))
	}

	return nil
}

func addAuthHeader(req *http.Request) {
	if internalAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+internalAuthToken)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
