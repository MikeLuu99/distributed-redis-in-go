package server

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/resp"
	"redis-go/internal/store"
)

type writeOnlyConn struct {
	bytes.Buffer
}

func (c *writeOnlyConn) Read(_ []byte) (int, error)         { return 0, nil }
func (c *writeOnlyConn) Close() error                       { return nil }
func (c *writeOnlyConn) LocalAddr() net.Addr                { return dummyAddr("local") }
func (c *writeOnlyConn) RemoteAddr() net.Addr               { return dummyAddr("remote") }
func (c *writeOnlyConn) SetDeadline(_ time.Time) error      { return nil }
func (c *writeOnlyConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *writeOnlyConn) SetWriteDeadline(_ time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func command(values ...string) resp.RESPValue {
	array := make([]resp.RESPValue, len(values))
	for i, value := range values {
		array[i] = resp.RESPValue{Type: resp.RESPBulkString, String: value}
	}
	return resp.RESPValue{Type: resp.RESPArray, Array: array}
}

func TestRouteToShardPreservesEmptyStringValue(t *testing.T) {
	previousClient := httpClient
	httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() != "http://shard-1/get?key=key" {
				t.Fatalf("unexpected routed URL: %s", r.URL.String())
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"found":true,"value":""}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	defer func() {
		httpClient = previousClient
	}()

	shards := &config.Shards{
		Count:  2,
		CurIdx: 0,
		Addrs:  map[int]string{1: "shard-1"},
	}
	conn := &writeOnlyConn{}

	if err := routeToShard(conn, shards, 1, "GET", "key", ""); err != nil {
		t.Fatalf("routeToShard() error = %v", err)
	}
	if got, want := conn.String(), "$0\r\n\r\n"; got != want {
		t.Fatalf("routeToShard() response = %q, want %q", got, want)
	}
}

func TestReplicaWriteReturnsErrorAndDoesNotMutateMemory(t *testing.T) {
	database, closeDB, err := db.NewDatabase(t.TempDir()+"/replica.db", true)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer closeDB()

	kv := store.NewKeyValueStoreWithDB(database)
	conn := &writeOnlyConn{}
	executeCommands(conn, command("SET", "key", "value"), kv, nil)

	if !strings.Contains(conn.String(), "-ERR read-only mode") {
		t.Fatalf("SET response = %q, want read-only error", conn.String())
	}

	value, exists, err := kv.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if exists {
		t.Fatalf("expected failed write not to mutate memory, got %q", value)
	}
}

func TestLocalGetReturnsEmptyBulkStringForEmptyValue(t *testing.T) {
	kv := store.NewKeyValueStore()

	setConn := &writeOnlyConn{}
	executeCommands(setConn, command("SET", "empty", ""), kv, nil)
	if got, want := setConn.String(), "+OK\r\n"; got != want {
		t.Fatalf("SET response = %q, want %q", got, want)
	}

	getConn := &writeOnlyConn{}
	executeCommands(getConn, command("GET", "empty"), kv, nil)
	if got, want := getConn.String(), "$0\r\n\r\n"; got != want {
		t.Fatalf("GET response = %q, want %q", got, want)
	}
}
