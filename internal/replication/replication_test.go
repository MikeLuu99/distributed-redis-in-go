package replication

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"redis-go/internal/db"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestClientLoopAppliesDeleteEventToReplica(t *testing.T) {
	database, closeDB, err := db.NewDatabase(t.TempDir()+"/replica.db", true)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer closeDB()

	if err := database.SetKeyOnReplica("key", []byte("value")); err != nil {
		t.Fatalf("SetKeyOnReplica() error = %v", err)
	}

	previousClient := httpClient
	deleteAcknowledged := false
	httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/next-replication-key":
				return response(`{"key":"key","value":"","deleted":true,"present":true}`), nil
			case "/delete-replication-key":
				if r.URL.Query().Get("key") != "key" || r.URL.Query().Get("deleted") != "true" {
					t.Fatalf("unexpected delete acknowledgement query: %s", r.URL.RawQuery)
				}
				deleteAcknowledged = true
				return response("ok"), nil
			default:
				t.Fatalf("unexpected request path: %s", r.URL.Path)
				return nil, nil
			}
		}),
	}
	defer func() {
		httpClient = previousClient
	}()

	present, err := (&client{db: database, leaderAddr: "leader"}).loop()
	if err != nil {
		t.Fatalf("loop() error = %v", err)
	}
	if !present {
		t.Fatal("expected loop() to process an event")
	}
	if !deleteAcknowledged {
		t.Fatal("expected delete event to be acknowledged")
	}

	value, err := database.GetKey("key")
	if err != nil {
		t.Fatalf("GetKey() error = %v", err)
	}
	if value != nil {
		t.Fatalf("expected replica key to be deleted, got %q", string(value))
	}
}

func response(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}
