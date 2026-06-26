package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/replication"
)

func newWebTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	database, closeDB, err := db.NewDatabase(t.TempDir()+"/test.db", false)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}

	shards := &config.Shards{
		Count:  1,
		CurIdx: 0,
		Addrs:  map[int]string{0: "127.0.0.1:8080"},
	}

	return NewServer(database, shards), func() {
		if err := closeDB(); err != nil {
			t.Fatalf("close database: %v", err)
		}
	}
}

func TestGetHandlerDistinguishesEmptyValueFromMissingKey(t *testing.T) {
	server, closeDB := newWebTestServer(t)
	defer closeDB()

	req := httptest.NewRequest(http.MethodGet, "/set?key=empty&value=", nil)
	rec := httptest.NewRecorder()
	server.SetHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("SetHandler() status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/get?key=empty", nil)
	rec = httptest.NewRecorder()
	server.GetHandler(rec, req)

	var res GetResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode GetResponse: %v", err)
	}
	if !res.Found || res.Value != "" || res.Error != "" {
		t.Fatalf("unexpected GetResponse for empty value: %+v", res)
	}

	req = httptest.NewRequest(http.MethodGet, "/get?key=missing", nil)
	rec = httptest.NewRecorder()
	server.GetHandler(rec, req)

	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode missing GetResponse: %v", err)
	}
	if res.Found || res.Value != "" || res.Error != "" {
		t.Fatalf("unexpected GetResponse for missing key: %+v", res)
	}
}

func TestReplicationHandlerReportsDeleteEvents(t *testing.T) {
	server, closeDB := newWebTestServer(t)
	defer closeDB()

	req := httptest.NewRequest(http.MethodGet, "/set?key=key&value=value", nil)
	rec := httptest.NewRecorder()
	server.SetHandler(rec, req)

	req = httptest.NewRequest(http.MethodGet, "/delete-replication-key?key=key&value=value", nil)
	rec = httptest.NewRecorder()
	server.DeleteReplicationKey(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DeleteReplicationKey() for set status = %d, body = %q", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/del?key=key", nil)
	rec = httptest.NewRecorder()
	server.DelHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DelHandler() status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/next-replication-key", nil)
	rec = httptest.NewRecorder()
	server.GetNextKeyForReplication(rec, req)

	var event replication.NextKeyValue
	if err := json.NewDecoder(rec.Body).Decode(&event); err != nil {
		t.Fatalf("decode NextKeyValue: %v", err)
	}
	if !event.Present || !event.Deleted || event.Key != "key" || event.Value != "" || event.Error != "" {
		t.Fatalf("unexpected delete replication event: %+v", event)
	}
}
