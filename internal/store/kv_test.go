package store

import (
	"strings"
	"testing"

	"redis-go/internal/db"
)

func newStoreTestDatabase(t *testing.T, readOnly bool) (*db.Database, func()) {
	t.Helper()

	database, closeDB, err := db.NewDatabase(t.TempDir()+"/test.db", readOnly)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}

	return database, func() {
		if err := closeDB(); err != nil {
			t.Fatalf("close database: %v", err)
		}
	}
}

func TestSetDoesNotMutateMemoryWhenPersistentWriteFails(t *testing.T) {
	database, closeDB := newStoreTestDatabase(t, true)
	defer closeDB()

	kv := NewKeyValueStoreWithDB(database)
	err := kv.Set("key", "value")
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected read-only error, got %v", err)
	}

	value, exists, err := kv.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if exists {
		t.Fatalf("expected key not to exist after failed write, got %q", value)
	}
}

func TestGetLoadsFromDatabaseAndCachesValue(t *testing.T) {
	database, closeDB := newStoreTestDatabase(t, false)
	defer closeDB()

	if err := database.SetKey("key", []byte("value")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}

	kv := NewKeyValueStoreWithDB(database)
	value, exists, err := kv.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !exists || value != "value" {
		t.Fatalf("Get() = %q, %v; want value, true", value, exists)
	}

	kv.mu.RLock()
	cached := kv.data["key"]
	kv.mu.RUnlock()
	if cached != "value" {
		t.Fatalf("expected value to be cached, got %q", cached)
	}
}
