package db

import (
	"testing"
)

func newTestDatabase(t *testing.T, readOnly bool) (*Database, func()) {
	t.Helper()

	database, closeDB, err := NewDatabase(t.TempDir()+"/test.db", readOnly)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}

	return database, func() {
		if err := closeDB(); err != nil {
			t.Fatalf("close database: %v", err)
		}
	}
}

func TestReplicationQueuePreservesEmptyStringSet(t *testing.T) {
	database, closeDB := newTestDatabase(t, false)
	defer closeDB()

	if err := database.SetKey("empty", []byte("")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}

	event, present, err := database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() error = %v", err)
	}
	if !present {
		t.Fatal("expected replication event to be present")
	}
	if event.Key != "empty" || event.Value != "" || event.Deleted {
		t.Fatalf("unexpected replication event: %+v", event)
	}

	if err := database.DeleteReplicationKey(event.Key, event.Value, event.Deleted); err != nil {
		t.Fatalf("DeleteReplicationKey() error = %v", err)
	}

	_, present, err = database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() after delete error = %v", err)
	}
	if present {
		t.Fatal("expected replication queue to be empty")
	}
}

func TestReplicationQueueRepresentsDeletesExplicitly(t *testing.T) {
	database, closeDB := newTestDatabase(t, false)
	defer closeDB()

	if err := database.SetKey("key", []byte("value")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}
	if err := database.DeleteReplicationKey("key", "value", false); err != nil {
		t.Fatalf("DeleteReplicationKey() for set error = %v", err)
	}

	existed, err := database.DelKey("key")
	if err != nil {
		t.Fatalf("DelKey() error = %v", err)
	}
	if !existed {
		t.Fatal("expected DelKey() to report existing key")
	}

	event, present, err := database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() error = %v", err)
	}
	if !present {
		t.Fatal("expected delete replication event")
	}
	if event.Key != "key" || event.Value != "" || !event.Deleted {
		t.Fatalf("unexpected delete replication event: %+v", event)
	}
}

func TestReplicaDeleteRemovesKey(t *testing.T) {
	database, closeDB := newTestDatabase(t, true)
	defer closeDB()

	if err := database.SetKeyOnReplica("key", []byte("value")); err != nil {
		t.Fatalf("SetKeyOnReplica() error = %v", err)
	}
	if err := database.DelKeyOnReplica("key"); err != nil {
		t.Fatalf("DelKeyOnReplica() error = %v", err)
	}

	value, err := database.GetKey("key")
	if err != nil {
		t.Fatalf("GetKey() error = %v", err)
	}
	if value != nil {
		t.Fatalf("expected key to be deleted, got %q", string(value))
	}
}
