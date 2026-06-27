package db

import (
	"bytes"
	"os"
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

	if err := database.DeleteReplicationKey(event.ID, event.Key, event.Value, event.Deleted); err != nil {
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
	event, present, err := database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() error = %v", err)
	}
	if !present {
		t.Fatal("expected set replication event")
	}
	if err := database.DeleteReplicationKey(event.ID, event.Key, event.Value, event.Deleted); err != nil {
		t.Fatalf("DeleteReplicationKey() for set error = %v", err)
	}

	existed, err := database.DelKey("key")
	if err != nil {
		t.Fatalf("DelKey() error = %v", err)
	}
	if !existed {
		t.Fatal("expected DelKey() to report existing key")
	}

	event, present, err = database.GetNextKeyForReplication()
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

func TestReplicationQueuePreservesSameKeyUpdateOrder(t *testing.T) {
	database, closeDB := newTestDatabase(t, false)
	defer closeDB()

	if err := database.SetKey("key", []byte("one")); err != nil {
		t.Fatalf("SetKey(one) error = %v", err)
	}
	if err := database.SetKey("key", []byte("two")); err != nil {
		t.Fatalf("SetKey(two) error = %v", err)
	}

	first, present, err := database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() first error = %v", err)
	}
	if !present || first.Key != "key" || first.Value != "one" {
		t.Fatalf("first event = %+v, present=%v; want key=key value=one", first, present)
	}
	if err := database.DeleteReplicationKey(first.ID, first.Key, first.Value, first.Deleted); err != nil {
		t.Fatalf("DeleteReplicationKey(first) error = %v", err)
	}

	second, present, err := database.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication() second error = %v", err)
	}
	if !present || second.Key != "key" || second.Value != "two" {
		t.Fatalf("second event = %+v, present=%v; want key=key value=two", second, present)
	}
	if second.ID <= first.ID {
		t.Fatalf("second ID = %d, want greater than first ID %d", second.ID, first.ID)
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

func TestPingAndReplicationQueueDepth(t *testing.T) {
	database, closeDB := newTestDatabase(t, false)
	defer closeDB()

	if err := database.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	if err := database.SetKey("key", []byte("value")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}

	depth, err := database.ReplicationQueueDepth()
	if err != nil {
		t.Fatalf("ReplicationQueueDepth() error = %v", err)
	}
	if depth != 1 {
		t.Fatalf("ReplicationQueueDepth() = %d, want 1", depth)
	}
}

func TestDatabasePersistsValuesAfterReopen(t *testing.T) {
	path := t.TempDir() + "/test.db"

	database, closeDB, err := NewDatabase(path, false)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	if err := database.SetKey("key", []byte("value")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	database, closeDB, err = NewDatabase(path, false)
	if err != nil {
		t.Fatalf("reopen NewDatabase() error = %v", err)
	}
	defer closeDB()

	value, err := database.GetKey("key")
	if err != nil {
		t.Fatalf("GetKey() error = %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("persisted value = %q, want value", string(value))
	}
}

func TestBackupToProducesRestorableDatabase(t *testing.T) {
	database, closeDB := newTestDatabase(t, false)
	defer closeDB()

	if err := database.SetKey("key", []byte("value")); err != nil {
		t.Fatalf("SetKey() error = %v", err)
	}

	var backup bytes.Buffer
	if err := database.BackupTo(&backup); err != nil {
		t.Fatalf("BackupTo() error = %v", err)
	}
	if backup.Len() == 0 {
		t.Fatal("expected non-empty backup")
	}

	backupPath := t.TempDir() + "/backup.db"
	if err := os.WriteFile(backupPath, backup.Bytes(), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	restored, closeRestored, err := NewDatabase(backupPath, false)
	if err != nil {
		t.Fatalf("NewDatabase(backup) error = %v", err)
	}
	defer closeRestored()

	value, err := restored.GetKey("key")
	if err != nil {
		t.Fatalf("restored GetKey() error = %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("restored value = %q, want value", string(value))
	}
}
