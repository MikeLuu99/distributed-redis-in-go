package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")
var replicaBucket = []byte("replication")

type replicationRecord struct {
	Op    string `json:"op"`
	Value string `json:"value,omitempty"`
}

// ReplicationEvent describes a pending change for replicas.
type ReplicationEvent struct {
	Key     string
	Value   string
	Deleted bool
}

// Database is an open bolt database.
type Database struct {
	db       *bolt.DB
	readOnly bool
}

// NewDatabase returns an instance of a database that we can work with.
func NewDatabase(dbPath string, readOnly bool) (db *Database, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDb, readOnly: readOnly}
	closeFunc = boltDb.Close

	if err := db.createBuckets(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets the key to the requested value into the default database or returns an error.
func (d *Database) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}

	record, err := encodeReplicationRecord(replicationRecord{
		Op:    "set",
		Value: string(value),
	})
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}

		return tx.Bucket(replicaBucket).Put([]byte(key), record)
	})
}

// SetKeyOnReplica sets the key to the requested value into the default database and does not write
// to the replication queue.
// This method is intended to be used only on replicas.
func (d *Database) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// GetNextKeyForReplication returns the next pending change for replicas.
// If there are no pending changes, present will be false.
func (d *Database) GetNextKeyForReplication() (event ReplicationEvent, present bool, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		if k == nil {
			return nil
		}

		record, err := decodeReplicationRecord(copyByteSlice(v))
		if err != nil {
			return err
		}

		event = ReplicationEvent{
			Key:     string(k),
			Value:   record.Value,
			Deleted: record.Op == "del",
		}
		present = true
		return nil
	})

	if err != nil {
		return ReplicationEvent{}, false, err
	}

	return event, present, nil
}

// DeleteReplicationKey deletes the key from the replication queue if the
// operation and value still match the pending change.
func (d *Database) DeleteReplicationKey(key string, value string, deleted bool) (err error) {
	op := "set"
	if deleted {
		op = "del"
	}
	record, err := encodeReplicationRecord(replicationRecord{
		Op:    op,
		Value: value,
	})
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)

		v := b.Get([]byte(key))
		if v == nil {
			return errors.New("key does not exist")
		}

		if !bytes.Equal(v, record) {
			return errors.New("value does not match")
		}

		return b.Delete([]byte(key))
	})
}

// GetKey get the value of the requested from a default database.
func (d *Database) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = copyByteSlice(b.Get([]byte(key)))
		return nil
	})

	if err == nil {
		return result, nil
	}
	return nil, err
}

// DelKey deletes the key from the database.
func (d *Database) DelKey(key string) (bool, error) {
	if d.readOnly {
		return false, errors.New("read-only mode")
	}

	record, err := encodeReplicationRecord(replicationRecord{Op: "del"})
	if err != nil {
		return false, err
	}

	existed := false
	err = d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b.Get([]byte(key)) != nil {
			existed = true
		}
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		return tx.Bucket(replicaBucket).Put([]byte(key), record)
	})
	return existed, err
}

// DeleteExtraKeys deletes the keys that do not belong to this shard.
func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			ks := string(k)
			if isExtra(ks) {
				keys = append(keys, ks)
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)

		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *Database) DelKeyOnReplica(key string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Delete([]byte(key))
	})
}

func encodeReplicationRecord(record replicationRecord) ([]byte, error) {
	return json.Marshal(record)
}

func decodeReplicationRecord(data []byte) (replicationRecord, error) {
	var record replicationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return replicationRecord{}, err
	}
	if record.Op != "set" && record.Op != "del" {
		return replicationRecord{}, fmt.Errorf("unknown replication operation %q", record.Op)
	}
	return record, nil
}
