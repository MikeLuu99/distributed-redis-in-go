package store

import (
	"sync"

	"redis-go/internal/db"
)

type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]string
	db   *db.Database // Optional database for persistence
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
		db:   nil,
	}
}

func NewKeyValueStoreWithDB(database *db.Database) *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
		db:   database,
	}
}

func (kv *KeyValueStore) Set(key, value string) error {
	if kv.db != nil {
		if err := kv.db.SetKey(key, []byte(value)); err != nil {
			return err
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value

	return nil
}

func (kv *KeyValueStore) Get(key string) (string, bool, error) {
	kv.mu.RLock()
	if value, exists := kv.data[key]; exists {
		kv.mu.RUnlock()
		return value, true, nil
	}
	kv.mu.RUnlock()

	if kv.db != nil {
		value, err := kv.db.GetKey(key)
		if err != nil {
			return "", false, err
		}
		if value != nil {
			kv.mu.Lock()
			kv.data[key] = string(value)
			kv.mu.Unlock()
			return string(value), true, nil
		}
	}

	return "", false, nil
}

func (kv *KeyValueStore) Del(key string) (bool, error) {
	var dbExisted bool
	if kv.db != nil {
		existed, err := kv.db.DelKey(key)
		if err != nil {
			return false, err
		}
		dbExisted = existed
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.data[key]; exists {
		delete(kv.data, key)
		return true, nil
	}

	return dbExisted, nil
}
