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

func (kv *KeyValueStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
	
	// Persist to database if available
	if kv.db != nil {
		kv.db.SetKey(key, []byte(value))
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	// First check in-memory cache
	if value, exists := kv.data[key]; exists {
		return value, exists
	}
	
	// If not in memory and we have a database, check there
	if kv.db != nil {
		if value, err := kv.db.GetKey(key); err == nil && value != nil {
			// Cache the value in memory for future reads
			kv.data[key] = string(value)
			return string(value), true
		}
	}
	
	return "", false
}

func (kv *KeyValueStore) Del(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	existed := false
	if _, exists := kv.data[key]; exists {
		delete(kv.data, key)
		existed = true
	}
	
	// Delete from database if available
	if kv.db != nil {
		kv.db.DelKey(key)
		existed = true // Consider it existed if it was in the database
	}
	
	return existed
}
