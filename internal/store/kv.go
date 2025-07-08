package store

import "sync"

type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
	}
}

func (kv *KeyValueStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.data[key]
	return value, exists
}

func (kv *KeyValueStore) Del(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.data[key]; exists {
		delete(kv.data, key)
		return exists
	}
	return false
}
