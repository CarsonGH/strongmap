package concurrentmap

import (
	"maps"
	"sync"
)

// SyncedMap is a generic, thread-safe map with basic operations.
type SyncedMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V // Initialized as nil, usable as zero value
}

// NewSyncedMap creates a new SyncedMap with an initialized internal map.
func NewSyncedMap[K comparable, V any]() *SyncedMap[K, V] {
	return &SyncedMap[K, V]{
		m: make(map[K]V),
	}
}

// Get retrieves a value by key, returning the value and a boolean indicating presence.
func (m *SyncedMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.m[key]
	return v, ok
}

// Exists checks if a key is present in the map.
func (m *SyncedMap[K, V]) Exists(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.m[key]
	return ok
}

// Set assigns a value to a key, initializing the map if necessary.
func (m *SyncedMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[K]V)
	}
	m.m[key] = value
}

// Delete removes a key from the map, safe even if the map is nil.
func (m *SyncedMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m != nil {
		delete(m.m, key)
	}
}

// Snapshot returns a copy of the current map state.
func (m *SyncedMap[K, V]) Snapshot() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.m == nil {
		return make(map[K]V)
	}
	copiedMap := make(map[K]V, len(m.m))
	maps.Copy(copiedMap, m.m)
	return copiedMap
}
