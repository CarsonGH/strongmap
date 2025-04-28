package sinker

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// tempFilePath creates a temporary file path for testing.
func tempFilePath(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	return filepath.Join(tempDir, "persistent_map_test.db")
}

// TestPersistentSyncedMap runs all test cases for PersistentSyncedMap.
func TestPersistentSyncedMap(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		path := tempFilePath(t)
		p, err := NewPersistentSyncedMap[string, int](path, time.Hour)
		if err != nil {
			t.Fatalf("NewPersistentSyncedMap() error = %v", err)
		}
		defer p.Close()

		// Set keys
		p.Set("one", 1)
		p.Set("two", 2)

		// Get keys
		if v, ok := p.Get("one"); !ok || v != 1 {
			t.Errorf("Get(\"one\") = %v, %v; want 1, true", v, ok)
		}
		if v, ok := p.Get("two"); !ok || v != 2 {
			t.Errorf("Get(\"two\") = %v, %v; want 2, true", v, ok)
		}

		// Delete key
		p.Delete("one")
		if _, ok := p.Get("one"); ok {
			t.Error("Get(\"one\") after delete = _, true; want _, false")
		}
		if v, ok := p.Get("two"); !ok || v != 2 {
			t.Errorf("Get(\"two\") after delete = %v, %v; want 2, true", v, ok)
		}

		// Close and reopen
		p.Close()
		p, err = NewPersistentSyncedMap[string, int](path, time.Hour)
		if err != nil {
			t.Fatalf("Reopen error = %v", err)
		}
		defer p.Close()

		// Check state after reopen
		if _, ok := p.Get("one"); ok {
			t.Error("Get(\"one\") after reopen = _, true; want _, false")
		}
		if v, ok := p.Get("two"); !ok || v != 2 {
			t.Errorf("Get(\"two\") after reopen = %v, %v; want 2, true", v, ok)
		}
	})

	t.Run("Compaction", func(t *testing.T) {
		path := tempFilePath(t)
		p, err := NewPersistentSyncedMap[string, int](path, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("NewPersistentSyncedMap() error = %v", err)
		}
		defer p.Close()

		// Set initial keys
		p.Set("a", 10)
		p.Set("b", 20)

		// Compact explicitly for control
		if err := p.Compact(); err != nil {
			t.Fatalf("Compact() error = %v", err)
		}

		// Set more keys and delete
		p.Set("c", 30)
		p.Delete("b")

		// Compact again to ensure deletion is persisted in snapshot
		if err := p.Compact(); err != nil {
			t.Fatalf("Second Compact() error = %v", err)
		}

		// Close and reopen
		p.Close()
		p, err = NewPersistentSyncedMap[string, int](path, time.Hour)
		if err != nil {
			t.Fatalf("Reopen error = %v", err)
		}
		defer p.Close()

		// Check state
		expected := map[string]int{"a": 10, "c": 30}
		snap := p.Map.Snapshot()
		if !reflect.DeepEqual(snap, expected) {
			t.Errorf("After compaction and reopen, map = %v; want %v", snap, expected)
		}
	})

	t.Run("Concurrency", func(t *testing.T) {
		path := tempFilePath(t)
		p, err := NewPersistentSyncedMap[int, int](path, time.Hour)
		if err != nil {
			t.Fatalf("NewPersistentSyncedMap() error = %v", err)
		}
		defer p.Close()

		var wg sync.WaitGroup
		numGoroutines := 10
		opsPerGoroutine := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := i*opsPerGoroutine + j
					p.Set(key, key*2)
					if j%2 == 0 {
						p.Delete(key)
					}
				}
			}(i)
		}
		wg.Wait()

		// Compact to ensure final state is based on snapshot
		if err := p.Compact(); err != nil {
			t.Fatalf("Compact after concurrency test error = %v", err)
		}

		// Close and reopen to verify persistence
		p.Close()
		p, err = NewPersistentSyncedMap[int, int](path, time.Hour)
		if err != nil {
			t.Fatalf("Reopen after concurrency test error = %v", err)
		}
		defer p.Close()

		// Check final state
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < opsPerGoroutine; j++ {
				key := i*opsPerGoroutine + j
				if j%2 == 0 {
					if _, ok := p.Get(key); ok {
						t.Errorf("Key %d should be deleted", key)
					}
				} else {
					if v, ok := p.Get(key); !ok || v != key*2 {
						t.Errorf("Get(%d) = %v, %v; want %d, true", key, v, ok, key*2)
					}
				}
			}
		}
	})
}
