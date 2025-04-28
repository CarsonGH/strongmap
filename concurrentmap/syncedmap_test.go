package concurrentmap

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

// TestSyncedMap_BasicOperations tests the basic functionality of Set, Get, Exists, and Delete.
func TestSyncedMap_BasicOperations(t *testing.T) {
	m := NewSyncedMap[string, int]()

	// Set keys
	m.Set("one", 1)
	m.Set("two", 2)
	m.Set("three", 3)

	// Test Get
	if v, ok := m.Get("one"); !ok || v != 1 {
		t.Errorf("Get(\"one\") = %v, %v; want 1, true", v, ok)
	}
	if v, ok := m.Get("four"); ok || v != 0 {
		t.Errorf("Get(\"four\") = %v, %v; want 0, false", v, ok)
	}

	// Test Exists
	if !m.Exists("one") {
		t.Error("Exists(\"one\") = false; want true")
	}
	if m.Exists("four") {
		t.Error("Exists(\"four\") = true; want false")
	}

	// Test Delete
	m.Delete("two")
	if _, ok := m.Get("two"); ok {
		t.Error("Get(\"two\") after delete = _, true; want _, false")
	}
	if m.Exists("two") {
		t.Error("Exists(\"two\") after delete = true; want false")
	}

	// Test Set after Delete
	m.Set("two", 22)
	if v, ok := m.Get("two"); !ok || v != 22 {
		t.Errorf("Get(\"two\") after re-set = %v, %v; want 22, true", v, ok)
	}

	// Test Delete non-existent key
	m.Delete("five") // Should be a no-op

	// Test Snapshot
	expected := map[string]int{"one": 1, "three": 3, "two": 22}
	snap := m.Snapshot()
	if !reflect.DeepEqual(snap, expected) {
		t.Errorf("Snapshot() = %v; want %v", snap, expected)
	}
}

// TestSyncedMap_Snapshot verifies that Snapshot returns an accurate, isolated copy.
func TestSyncedMap_Snapshot(t *testing.T) {
	m := NewSyncedMap[string, int]()

	m.Set("a", 1)
	m.Set("b", 2)

	snap1 := m.Snapshot()
	expected1 := map[string]int{"a": 1, "b": 2}
	if !reflect.DeepEqual(snap1, expected1) {
		t.Errorf("snap1 = %v; want %v", snap1, expected1)
	}

	m.Set("c", 3)
	m.Delete("a")

	snap2 := m.Snapshot()
	expected2 := map[string]int{"b": 2, "c": 3}
	if !reflect.DeepEqual(snap2, expected2) {
		t.Errorf("snap2 = %v; want %v", snap2, expected2)
	}

	// Verify snap1 remains unchanged
	if !reflect.DeepEqual(snap1, expected1) {
		t.Errorf("snap1 modified to %v; want %v", snap1, expected1)
	}

	// Modify snap1 and ensure original map is unaffected
	snap1["d"] = 4
	snap3 := m.Snapshot()
	if !reflect.DeepEqual(snap3, expected2) {
		t.Errorf("After modifying snap1, m.Snapshot() = %v; want %v", snap3, expected2)
	}
}

// TestSyncedMap_ConcurrentSetGet tests concurrent Set operations.
func TestSyncedMap_ConcurrentSetGet(t *testing.T) {
	m := NewSyncedMap[string, int]()
	var wg sync.WaitGroup

	const N = 10  // Number of goroutines
	const M = 100 // Keys per goroutine

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < M; j++ {
				key := fmt.Sprintf("key_%d_%d", i, j)
				value := i*100 + j
				m.Set(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			key := fmt.Sprintf("key_%d_%d", i, j)
			expected := i*100 + j
			if v, ok := m.Get(key); !ok || v != expected {
				t.Errorf("Get(%q) = %v, %v; want %d, true", key, v, ok, expected)
			}
		}
	}
}

// TestSyncedMap_ConcurrentDelete tests concurrent Delete operations.
func TestSyncedMap_ConcurrentDelete(t *testing.T) {
	m := NewSyncedMap[string, int]()
	var wgSet sync.WaitGroup

	const N = 10
	const M = 100

	// Set keys
	for i := 0; i < N; i++ {
		wgSet.Add(1)
		go func(i int) {
			defer wgSet.Done()
			for j := 0; j < M; j++ {
				key := fmt.Sprintf("key_%d_%d", i, j)
				value := i*100 + j
				m.Set(key, value)
			}
		}(i)
	}
	wgSet.Wait()

	// Delete even-indexed keys
	var wgDelete sync.WaitGroup
	for i := 0; i < N; i++ {
		wgDelete.Add(1)
		go func(i int) {
			defer wgDelete.Done()
			for j := 0; j < M; j += 2 {
				key := fmt.Sprintf("key_%d_%d", i, j)
				m.Delete(key)
			}
		}(i)
	}
	wgDelete.Wait()

	// Verify final state
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			key := fmt.Sprintf("key_%d_%d", i, j)
			if j%2 == 0 {
				if _, ok := m.Get(key); ok {
					t.Errorf("Key %q should be deleted", key)
				}
			} else {
				expected := i*100 + j
				if v, ok := m.Get(key); !ok || v != expected {
					t.Errorf("Get(%q) = %v, %v; want %d, true", key, v, ok, expected)
				}
			}
		}
	}
}

// TestSyncedMap_ConcurrentMixedOperations stress-tests with mixed operations.
func TestSyncedMap_ConcurrentMixedOperations(t *testing.T) {
	m := NewSyncedMap[string, int]()
	var wg sync.WaitGroup

	const N = 20
	const OpsPerGoroutine = 1000

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < OpsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d", i%5) // Overlapping keys
				switch rand.Intn(4) {
				case 0:
					m.Set(key, j)
				case 1:
					m.Get(key)
				case 2:
					m.Delete(key)
				case 3:
					m.Snapshot()
				}
			}
		}(i)
	}

	wg.Wait()
	// No specific assertions; rely on race detector
}

// TestSyncedMap_IntString ensures functionality with different types.
func TestSyncedMap_IntString(t *testing.T) {
	m := NewSyncedMap[int, string]()

	m.Set(1, "one")
	m.Set(2, "two")

	if v, ok := m.Get(1); !ok || v != "one" {
		t.Errorf("Get(1) = %v, %v; want \"one\", true", v, ok)
	}

	m.Delete(1)
	if _, ok := m.Get(1); ok {
		t.Error("Get(1) after delete = _, true; want _, false")
	}
}
