# strongmap

A Go package providing an implmentation of generic sync maps and persitent generic sync maps using redis like AOF (Append Only File). Use cases are for single instance go monolithic servers that store items in memory but want persistence for restarts.

- Data is stored into GOB data file, so it's not human readable and is only usable by go programs. 
- This is NOT made for multi-machine scaling usecases
- This still has a single failure point, if the machine storage is corrupted persitence will be destroyed. 



## Description

*   **`concurrentmap.SyncedMap`**: A thread-safe map suitable for concurrent read/write access. It uses a `sync.RWMutex` for synchronization.
*   **`persistentmap.PersistentSyncedMap`**: A thread-safe map that persists its state to a file. It uses an append-only log for write operations and supports periodic compaction. It builds upon `concurrentmap.SyncedMap` for in-memory storage.

## Installation

```bash
go get github.com/carsongh/strongmap
```

## Usage

(Generic Sync Map)
### `concurrentmap.SyncedMap`

```go
package main

import (
	"fmt"
	"github.com/carsongh/strongmap/concurrentmap"
)

func main() {
	// Create a new synced map for string keys and int values
	m := concurrentmap.NewSyncedMap[string, int]()

	// Set values
	m.Set("apple", 1)
	m.Set("banana", 2)

	// Get a value
	if val, ok := m.Get("apple"); ok {
		fmt.Println("Apple count:", val)
	}

	// Check existence
	if m.Exists("banana") {
		fmt.Println("Banana exists")
	}

	// Delete a key
	m.Delete("apple")

	// Get a snapshot (copy) of the map
	snapshot := m.Snapshot()
	fmt.Printf("Snapshot: %+v\n", snapshot)
}

```

(Persistent Generic Sync Map)
### `persistentmap.PersistentSyncedMap`

```go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/carsongh/strongmap/persistentmap"
)

func main() {
	filePath := "my_persistent_map.data"

	// Create a new persistent map, compacting every hour minimum
	pm, err := persistentmap.NewPersistentSyncedMap[string, string](filePath, 60*time.Minute)
	if err != nil {
		log.Fatalf("Failed to create persistent map: %v", err)
	}
	defer pm.Close() // Ensure the persisting file is properly closed

	// Set some values
	err = pm.Set("key1", "value1")
	if err != nil {
		log.Printf("Failed to set key1: %v", err)
	}
	err = pm.Set("key2", "value2")
	if err != nil {
		log.Printf("Failed to set key2: %v", err)
	}


	// Get a value
	if val, ok := pm.Get("key1"); ok {
		fmt.Println("Value for key1:", val)
	}

	// Delete a key
	err = pm.Delete("key2")
	if err != nil {
		log.Printf("Failed to delete key2: %v", err)
	}


	// Close the map persistence file and go operations (writes remaining buffer, stops background tasks)
	if err := pm.Close(); err != nil {
		log.Printf("Error closing map: %v", err)
	}

	// Re-load the map from the file (demonstrates persistence)
	pm2, err := persistentmap.NewPersistentSyncedMap[string, string](filePath, 1*time.Minute)
	if err != nil {
		log.Fatalf("Failed to reload persistent map: %v", err)
	}
	defer pm2.Close()

	if val, ok := pm2.Get("key1"); ok {
		fmt.Println("Reloaded value for key1:", val)
	}
	if _, ok := pm2.Get("key2"); !ok {
		fmt.Println("Key2 correctly deleted")
	}


}
```
