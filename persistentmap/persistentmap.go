package persistentmap

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/carsongh/strongmap/concurrentmap"
)

// Operation constants
const (
	OpSet = "SET"
	OpDel = "DEL"
)

// PersistentSyncedMap is a generic, thread-safe map with persistence.
type PersistentSyncedMap[K comparable, V any] struct {
	mu             sync.Mutex
	filePath       string
	appendFile     *os.File
	appendBuf      *bufio.Writer
	compactEvery   time.Duration
	stop           chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	Map            *concurrentmap.SyncedMap[K, V]
	lastCompaction time.Time
	entryCount     int
}

// NewPersistentSyncedMap creates a new PersistentSyncedMap with the given configuration.
func NewPersistentSyncedMap[K comparable, V any](
	filePath string,
	compactInterval time.Duration,
) (*PersistentSyncedMap[K, V], error) {
	// Register types for gob - we don't need to register pointers now
	// as we're handling them separately in our persist/load methods
	var k K
	var v V
	gob.Register(k)
	gob.Register(v)

	now := time.Now()
	p := &PersistentSyncedMap[K, V]{
		filePath:       filePath,
		compactEvery:   compactInterval,
		stop:           make(chan struct{}),
		Map:            concurrentmap.NewSyncedMap[K, V](),
		lastCompaction: now,
	}

	if err := p.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := p.openAppend(); err != nil {
		return nil, err
	}

	p.wg.Add(1)
	go p.maintenanceLoop()
	return p, nil
}

func (p *PersistentSyncedMap[K, V]) openAppend() error {
	f, err := os.OpenFile(p.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.appendFile = f
	p.appendBuf = bufio.NewWriter(f)
	return nil
}

// Set adds or updates a key-value pair and persists the operation.
func (p *PersistentSyncedMap[K, V]) Set(key K, val V) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.Map.Get(key)
	p.Map.Set(key, val)

	if !exists {
		p.entryCount++
	}

	return p.persist(OpSet, key, &val)
}

// Delete removes a key and persists the operation.
func (p *PersistentSyncedMap[K, V]) Delete(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.Map.Get(key)
	if exists {
		p.Map.Delete(key)
		p.entryCount--
		return p.persist(OpDel, key, nil)
	}
	return nil
}

// Get retrieves a value by key.
func (p *PersistentSyncedMap[K, V]) Get(key K) (V, bool) {
	return p.Map.Get(key)
}

// persist appends an operation to the file.
func (p *PersistentSyncedMap[K, V]) persist(op string, key K, valPtr *V) error {
	if p.appendFile == nil || p.appendBuf == nil {
		return fmt.Errorf("persist cannot write: append file handle or buffer is nil")
	}

	enc := gob.NewEncoder(p.appendBuf)
	if err := enc.Encode(op); err != nil {
		return err
	}
	if err := enc.Encode(key); err != nil {
		return err
	}

	// Handle nil pointer case for delete operations
	if op == OpDel {
		// For delete operations, we don't need to encode the value,
		// just encode a boolean to indicate there's no value
		if err := enc.Encode(false); err != nil {
			return err
		}
	} else {
		// For set operations, encode that there is a value followed by the value
		if err := enc.Encode(true); err != nil {
			return err
		}
		if err := enc.Encode(*valPtr); err != nil {
			return err
		}
	}

	return p.appendBuf.Flush()
}

// load reads the file and reconstructs the map.
func (p *PersistentSyncedMap[K, V]) load() error {
	f, err := os.Open(p.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			p.Map = concurrentmap.NewSyncedMap[K, V]()
			p.entryCount = 0
			return nil
		}
		return fmt.Errorf("failed to open persistence file %s: %w", p.filePath, err)
	}
	defer f.Close()

	p.Map = concurrentmap.NewSyncedMap[K, V]()
	p.entryCount = 0
	dec := gob.NewDecoder(f)

	var snapshot map[K]V
	if err = dec.Decode(&snapshot); err == nil {
		for k, v := range snapshot {
			p.Map.Set(k, v)
			p.entryCount++
		}
	} else {
		if _, seekErr := f.Seek(0, 0); seekErr != nil {
			return fmt.Errorf("failed to seek to beginning after snapshot decode error: %w", seekErr)
		}
		dec = gob.NewDecoder(f)
	}

	for {
		var op string
		if err := dec.Decode(&op); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("unexpected end of file while decoding operation: %w", err)
			}
			return fmt.Errorf("error decoding operation: %w", err)
		}

		var k K
		if err := dec.Decode(&k); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("unexpected end of file while decoding key for op %s: %w", op, err)
			}
			return fmt.Errorf("error decoding key for op %s: %w", op, err)
		}

		var hasValue bool
		if err := dec.Decode(&hasValue); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("unexpected end of file while decoding value flag for key %v, op %s: %w", k, op, err)
			}
			return fmt.Errorf("error decoding value flag for key %v, op %s: %w", k, op, err)
		}

		switch op {
		case OpSet:
			if hasValue {
				var v V
				if err := dec.Decode(&v); err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						return fmt.Errorf("unexpected end of file while decoding value for set key %v: %w", k, err)
					}
					return fmt.Errorf("error decoding value for set key %v: %w", k, err)
				}
				_, exists := p.Map.Get(k)
				p.Map.Set(k, v)
				if !exists {
					p.entryCount++
				}
			}
		case OpDel:
			_, exists := p.Map.Get(k)
			if exists {
				p.Map.Delete(k)
				p.entryCount--
			}
		default:
			return fmt.Errorf("unknown operation decoded: %s", op)
		}
	}

	return nil
}

// maintenanceLoop handles periodic compaction.
func (p *PersistentSyncedMap[K, V]) maintenanceLoop() {
	if p.compactEvery <= 0 {
		p.wg.Done()
		return
	}
	ticker := time.NewTicker(p.compactEvery)
	defer func() { ticker.Stop(); p.wg.Done() }()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if err := p.Compact(); err == nil {
				p.lastCompaction = now
			}
		case <-p.stop:
			return
		}
	}
}

// Compact writes a snapshot of the map to the file.
func (p *PersistentSyncedMap[K, V]) Compact() error {
	snapshot := p.Map.Snapshot()
	tempPath := p.filePath + ".tmp"

	// Write snapshot to temp file
	f, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err = enc.Encode(snapshot); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}

	// Replace main file with temp file
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.appendBuf != nil {
		p.appendBuf.Flush()
	}
	if p.appendFile != nil {
		p.appendFile.Close()
	}
	if err := os.Rename(tempPath, p.filePath); err != nil {
		return err
	}
	return p.openAppend()
}

// Close stops the maintenance loop and closes the file.
func (p *PersistentSyncedMap[K, V]) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.stop)
		p.wg.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.appendBuf != nil {
			err = p.appendBuf.Flush()
		}
		if p.appendFile != nil {
			err = p.appendFile.Close()
		}
	})
	return err
}
