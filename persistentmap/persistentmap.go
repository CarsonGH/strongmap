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
	appendEncoder  *gob.Encoder
	compactEvery   time.Duration
	stop           chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	m              *concurrentmap.SyncedMap[K, V]
	lastCompaction time.Time
	entryCount     int
}

// NewPersistentSyncedMap creates a new PersistentSyncedMap with the given configuration.
func NewPersistentSyncedMap[K comparable, V any](
	filePath string,
	compactInterval time.Duration,
) (*PersistentSyncedMap[K, V], error) {
	// Register types for gob
	var k K
	var v V
	gob.Register(k)
	gob.Register(v)

	now := time.Now()
	p := &PersistentSyncedMap[K, V]{
		filePath:       filePath,
		compactEvery:   compactInterval,
		stop:           make(chan struct{}),
		m:              concurrentmap.NewSyncedMap[K, V](),
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
	p.appendEncoder = gob.NewEncoder(p.appendBuf)
	return nil
}

// Set adds or updates a key-value pair and persists the operation.
func (p *PersistentSyncedMap[K, V]) Set(key K, val V) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.m.Get(key)
	p.m.Set(key, val)

	if !exists {
		p.entryCount++
	}

	return p.persist(OpSet, key, &val)
}

// Delete removes a key and persists the operation.
func (p *PersistentSyncedMap[K, V]) Delete(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.m.Get(key)
	if exists {
		p.m.Delete(key)
		p.entryCount--
		return p.persist(OpDel, key, nil)
	}
	return nil
}

// Get retrieves a value by key.
func (p *PersistentSyncedMap[K, V]) Get(key K) (V, bool) {
	return p.m.Get(key)
}

// Snapshot returns a snapshot of the current map state.
func (p *PersistentSyncedMap[K, V]) Snapshot() map[K]V {
	return p.m.Snapshot()
}

// persist appends an operation to the file using the single gob encoder.
func (p *PersistentSyncedMap[K, V]) persist(op string, key K, valPtr *V) error {
	if p.appendFile == nil || p.appendBuf == nil || p.appendEncoder == nil {
		return fmt.Errorf("persist cannot write: append file handle, buffer, or encoder is nil")
	}

	if err := p.appendEncoder.Encode(op); err != nil {
		return err
	}
	if err := p.appendEncoder.Encode(key); err != nil {
		return err
	}

	// Handle nil pointer case for delete operations
	if op == OpDel {
		// Encode a boolean to indicate no value
		if err := p.appendEncoder.Encode(false); err != nil {
			return err
		}
	} else {
		// Encode that there is a value followed by the value
		if err := p.appendEncoder.Encode(true); err != nil {
			return err
		}
		if err := p.appendEncoder.Encode(*valPtr); err != nil {
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
			p.m = concurrentmap.NewSyncedMap[K, V]()
			p.entryCount = 0
			return nil
		}
		return fmt.Errorf("failed to open persistence file %s: %w", p.filePath, err)
	}
	defer f.Close()

	p.m = concurrentmap.NewSyncedMap[K, V]()
	p.entryCount = 0
	dec := gob.NewDecoder(f)

	var snapshot map[K]V
	if err = dec.Decode(&snapshot); err == nil {
		for k, v := range snapshot {
			p.m.Set(k, v)
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
				_, exists := p.m.Get(k)
				p.m.Set(k, v)
				if !exists {
					p.entryCount++
				}
			}
		case OpDel:
			_, exists := p.m.Get(k)
			if exists {
				p.m.Delete(k)
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

// Compact writes a snapshot to a new file and swaps it with the current file.
func (p *PersistentSyncedMap[K, V]) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush current buffer
	if p.appendBuf != nil {
		if err := p.appendBuf.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer before compaction: %w", err)
		}
	}

	// Move current file to temp file
	tempPath := p.filePath + ".tmp"
	if err := os.Rename(p.filePath, tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to move current file to temp: %w", err)
	}

	// Create new file and write snapshot
	newFile, err := os.Create(p.filePath)
	if err != nil {
		// Revert by moving temp file back
		if err := os.Rename(tempPath, p.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to create new file and revert temp file: create error: %w, revert error: %v", err, err)
		}
		return fmt.Errorf("failed to create new file: %w", err)
	}

	// Write snapshot to new file
	newBuf := bufio.NewWriter(newFile)
	newEnc := gob.NewEncoder(newBuf)
	snapshot := p.m.Snapshot()
	if err := newEnc.Encode(snapshot); err != nil {
		newBuf.Flush()
		newFile.Close()
		// Revert by moving temp file back
		if err := os.Rename(tempPath, p.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to encode snapshot and revert temp file: encode error: %w, revert error: %v", err, err)
		}
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}
	if err := newBuf.Flush(); err != nil {
		newFile.Close()
		// Revert by moving temp file back
		if err := os.Rename(tempPath, p.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to flush new file and revert temp file: flush error: %w, revert error: %v", err, err)
		}
		return fmt.Errorf("failed to flush new file: %w", err)
	}
	if err := newFile.Close(); err != nil {
		// Revert by moving temp file back
		if err := os.Rename(tempPath, p.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to close new file and revert temp file: close error: %w, revert error: %v", err, err)
		}
		return fmt.Errorf("failed to close new file: %w", err)
	}

	// Close current file and switch to new file
	if p.appendFile != nil {
		if err := p.appendFile.Close(); err != nil {
			return fmt.Errorf("failed to close current file: %w", err)
		}
	}
	p.appendFile = nil
	p.appendBuf = nil
	p.appendEncoder = nil

	// Open new file for appending
	if err := p.openAppend(); err != nil {
		// Revert by moving temp file back
		if err := os.Rename(tempPath, p.filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to open new append file and revert temp file: open error: %w, revert error: %v", err, err)
		}
		return fmt.Errorf("failed to open new append file: %w", err)
	}

	// Delete temp file
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to delete temp file: %w", err)
	}

	return nil
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
		p.appendFile = nil
		p.appendBuf = nil
		p.appendEncoder = nil
	})
	return err
}