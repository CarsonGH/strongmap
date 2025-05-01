package persistentmap

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
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

// Operation represents a single action on the map
type Operation[K, V any] struct {
	Op    string `json:"op"`
	Key   K      `json:"key"`
	Value *V     `json:"value,omitempty"`
}

// PersistentSyncedMap is a thread-safe map that persists every operation
type PersistentSyncedMap[K comparable, V any] struct {
	mu             sync.Mutex
	filePath       string
	appendFile     *os.File
	appendBuf      *bufio.Writer
	compactEvery   time.Duration
	stop           chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	m              *concurrentmap.SyncedMap[K, V]
	lastCompaction time.Time
	entryCount     int
}

// NewPersistentSyncedMap sets up a new map with persistence via operation logs
func NewPersistentSyncedMap[K comparable, V any](
	filePath string,
	compactInterval time.Duration,
) (*PersistentSyncedMap[K, V], error) {
	now := time.Now()
	p := &PersistentSyncedMap[K, V]{
		filePath:       filePath,
		compactEvery:   compactInterval,
		stop:           make(chan struct{}),
		m:              concurrentmap.NewSyncedMap[K, V](),
		lastCompaction: now,
	}

	// Load existing operations if the file exists
	if err := p.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Open the file for appending operations
	if err := p.openAppend(); err != nil {
		return nil, err
	}

	// Start the background compaction task
	p.wg.Add(1)
	go p.maintenanceLoop()
	return p, nil
}

// openAppend opens the file for appending operations
func (p *PersistentSyncedMap[K, V]) openAppend() error {
	f, err := os.OpenFile(p.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.appendFile = f
	p.appendBuf = bufio.NewWriter(f)
	return nil
}

// Set adds a key-value pair and logs it as a SET operation
func (p *PersistentSyncedMap[K, V]) Set(key K, val V) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.m.Get(key)
	p.m.Set(key, val)
	if !exists {
		p.entryCount++
	}

	op := Operation[K, V]{Op: OpSet, Key: key, Value: &val}
	return p.persist(op)
}

// Delete removes a key and logs it as a DEL operation
func (p *PersistentSyncedMap[K, V]) Delete(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.m.Get(key)
	if exists {
		p.m.Delete(key)
		p.entryCount--
		op := Operation[K, V]{Op: OpDel, Key: key}
		return p.persist(op)
	}
	return nil
}

// Get retrieves a value by key
func (p *PersistentSyncedMap[K, V]) Get(key K) (V, bool) {
	return p.m.Get(key)
}

// Snapshot returns the current map state (for compaction or inspection)
func (p *PersistentSyncedMap[K, V]) Snapshot() map[K]V {
	return p.m.Snapshot()
}

// persist writes an operation to the file as a JSON line
func (p *PersistentSyncedMap[K, V]) persist(op Operation[K, V]) error {
	if p.appendFile == nil || p.appendBuf == nil {
		return fmt.Errorf("can’t write: append file or buffer is fucked")
	}

	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}
	if _, err := p.appendBuf.Write(data); err != nil {
		return fmt.Errorf("failed to write operation: %w", err)
	}
	if _, err := p.appendBuf.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	return p.appendBuf.Flush()
}

// load reads the file and replays all operations to rebuild the map
func (p *PersistentSyncedMap[K, V]) load() error {
	f, err := os.Open(p.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			p.m = concurrentmap.NewSyncedMap[K, V]()
			p.entryCount = 0
			return nil
		}
		return fmt.Errorf("failed to open file %s: %w", p.filePath, err)
	}
	defer f.Close()

	p.m = concurrentmap.NewSyncedMap[K, V]()
	p.entryCount = 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		var op Operation[K, V]
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return fmt.Errorf("failed to unmarshal operation: %w", err)
		}
		switch op.Op {
		case OpSet:
			if op.Value != nil {
				p.m.Set(op.Key, *op.Value)
				p.entryCount++
			}
		case OpDel:
			if _, exists := p.m.Get(op.Key); exists {
				p.m.Delete(op.Key)
				p.entryCount--
			}
		default:
			return fmt.Errorf("unknown operation: %s", op.Op)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	return nil
}

// maintenanceLoop runs periodic compaction
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

// Compact rewrites the file with one SET per current key-value pair
func (p *PersistentSyncedMap[K, V]) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush anything pending
	if p.appendBuf != nil {
		if err := p.appendBuf.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
	}

	// Write current state to a temp file
	tempPath := p.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	buf := bufio.NewWriter(tempFile)
	snapshot := p.m.Snapshot()
	for k, v := range snapshot {
		op := Operation[K, V]{Op: OpSet, Key: k, Value: &v}
		data, err := json.Marshal(op)
		if err != nil {
			return fmt.Errorf("failed to marshal operation: %w", err)
		}
		if _, err := buf.Write(data); err != nil {
			return fmt.Errorf("failed to write operation: %w", err)
		}
		if _, err := buf.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Swap in the new file
	if err := os.Rename(tempPath, p.filePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Close and reopen the append file
	if p.appendFile != nil {
		if err := p.appendFile.Close(); err != nil {
			return fmt.Errorf("failed to close current file: %w", err)
		}
	}
	p.appendFile = nil
	p.appendBuf = nil
	if err := p.openAppend(); err != nil {
		return fmt.Errorf("failed to reopen file: %w", err)
	}

	return nil
}

// Close shuts down the map and cleans up
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
	})
	return err
}
