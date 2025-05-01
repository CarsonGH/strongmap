package persistentmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

// Operation represents a single action on the map
type Operation[K, V any] struct {
	Op    string
	Key   K
	Value *V
}

// PersistentSyncedMap is a generic, thread-safe map with persistence
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

// NewPersistentSyncedMap creates a new PersistentSyncedMap with the given configuration
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
	return nil
}

// Set adds or updates a key-value pair and persists the operation
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

// Delete removes a key and persists the operation
func (p *PersistentSyncedMap[K, V]) Delete(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.m.Get(key)
	if exists {
		p.m.Delete(key)
		p.entryCount--
		op := Operation[K, V]{Op: OpDel, Key: key, Value: nil}
		return p.persist(op)
	}
	return nil
}

// Get retrieves a value by key
func (p *PersistentSyncedMap[K, V]) Get(key K) (V, bool) {
	return p.m.Get(key)
}

// Snapshot returns a snapshot of the current map state
func (p *PersistentSyncedMap[K, V]) Snapshot() map[K]V {
	return p.m.Snapshot()
}

// persist appends an operation to the file as a length-prefixed gob blob
func (p *PersistentSyncedMap[K, V]) persist(op Operation[K, V]) error {
	if p.appendFile == nil || p.appendBuf == nil {
		return fmt.Errorf("persist cannot write: append file or buffer is nil")
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(op); err != nil {
		return fmt.Errorf("failed to encode operation: %w", err)
	}

	data := buf.Bytes()
	length := uint32(len(data))

	if err := binary.Write(p.appendBuf, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write length prefix: %w", err)
	}
	if _, err := p.appendBuf.Write(data); err != nil {
		return fmt.Errorf("failed to write operation data: %w", err)
	}
	return p.appendBuf.Flush()
}

// load reads the file and reconstructs the map by replaying operations
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
	reader := bufio.NewReader(f)
	for {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read length prefix: %w", err)
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return fmt.Errorf("failed to read operation data: %w", err)
		}
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		var op Operation[K, V]
		if err := dec.Decode(&op); err != nil {
			return fmt.Errorf("failed to decode operation: %w", err)
		}
		switch op.Op {
		case OpSet:
			if op.Value != nil {
				_, exists := p.m.Get(op.Key)
				p.m.Set(op.Key, *op.Value)
				if !exists {
					p.entryCount++
				}
			}
		case OpDel:
			_, exists := p.m.Get(op.Key)
			if exists {
				p.m.Delete(op.Key)
				p.entryCount--
			}
		default:
			return fmt.Errorf("unknown operation: %s", op.Op)
		}
	}
	return nil
}

// maintenanceLoop handles periodic compaction
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

// Compact writes the current state as SET operations to a new file
func (p *PersistentSyncedMap[K, V]) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.appendBuf != nil {
		if err := p.appendBuf.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer before compaction: %w", err)
		}
	}

	tempPath := p.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	tempBuf := bufio.NewWriter(tempFile)
	snapshot := p.m.Snapshot()
	for k, v := range snapshot {
		op := Operation[K, V]{Op: OpSet, Key: k, Value: &v}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(op); err != nil {
			return fmt.Errorf("failed to encode operation: %w", err)
		}
		data := buf.Bytes()
		length := uint32(len(data))
		if err := binary.Write(tempBuf, binary.LittleEndian, length); err != nil {
			return fmt.Errorf("failed to write length prefix: %w", err)
		}
		if _, err := tempBuf.Write(data); err != nil {
			return fmt.Errorf("failed to write operation data: %w", err)
		}
	}
	if err := tempBuf.Flush(); err != nil {
		return fmt.Errorf("failed to flush temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if p.appendFile != nil {
		if err := p.appendFile.Close(); err != nil {
			return fmt.Errorf("failed to close current file: %w", err)
		}
	}
	if err := os.Rename(tempPath, p.filePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return p.openAppend()
}

// Close stops the maintenance loop and closes the file
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
