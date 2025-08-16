package memory

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/TFMV/icebox/pkg/errors"
)

// StorageType constant for this storage engine
const Type = "MEMORY"

// MemoryStorage implements a simple in-memory data store
type MemoryStorage struct {
	data   map[string][]byte
	tables map[string]*TableData
	mu     sync.RWMutex
}

// GetStorageType returns the storage type identifier
func (ms *MemoryStorage) GetStorageType() string {
	return Type
}

// TableData represents a table in memory storage
type TableData struct {
	Schema []byte
	Rows   [][]interface{}
}

// NewMemoryStorage creates a new in-memory data store
func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		data:   make(map[string][]byte),
		tables: make(map[string]*TableData),
	}, nil
}

// Open opens a file for reading
func (fsa *MemoryStorage) Open(path string) (io.ReadCloser, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	data, exists := fsa.data[path]
	if !exists {
		return nil, errors.New(ErrFileNotFound, "file not found").
			AddContext("path", path)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

// Create creates a new file for writing
func (fsa *MemoryStorage) Create(path string) (io.WriteCloser, error) {
	return &memoryWriteCloser{
		adapter: fsa,
		path:    path,
		buf:     bytes.NewBuffer(nil),
	}, nil
}

// Remove removes a file
func (fsa *MemoryStorage) Remove(path string) error {
	fsa.mu.Lock()
	defer fsa.mu.Unlock()

	if _, exists := fsa.data[path]; !exists {
		return errors.New(ErrFileNotFound, "file not found for removal").
			AddContext("path", path)
	}

	delete(fsa.data, path)
	return nil
}

// Exists checks if a file exists
func (fsa *MemoryStorage) Exists(path string) (bool, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	_, exists := fsa.data[path]
	return exists, nil
}

// MkdirAll creates directories recursively (no-op for memory storage)
func (fsa *MemoryStorage) MkdirAll(path string) error {
	// Memory storage doesn't need directories
	return nil
}

// WriteFile writes data to a file (convenience method)
func (fsa *MemoryStorage) WriteFile(path string, data []byte) error {
	fsa.mu.Lock()
	defer fsa.mu.Unlock()

	fsa.data[path] = data
	return nil
}

// ReadFile reads data from a file (convenience method)
func (fsa *MemoryStorage) ReadFile(path string) ([]byte, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	data, exists := fsa.data[path]
	if !exists {
		return nil, errors.New(ErrFileNotFound, "file not found").
			AddContext("path", path)
	}

	return data, nil
}

// memoryWriteCloser implements io.WriteCloser for memory storage
type memoryWriteCloser struct {
	adapter *MemoryStorage
	path    string
	buf     *bytes.Buffer
}

func (mwc *memoryWriteCloser) Write(p []byte) (n int, err error) {
	return mwc.buf.Write(p)
}

func (mwc *memoryWriteCloser) Close() error {
	mwc.adapter.mu.Lock()
	defer mwc.adapter.mu.Unlock()

	mwc.adapter.data[mwc.path] = mwc.buf.Bytes()
	return nil
}

// OpenForRead opens a file for streaming read
func (fsa *MemoryStorage) OpenForRead(path string) (io.ReadCloser, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	data, exists := fsa.data[path]
	if !exists {
		return nil, errors.New(ErrFileNotFound, "file not found for streaming read").
			AddContext("path", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// OpenForWrite opens a file for streaming write
func (fsa *MemoryStorage) OpenForWrite(path string) (io.WriteCloser, error) {
	return &memoryWriteCloser{
		adapter: fsa,
		path:    path,
		buf:     bytes.NewBuffer(nil),
	}, nil
}

// ============================================================================
// STORAGE ENVIRONMENT PREPARATION METHODS
// ============================================================================

// PrepareTableEnvironment creates the storage environment for a table
func (fsa *MemoryStorage) PrepareTableEnvironment(tableName string) error {
	fsa.mu.Lock()
	defer fsa.mu.Unlock()

	// Check if table already exists
	if _, exists := fsa.tables[tableName]; exists {
		return errors.New(ErrTableAlreadyExists, "table environment already exists").
			AddContext("table_name", tableName)
	}

	// Create in-memory table data structure
	fsa.tables[tableName] = &TableData{
		Schema: []byte{},
		Rows:   make([][]interface{}, 0),
	}
	return nil
}

// StoreTableData stores data for a table
func (fsa *MemoryStorage) StoreTableData(tableName string, data []byte) error {
	fsa.mu.Lock()
	defer fsa.mu.Unlock()

	// Check if table environment exists
	if _, exists := fsa.tables[tableName]; !exists {
		return errors.New(ErrTableNotFound, "table environment not found").
			AddContext("table_name", tableName)
	}

	// Store data in the table
	fsa.data[fmt.Sprintf("tables/%s/data", tableName)] = data
	return nil
}

// GetTableData retrieves data for a table
func (fsa *MemoryStorage) GetTableData(tableName string) ([]byte, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	// Check if table environment exists
	if _, exists := fsa.tables[tableName]; !exists {
		return nil, errors.New(ErrTableNotFound, "table environment not found").
			AddContext("table_name", tableName)
	}

	data, exists := fsa.data[fmt.Sprintf("tables/%s/data", tableName)]
	if !exists {
		return nil, errors.New(ErrTableDataNotFound, "table data not found").
			AddContext("table_name", tableName)
	}

	return data, nil
}

// RemoveTableEnvironment removes the storage environment for a table
func (fsa *MemoryStorage) RemoveTableEnvironment(tableName string) error {
	fsa.mu.Lock()
	defer fsa.mu.Unlock()

	// Check if table environment exists
	if _, exists := fsa.tables[tableName]; !exists {
		return errors.New(ErrTableNotFound, "table environment not found for removal").
			AddContext("table_name", tableName)
	}

	// Remove table data structure
	delete(fsa.tables, tableName)

	// Remove table data
	delete(fsa.data, fmt.Sprintf("tables/%s/data", tableName))

	return nil
}
