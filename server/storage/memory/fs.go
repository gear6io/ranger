package memory

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
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
func (fsa *MemoryStorage) PrepareTableEnvironment(database, tableName string) error {
	// Create table directory structure with database namespace
	tablePath := filepath.Join("tables", database, tableName)
	if err := fsa.MkdirAll(tablePath); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// Create data subdirectory
	dataPath := filepath.Join(tablePath, "data")
	if err := fsa.MkdirAll(dataPath); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create empty data file
	emptyData := []byte("[]")
	dataFilePath := filepath.Join(dataPath, "data.json")
	if err := fsa.WriteFile(dataFilePath, emptyData); err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}

	return nil
}

// StoreTableData stores data for a table
func (fsa *MemoryStorage) StoreTableData(database, tableName string, data []byte) error {
	// Store data in the data file with database namespace
	dataPath := filepath.Join("tables", database, tableName, "data", "data.json")
	if err := fsa.WriteFile(dataPath, data); err != nil {
		return fmt.Errorf("failed to write data file: %w", err)
	}

	return nil
}

// GetTableData retrieves data for a table
func (fsa *MemoryStorage) GetTableData(database, tableName string) ([]byte, error) {
	// Read data from the data file with database namespace
	dataPath := filepath.Join("tables", database, tableName, "data", "data.json")

	// Try to read the file first
	data, err := fsa.ReadFile(dataPath)
	if err != nil {
		// If file doesn't exist, create it with empty data
		emptyData := []byte("[]")
		if err := fsa.WriteFile(dataPath, emptyData); err != nil {
			return nil, fmt.Errorf("failed to create data file: %w", err)
		}
		return emptyData, nil
	}

	return data, nil
}

// RemoveTableEnvironment removes the storage environment for a table
func (fsa *MemoryStorage) RemoveTableEnvironment(database, tableName string) error {
	// Remove table directory and all files with database namespace
	tablePath := filepath.Join("tables", database, tableName)

	// Remove data file
	dataPath := filepath.Join(tablePath, "data", "data.json")
	if err := fsa.Remove(dataPath); err != nil {
		// Log warning but continue
		fmt.Printf("Warning: failed to remove data file %s: %v\n", dataPath, err)
	}

	// Remove data directory
	dataDir := filepath.Join(tablePath, "data")
	if err := fsa.Remove(dataDir); err != nil {
		// Log warning but continue
		fmt.Printf("Warning: failed to remove data directory %s: %v\n", dataDir, err)
	}

	// Remove table directory
	if err := fsa.Remove(tablePath); err != nil {
		// Log warning but continue
		fmt.Printf("Warning: failed to remove table directory %s: %v\n", tablePath, err)
	}

	return nil
}
