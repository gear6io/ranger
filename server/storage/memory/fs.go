package memory

import (
	"bytes"
	"io"
	"sync"
)

// MemoryStorage implements a simple in-memory data store
type MemoryStorage struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemoryStorage creates a new in-memory data store
func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}, nil
}

// Open opens a file for reading
func (fsa *MemoryStorage) Open(path string) (io.ReadCloser, error) {
	fsa.mu.RLock()
	defer fsa.mu.RUnlock()

	data, exists := fsa.data[path]
	if !exists {
		return nil, io.ErrUnexpectedEOF
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
		return nil, io.ErrUnexpectedEOF
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
	data, exists := fsa.data[path]
	if !exists {
		return nil, io.ErrUnexpectedEOF
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
