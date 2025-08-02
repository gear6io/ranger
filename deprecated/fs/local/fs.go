package local

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FileSystem implements the iceberg-go FileIO interface for local filesystem
type FileSystem struct {
	basePath string
}

// NewFileSystem creates a new local filesystem implementation
func NewFileSystem(basePath string) *FileSystem {
	return &FileSystem{
		basePath: basePath,
	}
}

// EnsureDir creates a directory if it doesn't exist
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	return nil
}

// Open opens a file for reading
func (fs *FileSystem) Open(path string) (io.ReadCloser, error) {
	localPath := fs.toLocalPath(path)
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}

// Create creates a new file for writing
func (fs *FileSystem) Create(path string) (io.WriteCloser, error) {
	localPath := fs.toLocalPath(path)

	// Ensure directory exists
	if err := EnsureDir(filepath.Dir(localPath)); err != nil {
		return nil, err
	}

	file, err := os.Create(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", path, err)
	}
	return file, nil
}

// Remove removes a file
func (fs *FileSystem) Remove(path string) error {
	localPath := fs.toLocalPath(path)
	if err := os.Remove(localPath); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", path, err)
	}
	return nil
}

// Exists checks if a file exists
func (fs *FileSystem) Exists(path string) (bool, error) {
	localPath := fs.toLocalPath(path)
	_, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence %s: %w", path, err)
	}
	return true, nil
}

// toLocalPath converts a URI to a local filesystem path
func (fs *FileSystem) toLocalPath(uri string) string {
	// Remove file:// prefix if present
	path := strings.TrimPrefix(uri, "file://")

	// If path is relative to base, join with base path
	if !filepath.IsAbs(path) {
		path = filepath.Join(fs.basePath, path)
	}

	return path
}
