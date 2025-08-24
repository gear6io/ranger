package filesystem

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/paths"
)

// Package-specific error codes for filesystem storage
var (
	FileStorageSetupFailed      = errors.MustNewCode("filesystem.setup_failed")
	FileStorageCreateFileFailed = errors.MustNewCode("filesystem.create_file_failed")
	FileStorageFileNotFound     = errors.MustNewCode("filesystem.file_not_found")
	FileStorageOpenFileFailed   = errors.MustNewCode("filesystem.open_file_failed")
	FileStorageCreateDirFailed  = errors.MustNewCode("filesystem.create_dir_failed")
)

// StorageType constant for this storage engine
const Type = "FILESYSTEM"

// FileStorage implements real filesystem-based storage
type FileStorage struct {
	pathManager paths.PathManager
	mu          sync.RWMutex
}

// NewFileStorage creates a new filesystem storage with path management
func NewFileStorage(pathManager paths.PathManager) *FileStorage {
	return &FileStorage{
		pathManager: pathManager,
	}
}

// GetStorageType returns the storage type identifier
func (fs *FileStorage) GetStorageType() string {
	return Type
}

// OpenForRead opens a file for streaming read
func (mfs *FileStorage) OpenForRead(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// OpenForWrite opens a file for streaming write
func (mfs *FileStorage) OpenForWrite(path string) (io.WriteCloser, error) {
	// Ensure parent directories exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// OpenTableForWrite opens a table for streaming write operations
func (mfs *FileStorage) OpenTableForWrite(database, tableName string) (io.WriteCloser, error) {
	// Ensure table environment exists
	if err := mfs.SetupTable(database, tableName); err != nil {
		return nil, err
	}

	// Create data file path using PathManager
	dataPath := mfs.pathManager.GetTableDataPath([]string{database}, tableName)
	parquetPath := filepath.Join(dataPath, "data.parquet")

	// Open file for direct writing
	file, err := os.Create(parquetPath)
	if err != nil {
		return nil, errors.New(FileStorageCreateFileFailed, "failed to create data file", err).AddContext("filesystem", "external_library_call_failed")
	}

	return &tableWriter{
		file:     file,
		fs:       mfs,
		database: database,
		table:    tableName,
		path:     parquetPath,
	}, nil
}

// OpenTableForRead opens a table for streaming read operations
func (mfs *FileStorage) OpenTableForRead(database, tableName string) (io.ReadCloser, error) {
	// Create data file path using PathManager
	dataPath := mfs.pathManager.GetTableDataPath([]string{database}, tableName)
	parquetPath := filepath.Join(dataPath, "data.parquet")

	// Check if file exists
	if _, err := os.Stat(parquetPath); err != nil {
		return nil, errors.New(FileStorageFileNotFound, "table data file does not exist", err).AddContext("path", parquetPath)
	}

	// Open file for direct reading
	file, err := os.Open(parquetPath)
	if err != nil {
		return nil, errors.New(FileStorageOpenFileFailed, "failed to open data file", err).AddContext("filesystem", "external_library_call_failed")
	}

	return file, nil
}

// SetupTable creates the storage environment for a table
func (mfs *FileStorage) SetupTable(database, tableName string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	// Create table directory structure using PathManager
	tablePath := mfs.pathManager.GetTablePath(database, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return errors.New(FileStorageCreateDirFailed, "failed to create table directory", err).AddContext("filesystem", "external_library_call_failed")
	}

	// Create data subdirectory
	dataPath := mfs.pathManager.GetTableDataPath([]string{database}, tableName)
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return errors.New(FileStorageCreateDirFailed, "failed to create data directory", err).AddContext("filesystem", "external_library_call_failed")
	}

	// Create metadata subdirectory
	metadataPath := mfs.pathManager.GetTableMetadataPath([]string{database}, tableName)
	if err := os.MkdirAll(metadataPath, 0755); err != nil {
		return errors.New(FileStorageCreateDirFailed, "failed to create metadata directory", err).AddContext("filesystem", "external_library_call_failed")
	}

	return nil
}

// RemoveTableEnvironment removes the storage environment for a table
func (mfs *FileStorage) RemoveTableEnvironment(database, tableName string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	// Remove table directory and all files using PathManager
	tablePath := mfs.pathManager.GetTablePath(database, tableName)
	return os.RemoveAll(tablePath)
}

// tableWriter implements io.WriteCloser for table data
type tableWriter struct {
	file     *os.File
	fs       *FileStorage
	database string
	table    string
	path     string
}

func (tw *tableWriter) Write(p []byte) (n int, err error) {
	return tw.file.Write(p)
}

func (tw *tableWriter) Close() error {
	return tw.file.Close()
}
