package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/TFMV/icebox/server/storage/s3"
	"github.com/TFMV/icebox/utils"
	"github.com/rs/zerolog"
)

// Manager represents a data storage manager for icebox
type Manager struct {
	config *Config
	logger zerolog.Logger
	fs     FileSystem
	meta   *MetadataManager
}

// Config holds data storage configuration
type Config struct {
	Type string `yaml:"type"` // filesystem, s3, memory
	Path string `yaml:"path"` // for filesystem storage
}

// FileSystem interface for data storage operations
type FileSystem interface {
	// Core operations for table management
	WriteFile(path string, data []byte) error
	ReadFile(path string) ([]byte, error)
	MkdirAll(path string) error // No perm parameter, always succeeds

	// Streaming operations for large files
	OpenForRead(path string) (io.ReadCloser, error)
	OpenForWrite(path string) (io.WriteCloser, error)

	// Utility operations
	Remove(path string) error
	Exists(path string) (bool, error)
}

// NewManager creates a new data storage manager
func NewManager(cfg *config.Config, logger zerolog.Logger) (*Manager, error) {
	var fs FileSystem
	var err error

	// Create storage config from server config
	storageCfg := &Config{
		Type: cfg.GetStorageType(),
		Path: cfg.GetStoragePath(),
	}

	// Validate that path is provided when required
	if storageCfg.Type == "filesystem" || storageCfg.Type == "s3" {
		if storageCfg.Path == "" {
			return nil, fmt.Errorf("path is required for %s storage type", storageCfg.Type)
		}
	}

	switch storageCfg.Type {
	case "filesystem":
		fs = filesystem.NewFileStorage()
	case "memory":
		fs, err = memory.NewMemoryStorage()
	case "s3":
		fs, err = s3.NewS3FileSystem(cfg)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", storageCfg.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem: %w", err)
	}

	// Create metadata manager
	meta := NewMetadataManager(storageCfg.Path)

	return &Manager{
		config: storageCfg,
		logger: logger,
		fs:     fs,
		meta:   meta,
	}, nil
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Str("type", m.config.Type).Msg("Initializing data storage")

	// Ensure base directory exists
	if err := m.fs.MkdirAll(m.config.Path); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// Ensure tables directory exists
	tablesPath := getTablePath(m.config.Path, "")
	if err := m.fs.MkdirAll(tablesPath); err != nil {
		return fmt.Errorf("failed to create tables directory: %w", err)
	}

	return nil
}

// Close closes the data storage
func (m *Manager) Close() error {
	m.logger.Info().Msg("Closing data storage")
	// TODO: Implement actual storage cleanup
	return nil
}

// GetStatus returns data storage status
func (m *Manager) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"type": m.config.Type,
		"path": m.config.Path,
	}
}

// GetFileSystem returns the underlying filesystem
func (m *Manager) GetFileSystem() FileSystem {
	return m.fs
}

// ============================================================================
// INTERNAL UTILITIES (moved from utils to storage package)
// ============================================================================

// generateTableFileName creates a filename for a specific table
func generateTableFileName(tableName string, date time.Time, ulid string) string {
	dateStr := date.Format("20060102") // YYYYMMDD format
	return fmt.Sprintf("%s_data_%s_%s.parquet", tableName, dateStr, ulid)
}

// ============================================================================
// TABLE MANAGEMENT METHODS
// ============================================================================

// CreateTable creates a new table with schema
func (m *Manager) CreateTable(tableName string, schema []byte) error {
	m.logger.Info().Str("table", tableName).Msg("Creating new table")

	// Create table metadata
	_, err := m.meta.CreateTableMetadata(tableName, schema)
	if err != nil {
		return fmt.Errorf("failed to create table metadata: %w", err)
	}

	// Save schema file
	schemaPath := getSchemaFilePath(m.config.Path, tableName)
	if err := m.fs.WriteFile(schemaPath, schema); err != nil {
		return fmt.Errorf("failed to save schema file: %w", err)
	}

	m.logger.Info().
		Str("table", tableName).
		Str("schema_path", schemaPath).
		Msg("Table created successfully")

	return nil
}

// InsertData inserts data into a table with automatic file management
func (m *Manager) InsertData(tableName string, data [][]interface{}) error {
	m.logger.Info().
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Inserting data into table")

	// Check if table exists
	if !m.meta.TableExists(tableName) {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	// TODO: Convert data to Parquet format
	// For now, just serialize as JSON for demonstration
	// In production, this would use a proper Parquet writer

	// Generate filename with current date and ULID
	now := time.Now()
	fileName := generateTableFileName(tableName, now, utils.GenerateULIDString())

	// Create file path
	filePath := getDataFilePath(m.config.Path, tableName, fileName)

	// TODO: Convert data to proper format and write
	// For now, create a placeholder file
	placeholderData := []byte(fmt.Sprintf("Data for table %s, %d rows", tableName, len(data)))

	if err := m.fs.WriteFile(filePath, placeholderData); err != nil {
		return fmt.Errorf("failed to write data file: %w", err)
	}

	// Update metadata
	fileInfo := FileInfo{
		Name:     fileName,
		Size:     int64(len(placeholderData)),
		Created:  now,
		Modified: now,
		Date:     now,
	}

	if err := m.meta.AddFileToMetadata(tableName, fileInfo); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	m.logger.Info().
		Str("table", tableName).
		Str("file", fileName).
		Str("path", filePath).
		Msg("Data inserted successfully")

	return nil
}

// GetTableMetadata returns metadata for a table
func (m *Manager) GetTableMetadata(tableName string) (*TableMetadata, error) {
	return m.meta.LoadTableMetadata(tableName)
}

// ListTableFiles returns a list of files for a table
func (m *Manager) ListTableFiles(tableName string) ([]string, error) {
	metadata, err := m.meta.LoadTableMetadata(tableName)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, file := range metadata.Files {
		files = append(files, file.Name)
	}

	return files, nil
}

// GetTableData returns data from a table (placeholder for now)
func (m *Manager) GetTableData(tableName string) ([][]interface{}, error) {
	// TODO: Implement actual data reading from Parquet files
	// This is a placeholder that would be implemented based on the actual data format

	_, err := m.meta.LoadTableMetadata(tableName)
	if err != nil {
		return nil, err
	}

	// For now, return empty data
	// In production, this would read from all data files and combine results
	return [][]interface{}{}, nil
}

// RemoveTable removes a table and all its data
func (m *Manager) RemoveTable(tableName string) error {
	m.logger.Info().Str("table", tableName).Msg("Removing table")

	// Get table metadata to find all files
	metadata, err := m.meta.LoadTableMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to load table metadata: %w", err)
	}

	// Remove all data files
	for _, file := range metadata.Files {
		filePath := getDataFilePath(m.config.Path, tableName, file.Name)
		if err := m.fs.Remove(filePath); err != nil {
			m.logger.Warn().
				Str("file", file.Name).
				Err(err).
				Msg("Failed to remove data file")
		}
	}

	// Remove schema and metadata files
	schemaPath := getSchemaFilePath(m.config.Path, tableName)
	metadataPath := getMetadataFilePath(m.config.Path, tableName)

	if err := m.fs.Remove(schemaPath); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to remove schema file")
	}

	if err := m.fs.Remove(metadataPath); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to remove metadata file")
	}

	// Remove table directory
	tablePath := getTablePath(m.config.Path, tableName)
	if err := m.fs.Remove(tablePath); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to remove table directory")
	}

	m.logger.Info().Str("table", tableName).Msg("Table removed successfully")
	return nil
}

// ListTables returns a list of all tables
func (m *Manager) ListTables() ([]string, error) {
	return m.meta.ListTables()
}

// TableExists checks if a table exists
func (m *Manager) TableExists(tableName string) bool {
	return m.meta.TableExists(tableName)
}
