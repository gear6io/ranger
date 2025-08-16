package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/TFMV/icebox/server/storage/s3"
	"github.com/rs/zerolog"
)

// Manager manages data storage operations
type Manager struct {
	config      *Config
	logger      zerolog.Logger
	fs          FileSystem
	meta        *MetadataManager
	pathManager *PathManager
	catalog     catalog.CatalogInterface // Fix interface reference
}

// Config holds data storage configuration
type Config struct {
	Type string `yaml:"type"` // filesystem, s3, memory
	Path string `yaml:"path"` // for filesystem storage
}

// FileSystem interface for data storage operations
type FileSystem interface {
	// Core file operations
	WriteFile(path string, data []byte) error
	ReadFile(path string) ([]byte, error)
	MkdirAll(path string) error // No perm parameter, always succeeds

	// Streaming operations for large files
	OpenForRead(path string) (io.ReadCloser, error)
	OpenForWrite(path string) (io.WriteCloser, error)

	// Utility operations
	Remove(path string) error
	Exists(path string) (bool, error)

	// Storage environment preparation
	PrepareTableEnvironment(tableName string) error
	StoreTableData(tableName string, data []byte) error
	GetTableData(tableName string) ([]byte, error)
	RemoveTableEnvironment(tableName string) error
}

// NewManager creates a new data storage manager
func NewManager(cfg *config.Config, logger zerolog.Logger) (*Manager, error) {
	var fs FileSystem
	var err error

	// Get the base data path (already validated in config layer)
	basePath := cfg.GetStoragePath()

	// Create path manager
	pathManager := NewPathManager(basePath)

	// Create storage config from server config
	storageCfg := &Config{
		Type: cfg.GetStorageType(),
		Path: pathManager.GetDataPath(), // Use PathManager for data path
	}

	// Create filesystem based on storage type
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

	// Create metadata manager with internal metadata path from PathManager
	meta := NewMetadataManager(pathManager.GetInternalMetadataDBPath())

	// Initialize catalog
	catalog, err := catalog.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize catalog: %w", err)
	}

	// Log the storage configuration
	logger.Info().
		Str("base_path", basePath).
		Str("storage_type", storageCfg.Type).
		Str("data_path", storageCfg.Path).
		Str("metadata_path", pathManager.GetInternalMetadataDBPath()).
		Str("catalog_uri", pathManager.GetCatalogURI(cfg.GetCatalogType())).
		Msg("Storage manager initialized")

	return &Manager{
		config:      storageCfg,
		logger:      logger,
		fs:          fs,
		meta:        meta,
		pathManager: pathManager,
		catalog:     catalog,
	}, nil
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Str("type", m.config.Type).Msg("Initializing data storage")

	// Ensure the standardized directory structure exists
	if err := m.pathManager.EnsureDirectoryStructure(); err != nil {
		return fmt.Errorf("failed to create directory structure: %w", err)
	}

	// Ensure base directory exists for filesystem operations
	if err := m.fs.MkdirAll(m.config.Path); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// Ensure tables directory exists
	tablesPath := filepath.Join(m.config.Path, "")
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

// GetStatus returns the current status of the storage manager
func (m *Manager) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"type":          m.config.Type,
		"base_path":     m.pathManager.GetBasePath(),
		"catalog_path":  m.pathManager.GetCatalogPath(),
		"data_path":     m.pathManager.GetDataPath(),
		"metadata_path": m.pathManager.GetInternalMetadataDBPath(),
		"initialized":   true,
	}
}

// GetPathManager returns the path manager for external use
func (m *Manager) GetPathManager() *PathManager {
	return m.pathManager
}

// GetCatalog returns the catalog for external use
func (m *Manager) GetCatalog() catalog.CatalogInterface {
	return m.catalog
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
func (m *Manager) CreateTable(tableIdentifier string, schema []byte) error {
	// Parse table identifier (database.table or just table)
	database, tableName := m.pathManager.ParseTableIdentifier(tableIdentifier)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Creating new table")

	// First create table metadata in internal storage
	_, err := m.meta.CreateTableMetadata(tableIdentifier, schema)
	if err != nil {
		return fmt.Errorf("failed to create table metadata: %w", err)
	}

	// Then prepare storage environment
	if err := m.fs.PrepareTableEnvironment(tableIdentifier); err != nil {
		// Clean up metadata if storage preparation fails
		// TODO: Add RemoveTableMetadata method to MetadataManager
		return fmt.Errorf("failed to prepare table storage environment: %w", err)
	}

	// Create Iceberg metadata structure
	if err := m.createIcebergMetadata(database, tableName, schema); err != nil {
		return fmt.Errorf("failed to create Iceberg metadata: %w", err)
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Table created successfully")

	return nil
}

// InsertData inserts data into a table
func (m *Manager) InsertData(tableIdentifier string, data [][]interface{}) error {
	// Parse table identifier (database.table or just table)
	database, tableName := m.pathManager.ParseTableIdentifier(tableIdentifier)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Inserting data into table")

	// Check if table exists in metadata
	if !m.meta.TableExists(tableIdentifier) {
		return fmt.Errorf("table does not exist: %s", tableIdentifier)
	}

	// Serialize data to bytes
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize data: %w", err)
	}

	// Store data in storage
	if err := m.fs.StoreTableData(tableIdentifier, dataBytes); err != nil {
		return fmt.Errorf("failed to store data in storage: %w", err)
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Data inserted successfully")

	return nil
}

// GetTableMetadata returns metadata for a table
func (m *Manager) GetTableMetadata(tableIdentifier string) (*TableMetadata, error) {
	return m.meta.LoadTableMetadata(tableIdentifier)
}

// ListTableFiles returns a list of files for a table
func (m *Manager) ListTableFiles(tableIdentifier string) ([]string, error) {
	metadata, err := m.meta.LoadTableMetadata(tableIdentifier)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, file := range metadata.Files {
		files = append(files, file.Name)
	}

	return files, nil
}

// GetTableData returns data from a table
func (m *Manager) GetTableData(tableIdentifier string) ([][]interface{}, error) {
	// Check if table exists in metadata
	if !m.meta.TableExists(tableIdentifier) {
		return nil, fmt.Errorf("table does not exist: %s", tableIdentifier)
	}

	// Get data from storage
	dataBytes, err := m.fs.GetTableData(tableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to get data from storage: %w", err)
	}

	// Deserialize data
	var data [][]interface{}
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to deserialize data: %w", err)
	}

	return data, nil
}

// GetTableSchema returns schema for a table
func (m *Manager) GetTableSchema(tableIdentifier string) ([]byte, error) {
	metadata, err := m.meta.LoadTableMetadata(tableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to load table metadata: %w", err)
	}

	return metadata.Schema, nil
}

// RemoveTable removes a table and all its data
func (m *Manager) RemoveTable(tableIdentifier string) error {
	// Parse table identifier (database.table or just table)
	database, tableName := m.pathManager.ParseTableIdentifier(tableIdentifier)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Removing table")

	// Check if table exists in metadata
	if !m.meta.TableExists(tableIdentifier) {
		return fmt.Errorf("table does not exist: %s", tableIdentifier)
	}

	// Remove storage environment
	if err := m.fs.RemoveTableEnvironment(tableIdentifier); err != nil {
		return fmt.Errorf("failed to remove table storage environment: %w", err)
	}

	// Remove metadata (if method exists)
	// TODO: Add RemoveTableMetadata method to MetadataManager if needed

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Table removed successfully")
	return nil
}

// ListTables returns a list of all tables
func (m *Manager) ListTables() ([]string, error) {
	return m.meta.ListTables()
}

// TableExists checks if a table exists
func (m *Manager) TableExists(tableIdentifier string) bool {
	return m.meta.TableExists(tableIdentifier)
}

// createIcebergMetadata creates proper Iceberg metadata structure
func (m *Manager) createIcebergMetadata(database, tableName string, schema []byte) error {
	// Create table metadata directory
	metadataDir := m.pathManager.GetTableMetadataPath(database, tableName)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Create data directory
	dataDir := m.pathManager.GetTableDataPath(database, tableName)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create Iceberg metadata file (version 1)
	metadataFile := m.pathManager.GetTableMetadataFile(database, tableName, 1)

	// Parse the schema to get column information
	var schemaData map[string]interface{}
	if err := json.Unmarshal(schema, &schemaData); err != nil {
		return fmt.Errorf("failed to parse schema: %w", err)
	}

	// Create proper Iceberg metadata structure
	icebergMetadata := map[string]interface{}{
		"format-version":  2,
		"table-uuid":      generateUUID(),
		"location":        fmt.Sprintf("file://%s", dataDir),
		"last-updated-ms": time.Now().UnixMilli(),
		"last-column-id":  0,
		"schemas": []map[string]interface{}{
			{
				"schema-id": 0,
				"type":      "struct",
				"fields":    schemaData["fields"],
			},
		},
		"current-schema-id": 0,
		"partition-specs": []map[string]interface{}{
			{
				"spec-id": 0,
				"fields":  []interface{}{},
			},
		},
		"default-spec-id":   0,
		"last-partition-id": 999,
		"sort-orders": []map[string]interface{}{
			{
				"order-id": 0,
				"fields":   []interface{}{},
			},
		},
		"default-sort-order-id": 0,
		"snapshots":             []interface{}{},
		"current-snapshot-id":   nil,
		"refs":                  map[string]interface{}{},
		"snapshot-log":          []interface{}{},
		"metadata-log":          []interface{}{},
		"properties": map[string]interface{}{
			"engine-name":    "icebox",
			"engine-version": "0.1.0",
		},
	}

	// Write metadata atomically
	tempFile := metadataFile + ".tmp"
	defer os.Remove(tempFile) // Clean up temp file

	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary metadata file: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(icebergMetadata); err != nil {
		file.Close()
		return fmt.Errorf("failed to encode metadata JSON: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync metadata file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close metadata file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, metadataFile); err != nil {
		return fmt.Errorf("failed to atomically write metadata file: %w", err)
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("metadata_file", metadataFile).
		Msg("Created Iceberg metadata")

	return nil
}

// generateUUID generates a simple UUID for table identification
func generateUUID() string {
	// Simple UUID generation - in production, use proper UUID library
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix())
}
