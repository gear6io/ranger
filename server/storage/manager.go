package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/metadata"
	"github.com/TFMV/icebox/server/metadata/types"
	"github.com/rs/zerolog"
)

// Manager manages data storage operations
type Manager struct {
	config         *Config
	logger         zerolog.Logger
	engineRegistry *StorageEngineRegistry
	meta           metadata.MetadataManagerInterface
	pathManager    *PathManager
	catalog        catalog.CatalogInterface
}

// Config holds data storage configuration
type Config struct {
	Path string `yaml:"path"` // for filesystem storage (no more Type field)
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
	PrepareTableEnvironment(database, tableName string) error
	StoreTableData(database, tableName string, data []byte) error
	GetTableData(database, tableName string) ([]byte, error)
	RemoveTableEnvironment(database, tableName string) error
}

// NewManager creates a new data storage manager
func NewManager(cfg *config.Config, logger zerolog.Logger) (*Manager, error) {
	// Get the base data path (already validated in config layer)
	basePath := cfg.GetStoragePath()

	// Create path manager
	pathManager := NewPathManager(basePath)

	// Create storage config from server config
	storageCfg := &Config{
		Path: pathManager.GetDataPath(), // Use PathManager for data path
	}

	// Create storage engine registry (initializes all available engines)
	engineRegistry, err := NewStorageEngineRegistry(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage engine registry: %w", err)
	}

	// Initialize catalog
	catalog, err := catalog.NewCatalog(cfg, pathManager)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize catalog: %w", err)
	}

	// Create metadata manager with internal metadata path from PathManager
	meta, err := metadata.NewMetadataManager(catalog, pathManager.GetInternalMetadataDBPath(), basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata manager: %w", err)
	}

	// Log the storage configuration
	logger.Info().
		Str("base_path", basePath).
		Str("data_path", storageCfg.Path).
		Str("metadata_path", pathManager.GetInternalMetadataDBPath()).
		Str("catalog_uri", pathManager.GetCatalogURI(cfg.GetCatalogType())).
		Msg("Storage manager initialized with multi-engine support")

	return &Manager{
		config:         storageCfg,
		logger:         logger,
		engineRegistry: engineRegistry,
		meta:           meta,
		pathManager:    pathManager,
		catalog:        catalog,
	}, nil
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Msg("Initializing data storage with multi-engine support")

	// Ensure the standardized directory structure exists
	if err := m.pathManager.EnsureDirectoryStructure(); err != nil {
		return fmt.Errorf("failed to create directory structure: %w", err)
	}

	// Get default engine for initialization
	defaultEngine, err := m.engineRegistry.GetDefaultEngine()
	if err != nil {
		return fmt.Errorf("failed to get default storage engine: %w", err)
	}

	// Ensure base directory exists for filesystem operations
	if err := defaultEngine.MkdirAll(m.config.Path); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	// Ensure tables directory exists
	tablesPath := filepath.Join(m.config.Path, "")
	if err := defaultEngine.MkdirAll(tablesPath); err != nil {
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
	status := map[string]interface{}{
		"base_path":     m.pathManager.GetBasePath(),
		"catalog_path":  m.pathManager.GetCatalogPath(),
		"data_path":     m.pathManager.GetDataPath(),
		"metadata_path": m.pathManager.GetInternalMetadataDBPath(),
		"initialized":   true,
	}

	// Add engine registry status
	engineStatus := m.engineRegistry.GetEngineStatus()
	for k, v := range engineStatus {
		status[k] = v
	}

	return status
}

// GetPathManager returns the path manager for external use
func (m *Manager) GetPathManager() *PathManager {
	return m.pathManager
}

// GetCatalog returns the catalog for external use
func (m *Manager) GetCatalog() catalog.CatalogInterface {
	return m.catalog
}

// GetEngineRegistry returns the engine registry for external use
func (m *Manager) GetEngineRegistry() *StorageEngineRegistry {
	return m.engineRegistry
}

// GetFileSystem returns the default filesystem (for backward compatibility)
func (m *Manager) GetFileSystem() FileSystem {
	engine, err := m.engineRegistry.GetDefaultEngine()
	if err != nil {
		// Return nil if no default engine available
		return nil
	}
	return engine
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

// CreateTable creates a new table with schema and storage engine
func (m *Manager) CreateTable(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", storageEngine).
		Msg("Creating new table")

	// Validate storage engine
	if !m.engineRegistry.EngineExists(storageEngine) {
		return fmt.Errorf("unsupported storage engine: %s", storageEngine)
	}

	// First create table metadata in internal storage
	_, err := m.meta.CreateTableMetadata(ctx, database, tableName, schema, storageEngine, engineConfig)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to create table metadata").AddContext("database", database).AddContext("table", tableName).AddContext("storage_engine", storageEngine)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(storageEngine)
	if err != nil {
		return fmt.Errorf("failed to get storage engine %s: %w", storageEngine, err)
	}

	// Then prepare storage environment using the specified engine
	if err := engine.PrepareTableEnvironment(database, tableName); err != nil {
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
		Str("storage_engine", storageEngine).
		Msg("Table created successfully")

	return nil
}

// InsertData inserts data into a table
func (m *Manager) InsertData(ctx context.Context, database, tableName string, data [][]interface{}) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Inserting data into table")

	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("table", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("table", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	// Serialize data to bytes
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize data: %w", err)
	}

	// Store data in storage using the appropriate engine
	if err := engine.StoreTableData(database, tableName, dataBytes); err != nil {
		return fmt.Errorf("failed to store data in storage: %w", err)
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(data)).
		Msg("Data inserted successfully")

	return nil
}

// GetTableMetadata returns metadata for a table
func (m *Manager) GetTableMetadata(ctx context.Context, database, tableName string) (*types.TableMetadata, error) {
	return m.meta.LoadTableMetadata(ctx, database, tableName)
}

// ListTableFiles returns a list of files for a table
func (m *Manager) ListTableFiles(ctx context.Context, database, tableName string) ([]string, error) {
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
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
func (m *Manager) GetTableData(ctx context.Context, database, tableName string) ([][]interface{}, error) {
	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("table", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("table", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	// Get data from storage using the appropriate engine
	dataBytes, err := engine.GetTableData(database, tableName)
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

// GetTableSchema retrieves the schema for the specified table
func (m *Manager) GetTableSchema(ctx context.Context, database, tableName string) ([]byte, error) {
	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("table", tableName)
	}

	// Get table metadata
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("table", tableName)
	}

	return metadata.Schema, nil
}

// RemoveTable removes a table and all its data
func (m *Manager) RemoveTable(ctx context.Context, database, tableName string) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Removing table")

	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("table", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("table", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	// Remove storage environment using the appropriate engine
	if err := engine.RemoveTableEnvironment(database, tableName); err != nil {
		return fmt.Errorf("failed to remove table storage environment: %w", err)
	}

	// Remove metadata (if method exists)
	// TODO: Add RemoveTableMetadata method to MetadataManager

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Table removed successfully")
	return nil
}

// ListTables returns a list of all tables
func (m *Manager) ListTables(ctx context.Context) ([]string, error) {
	return m.meta.ListAllTables(ctx)
}

// TableExists checks if a table exists
func (m *Manager) TableExists(ctx context.Context, database, tableName string) bool {
	return m.meta.TableExists(ctx, database, tableName)
}

// createIcebergMetadata creates proper Iceberg metadata structure
func (m *Manager) createIcebergMetadata(database, tableName string, schema []byte) error {
	// Create table metadata directory
	metadataDir := m.pathManager.GetTableMetadataPath([]string{database}, tableName)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Create data directory
	dataDir := m.pathManager.GetTableDataPath([]string{database}, tableName)
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
		"location":        dataDir,
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
