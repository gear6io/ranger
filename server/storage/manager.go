package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"bufio"
	"strings"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/metadata"
	"github.com/TFMV/icebox/server/metadata/types"
	"github.com/TFMV/icebox/server/paths"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/TFMV/icebox/server/storage/s3"
	"github.com/rs/zerolog"
)

// ComponentType defines the storage component type identifier
const ComponentType = "storage"

// Manager manages data storage operations
type Manager struct {
	config         *Config
	logger         zerolog.Logger
	engineRegistry *StorageEngineRegistry
	meta           metadata.MetadataManagerInterface
	pathManager    paths.PathManager
	catalog        catalog.CatalogInterface
}

// Config holds data storage configuration
type Config struct {
	Path string `yaml:"path"` // for filesystem storage (no more Type field)
}

// FileSystem interface for data storage operations
type FileSystem interface {
	// Core streaming operations for large files
	OpenForRead(path string) (io.ReadCloser, error)
	OpenForWrite(path string) (io.WriteCloser, error)

	// Streaming table operations
	OpenTableForWrite(database, tableName string) (io.WriteCloser, error)
	OpenTableForRead(database, tableName string) (io.ReadCloser, error)

	// Storage environment preparation
	SetupTable(database, tableName string) error
	RemoveTableEnvironment(database, tableName string) error
}

// NewManager creates a new data storage manager
func NewManager(cfg *config.Config, logger zerolog.Logger) (*Manager, error) {
	// Get the base data path (already validated in config layer)
	basePath := cfg.GetStoragePath()

	// Create path manager
	pathManager := paths.NewManager(basePath)

	// Create storage config from server config
	storageCfg := &Config{
		Path: pathManager.GetDataPath(), // Use PathManager for data path
	}

	// Create storage engine registry (without initializing engines)
	engineRegistry := NewStorageEngineRegistry(logger)

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

	manager := &Manager{
		config:         storageCfg,
		logger:         logger,
		engineRegistry: engineRegistry,
		meta:           meta,
		pathManager:    pathManager,
		catalog:        catalog,
	}

	// Initialize storage engines with the PathManager
	if err := manager.initializeStorageEngines(cfg); err != nil {
		return nil, fmt.Errorf("failed to initialize storage engines: %w", err)
	}

	return manager, nil
}

// initializeStorageEngines initializes all available storage engines with proper dependencies
func (m *Manager) initializeStorageEngines(cfg *config.Config) error {
	// Initialize filesystem engine with PathManager
	fsEngine := filesystem.NewFileStorage(m.pathManager)
	m.engineRegistry.RegisterEngine(filesystem.Type, fsEngine)

	// Initialize memory engine (no dependencies needed)
	memEngine, err := memory.NewMemoryStorage()
	if err != nil {
		return fmt.Errorf("failed to initialize memory engine: %w", err)
	}
	m.engineRegistry.RegisterEngine(memory.Type, memEngine)

	// Initialize S3 engine (if credentials are available)
	if s3Engine, err := s3.NewS3FileSystem(cfg); err == nil {
		m.engineRegistry.RegisterEngine(s3.Type, s3Engine)
		m.logger.Info().Msg("S3 storage engine initialized successfully")
	} else {
		m.logger.Warn().Err(err).Msg("S3 storage engine not available (credentials missing or invalid)")
	}

	// Set default engine based on available engines
	if _, exists := m.engineRegistry.engines[filesystem.Type]; exists {
		m.engineRegistry.defaultEngine = filesystem.Type
	} else if _, exists := m.engineRegistry.engines[memory.Type]; exists {
		m.engineRegistry.defaultEngine = memory.Type
	} else {
		return fmt.Errorf("no storage engines available")
	}

	return nil
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Msg("Initializing data storage with multi-engine support")

	// Ensure the standardized directory structure exists
	if err := m.pathManager.EnsureDirectoryStructure(); err != nil {
		return fmt.Errorf("failed to create directory structure: %w", err)
	}

	// Storage engines handle their own directory creation during SetupTable
	// No need to create generic directories here

	return nil
}

// GetType returns the component type identifier
func (m *Manager) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the storage manager
func (m *Manager) Shutdown(ctx context.Context) error {
	m.logger.Info().Msg("Shutting down storage manager")

	// Close storage manager
	if err := m.Close(); err != nil {
		return fmt.Errorf("failed to close storage manager: %w", err)
	}

	m.logger.Info().Msg("Storage manager shut down successfully")
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
func (m *Manager) GetPathManager() paths.PathManager {
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

// GetEngineForTable returns the appropriate storage engine for a specific table
func (m *Manager) GetEngineForTable(ctx context.Context, database, tableName string) (FileSystem, error) {
	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	return engine, nil
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

	// Create table with complete metadata in internal storage
	if err := m.meta.CreateTable(ctx, database, tableName, schema, storageEngine, engineConfig); err != nil {
		return errors.New(errors.CommonInternal, "failed to create table").AddContext("database", database).AddContext("table", tableName).AddContext("storage_engine", storageEngine)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(storageEngine)
	if err != nil {
		return fmt.Errorf("failed to get storage engine %s: %w", storageEngine, err)
	}

	// Then prepare storage environment using the specified engine
	if err := engine.SetupTable(database, tableName); err != nil {
		// Clean up metadata if storage preparation fails
		// TODO: Add RemoveTableMetadata method to MetadataManager
		return fmt.Errorf("failed to setup table storage environment: %w", err)
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

// InsertData inserts data into a table using streaming for memory efficiency
func (m *Manager) InsertData(ctx context.Context, database, tableName string, data [][]interface{}) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Inserting data into table using streaming")

	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	// Open streaming writer for the table
	writer, err := engine.OpenTableForWrite(database, tableName)
	if err != nil {
		return fmt.Errorf("failed to open table for writing: %w", err)
	}

	// Ensure writer is closed and handle rollback on failure
	var writeErr error
	defer func() {
		if writeErr != nil {
			// Rollback: remove any partially written data
			m.logger.Warn().
				Str("database", database).
				Str("table", tableName).
				Err(writeErr).
				Msg("Rolling back failed data insertion")

			if rollbackErr := engine.RemoveTableEnvironment(database, tableName); rollbackErr != nil {
				m.logger.Error().
					Err(rollbackErr).
					Str("database", database).
					Str("table", tableName).
					Msg("Failed to rollback table environment")
			}
		}
		writer.Close()
	}()

	// Stream data in batches to avoid memory buildup
	batchSize := 1000 // Configurable batch size
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]

		// Convert batch to JSON and write directly to storage
		batchBytes, err := json.Marshal(batch)
		if err != nil {
			writeErr = fmt.Errorf("failed to serialize batch %d-%d: %w", i, end-1, err)
			return writeErr
		}

		// Write batch directly to storage without intermediate buffering
		if _, err := writer.Write(batchBytes); err != nil {
			writeErr = fmt.Errorf("failed to write batch %d-%d: %w", i, end-1, err)
			return writeErr
		}

		// Add newline separator between batches for readability
		if _, err := writer.Write([]byte("\n")); err != nil {
			writeErr = fmt.Errorf("failed to write batch separator: %w", err)
			return writeErr
		}
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(data)).
		Msg("Data inserted successfully using streaming")

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

// GetTableData retrieves data from a table using streaming for memory efficiency
func (m *Manager) GetTableData(ctx context.Context, database, tableName string) ([][]interface{}, error) {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Retrieving table data using streaming")

	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to load table metadata").AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage engine %s: %w", metadata.StorageEngine, err)
	}

	// Open streaming reader for the table
	reader, err := engine.OpenTableForRead(database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to open table for reading: %w", err)
	}
	defer reader.Close()

	// Read data in streaming fashion
	var allData [][]interface{}
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Parse each line as a JSON batch
		var batch [][]interface{}
		if err := json.Unmarshal([]byte(line), &batch); err != nil {
			m.logger.Warn().
				Err(err).
				Str("line", line).
				Msg("Failed to parse data line, skipping")
			continue
		}

		allData = append(allData, batch...)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading table data: %w", err)
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(allData)).
		Msg("Data retrieved successfully using streaming")

	return allData, nil
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

// GetMetadataManager returns the metadata manager instance
func (m *Manager) GetMetadataManager() metadata.MetadataManagerInterface {
	return m.meta
}
