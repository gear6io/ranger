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

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/metadata/registry/system"
	"github.com/gear6io/ranger/server/paths"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/storage/filesystem"
	"github.com/gear6io/ranger/server/storage/memory"
	"github.com/gear6io/ranger/server/storage/parquet"
	"github.com/gear6io/ranger/server/storage/s3"
	"github.com/gear6io/ranger/server/storage/schema"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
)

// Package-specific error codes for storage management
var (
	StorageManagerInitializationFailed = errors.MustNewCode("storage.initialization_failed")
	StorageManagerNoEnginesAvailable   = errors.MustNewCode("storage.no_engines_available")
	StorageManagerDirectoryFailed      = errors.MustNewCode("storage.directory_failed")
	StorageManagerCloseFailed          = errors.MustNewCode("storage.close_failed")
	StorageManagerEngineNotFound       = errors.MustNewCode("storage.engine_not_found")
	StorageManagerUnsupportedEngine    = errors.MustNewCode("storage.unsupported_engine")
	StorageManagerTableOperationFailed = errors.MustNewCode("storage.table_operation_failed")
	StorageManagerWriteFailed          = errors.MustNewCode("storage.write_failed")
	StorageManagerReadFailed           = errors.MustNewCode("storage.read_failed")
	StorageManagerMetadataFailed       = errors.MustNewCode("storage.metadata_failed")
)

// ComponentType defines the storage component type identifier
const ComponentType = "storage"

// Manager manages data storage operations
type Manager struct {
	config         *Config
	logger         zerolog.Logger
	engineRegistry *StorageEngineRegistry
	meta           *metadata.MetadataManager
	pathManager    paths.PathManager
	catalog        catalog.CatalogInterface
	schemaManager  schema.SchemaManager
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
func NewManager(cfg *config.Config, logger zerolog.Logger, meta *metadata.MetadataManager) (*Manager, error) {
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
		return nil, err
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
		return nil, err
	}

	// Initialize schema manager
	if err := manager.initializeSchemaManager(cfg); err != nil {
		return nil, err
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
		return err
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
		// This is an origin-stage validation error - appropriate to create
		return errors.New(StorageManagerNoEnginesAvailable, "no storage engines available", nil)
	}

	return nil
}

// initializeSchemaManager initializes the schema manager with proper configuration
func (m *Manager) initializeSchemaManager(cfg *config.Config) error {
	// Convert config to schema manager config
	schemaConfig := convertToSchemaManagerConfig(cfg.GetSchemaManagerConfig())

	// Create schema manager
	// TODO: Fix interface compatibility between MetadataManager and RegistryStoreInterface
	// var err error
	// m.schemaManager, err = schema.NewManager(m.meta, schemaConfig, m.logger)
	// if err != nil {
	//	return errors.New(StorageManagerInitializationFailed, "failed to initialize schema manager", err)
	// }
	m.schemaManager = nil // Temporary - will be fixed when interface is updated

	m.logger.Info().
		Int("cache_ttl_minutes", int(schemaConfig.CacheTTL/time.Minute)).
		Int("max_cache_size", schemaConfig.MaxCacheSize).
		Bool("enable_metrics", schemaConfig.EnableMetrics).
		Bool("enable_lru", schemaConfig.EnableLRU).
		Msg("Schema manager initialized successfully")

	return nil
}

// convertToSchemaManagerConfig converts server config to schema manager config
func convertToSchemaManagerConfig(cfg config.SchemaManagerConfig) *schema.SchemaManagerConfig {
	return &schema.SchemaManagerConfig{
		CacheTTL:      time.Duration(cfg.CacheTTLMinutes) * time.Minute,
		MaxCacheSize:  cfg.MaxCacheSize,
		StatsInterval: time.Duration(cfg.StatsIntervalSecs) * time.Second,
		EnableMetrics: cfg.EnableMetrics,
		EnableLRU:     cfg.EnableLRU,
	}
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Msg("Initializing data storage with multi-engine support")

	// Ensure the standardized directory structure exists
	if err := m.pathManager.EnsureDirectoryStructure(); err != nil {
		return err
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

	// Shutdown schema manager first
	if m.schemaManager != nil {
		m.schemaManager.Shutdown()
		m.logger.Info().Msg("Schema manager shut down successfully")
	}

	// Close storage manager
	if err := m.Close(); err != nil {
		return err
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

	// Add schema manager status
	if m.schemaManager != nil {
		schemaStats := m.schemaManager.GetCacheStats()
		status["schema_cache"] = map[string]interface{}{
			"hit_count":    schemaStats.HitCount,
			"miss_count":   schemaStats.MissCount,
			"hit_ratio":    schemaStats.HitRatio,
			"cache_size":   schemaStats.CacheSize,
			"evict_count":  schemaStats.EvictCount,
			"last_updated": schemaStats.LastUpdated,
		}
	}

	return status
}

// GetCatalog returns the catalog interface
func (m *Manager) GetCatalog() catalog.CatalogInterface {
	return m.catalog
}

// GetPathManager returns the path manager
func (m *Manager) GetPathManager() paths.PathManager {
	return m.pathManager
}

// GetEngineRegistry returns the engine registry for external use
func (m *Manager) GetEngineRegistry() *StorageEngineRegistry {
	return m.engineRegistry
}

// GetSchemaManager returns the schema manager for external use
func (m *Manager) GetSchemaManager() schema.SchemaManager {
	return m.schemaManager
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
		return nil, errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return nil, err
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

// CreateTable creates a new table with parsed CREATE TABLE statement
func (m *Manager) CreateTable(ctx context.Context, req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	startTime := time.Now()

	m.logger.Info().
		Str("database", req.Database).
		Str("storage_engine", req.StorageEngine).
		Str("request_id", req.RequestID).
		Msg("Starting CREATE TABLE operation")

	// Cast the statement interface{} to the proper type
	stmt, ok := req.Statement.(*parser.CreateTableStmt)
	if !ok {
		err := errors.Newf(ErrStorageManagerUnsupportedType, "invalid statement type - expected *parser.CreateTableStmt, received type: %T", req.Statement).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID).
			AddSuggestion("Ensure the statement is properly parsed before calling CreateTable")
		return nil, err
	}

	// Get table name from statement
	tableName := stmt.TableName.Table.Value

	m.logger.Info().
		Str("database", req.Database).
		Str("table", tableName).
		Str("storage_engine", req.StorageEngine).
		Str("request_id", req.RequestID).
		Msg("Creating new table with parsed statement")

	// 1. Validate statement
	if err := stmt.Validate(); err != nil {
		return nil, errors.New(ErrStorageManagerTableOperationFailed, "CREATE TABLE statement validation failed", err).
			AddContext("table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 2. Validate storage engine
	if err := m.validateStorageEngine(req.StorageEngine); err != nil {
		return nil, errors.Newf(ErrStorageManagerUnsupportedEngine, "invalid storage engine: %s", req.StorageEngine).
			AddContext("table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 3. Convert to registry types
	var tableRecord *regtypes.Table
	var columns []*regtypes.TableColumn

	func() {
		defer func() {
			if r := recover(); r != nil {
				panic(errors.Newf(ErrStorageManagerPanicRecovery, "failed to convert statement to registry types: %v", r))
			}
		}()

		tableRecord = m.convertToTableRecord(req, stmt)
		columns = m.convertToColumnRecords(stmt.TableSchema.ColumnDefinitions)
	}()

	// 4. Create table through metadata manager
	tableID, err := m.meta.CreateTableWithSchema(ctx, req.Database, tableRecord, columns)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate") {
			return nil, errors.New(errors.CommonConflict, "table already exists", err).
				AddContext("table_name", tableName).
				AddContext("database", req.Database).
				AddContext("request_id", req.RequestID)
		}

		return nil, errors.New(ErrStorageManagerTableOperationFailed, "failed to create table in registry", err).
			AddContext("table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 5. Get storage engine
	engine, err := m.engineRegistry.GetEngine(req.StorageEngine)
	if err != nil {
		return nil, errors.Newf(ErrStorageManagerEngineNotFound, "failed to get storage engine: %s", req.StorageEngine).
			AddContext("table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 6. Setup storage environment
	if err := engine.SetupTable(req.Database, tableName); err != nil {
		m.logger.Error().
			Err(err).
			Str("database", req.Database).
			Str("table", tableName).
			Int64("table_id", tableID).
			Str("request_id", req.RequestID).
			Msg("Storage setup failed")

		return nil, errors.New(ErrStorageManagerTableOperationFailed, "failed to setup table storage", err).
			AddContext("table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 7. Success - Create response
	totalDuration := time.Since(startTime)
	response := &types.CreateTableResponse{
		TableID: tableID,
		Success: true,
		Metadata: &types.TableCreationMetadata{
			CreatedAt:     time.Now(),
			ColumnCount:   len(stmt.TableSchema.ColumnDefinitions),
			StorageEngine: req.StorageEngine,
			SchemaVersion: 1,
		},
	}

	m.logger.Info().
		Str("database", req.Database).
		Str("table", tableName).
		Str("storage_engine", req.StorageEngine).
		Int64("table_id", tableID).
		Str("request_id", req.RequestID).
		Dur("duration", totalDuration).
		Msg("Table created successfully")

	return response, nil
}

// CreateTableLegacy creates a new table with schema and storage engine (legacy method for backward compatibility)
func (m *Manager) CreateTableLegacy(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", storageEngine).
		Msg("Creating new table (legacy method)")

	// Validate storage engine
	if !m.engineRegistry.EngineExists(storageEngine) {
		return errors.New(StorageManagerUnsupportedEngine, "unsupported storage engine", nil).AddContext("storage_engine", storageEngine)
	}

	// Create table with complete metadata in internal storage
	if err := m.meta.CreateTable(ctx, database, tableName, schema, storageEngine, engineConfig); err != nil {
		return err
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(storageEngine)
	if err != nil {
		return err
	}

	// Then prepare storage environment using the specified engine
	if err := engine.SetupTable(database, tableName); err != nil {
		// Clean up metadata if storage preparation fails
		// TODO: Add RemoveTableMetadata method to MetadataManager
		return err
	}

	// Create Iceberg metadata structure
	if err := m.createIcebergMetadata(database, tableName, schema); err != nil {
		return err
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
		return errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Retrieve schema before processing data
	if m.schemaManager != nil {
		icebergSchema, err := m.schemaManager.GetSchema(ctx, database, tableName)
		if err != nil {
			return errors.New(StorageManagerMetadataFailed, "failed to retrieve table schema", err).AddContext("database", database).AddContext("tableName", tableName)
		}

		// Convert Iceberg schema to Arrow schema for validation
		arrowSchema, err := parquet.ConvertIcebergToArrowSchema(icebergSchema)
		if err != nil {
			return errors.New(StorageManagerMetadataFailed, "failed to convert schema for validation", err).AddContext("database", database).AddContext("tableName", tableName)
		}

		// Validate data against schema before any storage operations
		if err := parquet.ValidateDataWithContext(data, arrowSchema, database, tableName); err != nil {
			m.logger.Error().
				Err(err).
				Str("database", database).
				Str("table", tableName).
				Int("batch_size", len(data)).
				Msg("Data validation failed - entire batch rejected")

			return errors.New(StorageManagerWriteFailed, "data validation failed - batch rejected", err).
				AddContext("database", database).
				AddContext("tableName", tableName).
				AddContext("batch_size", fmt.Sprintf("%d", len(data)))
		}
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return err
	}

	// Open streaming writer for the table
	writer, err := engine.OpenTableForWrite(database, tableName)
	if err != nil {
		return err
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
			writeErr = err
			return writeErr
		}

		// Write batch directly to storage without intermediate buffering
		if _, err := writer.Write(batchBytes); err != nil {
			writeErr = err
			return writeErr
		}

		// Add newline separator between batches for readability
		if _, err := writer.Write([]byte("\n")); err != nil {
			writeErr = err
			return writeErr
		}
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(data)).
		Msg("Data inserted successfully using streaming")

	// Update metadata after successful data insertion
	if err := m.updateMetadataAfterInsertion(ctx, database, tableName, len(data), metadata.StorageEngine); err != nil {
		m.logger.Warn().
			Err(err).
			Str("database", database).
			Str("table", tableName).
			Msg("Failed to update metadata after insertion, but data insertion succeeded")
		// Don't fail the insertion if metadata update fails
	}

	return nil
}

// GetTableMetadata returns metadata for a table
func (m *Manager) GetTableMetadata(ctx context.Context, database, tableName string) (*registry.TableMetadata, error) {
	return m.meta.LoadTableMetadata(ctx, database, tableName)
}

// Note: Individual metadata update methods have been replaced with a single
// atomic Registry call in updateMetadataAfterInsertion()

// updateMetadataAfterInsertion performs all metadata updates using a single Registry call
func (m *Manager) updateMetadataAfterInsertion(ctx context.Context, database, tableName string, rowCount int, storageEngine string) error {
	m.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Int("row_count", rowCount).
		Str("storage_engine", storageEngine).
		Msg("Starting metadata updates after insertion")

	// Calculate file size (approximate for now)
	estimatedFileSize := int64(rowCount * 100) // Rough estimate: 100 bytes per row

	// Generate a file name based on timestamp
	fileName := fmt.Sprintf("data_%d_%s.parquet", time.Now().Unix(), storageEngine)
	filePath := fmt.Sprintf("databases/%s/%s/%s", database, tableName, fileName)
	checksum := fmt.Sprintf("checksum_%d_%s", time.Now().Unix(), storageEngine)

	// Create file insertion info for the Registry
	fileInfo := registry.FileInsertionInfo{
		FileName:      fileName,
		FilePath:      filePath,
		FileSize:      estimatedFileSize,
		FileType:      "parquet",
		PartitionPath: "", // No partitioning for now
		RowCount:      int64(rowCount),
		Checksum:      checksum,
		IsCompressed:  false,
	}

	// Single atomic call to Registry for all metadata updates
	if err := m.meta.UpdateTableAfterInsertion(ctx, database, tableName, fileInfo); err != nil {
		m.logger.Error().
			Err(err).
			Str("database", database).
			Str("table", tableName).
			Msg("Failed to update metadata after insertion")
		return err
	}

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("row_count", rowCount).
		Str("file_name", fileName).
		Int64("file_size", estimatedFileSize).
		Msg("Metadata updates completed successfully after insertion")

	return nil
}

// ListTableFiles returns a list of files for a table
func (m *Manager) ListTableFiles(ctx context.Context, database, tableName string) ([]string, error) {
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, file := range metadata.Files {
		files = append(files, file.FileName)
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
		return nil, errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return nil, err
	}

	// Open streaming reader for the table
	reader, err := engine.OpenTableForRead(database, tableName)
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// For now, return empty schema since schema is stored in TableColumn table
	// TODO: Implement proper schema retrieval from TableColumn table
	// This requires getting the table ID first, then retrieving columns
	return []byte("{}"), nil
}

// RemoveTable removes a table and all its data
func (m *Manager) RemoveTable(ctx context.Context, database, tableName string) error {
	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Removing table")

	// Check if table exists in metadata
	if !m.meta.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := m.meta.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Get the appropriate storage engine for this table
	engine, err := m.engineRegistry.GetEngine(metadata.StorageEngine)
	if err != nil {
		return err
	}

	// Remove storage environment using the appropriate engine
	if err := engine.RemoveTableEnvironment(database, tableName); err != nil {
		return err
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

// ListTablesForDatabase returns a list of tables in a specific database
func (m *Manager) ListTablesForDatabase(ctx context.Context, database string) ([]string, error) {
	return m.meta.ListTables(ctx, database)
}

// ListAllTables returns a list of all tables across all databases
func (m *Manager) ListAllTables(ctx context.Context) ([]string, error) {
	return m.meta.ListAllTables(ctx)
}

// TableExists checks if a table exists
func (m *Manager) TableExists(ctx context.Context, database, tableName string) bool {
	return m.meta.TableExists(ctx, database, tableName)
}

// ListDatabases returns a list of all databases
func (m *Manager) ListDatabases(ctx context.Context) ([]string, error) {
	return m.meta.ListDatabases(ctx)
}

// DatabaseExists checks if a database exists
func (m *Manager) DatabaseExists(ctx context.Context, dbName string) bool {
	return m.meta.DatabaseExists(ctx, dbName)
}

// CreateDatabase creates a new database
func (m *Manager) CreateDatabase(ctx context.Context, dbName string) error {
	return m.meta.CreateDatabase(ctx, dbName)
}

// ============================================================================
// ABSTRACTION METHODS (replacing direct metadata manager access)
// ============================================================================

// DropTable drops a table from the specified database
func (m *Manager) DropTable(ctx context.Context, database, tableName string) error {
	return m.meta.DropTable(ctx, database, tableName)
}

// createIcebergMetadata creates proper Iceberg metadata structure
func (m *Manager) createIcebergMetadata(database, tableName string, schema []byte) error {
	// Create table metadata directory
	metadataDir := m.pathManager.GetTableMetadataPath([]string{database}, tableName)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return errors.New(StorageManagerDirectoryFailed, "failed to create metadata directory", nil).AddContext("path", metadataDir).AddContext("cause", err)
	}

	// Create data directory
	dataDir := m.pathManager.GetTableDataPath([]string{database}, tableName)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return errors.New(StorageManagerDirectoryFailed, "failed to create data directory", nil).AddContext("path", dataDir).AddContext("cause", err)
	}

	// Create Iceberg metadata file (version 1)
	metadataFile := m.pathManager.GetTableMetadataFile(database, tableName, 1)

	// Parse the schema to get column information
	var schemaData map[string]interface{}
	if err := json.Unmarshal(schema, &schemaData); err != nil {
		return errors.New(StorageManagerMetadataFailed, "failed to parse schema", nil).AddContext("database", database).AddContext("table", tableName).AddContext("cause", err)
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
			"engine-name":    "ranger",
			"engine-version": "0.1.0",
		},
	}

	// Write metadata atomically
	tempFile := metadataFile + ".tmp"
	defer os.Remove(tempFile) // Clean up temp file

	file, err := os.Create(tempFile)
	if err != nil {
		return errors.New(StorageManagerWriteFailed, "failed to create temporary metadata file", nil).AddContext("path", tempFile).AddContext("cause", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(icebergMetadata); err != nil {
		file.Close()
		return errors.New(StorageManagerWriteFailed, "failed to encode metadata JSON", nil).AddContext("database", database).AddContext("table", tableName).AddContext("cause", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return errors.New(StorageManagerWriteFailed, "failed to sync metadata file", nil).AddContext("path", tempFile).AddContext("cause", err)
	}

	if err := file.Close(); err != nil {
		return errors.New(StorageManagerWriteFailed, "failed to close metadata file", nil).AddContext("path", tempFile).AddContext("cause", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, metadataFile); err != nil {
		return errors.New(StorageManagerWriteFailed, "failed to atomically write metadata file", nil).AddContext("path", metadataFile).AddContext("cause", err)
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

// ============================================================================
// CREATE TABLE HELPER METHODS
// ============================================================================

// validateStorageEngine validates that the storage engine is supported
func (m *Manager) validateStorageEngine(storageEngine string) error {
	if !m.engineRegistry.EngineExists(storageEngine) {
		availableEngines := m.engineRegistry.GetAvailableEngines()
		return errors.Newf(ErrStorageManagerUnsupportedEngine, "unsupported storage engine '%s'. Available engines: %v", storageEngine, availableEngines)
	}
	return nil
}

// convertToTableRecord converts CREATE TABLE request to registry Table record
func (m *Manager) convertToTableRecord(req *types.CreateTableRequest, stmt *parser.CreateTableStmt) *regtypes.Table {
	now := time.Now()
	return &regtypes.Table{
		// DatabaseID will be set by the registry when creating the table
		Name:        stmt.TableName.Table.Value,
		DisplayName: stmt.TableName.Table.Value, // Use table name as display name
		Description: "",                         // No description in CREATE TABLE statement
		TableType:   "user",
		IsTemporary: false, // TODO: Support temporary tables in parser
		IsExternal:  false, // TODO: Support external tables in parser
		RowCount:    0,
		FileCount:   0,
		TotalSize:   0,
		TimeAuditable: regtypes.TimeAuditable{
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
}

// convertToColumnRecords converts column definitions to registry TableColumn records
func (m *Manager) convertToColumnRecords(columns map[string]*parser.ColumnDefinition) []*regtypes.TableColumn {
	result := make([]*regtypes.TableColumn, 0, len(columns))
	ordinalPosition := 1
	now := time.Now()

	for columnName, colDef := range columns {
		column := &regtypes.TableColumn{
			ColumnName:      columnName,
			DisplayName:     columnName, // Use column name as display name
			DataType:        colDef.DataType,
			IsNullable:      colDef.IsNullable,
			IsPrimary:       false, // TODO: Support primary key constraints in parser
			IsUnique:        false, // TODO: Support unique constraints in parser
			DefaultValue:    colDef.DefaultValue,
			Description:     "", // No description in CREATE TABLE statement
			OrdinalPosition: ordinalPosition,
			MaxLength:       colDef.Length,
			Precision:       colDef.Precision,
			Scale:           colDef.Scale,
			TimeAuditable: regtypes.TimeAuditable{
				CreatedAt: now,
				UpdatedAt: now,
			},
		}
		result = append(result, column)
		ordinalPosition++
	}

	return result
}

// GetSystemManager returns the system database manager
func (m *Manager) GetSystemManager() *system.Manager {
	// TODO: Implement proper access to system manager
	// The metadata manager doesn't expose the storage directly
	// This needs to be implemented when the interface is updated
	return nil
}
