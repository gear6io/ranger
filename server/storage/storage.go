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
type Storage struct {
	*StorageEngineRegistry
	*metadata.MetadataManager
	*schema.Schema

	config      *Config
	logger      zerolog.Logger
	pathManager paths.PathManager
	catalog     catalog.CatalogInterface
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
func NewStorage(ctx context.Context, cfg *config.Config, logger zerolog.Logger, meta *metadata.MetadataManager) (*Storage, error) {
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

	storage := &Storage{
		config:                storageCfg,
		logger:                logger,
		StorageEngineRegistry: engineRegistry,
		MetadataManager:       meta,
		pathManager:           pathManager,
		catalog:               catalog,
	}

	// Initialize storage engines with the PathManager
	if err := storage.initializeStorageEngines(cfg); err != nil {
		return nil, err
	}

	// Initialize schema manager
	if err := storage.initializeSchemaManager(ctx, cfg); err != nil {
		return nil, err
	}

	return storage, nil
}

// initializeStorageEngines initializes all available storage engines with proper dependencies
func (s *Storage) initializeStorageEngines(cfg *config.Config) error {
	// Initialize filesystem engine with PathManager
	fsEngine := filesystem.NewFileStorage(s.pathManager)
	s.RegisterEngine(filesystem.Type, fsEngine)

	// Initialize memory engine (no dependencies needed)
	memEngine, err := memory.NewMemoryStorage()
	if err != nil {
		return err
	}
	s.RegisterEngine(memory.Type, memEngine)

	// Initialize S3 engine (if credentials are available)
	if s3Engine, err := s3.NewS3FileSystem(cfg); err == nil {
		s.RegisterEngine(s3.Type, s3Engine)
		s.logger.Info().Msg("S3 storage engine initialized successfully")
	} else {
		s.logger.Warn().Err(err).Msg("S3 storage engine not available (credentials missing or invalid)")
	}

	// Set default engine based on available engines
	if _, exists := s.engines[filesystem.Type]; exists {
		s.defaultEngine = filesystem.Type
	} else if _, exists := s.engines[memory.Type]; exists {
		s.defaultEngine = memory.Type
	} else {
		// This is an origin-stage validation error - appropriate to create
		return errors.New(StorageManagerNoEnginesAvailable, "no storage engines available", nil)
	}

	return nil
}

// initializeSchemaManager initializes the schema manager with proper configuration
func (s *Storage) initializeSchemaManager(ctx context.Context, cfg *config.Config) error {
	// Convert config to schema manager config
	schemaConfig := cfg.GetSchemaManagerConfig()
	initialSchema, err := s.MetadataManager.RetrieveAllSchemas(ctx)
	if err != nil {
		return errors.New(StorageManagerInitializationFailed, "failed to retrieve all schemas", err)
	}

	loader := s.MetadataManager.CreateSchemaDataLoader()

	// Create schema manager
	s.Schema, err = schema.NewSchema(ctx, initialSchema, &schemaConfig, s.logger, loader)
	if err != nil {
		return errors.New(StorageManagerInitializationFailed, "failed to initialize schema manager", err)
	}
	// s.schemaManager = nil // Temporary - will be fixed when interface is updated

	s.logger.Debug().
		Int("cache_ttl_minutes", int(schemaConfig.CacheTTLMinutes)).
		Int("max_cache_size", schemaConfig.MaxCacheSize).
		Bool("enable_metrics", schemaConfig.EnableMetrics).
		Bool("enable_lru", schemaConfig.EnableLRU).
		Msg("Schema initialized successfully")

	return nil
}

// Initialize initializes the data storage
func (s *Storage) Initialize(ctx context.Context) error {
	s.logger.Info().Msg("Initializing data storage with multi-engine support")

	// Ensure the standardized directory structure exists
	if err := s.pathManager.EnsureDirectoryStructure(); err != nil {
		return err
	}

	// Storage engines handle their own directory creation during SetupTable
	// No need to create generic directories here

	return nil
}

// GetType returns the component type identifier
func (s *Storage) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the storage manager
func (s *Storage) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down storage manager")

	// Shutdown schema manager first
	if s.Schema != nil {
		s.Schema.Shutdown()
		s.logger.Info().Msg("Schema manager shut down successfully")
	}

	// Close storage manager
	if err := s.Close(); err != nil {
		return err
	}

	s.logger.Info().Msg("Storage manager shut down successfully")
	return nil
}

// Close closes the data storage
func (s *Storage) Close() error {
	s.logger.Info().Msg("Closing data storage")
	// TODO: Implement actual storage cleanup
	return nil
}

// GetStatus returns the current status of the storage manager
func (s *Storage) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"base_path":     s.pathManager.GetBasePath(),
		"catalog_path":  s.pathManager.GetCatalogPath(),
		"data_path":     s.pathManager.GetDataPath(),
		"metadata_path": s.pathManager.GetInternalMetadataDBPath(),
		"initialized":   true,
	}

	// Add engine registry status
	engineStatus := s.GetEngineStatus()
	for k, v := range engineStatus {
		status[k] = v
	}

	// Add schema manager status
	schemaStats := s.GetCacheStats()
	status["schema_cache"] = map[string]interface{}{
		"hit_count":    schemaStats.HitCount,
		"miss_count":   schemaStats.MissCount,
		"hit_ratio":    schemaStats.HitRatio,
		"cache_size":   schemaStats.CacheSize,
		"evict_count":  schemaStats.EvictCount,
		"last_updated": schemaStats.LastUpdated,
	}

	return status
}

// GetCatalog returns the catalog interface
func (s *Storage) GetCatalog() catalog.CatalogInterface {
	return s.catalog
}

// GetPathManager returns the path manager
func (s *Storage) GetPathManager() paths.PathManager {
	return s.pathManager
}

// GetEngineForTable returns the appropriate storage engine for a specific table
func (s *Storage) getEngineForTable(ctx context.Context, database, tableName string) (FileSystem, error) {
	// Check if table exists in metadata
	if !s.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := s.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// Get the appropriate storage engine for this table
	engine, err := s.GetEngine(metadata.StorageEngine)
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
func (s *Storage) CreateTable(ctx context.Context, req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	startTime := time.Now()

	s.logger.Info().
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

	s.logger.Info().
		Str("database", req.Database).
		Str("table", tableName).
		Str("storage_engine", req.StorageEngine).
		Str("request_id", req.RequestID).
		Msg("Creating new table with parsed statement")

	// 1. Validate statement
	if err := stmt.Validate(); err != nil {
		return nil, errors.AddContext(err, "table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 2. Validate storage engine
	if err := s.validateStorageEngine(req.StorageEngine); err != nil {
		return nil, errors.AddContext(err, "table_name", tableName).
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

		tableRecord = s.convertToTableRecord(req, stmt)
		columns = s.convertToColumnRecords(stmt.TableSchema.ColumnDefinitions)
	}()

	// 4. Create table through metadata manager
	tableID, err := s.CreateTableWithSchema(ctx, req.Database, tableRecord, columns)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate") {
			return nil, errors.AddContext(err, "table_name", tableName).
				AddContext("database", req.Database).
				AddContext("request_id", req.RequestID)
		}

		return nil, errors.AddContext(err, "table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 5. Get storage engine
	engine, err := s.GetEngine(req.StorageEngine)
	if err != nil {
		return nil, errors.AddContext(err, "table_name", tableName).
			AddContext("database", req.Database).
			AddContext("request_id", req.RequestID)
	}

	// 6. Setup storage environment
	if err := engine.SetupTable(req.Database, tableName); err != nil {
		return nil, err
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

	s.logger.Info().
		Str("database", req.Database).
		Str("table", tableName).
		Str("storage_engine", req.StorageEngine).
		Int64("table_id", tableID).
		Str("request_id", req.RequestID).
		Dur("duration", totalDuration).
		Msg("Table created successfully")

	return response, nil
}

// InsertData inserts data into a table using streaming for memory efficiency
func (s *Storage) InsertData(ctx context.Context, database, tableName string, data [][]interface{}) error {
	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("rows", len(data)).
		Msg("Inserting data into table using streaming")

	// Check if table exists in metadata
	if !s.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Retrieve schema before processing data
	icebergSchema, err := s.GetSchema(ctx, database, tableName)
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
		s.logger.Error().
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

	// Get table metadata to determine storage engine
	metadata, err := s.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Get the appropriate storage engine for this table
	engine, err := s.GetEngine(metadata.StorageEngine)
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
			if rollbackErr := engine.RemoveTableEnvironment(database, tableName); rollbackErr != nil {
				s.logger.Error().
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

	s.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(data)).
		Msg("Data inserted successfully using streaming")

	// Update metadata after successful data insertion
	if err := s.updateMetadataAfterInsertion(ctx, database, tableName, len(data), metadata.StorageEngine); err != nil {
		// Don't fail the insertion if metadata update fails
	}

	return nil
}

// Note: Individual metadata update methods have been replaced with a single
// atomic Registry call in updateMetadataAfterInsertion()

// updateMetadataAfterInsertion performs all metadata updates using a single Registry call
func (s *Storage) updateMetadataAfterInsertion(ctx context.Context, database, tableName string, rowCount int, storageEngine string) error {
	s.logger.Debug().
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
	if err := s.UpdateTableAfterInsertion(ctx, database, tableName, fileInfo); err != nil {
		s.logger.Error().
			Err(err).
			Str("database", database).
			Str("table", tableName).
			Msg("Failed to update metadata after insertion")
		return err
	}

	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("row_count", rowCount).
		Str("file_name", fileName).
		Int64("file_size", estimatedFileSize).
		Msg("Metadata updates completed successfully after insertion")

	return nil
}

// GetTableData retrieves data from a table using streaming for memory efficiency
func (s *Storage) GetTableData(ctx context.Context, database, tableName string) ([][]interface{}, error) {
	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Retrieving table data using streaming")

	// Check if table exists in metadata
	if !s.TableExists(ctx, database, tableName) {
		return nil, errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := s.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// Get the appropriate storage engine for this table
	engine, err := s.GetEngine(metadata.StorageEngine)
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
			s.logger.Warn().
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

	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("storage_engine", metadata.StorageEngine).
		Int("rows", len(allData)).
		Msg("Data retrieved successfully using streaming")

	return allData, nil
}

// RemoveTable removes a table and all its data
func (s *Storage) RemoveTable(ctx context.Context, database, tableName string) error {
	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Removing table")

	// Check if table exists in metadata
	if !s.TableExists(ctx, database, tableName) {
		return errors.New(errors.CommonNotFound, "table does not exist", nil).AddContext("database", database).AddContext("tableName", tableName)
	}

	// Get table metadata to determine storage engine
	metadata, err := s.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Get the appropriate storage engine for this table
	engine, err := s.GetEngine(metadata.StorageEngine)
	if err != nil {
		return err
	}

	// Remove storage environment using the appropriate engine
	if err := engine.RemoveTableEnvironment(database, tableName); err != nil {
		return err
	}

	// Remove metadata (if method exists)
	// TODO: Add RemoveTableMetadata method to MetadataManager

	s.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Table removed successfully")
	return nil
}

// createIcebergMetadata creates proper Iceberg metadata structure
func (s *Storage) createIcebergMetadata(database, tableName string, schema []byte) error {
	// Create table metadata directory
	metadataDir := s.pathManager.GetTableMetadataPath([]string{database}, tableName)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return errors.New(StorageManagerDirectoryFailed, "failed to create metadata directory", nil).AddContext("path", metadataDir).AddContext("cause", err)
	}

	// Create data directory
	dataDir := s.pathManager.GetTableDataPath([]string{database}, tableName)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return errors.New(StorageManagerDirectoryFailed, "failed to create data directory", nil).AddContext("path", dataDir).AddContext("cause", err)
	}

	// Create Iceberg metadata file (version 1)
	metadataFile := s.pathManager.GetTableMetadataFile(database, tableName, 1)

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

	s.logger.Info().
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
func (s *Storage) validateStorageEngine(storageEngine string) error {
	if !s.EngineExists(storageEngine) {
		availableEngines := s.GetAvailableEngines()
		return errors.Newf(ErrStorageManagerUnsupportedEngine, "unsupported storage engine '%s'. Available engines: %v", storageEngine, availableEngines)
	}
	return nil
}

// convertToTableRecord converts CREATE TABLE request to registry Table record
func (s *Storage) convertToTableRecord(req *types.CreateTableRequest, stmt *parser.CreateTableStmt) *regtypes.Table {
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
func (s *Storage) convertToColumnRecords(columns map[string]*parser.ColumnDefinition) []*regtypes.TableColumn {
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
