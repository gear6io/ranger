package metadata

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/astha"
	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/metadata/iceberg"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/paths"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
	"github.com/uptrace/bun"
)

// Package-specific error codes for metadata management
var (
	MetadataManagerAlreadyRunning = errors.MustNewCode("metadata.already_running")
	MetadataManagerNotRunning     = errors.MustNewCode("metadata.not_running")
	MetadataManagerStartupFailed  = errors.MustNewCode("metadata.startup_failed")
	MetadataManagerShutdownFailed = errors.MustNewCode("metadata.shutdown_failed")
	MetadataManagerNotAvailable   = errors.MustNewCode("metadata.not_available")
	MetadataOperationFailed       = errors.MustNewCode("metadata.operation_failed")
	// Additional error codes for enhanced schema validation
	RegistryTableExists       = errors.MustNewCode("metadata.table_exists")
	RegistryDatabaseNotFound  = errors.MustNewCode("metadata.database_not_found")
	RegistryTransactionFailed = errors.MustNewCode("metadata.transaction_failed")
)

// ComponentType defines the metadata manager component type identifier
const ComponentType = "metadata"

// No-op diagnostic logger stub (TODO: remove all diagnostic logging)
type noOpDiagLogger struct{}

func (n *noOpDiagLogger) LogOperationStart(operation string, context map[string]interface{}) {}
func (n *noOpDiagLogger) LogOperationEnd(operation string, success bool, err error, result map[string]interface{}) {
}
func (n *noOpDiagLogger) LogValidationError(operation string, details interface{})       {}
func (n *noOpDiagLogger) LogRegistryOperation(operation string, diagnostics interface{}) {}
func (n *noOpDiagLogger) LogPerformanceMetrics(operation string, duration time.Duration, metrics map[string]interface{}) {
}
func (n *noOpDiagLogger) LogSummary() {}

// MetadataManager coordinates between Iceberg catalog and personal metadata storage
type MetadataManager struct {
	iceberg        catalog.CatalogInterface
	storage        *registry.Store
	hybrid         *registry.HybridDeploymentManager
	icebergManager *iceberg.Manager
	astha          *astha.Astha
	pathManager    paths.PathManager
	logger         zerolog.Logger
	mu             sync.RWMutex
	running        bool
}

// NewMetadataManager creates a new metadata manager with bun migrations
func NewMetadataManager(catalog catalog.CatalogInterface, dbPath, basePath string, logger zerolog.Logger) (*MetadataManager, error) {
	// Create storage with bun migrations
	storage, err := registry.NewStore(dbPath, basePath)
	if err != nil {
		return nil, err
	}

	// Create path manager
	pathManager := paths.NewManager(basePath)

	// Create Iceberg manager
	icebergManager := iceberg.NewManager(pathManager, logger)

	manager := &MetadataManager{
		iceberg:        catalog,
		storage:        storage,
		icebergManager: icebergManager,
		pathManager:    pathManager,
		logger:         logger,
	}

	// Create hybrid deployment manager
	bunMigrator := storage.GetBunMigrationManager()
	if bunMigrator == nil {
		return nil, errors.New(MetadataManagerNotAvailable, "bun migrator not available", nil)
	}
	manager.hybrid = registry.NewHybridDeploymentManager(storage, bunMigrator)

	return manager, nil
}

// Start starts the MetadataManager and its components
func (mm *MetadataManager) Start(ctx context.Context) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.running {
		return errors.New(MetadataManagerAlreadyRunning, "metadata manager is already running", nil)
	}

	// Start Iceberg manager
	if err := mm.icebergManager.Start(); err != nil {
		return err
	}

	// Initialize Astha CDC scheduler
	if err := mm.initializeAstha(ctx); err != nil {
		return err
	}

	// Start Astha
	if err := mm.astha.Start(); err != nil {
		return err
	}

	// Load pending files for Iceberg metadata generation
	if err := mm.loadPendingFiles(ctx); err != nil {
		mm.logger.Warn().Err(err).Msg("failed to load pending files during startup")
	}

	mm.running = true
	mm.logger.Info().Msg("Metadata manager started successfully")
	return nil
}

// Stop stops the MetadataManager and its components
func (mm *MetadataManager) Stop(ctx context.Context) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if !mm.running {
		return errors.New(MetadataManagerNotRunning, "metadata manager is not running", nil)
	}

	// Stop Astha
	if mm.astha != nil {
		if err := mm.astha.Stop(); err != nil {
			mm.logger.Warn().Err(err).Msg("failed to stop astha gracefully")
		}
	}

	// Stop Iceberg manager
	if mm.icebergManager != nil {
		if err := mm.icebergManager.Stop(); err != nil {
			mm.logger.Warn().Err(err).Msg("failed to stop iceberg manager gracefully")
		}
	}

	mm.running = false
	mm.logger.Info().Msg("Metadata manager stopped successfully")
	return nil
}

// initializeAstha initializes the Astha CDC scheduler
func (mm *MetadataManager) initializeAstha(ctx context.Context) error {
	// Get database connection from bun migration manager
	bunDB := mm.hybrid.GetBunDB()
	if bunDB == nil {
		return errors.New(MetadataManagerNotAvailable, "database connection not available", nil)
	}

	// Get the underlying sql.DB from bun.DB
	sqlDB := bunDB.DB

	// Create Astha configuration
	cfg := &astha.Config{
		Database:     sqlDB,
		Logger:       mm.logger,
		BatchSize:    10,
		PollInterval: 100, // 100ms
	}

	// Create Astha instance
	asthaInstance, err := astha.NewAstha(cfg)
	if err != nil {
		return err
	}

	// Register Iceberg component with Astha
	icebergComponent := iceberg.NewIcebergComponent(mm.icebergManager, mm.logger)

	if err := asthaInstance.RegisterComponentWithInstance(icebergComponent.GetComponentInfo(), icebergComponent.AsSubscriberAny()); err != nil {
		return err
	}

	mm.astha = asthaInstance
	return nil
}

// loadPendingFiles loads files that need Iceberg metadata generation during startup
func (mm *MetadataManager) loadPendingFiles(ctx context.Context) error {
	// Get pending files from Registry
	pendingFiles, err := mm.storage.GetPendingFilesForIceberg(ctx)
	if err != nil {
		return err
	}

	if len(pendingFiles) > 0 {
		mm.logger.Info().
			Int("count", len(pendingFiles)).
			Msg("Loading pending files for Iceberg metadata generation")

		// Process pending files in batches
		for _, file := range pendingFiles {
			if err := mm.icebergManager.ProcessFile(file); err != nil {
				mm.logger.Warn().
					Err(err).
					Int64("file_id", file.ID).
					Msg("Failed to process pending file")
			}
		}
	}

	return nil
}

// EnsureDeploymentReady ensures the system is ready for deployment
func (mm *MetadataManager) EnsureDeploymentReady(ctx context.Context) error {
	// Use hybrid system with bun
	return mm.hybrid.EnsureDeploymentReady(ctx)
}

// CreateDatabase creates a new database
func (mm *MetadataManager) CreateDatabase(ctx context.Context, dbName string) error {
	// Create in personal metadata storage
	if err := mm.storage.CreateDatabase(ctx, dbName); err != nil {
		return err
	}

	// Create in Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Created database %s in personal metadata", dbName)

	return nil
}

// DropDatabase drops a database
func (mm *MetadataManager) DropDatabase(ctx context.Context, dbName string) error {
	// Drop from personal metadata storage
	if err := mm.storage.DropDatabase(ctx, dbName); err != nil {
		return err
	}

	// Drop from Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Dropped database %s from personal metadata", dbName)

	return nil
}

// ListDatabases returns a list of all databases
func (mm *MetadataManager) ListDatabases(ctx context.Context) ([]string, error) {
	return mm.storage.ListDatabases(ctx)
}

// DatabaseExists checks if a database exists
func (mm *MetadataManager) DatabaseExists(ctx context.Context, dbName string) bool {
	return mm.storage.DatabaseExists(ctx, dbName)
}

// CreateTable creates a new table with complete metadata
func (mm *MetadataManager) CreateTable(ctx context.Context, dbName, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) error {
	// Create table with complete metadata in a single operation
	_, err := mm.storage.CreateTable(ctx, dbName, tableName, schema, storageEngine, engineConfig)
	if err != nil {
		return err
	}

	// Create in Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Created table %s.%s with complete metadata in personal metadata", dbName, tableName)

	return nil
}

// DropTable drops a table
func (mm *MetadataManager) DropTable(ctx context.Context, dbName, tableName string) error {
	// Drop from personal metadata storage
	if err := mm.storage.DropTable(ctx, dbName, tableName); err != nil {
		return err
	}

	// Drop from Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Dropped table %s from personal metadata", dbName)

	return nil
}

// ListTables returns a list of tables in a database
func (mm *MetadataManager) ListTables(ctx context.Context, dbName string) ([]string, error) {
	return mm.storage.ListTables(ctx, dbName)
}

// TableExists checks if a table exists
func (mm *MetadataManager) TableExists(ctx context.Context, dbName, tableName string) bool {
	return mm.storage.TableExists(ctx, dbName, tableName)
}

// Close releases resources
func (mm *MetadataManager) Close() error {
	if mm.storage != nil {
		return mm.storage.Close()
	}
	return nil
}

// GetType returns the component type identifier
func (mm *MetadataManager) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the metadata manager
func (mm *MetadataManager) Shutdown(ctx context.Context) error {
	log.Printf("Shutting down metadata manager")

	// Stop the manager first
	if err := mm.Stop(ctx); err != nil {
		return err
	}

	// Close metadata manager
	if err := mm.Close(); err != nil {
		return err
	}

	log.Printf("Metadata manager shut down successfully")
	return nil
}

// GetCatalog returns the Iceberg catalog
func (mm *MetadataManager) GetCatalog() catalog.CatalogInterface {
	return mm.iceberg
}

// GetStorage returns the personal metadata storage
func (mm *MetadataManager) GetStorage() *registry.Store {
	return mm.storage
}

// GetHybridManager returns the hybrid deployment manager
func (mm *MetadataManager) GetHybridManager() *registry.HybridDeploymentManager {
	return mm.hybrid
}

// GetIcebergManager returns the Iceberg metadata manager
func (mm *MetadataManager) GetIcebergManager() *iceberg.Manager {
	return mm.icebergManager
}

// IsUsingBun returns true if the manager is using bun migrations
func (mm *MetadataManager) IsUsingBun() bool {
	return true // Always true now
}

// GetBunDB returns the underlying bun DB for advanced operations
func (mm *MetadataManager) GetBunDB() *bun.DB {
	if mm.hybrid != nil {
		return mm.hybrid.GetBunDB()
	}
	return nil
}

// CreateTableMetadata creates detailed metadata for a table (for storage operations)
func (mm *MetadataManager) CreateTableMetadata(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) (*registry.TableMetadata, error) {
	return mm.storage.CreateTableMetadata(ctx, database, tableName, schema, storageEngine, engineConfig)
}

// LoadTableMetadata loads detailed metadata for a table
func (mm *MetadataManager) LoadTableMetadata(ctx context.Context, database, tableName string) (*registry.TableMetadata, error) {
	return mm.storage.LoadTableMetadata(ctx, database, tableName)
}

// ListAllTables returns a list of all tables across all databases (for storage manager)
func (mm *MetadataManager) ListAllTables(ctx context.Context) ([]string, error) {
	return mm.storage.ListAllTables(ctx)
}

// UpdateTableAfterInsertion performs metadata updates after successful data insertion
func (mm *MetadataManager) UpdateTableAfterInsertion(ctx context.Context, database, tableName string, fileInfo registry.FileInsertionInfo) error {
	return mm.storage.UpdateTableAfterInsertion(ctx, database, tableName, fileInfo)
}

// GetCompleteTableInfoByID retrieves complete table information by table ID
func (mm *MetadataManager) GetCompleteTableInfoByID(ctx context.Context, tableID int64) (*registry.CompleteTableInfo, error) {
	return mm.storage.GetCompleteTableInfoByID(ctx, tableID)
}

// GetTableReferenceByID retrieves basic table reference (database + table name) by table ID
func (mm *MetadataManager) GetTableReferenceByID(ctx context.Context, tableID int64) (*registry.TableReference, error) {
	return mm.storage.GetTableReferenceByID(ctx, tableID)
}

// ValidateTableMetadata validates that a table has complete metadata for Iceberg operations
func (mm *MetadataManager) ValidateTableMetadata(ctx context.Context, tableID int64) error {
	return mm.storage.ValidateTableMetadata(ctx, tableID)
}

// GetPendingFilesForIceberg retrieves files that need Iceberg metadata generation
func (mm *MetadataManager) GetPendingFilesForIceberg(ctx context.Context) ([]*regtypes.TableFile, error) {
	return mm.storage.GetPendingFilesForIceberg(ctx)
}

// CreateTableWithSchema creates a table with schema in a single transaction
// Enhanced with Iceberg type validation, proper error handling, and rollback mechanisms
// This method implements Requirements 3.1-3.7 for transactional schema storage
func (mm *MetadataManager) CreateTableWithSchema(ctx context.Context, databaseName string, table *regtypes.Table, columns []*regtypes.TableColumn) (int64, error) {
	startTime := time.Now()

	// Generate transaction ID for tracking
	transactionID := fmt.Sprintf("create_table_%d", time.Now().UnixNano())

	// No-op diagnostic logger stub (TODO: remove diagnostic logging)
	diagLogger := &noOpDiagLogger{}

	// Log operation start
	mm.logger.Info().
		Str("database", databaseName).
		Str("table_name", table.Name).
		Int("column_count", len(columns)).
		Str("transaction_id", transactionID).
		Msg("CREATE TABLE with schema started")

	mm.mu.RLock()
	if !mm.running {
		mm.mu.RUnlock()

		err := types.NewCreateTableRegistryError(
			"metadata manager is not running",
			table.Name,
			databaseName,
			transactionID,
			transactionID,
			errors.New(MetadataManagerNotRunning, "metadata manager is not running", nil),
		).AddSuggestion("Start the metadata manager before creating tables").
			AddRecoveryHint(types.RecoveryHint{
				Type:        "restart_service",
				Description: "Restart the metadata manager service",
				Action:      "Check metadata manager status and restart if necessary",
				Automatic:   false,
			})

		diagLogger.LogOperationEnd("check_manager_status", false, err, nil)
		return 0, err
	}
	mm.mu.RUnlock()

	// Validate input parameters with comprehensive error handling
	diagLogger.LogOperationStart("validate_input_parameters", map[string]interface{}{
		"database_provided": databaseName != "",
		"table_provided":    table != nil,
		"columns_provided":  len(columns),
	})

	if databaseName == "" {
		err := types.NewCreateTableRegistryError(
			"database name is required",
			table.Name,
			databaseName,
			transactionID,
			transactionID,
			errors.New(MetadataOperationFailed, "database name is required", nil),
		).AddSuggestion("Provide a valid database name")

		diagLogger.LogOperationEnd("validate_input_parameters", false, err, nil)
		return 0, err
	}

	if table == nil {
		err := types.NewCreateTableRegistryError(
			"table metadata cannot be nil",
			"",
			databaseName,
			transactionID,
			transactionID,
			errors.New(MetadataOperationFailed, "table cannot be nil", nil),
		).AddSuggestion("Provide valid table metadata")

		diagLogger.LogOperationEnd("validate_input_parameters", false, err, nil)
		return 0, err
	}

	if len(columns) == 0 {
		err := types.NewCreateTableRegistryError(
			"table must have at least one column",
			table.Name,
			databaseName,
			transactionID,
			transactionID,
			errors.New(MetadataOperationFailed, "table must have at least one column", nil),
		).AddSuggestion("Add at least one column definition to the table")

		diagLogger.LogOperationEnd("validate_input_parameters", false, err, nil)
		return 0, err
	}

	diagLogger.LogOperationEnd("validate_input_parameters", true, nil, map[string]interface{}{
		"validation_passed": true,
	})

	// Requirement 3.3: Schema validation before registry storage using Iceberg types
	diagLogger.LogOperationStart("validate_schema_iceberg_types", map[string]interface{}{
		"table_name":   table.Name,
		"column_count": len(columns),
	})

	if err := mm.validateSchemaWithIcebergTypes(table, columns); err != nil {
		// Extract validation details for comprehensive error
		var invalidTypes []string

		// Parse validation error to extract specific issues
		errorMsg := err.Error()
		if strings.Contains(errorMsg, "invalid type") {
			// Extract invalid types from error message
			// This is simplified - in practice, you'd want more sophisticated parsing
			invalidTypes = append(invalidTypes, "extracted_from_error")
		}

		createTableErr := types.NewCreateTableTypeValidationError(
			"schema validation failed - invalid Iceberg types detected",
			table.Name,
			invalidTypes,
			err,
		).AddContext("database", databaseName).
			AddContext("transaction_id", transactionID).
			AddSuggestion("Use only supported Iceberg data types").
			AddSuggestion("Check the centralized Iceberg type system documentation")

		diagLogger.LogValidationError("validate_schema_iceberg_types", createTableErr.Diagnostics.ValidationDetails)
		diagLogger.LogOperationEnd("validate_schema_iceberg_types", false, createTableErr, nil)

		mm.logger.Error().Err(err).
			Str("database", databaseName).
			Str("table_name", table.Name).
			Int("column_count", len(columns)).
			Str("transaction_id", transactionID).
			Msg("Schema validation failed")

		return 0, createTableErr
	}

	diagLogger.LogOperationEnd("validate_schema_iceberg_types", true, nil, map[string]interface{}{
		"schema_validation_passed": true,
	})

	// Validate table metadata consistency
	diagLogger.LogOperationStart("validate_table_metadata", map[string]interface{}{
		"table_name": table.Name,
	})

	if err := mm.validateTableMetadata(table); err != nil {
		createTableErr := types.NewCreateTableRegistryError(
			"table metadata validation failed",
			table.Name,
			databaseName,
			transactionID,
			transactionID,
			err,
		).AddSuggestion("Check table metadata for consistency").
			AddSuggestion("Verify storage engine and configuration settings")

		diagLogger.LogOperationEnd("validate_table_metadata", false, createTableErr, nil)

		mm.logger.Error().Err(err).
			Str("database", databaseName).
			Str("table_name", table.Name).
			Str("transaction_id", transactionID).
			Msg("Table metadata validation failed")

		return 0, createTableErr
	}

	diagLogger.LogOperationEnd("validate_table_metadata", true, nil, map[string]interface{}{
		"metadata_validation_passed": true,
	})

	// Requirement 3.1 & 3.2: Create table metadata and columns in single transaction
	// Requirement 3.4: Proper error handling and rollback mechanisms
	diagLogger.LogOperationStart("create_table_registry_transaction", map[string]interface{}{
		"database":       databaseName,
		"table_name":     table.Name,
		"column_count":   len(columns),
		"transaction_id": transactionID,
	})

	tableID, err := mm.storage.CreateTableWithColumns(ctx, databaseName, table, columns)
	if err != nil {
		// Enhanced error handling with comprehensive context and recovery suggestions
		registryDiagnostics := &types.RegistryDiagnostics{
			TransactionID:     transactionID,
			TablesCreated:     []string{}, // No tables created due to failure
			ColumnsCreated:    0,
			RollbackPerformed: true, // Assume rollback was performed
			DatabaseState: map[string]string{
				"database": databaseName,
				"status":   "transaction_failed",
			},
		}

		var createTableErr *types.CreateTableError

		// Check for specific error types and provide better error messages
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "already exists") {
			registryDiagnostics.ConstraintViolations = []string{
				fmt.Sprintf("UNIQUE constraint violation: table '%s' already exists", table.Name),
			}
		} else if strings.Contains(err.Error(), "database does not exist") {
			// Database doesn't exist
		} else if strings.Contains(err.Error(), "transaction") {
			createTableErr = types.NewCreateTableRegistryError(
				"transaction failed during table creation",
				table.Name,
				databaseName,
				transactionID,
				transactionID,
				errors.New(RegistryTransactionFailed, "transaction failed during table creation", err),
			).AddSuggestion("Check database connectivity and locks").
				AddSuggestion("Retry the operation with a new transaction").
				AddRecoveryHint(types.RecoveryHint{
					Type:        "retry",
					Description: "Retry with a new transaction",
					Action:      "Start a new transaction and retry the table creation",
					Automatic:   true,
				})
		} else {
			createTableErr = types.NewCreateTableRegistryError(
				"failed to create table with schema in registry",
				table.Name,
				databaseName,
				transactionID,
				transactionID,
				err,
			).AddSuggestion("Check database connectivity").
				AddSuggestion("Verify registry schema integrity").
				AddRecoveryHint(types.RecoveryHint{
					Type:        "check_database",
					Description: "Verify database connectivity and state",
					Action:      "Check database connection and schema integrity",
					Automatic:   false,
				})
		}

		// Add registry diagnostics to the error
		createTableErr.Diagnostics.RegistryState = registryDiagnostics

		diagLogger.LogRegistryOperation("create_table_registry_transaction", registryDiagnostics)
		diagLogger.LogOperationEnd("create_table_registry_transaction", false, createTableErr, nil)

		mm.logger.Error().Err(err).
			Str("database", databaseName).
			Str("table_name", table.Name).
			Int("column_count", len(columns)).
			Str("transaction_id", transactionID).
			Msg("Failed to create table with schema in registry")

		return 0, createTableErr
	}

	// Success - log comprehensive information
	successDiagnostics := &types.RegistryDiagnostics{
		TransactionID:     transactionID,
		TablesCreated:     []string{table.Name},
		ColumnsCreated:    len(columns),
		RollbackPerformed: false,
		DatabaseState: map[string]string{
			"database": databaseName,
			"status":   "transaction_successful",
			"table_id": fmt.Sprintf("%d", tableID),
		},
	}

	diagLogger.LogRegistryOperation("create_table_registry_transaction", successDiagnostics)
	diagLogger.LogOperationEnd("create_table_registry_transaction", true, nil, map[string]interface{}{
		"table_id":       tableID,
		"transaction_id": transactionID,
	})

	// Requirement 3.5: Complete schema immediately queryable from registry
	totalDuration := time.Since(startTime)

	diagLogger.LogPerformanceMetrics("create_table_with_schema_complete", totalDuration, map[string]interface{}{
		"table_id":       tableID,
		"column_count":   len(columns),
		"transaction_id": transactionID,
	})

	// Log successful creation for audit trail with enhanced context
	mm.logger.Info().
		Int64("table_id", tableID).
		Str("database", databaseName).
		Str("table_name", table.Name).
		Int("column_count", len(columns)).
		Bool("is_temporary", table.IsTemporary).
		Bool("is_external", table.IsExternal).
		Str("transaction_id", transactionID).
		Dur("duration", totalDuration).
		Msg("Successfully created table with schema")

	// Log operation summary
	diagLogger.LogSummary()

	return tableID, nil
}

// validateSchemaWithIcebergTypes validates all column types using Iceberg type system
// This method implements comprehensive schema validation using the centralized Iceberg type system
func (mm *MetadataManager) validateSchemaWithIcebergTypes(table *regtypes.Table, columns []*regtypes.TableColumn) error {
	validator := types.NewIcebergTypeValidator()
	columnNames := make(map[string]bool)

	for i, column := range columns {
		// Validate column name
		if column.ColumnName == "" {
			return fmt.Errorf("column %d: name cannot be empty", i+1)
		}

		// Check for duplicate column names
		if columnNames[column.ColumnName] {
			return fmt.Errorf("duplicate column name '%s'", column.ColumnName)
		}
		columnNames[column.ColumnName] = true

		// Validate Iceberg type using centralized validator
		if !validator.IsValidType(column.DataType) {
			supportedTypes := validator.GetSupportedTypes()

			return fmt.Errorf("column '%s': invalid Iceberg type '%s'. Supported types: %v",
				column.ColumnName, column.DataType, supportedTypes)
		}

		// Validate complex types (list, map, struct) using centralized validation
		if err := validator.ValidateComplexType(column.DataType); err != nil {
			return fmt.Errorf("column '%s': complex type validation failed: %w", column.ColumnName, err)
		}

		// Parse the type to get detailed validation for precision/scale
		parsedType, err := validator.ParseType(column.DataType)
		if err != nil {
			return fmt.Errorf("column '%s': failed to parse Iceberg type '%s': %w", column.ColumnName, column.DataType, err)
		}

		// Validate the parsed type structure
		if err := parsedType.Validate(); err != nil {
			return fmt.Errorf("column '%s': type validation failed: %w", column.ColumnName, err)
		}

		// Validate ordinal position consistency
		if column.OrdinalPosition <= 0 {
			return fmt.Errorf("column '%s': ordinal position must be greater than 0", column.ColumnName)
		}

		// Additional validation for decimal types using parsed type information
		if primitiveType, ok := parsedType.(*types.PrimitiveType); ok && primitiveType.TypeName == types.IcebergDecimal {
			// Ensure registry column precision/scale matches parsed type
			if column.Precision != primitiveType.Precision {
				return fmt.Errorf("column '%s': registry precision (%d) does not match type precision (%d)",
					column.ColumnName, column.Precision, primitiveType.Precision)
			}
			if column.Scale != primitiveType.Scale {
				return fmt.Errorf("column '%s': registry scale (%d) does not match type scale (%d)",
					column.ColumnName, column.Scale, primitiveType.Scale)
			}
		}

		// Validate type complexity for performance considerations
		complexity := types.GetTypeComplexity(parsedType)
		if complexity > 5 { // Reasonable limit for nested types
			return fmt.Errorf("column '%s': type complexity (%d) exceeds maximum allowed depth (5)", column.ColumnName, complexity)
		}
	}

	return nil
}

// validateTableMetadata validates table metadata for consistency and completeness
// This method implements comprehensive table metadata validation
func (mm *MetadataManager) validateTableMetadata(table *regtypes.Table) error {
	// Validate table name
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Validate table name format (basic validation)
	if strings.Contains(table.Name, " ") || strings.Contains(table.Name, ".") {
		return fmt.Errorf("table name '%s' contains invalid characters (spaces and dots not allowed)", table.Name)
	}

	// Validate table name length
	if len(table.Name) > 255 {
		return fmt.Errorf("table name '%s' exceeds maximum length of 255 characters", table.Name)
	}

	// Validate database name if provided (table.Database is a relation, not a string field)
	// Database validation is handled at the method parameter level

	// Validate table type
	if table.TableType != "" {
		validTypes := []string{"user", "system", "view", "temporary"}
		isValid := false
		for _, tableType := range validTypes {
			if table.TableType == tableType {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("invalid table type '%s'. Supported types: %v",
				table.TableType, validTypes)
		}
	}

	// Validate numeric fields
	if table.RowCount < 0 {
		return fmt.Errorf("row count cannot be negative")
	}
	if table.FileCount < 0 {
		return fmt.Errorf("file count cannot be negative")
	}
	if table.TotalSize < 0 {
		return fmt.Errorf("total size cannot be negative")
	}

	// Validate consistency between temporary flag and table type
	if table.IsTemporary && table.TableType != "temporary" && table.TableType != "" {
		return fmt.Errorf("temporary table flag conflicts with table type '%s'", table.TableType)
	}

	// Validate external table constraints
	if table.IsExternal && table.IsTemporary {
		return fmt.Errorf("table cannot be both external and temporary")
	}

	return nil
}

// validateStorageEngine validates that the storage engine is supported
// This method implements storage engine validation for CREATE TABLE operations
func (mm *MetadataManager) validateStorageEngine(storageEngine string) error {
	if storageEngine == "" {
		return fmt.Errorf("storage engine cannot be empty")
	}

	// Define supported storage engines
	supportedEngines := map[string]bool{
		"iceberg":    true,
		"parquet":    true,
		"delta":      true,
		"memory":     true,
		"filesystem": true,
	}

	if !supportedEngines[strings.ToLower(storageEngine)] {
		var engines []string
		for engine := range supportedEngines {
			engines = append(engines, engine)
		}
		return fmt.Errorf("unsupported storage engine '%s'. Supported engines: %v", storageEngine, engines)
	}

	return nil
}

// validateEngineConfig validates the engine configuration for the specified storage engine
// This method implements engine-specific configuration validation
func (mm *MetadataManager) validateEngineConfig(storageEngine string, engineConfig map[string]interface{}) error {
	if engineConfig == nil {
		return nil // Empty config is allowed
	}

	switch strings.ToLower(storageEngine) {
	case "iceberg":
		return mm.validateIcebergConfig(engineConfig)
	case "parquet":
		return mm.validateParquetConfig(engineConfig)
	case "delta":
		return mm.validateDeltaConfig(engineConfig)
	case "memory":
		return mm.validateMemoryConfig(engineConfig)
	case "filesystem":
		return mm.validateFilesystemConfig(engineConfig)
	default:
		// Unknown engine, but we already validated it exists
		return nil
	}
}

// validateIcebergConfig validates Iceberg-specific configuration
func (mm *MetadataManager) validateIcebergConfig(config map[string]interface{}) error {
	// Validate format
	if format, exists := config["format"]; exists {
		if formatStr, ok := format.(string); ok {
			validFormats := []string{"parquet", "orc", "avro"}
			isValid := false
			for _, validFormat := range validFormats {
				if formatStr == validFormat {
					isValid = true
					break
				}
			}
			if !isValid {
				return fmt.Errorf("invalid Iceberg format '%s'. Supported formats: %v", formatStr, validFormats)
			}
		} else {
			return fmt.Errorf("Iceberg format must be a string")
		}
	}

	// Validate compression
	if compression, exists := config["compression"]; exists {
		if compressionStr, ok := compression.(string); ok {
			validCompressions := []string{"none", "snappy", "gzip", "lz4", "zstd"}
			isValid := false
			for _, validCompression := range validCompressions {
				if compressionStr == validCompression {
					isValid = true
					break
				}
			}
			if !isValid {
				return fmt.Errorf("invalid compression '%s'. Supported compressions: %v", compressionStr, validCompressions)
			}
		} else {
			return fmt.Errorf("compression must be a string")
		}
	}

	return nil
}

// validateParquetConfig validates Parquet-specific configuration
func (mm *MetadataManager) validateParquetConfig(config map[string]interface{}) error {
	// Similar validation for Parquet-specific settings
	if compression, exists := config["compression"]; exists {
		if compressionStr, ok := compression.(string); ok {
			validCompressions := []string{"none", "snappy", "gzip", "lz4", "brotli"}
			isValid := false
			for _, validCompression := range validCompressions {
				if compressionStr == validCompression {
					isValid = true
					break
				}
			}
			if !isValid {
				return fmt.Errorf("invalid Parquet compression '%s'. Supported compressions: %v", compressionStr, validCompressions)
			}
		} else {
			return fmt.Errorf("compression must be a string")
		}
	}

	return nil
}

// validateDeltaConfig validates Delta Lake-specific configuration
func (mm *MetadataManager) validateDeltaConfig(config map[string]interface{}) error {
	// Delta Lake specific validation
	return nil
}

// validateMemoryConfig validates memory storage-specific configuration
func (mm *MetadataManager) validateMemoryConfig(config map[string]interface{}) error {
	// Memory storage specific validation
	if maxSize, exists := config["max_size"]; exists {
		if _, ok := maxSize.(int64); !ok {
			if _, ok := maxSize.(float64); !ok {
				return fmt.Errorf("max_size must be a number")
			}
		}
	}

	return nil
}

// validateFilesystemConfig validates filesystem storage-specific configuration
func (mm *MetadataManager) validateFilesystemConfig(config map[string]interface{}) error {
	// Filesystem storage specific validation
	if basePath, exists := config["base_path"]; exists {
		if _, ok := basePath.(string); !ok {
			return fmt.Errorf("base_path must be a string")
		}
	}

	return nil
}
