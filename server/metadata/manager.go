package metadata

import (
	"context"
	"log"
	"sync"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/astha"
	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/metadata/iceberg"
	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
	"github.com/TFMV/icebox/server/paths"
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
)

// ComponentType defines the metadata manager component type identifier
const ComponentType = "metadata"

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
