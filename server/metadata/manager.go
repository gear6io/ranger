package metadata

import (
	"context"
	"fmt"
	"log"

	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/TFMV/icebox/server/metadata/types"
	"github.com/uptrace/bun"
)

// MetadataManager coordinates between Iceberg catalog and personal metadata storage
type MetadataManager struct {
	iceberg catalog.CatalogInterface
	storage *registry.Store
	hybrid  *registry.HybridDeploymentManager
}

// NewMetadataManager creates a new metadata manager with bun migrations
func NewMetadataManager(catalog catalog.CatalogInterface, dbPath, basePath string) (*MetadataManager, error) {
	// Create storage with bun migrations
	storage, err := registry.NewStore(dbPath, basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	manager := &MetadataManager{
		iceberg: catalog,
		storage: storage,
	}

	// Create hybrid deployment manager
	bunMigrator := storage.GetBunMigrationManager()
	if bunMigrator == nil {
		return nil, fmt.Errorf("bun migrator not available")
	}
	manager.hybrid = registry.NewHybridDeploymentManager(storage, bunMigrator)

	return manager, nil
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
		return fmt.Errorf("failed to create database in storage: %w", err)
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
		return fmt.Errorf("failed to drop database from storage: %w", err)
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

// CreateTable creates a new table
func (mm *MetadataManager) CreateTable(ctx context.Context, dbName, tableName string) error {
	// Create in personal metadata storage
	if err := mm.storage.CreateTable(ctx, dbName, tableName); err != nil {
		return fmt.Errorf("failed to create table in storage: %w", err)
	}

	// Create in Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Created table %s.%s in personal metadata", dbName, tableName)

	return nil
}

// DropTable drops a table
func (mm *MetadataManager) DropTable(ctx context.Context, dbName, tableName string) error {
	// Drop from personal metadata storage
	if err := mm.storage.DropTable(ctx, dbName, tableName); err != nil {
		return fmt.Errorf("failed to drop table from storage: %w", err)
	}

	// Drop from Iceberg catalog if needed
	// This is where you'd coordinate with the Iceberg catalog
	log.Printf("Dropped table %s.%s from personal metadata", dbName, tableName)

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
func (mm *MetadataManager) CreateTableMetadata(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) (*types.TableMetadata, error) {
	return mm.storage.CreateTableMetadata(ctx, database, tableName, schema, storageEngine, engineConfig)
}

// LoadTableMetadata loads detailed metadata for a table
func (mm *MetadataManager) LoadTableMetadata(ctx context.Context, database, tableName string) (*types.TableMetadata, error) {
	return mm.storage.LoadTableMetadata(ctx, database, tableName)
}

// ListAllTables returns a list of all tables across all databases (for storage manager)
func (mm *MetadataManager) ListAllTables(ctx context.Context) ([]string, error) {
	return mm.storage.ListAllTables(ctx)
}
