package metadata

import (
	"context"

	"github.com/TFMV/icebox/server/metadata/types"
)

// MetadataManagerInterface defines the interface for the metadata manager
type MetadataManagerInterface interface {
	// Database operations
	CreateDatabase(ctx context.Context, dbName string) error
	DropDatabase(ctx context.Context, dbName string) error
	ListDatabases(ctx context.Context) ([]string, error)
	DatabaseExists(ctx context.Context, dbName string) bool

	// Table operations
	CreateTable(ctx context.Context, dbName, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) error
	DropTable(ctx context.Context, dbName, tableName string) error
	ListTables(ctx context.Context, dbName string) ([]string, error)
	TableExists(ctx context.Context, dbName, tableName string) bool

	// Table metadata operations (for storage manager) - these work with explicit database and table names
	LoadTableMetadata(ctx context.Context, database, tableName string) (*types.TableMetadata, error)
	ListAllTables(ctx context.Context) ([]string, error)

	// Close releases resources
	Close() error
}

// DatabaseInfo represents database metadata
type DatabaseInfo struct {
	Name       string `json:"name"`
	Created    string `json:"created"`
	Modified   string `json:"modified"`
	TableCount int    `json:"table_count"`
}

// TableInfo represents table metadata
type TableInfo struct {
	DatabaseName string `json:"database_name"`
	TableName    string `json:"table_name"`
	Created      string `json:"created"`
	Modified     string `json:"modified"`
}

// ManagerConfig holds configuration for the metadata manager
type ManagerConfig struct {
	SQLitePath string `json:"sqlite_path"`
	BasePath   string `json:"base_path"`
}
