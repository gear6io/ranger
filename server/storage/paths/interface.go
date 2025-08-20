package paths

// PathManager defines the interface for path management operations
// This interface abstracts path resolution to avoid import cycles
// It aligns with server/catalog/shared/paths.go to maintain consistency
type PathManager interface {
	// Base paths
	GetBasePath() string
	GetCatalogPath() string
	GetDataPath() string
	GetInternalMetadataPath() string
	GetInternalMetadataDBPath() string
	GetMigrationsPath() string

	// Catalog paths
	GetCatalogURI(catalogType string) string

	// Table paths (aligned with shared.PathManager)
	GetTablePath(database, tableName string) string
	GetTableDataPath(namespace []string, tableName string) string
	GetTableMetadataPath(namespace []string, tableName string) string
	GetTableMetadataFile(database, tableName string, version int) string

	// View paths
	GetViewMetadataPath(namespace []string, viewName string) string

	// Parquet-specific paths
	GetParquetDataPath(database, tableName string) string
	GetParquetFileName(timestamp string, fileCount int) string
	GetParquetFilePath(database, tableName, filename string) string
	GetParquetFilePattern(database, tableName string) string

	// Namespace paths
	GetNamespacePath(namespace []string) string

	// Utility paths
	GetMetadataDir() string
	GetDataDir() string

	// Directory operations
	EnsureDirectoryStructure() error
}
