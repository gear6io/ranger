package shared

// PathManager defines the interface for path management operations
// This interface abstracts path resolution to avoid import cycles
type PathManager interface {
	// Catalog paths
	GetCatalogURI(catalogType string) string

	// Table paths
	GetTableMetadataPath(namespace []string, tableName string) string
	GetTableDataPath(namespace []string, tableName string) string

	// View paths
	GetViewMetadataPath(namespace []string, viewName string) string

	// Namespace paths
	GetNamespacePath(namespace []string) string

	// Utility paths
	GetMetadataDir() string
	GetDataDir() string
}
