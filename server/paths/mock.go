package paths

import (
	"fmt"
	"strings"
)

// MockPathManager implements PathManager for testing
type MockPathManager struct {
	BasePath string
}

// GetBasePath returns the base data path
func (m *MockPathManager) GetBasePath() string {
	return m.BasePath
}

// GetCatalogPath returns the catalog directory path
func (m *MockPathManager) GetCatalogPath() string {
	return m.BasePath + "/catalog"
}

// GetDataPath returns the data storage directory path
func (m *MockPathManager) GetDataPath() string {
	return m.BasePath + "/data"
}

// GetInternalMetadataPath returns the internal metadata directory path
func (m *MockPathManager) GetInternalMetadataPath() string {
	return m.BasePath + "/.ranger"
}

// GetInternalMetadataDBPath returns the internal metadata database path
func (m *MockPathManager) GetInternalMetadataDBPath() string {
	return m.BasePath + "/.ranger/metadata.db"
}

// GetMigrationsPath returns the migrations directory path
func (m *MockPathManager) GetMigrationsPath() string {
	return m.BasePath + "/.ranger/migrations"
}

// GetCatalogURI returns the catalog URI based on catalog type
func (m *MockPathManager) GetCatalogURI(catalogType string) string {
	switch catalogType {
	case "json":
		return m.BasePath + "/catalog/catalog.json"
	case "sqlite":
		return m.BasePath + "/catalog/catalog.db"
	default:
		return ""
	}
}

// GetTablePath returns the full path for a table
func (m *MockPathManager) GetTablePath(database, tableName string) string {
	return m.BasePath + "/data/" + database + "/" + tableName
}

// GetTableDataPath returns the data directory path for a table
func (m *MockPathManager) GetTableDataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return m.BasePath + "/tables/" + nsPath + "/" + tableName + "/data"
}

// GetTableMetadataPath returns the metadata directory path for a table
func (m *MockPathManager) GetTableMetadataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return m.BasePath + "/tables/" + nsPath + "/" + tableName + "/metadata"
}

// GetViewMetadataPath returns the metadata directory path for a view
func (m *MockPathManager) GetViewMetadataPath(namespace []string, viewName string) string {
	nsPath := strings.Join(namespace, "/")
	return m.BasePath + "/views/" + nsPath + "/" + viewName + "/metadata"
}

// GetParquetDataPath returns the data directory path for Parquet files
func (m *MockPathManager) GetParquetDataPath(database, tableName string) string {
	return m.BasePath + "/tables/" + database + "/" + tableName + "/data"
}

// GetParquetFileName generates a Parquet filename with timestamp and counter
func (m *MockPathManager) GetParquetFileName(timestamp string, fileCount int) string {
	return fmt.Sprintf("data_%s_%04d.parquet", timestamp, fileCount)
}

// GetParquetFilePath returns the full path for a Parquet file
func (m *MockPathManager) GetParquetFilePath(database, tableName, filename string) string {
	return m.BasePath + "/tables/" + database + "/" + tableName + "/data/" + filename
}

// GetParquetFilePattern returns the glob pattern for finding Parquet files
func (m *MockPathManager) GetParquetFilePattern(database, tableName string) string {
	return m.BasePath + "/tables/" + database + "/" + tableName + "/data/*.parquet"
}

// GetTableMetadataFile returns the metadata file path for a specific version
func (m *MockPathManager) GetTableMetadataFile(database, tableName string, version int) string {
	return m.BasePath + "/data/" + database + "/" + tableName + "/metadata/v" + fmt.Sprintf("%d", version) + ".metadata.json"
}

// GetTableManifestPath returns the manifest directory path for a table
func (m *MockPathManager) GetTableManifestPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return m.BasePath + "/tables/" + nsPath + "/" + tableName + "/manifests"
}

// GetNamespacePath returns the namespace path
func (m *MockPathManager) GetNamespacePath(namespace []string) string {
	if len(namespace) == 0 {
		return m.GetDataPath()
	}
	path := m.BasePath + "/data"
	for _, ns := range namespace {
		path += "/" + ns
	}
	return path
}

// GetMetadataDir returns the metadata directory
func (m *MockPathManager) GetMetadataDir() string {
	return m.GetInternalMetadataPath()
}

// GetDataDir returns the data directory
func (m *MockPathManager) GetDataDir() string {
	return m.GetDataPath()
}

// EnsureDirectoryStructure creates all necessary directories
func (m *MockPathManager) EnsureDirectoryStructure() error {
	// Mock implementation - always succeeds
	return nil
}
