package shared

import (
	"path/filepath"
	"strings"
)

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

// MockPathManager implements shared.PathManager for testing
// This is a shared implementation to avoid duplication across test files
var _ PathManager = (*MockPathManager)(nil)

type MockPathManager struct {
	BasePath string
}

func (m *MockPathManager) GetCatalogURI(catalogType string) string {
	if m.BasePath == "" {
		return ""
	}

	switch catalogType {
	case "json":
		return filepath.Join(m.BasePath, "catalog", "catalog.json")
	case "sqlite":
		return filepath.Join(m.BasePath, "catalog", "catalog.db")
	case "rest":
		return "http://localhost:8181" // Mock REST catalog URI
	default:
		return ""
	}
}

func (m *MockPathManager) GetTableMetadataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(m.BasePath, "tables", nsPath, tableName, "metadata")
}

func (m *MockPathManager) GetTableDataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(m.BasePath, "tables", nsPath, tableName, "data")
}

func (m *MockPathManager) GetViewMetadataPath(namespace []string, viewName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(m.BasePath, "views", nsPath, viewName, "metadata")
}

func (m *MockPathManager) GetNamespacePath(namespace []string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(m.BasePath, "namespaces", nsPath)
}

func (m *MockPathManager) GetMetadataDir() string {
	return filepath.Join(m.BasePath, "metadata")
}

func (m *MockPathManager) GetDataDir() string {
	return filepath.Join(m.BasePath, "data")
}
