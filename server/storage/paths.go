package storage

import (
	"fmt"
	"path/filepath"
	"strings"
)

// PathManager handles standardized path construction for Icebox storage
type PathManager struct {
	basePath string
}

// NewPathManager creates a new path manager
func NewPathManager(basePath string) *PathManager {
	return &PathManager{
		basePath: basePath,
	}
}

// GetBasePath returns the base data path
func (pm *PathManager) GetBasePath() string {
	return pm.basePath
}

// GetCatalogPath returns the catalog directory path
func (pm *PathManager) GetCatalogPath() string {
	return filepath.Join(pm.basePath, "catalog")
}

// GetDataPath returns the data storage directory path
func (pm *PathManager) GetDataPath() string {
	return filepath.Join(pm.basePath, "data")
}

// GetInternalMetadataPath returns the internal metadata directory path
func (pm *PathManager) GetInternalMetadataPath() string {
	return filepath.Join(pm.basePath, ".icebox")
}

// GetInternalMetadataDBPath returns the internal metadata database path
func (pm *PathManager) GetInternalMetadataDBPath() string {
	return filepath.Join(pm.GetInternalMetadataPath(), "metadata.db")
}

// GetMigrationsPath returns the migrations directory path
func (pm *PathManager) GetMigrationsPath() string {
	if pm.basePath == "" {
		return ""
	}
	return filepath.Join(pm.basePath, ".icebox", "migrations")
}

// GetCatalogURI returns the catalog URI based on catalog type
func (pm *PathManager) GetCatalogURI(catalogType string) string {
	if pm.basePath == "" {
		return ""
	}

	switch catalogType {
	case "json":
		return fmt.Sprintf("%s/catalog/catalog.json", pm.basePath)
	case "sqlite":
		return fmt.Sprintf("%s/catalog/catalog.db", pm.basePath)
	default:
		return ""
	}
}

// GetTablePath returns the full path for a table
func (pm *PathManager) GetTablePath(database, tableName string) string {
	return filepath.Join(pm.GetDataPath(), database, tableName)
}

// GetTableDataPath returns the data directory path for a table (implements shared.PathManager)
func (pm *PathManager) GetTableDataPath(namespace []string, tableName string) string {
	if len(namespace) == 0 {
		return filepath.Join(pm.GetDataPath(), "default", tableName, "data")
	}
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.GetDataPath(), nsPath, tableName, "data")
}

// GetTableMetadataPath returns the metadata directory path for a table (implements shared.PathManager)
func (pm *PathManager) GetTableMetadataPath(namespace []string, tableName string) string {
	if len(namespace) == 0 {
		return filepath.Join(pm.GetDataPath(), "default", tableName, "metadata")
	}
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.GetDataPath(), nsPath, tableName, "metadata")
}

// GetTableMetadataFile returns the path for a specific metadata version file
func (pm *PathManager) GetTableMetadataFile(database, tableName string, version int) string {
	namespace := []string{database}
	return filepath.Join(pm.GetTableMetadataPath(namespace, tableName), fmt.Sprintf("v%d.metadata.json", version))
}

// GetTableDataFile returns the path for a data file
func (pm *PathManager) GetTableDataFile(database, tableName, fileName string) string {
	namespace := []string{database}
	return filepath.Join(pm.GetTableDataPath(namespace, tableName), fileName)
}

// GetNamespacePath returns the path for a namespace
func (pm *PathManager) GetNamespacePath(namespace []string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.GetDataPath(), nsPath)
}

// GetViewMetadataPath returns the metadata path for a view (implements shared.PathManager)
func (pm *PathManager) GetViewMetadataPath(namespace []string, viewName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.GetDataPath(), nsPath, viewName, "metadata")
}

// GetMetadataDir returns the metadata directory (implements shared.PathManager)
func (pm *PathManager) GetMetadataDir() string {
	return pm.GetInternalMetadataPath()
}

// GetDataDir returns the data directory (implements shared.PathManager)
func (pm *PathManager) GetDataDir() string {
	return pm.GetDataPath()
}

// EnsureDirectoryStructure creates all necessary directories
func (pm *PathManager) EnsureDirectoryStructure() error {
	dirs := []string{
		pm.basePath,
		pm.GetCatalogPath(),
		pm.GetDataPath(),
		pm.GetInternalMetadataPath(),
		pm.GetMigrationsPath(),
	}

	for _, dir := range dirs {
		if err := ensureDir(dir); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// ensureDir creates a directory if it doesn't exist
func ensureDir(path string) error {
	// This will be implemented by the filesystem interface
	// For now, we'll return nil and let the filesystem handle it
	return nil
}
