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
		return fmt.Sprintf("file://%s/catalog/catalog.json", pm.basePath)
	case "sqlite":
		return fmt.Sprintf("file://%s/catalog/catalog.db", pm.basePath)
	default:
		return ""
	}
}

// GetTablePath returns the full path for a table
func (pm *PathManager) GetTablePath(database, tableName string) string {
	return filepath.Join(pm.GetDataPath(), database, tableName)
}

// GetTableDataPath returns the data directory path for a table
func (pm *PathManager) GetTableDataPath(database, tableName string) string {
	return filepath.Join(pm.GetTablePath(database, tableName), "data")
}

// GetTableMetadataPath returns the metadata directory path for a table
func (pm *PathManager) GetTableMetadataPath(database, tableName string) string {
	return filepath.Join(pm.GetTablePath(database, tableName), "metadata")
}

// GetTableMetadataFile returns the path for a specific metadata version file
func (pm *PathManager) GetTableMetadataFile(database, tableName string, version int) string {
	return filepath.Join(pm.GetTableMetadataPath(database, tableName), fmt.Sprintf("v%d.metadata.json", version))
}

// GetTableDataFile returns the path for a data file
func (pm *PathManager) GetTableDataFile(database, tableName, fileName string) string {
	return filepath.Join(pm.GetTableDataPath(database, tableName), fileName)
}

// GetNamespacePath returns the path for a namespace
func (pm *PathManager) GetNamespacePath(namespace string) string {
	return filepath.Join(pm.GetDataPath(), namespace)
}

// ParseTableIdentifier parses a table identifier (database.table or just table)
func (pm *PathManager) ParseTableIdentifier(tableIdentifier string) (database, tableName string) {
	parts := strings.Split(tableIdentifier, ".")
	if len(parts) == 1 {
		// Just table name, use default database
		return "default", parts[0]
	} else if len(parts) == 2 {
		// database.table format
		return parts[0], parts[1]
	}
	// Invalid format, return as-is
	return "default", tableIdentifier
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
