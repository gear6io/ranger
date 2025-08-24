package paths

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/gear6io/ranger/pkg/errors"
)

// ComponentType defines the path manager component type identifier
const ComponentType = "paths"

// Manager implements the PathManager interface
type Manager struct {
	basePath string
}

// NewManager creates a new path manager
func NewManager(basePath string) *Manager {
	return &Manager{
		basePath: basePath,
	}
}

// GetBasePath returns the base data path
func (pm *Manager) GetBasePath() string {
	return pm.basePath
}

// GetCatalogPath returns the catalog directory path
func (pm *Manager) GetCatalogPath() string {
	return filepath.Join(pm.basePath, "catalog")
}

// GetDataPath returns the data storage directory path
func (pm *Manager) GetDataPath() string {
	return filepath.Join(pm.basePath, "data")
}

// GetInternalMetadataPath returns the internal metadata directory path
func (pm *Manager) GetInternalMetadataPath() string {
	return filepath.Join(pm.basePath, ".ranger")
}

// GetInternalMetadataDBPath returns the internal metadata database path
func (pm *Manager) GetInternalMetadataDBPath() string {
	return filepath.Join(pm.GetInternalMetadataPath(), "metadata.db")
}

// GetMigrationsPath returns the migrations directory path
func (pm *Manager) GetMigrationsPath() string {
	if pm.basePath == "" {
		return ""
	}
	return filepath.Join(pm.basePath, ".ranger", "migrations")
}

// GetCatalogURI returns the catalog URI based on catalog type
func (pm *Manager) GetCatalogURI(catalogType string) string {
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
func (pm *Manager) GetTablePath(database, tableName string) string {
	return filepath.Join(pm.GetDataPath(), database, tableName)
}

// GetTableDataPath returns the data directory path for a table
func (pm *Manager) GetTableDataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.basePath, "tables", nsPath, tableName, "data")
}

// GetTableMetadataPath returns the metadata directory path for a table
func (pm *Manager) GetTableMetadataPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.basePath, "tables", nsPath, tableName, "metadata")
}

// GetViewMetadataPath returns the metadata directory path for a view
func (pm *Manager) GetViewMetadataPath(namespace []string, viewName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.basePath, "views", nsPath, viewName, "metadata")
}

// GetParquetDataPath returns the data directory path for Parquet files
func (pm *Manager) GetParquetDataPath(database, tableName string) string {
	return filepath.Join(pm.basePath, "tables", database, tableName, "data")
}

// GetParquetFileName generates a Parquet filename with timestamp and counter
func (pm *Manager) GetParquetFileName(timestamp string, fileCount int) string {
	return fmt.Sprintf("data_%s_%04d.parquet", timestamp, fileCount)
}

// GetParquetFilePath returns the full path for a Parquet file
func (pm *Manager) GetParquetFilePath(database, tableName, filename string) string {
	return filepath.Join(pm.GetParquetDataPath(database, tableName), filename)
}

// GetParquetFilePattern returns the glob pattern for finding Parquet files
func (pm *Manager) GetParquetFilePattern(database, tableName string) string {
	return filepath.Join(pm.GetParquetDataPath(database, tableName), "*.parquet")
}

// GetTableMetadataFile returns the metadata file path for a specific version
func (pm *Manager) GetTableMetadataFile(database, tableName string, version int) string {
	return filepath.Join(pm.GetTableMetadataPath([]string{database}, tableName), fmt.Sprintf("v%d.metadata.json", version))
}

// GetTableManifestPath returns the manifest directory path for a table
func (pm *Manager) GetTableManifestPath(namespace []string, tableName string) string {
	nsPath := strings.Join(namespace, "/")
	return filepath.Join(pm.basePath, "tables", nsPath, tableName, "manifests")
}

// GetNamespacePath returns the namespace path
func (pm *Manager) GetNamespacePath(namespace []string) string {
	if len(namespace) == 0 {
		return pm.GetDataPath()
	}
	return filepath.Join(pm.GetDataPath(), filepath.Join(namespace...))
}

// GetMetadataDir returns the metadata directory
func (pm *Manager) GetMetadataDir() string {
	return pm.GetInternalMetadataPath()
}

// GetDataDir returns the data directory
func (pm *Manager) GetDataDir() string {
	return pm.GetDataPath()
}

// EnsureDirectoryStructure creates all necessary directories
func (pm *Manager) EnsureDirectoryStructure() error {
	dirs := []string{
		pm.basePath,
		pm.GetCatalogPath(),
		pm.GetDataPath(),
		pm.GetInternalMetadataPath(),
		pm.GetMigrationsPath(),
	}

	for _, dir := range dirs {
		if err := ensureDir(dir); err != nil {
			return errors.New(ErrDirectoryCreationFailed, "failed to create directory", err).AddContext("directory", dir)
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

// GetType returns the component type identifier
func (pm *Manager) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the path manager
func (pm *Manager) Shutdown(ctx context.Context) error {
	// Path manager doesn't have any resources to clean up
	// Just log the shutdown for consistency
	return nil
}
