package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCatalogSQLite(t *testing.T) {
	cfg := createTestSQLiteConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != cfg.Name {
		t.Errorf("Expected catalog name %s, got %s", cfg.Name, catalog.Name())
	}

	if catalog.CatalogType() != "sql" {
		t.Errorf("Expected catalog type 'sql', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogREST(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestRESTConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create REST catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != cfg.Name {
		t.Errorf("Expected catalog name %s, got %s", cfg.Name, catalog.Name())
	}

	if catalog.CatalogType() != "rest" {
		t.Errorf("Expected catalog type 'rest', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogUnsupportedType(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "unsupported",
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error for unsupported catalog type")
	}

	expectedError := "unsupported catalog type: unsupported"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestNewCatalogWithMissingConfig(t *testing.T) {
	// Test SQLite catalog with missing config
	sqliteConfig := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			// No SQLite config provided
		},
	}

	_, err := NewCatalog(sqliteConfig)
	if err == nil {
		t.Error("Expected error for missing SQLite config")
	}

	// Test REST catalog with missing config
	restConfig := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "rest",
			// No REST config provided
		},
	}

	_, err = NewCatalog(restConfig)
	if err == nil {
		t.Error("Expected error for missing REST config")
	}
}

func TestNewCatalogJSON(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "json-catalog-factory-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Name: "test-json-catalog",
		Catalog: config.CatalogConfig{
			Type: "json",
			JSON: &config.JSONConfig{
				URI:       filepath.Join(tempDir, "catalog.json"),
				Warehouse: tempDir,
			},
		},
	}

	catalog, err := NewCatalog(cfg)
	require.NoError(t, err)
	assert.NotNil(t, catalog)
	assert.Equal(t, "test-json-catalog", catalog.Name())
	assert.Equal(t, icebergcatalog.Hive, catalog.CatalogType())

	// Test basic functionality
	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}

	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"description": "Test namespace"})
	require.NoError(t, err)

	exists, err := catalog.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	err = catalog.Close()
	assert.NoError(t, err)
}

// Helper functions

func createTestSQLiteConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-catalog-factory-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-sqlite-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(tempDir, "data"),
			},
		},
	}

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return cfg
}

func createTestRESTConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-catalog-factory-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-rest-catalog",
		Catalog: config.CatalogConfig{
			Type: "rest",
			REST: &config.RESTConfig{
				URI: "http://localhost:8181",
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(tempDir, "data"),
			},
		},
	}

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return cfg
}
