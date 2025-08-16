package catalog

import (
	"context"
	"os"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create test config
func createTestConfig(catalogType, dataPath string) *config.Config {
	return &config.Config{
		Storage: config.StorageConfig{
			DataPath: dataPath,
			Catalog: config.CatalogConfig{
				Type: catalogType,
			},
			Data: config.DataConfig{
				Type: "filesystem",
			},
		},
	}
}

func TestNewCatalogSQLite(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "sqlite-catalog-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := createTestConfig("sqlite", tempDir)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != "icebox-sqlite-catalog" {
		t.Errorf("Expected catalog name 'icebox-sqlite-catalog', got %s", catalog.Name())
	}

	if catalog.CatalogType() != icebergcatalog.SQL {
		t.Errorf("Expected catalog type 'sql', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogREST(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "rest-catalog-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := createTestConfig("rest", tempDir)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create REST catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != "icebox-rest-catalog" {
		t.Errorf("Expected catalog name 'icebox-rest-catalog', got %s", catalog.Name())
	}

	if catalog.CatalogType() != icebergcatalog.REST {
		t.Errorf("Expected catalog type 'rest', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogUnsupportedType(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "unsupported-catalog-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := createTestConfig("unsupported", tempDir)

	_, err = NewCatalog(cfg)
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
	sqliteConfig := createTestConfig("sqlite", "")

	_, err := NewCatalog(sqliteConfig)
	if err == nil {
		t.Error("Expected error for missing SQLite config")
	}

	// Test REST catalog with missing config
	restConfig := createTestConfig("rest", "")

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

	cfg := createTestConfig("json", tempDir)

	catalog, err := NewCatalog(cfg)
	require.NoError(t, err)
	assert.NotNil(t, catalog)
	assert.Equal(t, "icebox-json-catalog", catalog.Name())
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
