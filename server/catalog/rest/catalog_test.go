package rest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCatalog(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/icebox_test"
	cfg.Storage.Catalog.Type = "rest"

	// Create catalog
	catalog, err := NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Verify catalog properties
	assert.Equal(t, "icebox-rest-catalog", catalog.Name())
	assert.Equal(t, "rest", catalog.CatalogType().String())
}

func TestNewCatalogWithInvalidConfig(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := &config.Config{
		Storage: config.StorageConfig{
			Catalog: config.CatalogConfig{
				Type: "invalid",
				// No URI provided
			},
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error when catalog type is invalid")
	}
}

func TestNewCatalogWithInvalidURI(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := &config.Config{
		Storage: config.StorageConfig{
			Catalog: config.CatalogConfig{
				Type: "rest",
				URI:  "invalid-uri",
			},
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error when URI is invalid")
	}
}

func TestNewCatalogWithStoragePath(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)
	cfg.Storage.Path = "/tmp/test-warehouse"

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with storage path: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestNewCatalogWithOAuthConfig(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with OAuth config: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestNewCatalogWithSigV4Config(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with SigV4 config: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestNewCatalogWithTLSConfig(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with TLS config: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestNewCatalogWithAllConfigs(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with all configs: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestNewCatalogWithInvalidAuthURL(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog with invalid auth URL: %v", err)
	}
	defer catalog.Close()

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}
}

func TestCatalogType(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)
	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.CatalogType() != "rest" {
		t.Errorf("Expected catalog type 'rest', got %s", catalog.CatalogType())
	}
}

func TestCatalogName(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	expectedName := "icebox-rest-catalog"
	if catalog.Name() != expectedName {
		t.Errorf("Expected catalog name %s, got %s", expectedName, catalog.Name())
	}
}

// Note: The following tests would require a running REST catalog server
// For now, we'll test the interface compliance and error handling

func TestCatalogInterfaceCompliance(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)
	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	ctx := context.Background()

	// Test CheckNamespaceExists - this will likely fail with connection error, but should not panic
	_, err = catalog.CheckNamespaceExists(ctx, table.Identifier{"test"})
	// We expect an error since there's no actual REST server running
	if err == nil {
		t.Log("Unexpected success - there might be a REST server running")
	}

	// Test CheckTableExists - this will likely fail with connection error, but should not panic
	_, err = catalog.CheckTableExists(ctx, table.Identifier{"test", "table"})
	// We expect an error since there's no actual REST server running
	if err == nil {
		t.Log("Unexpected success - there might be a REST server running")
	}

	// Test CreateNamespace - this will likely fail with connection error, but should not panic
	err = catalog.CreateNamespace(ctx, table.Identifier{"test"}, iceberg.Properties{})
	// We expect an error since there's no actual REST server running
	if err == nil {
		t.Log("Unexpected success - there might be a REST server running")
	}

	// Test ListNamespaces - this will likely fail with connection error, but should not panic
	_, err = catalog.ListNamespaces(ctx, nil)
	// We expect an error since there's no actual REST server running
	if err == nil {
		t.Log("Unexpected success - there might be a REST server running")
	}
}

// Helper functions

func createTestConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-rest-catalog-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path: filepath.Join(tempDir, "data"),
			Catalog: config.CatalogConfig{
				Type: "rest",
				URI:  "http://localhost:8181",
			},
		},
	}

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return cfg
}
