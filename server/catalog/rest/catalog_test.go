package rest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestNewCatalog(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.name != cfg.Name {
		t.Errorf("Expected catalog name %s, got %s", cfg.Name, catalog.name)
	}

	if catalog.restCatalog == nil {
		t.Error("Expected REST catalog to be initialized")
	}

	if catalog.fileIO == nil {
		t.Error("Expected FileIO to be initialized")
	}
}

func TestNewCatalogWithInvalidConfig(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := &config.Config{
		Name: "test",
		Catalog: config.CatalogConfig{
			Type: "rest",
			// No REST config provided
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error when REST config is missing")
	}
}

func TestNewCatalogWithOAuthConfig(t *testing.T) {
	t.Skip("Skipping REST catalog tests - requires running REST catalog server")

	cfg := createTestConfig(t)
	cfg.Catalog.REST.OAuth = &config.OAuthConfig{
		Token:      "test-token",
		Credential: "test-credential",
		AuthURL:    "https://auth.example.com/oauth",
		Scope:      "test-scope",
	}

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
	cfg.Catalog.REST.SigV4 = &config.SigV4Config{
		Enabled: true,
		Region:  "us-east-1",
		Service: "execute-api",
	}

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
	cfg.Catalog.REST.TLS = &config.TLSConfig{
		SkipVerify: true,
	}

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
	cfg.Catalog.REST.OAuth = &config.OAuthConfig{
		Token:      "test-token",
		Credential: "test-credential",
		AuthURL:    "https://auth.example.com/oauth",
		Scope:      "test-scope",
	}
	cfg.Catalog.REST.SigV4 = &config.SigV4Config{
		Enabled: true,
		Region:  "us-east-1",
		Service: "execute-api",
	}
	cfg.Catalog.REST.TLS = &config.TLSConfig{
		SkipVerify: true,
	}
	cfg.Catalog.REST.WarehouseLocation = "s3://test-bucket/warehouse"
	cfg.Catalog.REST.MetadataLocation = "s3://test-bucket/metadata"
	cfg.Catalog.REST.Prefix = "v1/catalog"
	cfg.Catalog.REST.AdditionalProps = map[string]string{
		"custom-prop": "custom-value",
	}
	cfg.Catalog.REST.Credentials = map[string]string{
		"username": "test-user",
		"password": "test-pass",
	}

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
	cfg.Catalog.REST.OAuth = &config.OAuthConfig{
		AuthURL: "invalid-url",
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error when auth URL is invalid")
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
	expectedName := "test-rest-catalog"
	cfg.Name = expectedName

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

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
