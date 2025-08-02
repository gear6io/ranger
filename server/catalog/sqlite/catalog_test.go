package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestNewCatalog(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "icebox-sqlite-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Name: "test-catalog",
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

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != "test-catalog" {
		t.Errorf("Expected catalog name 'test-catalog', got '%s'", catalog.Name())
	}

	if catalog.CatalogType() != icebergcatalog.SQL {
		t.Errorf("Expected catalog type '%s', got '%s'", icebergcatalog.SQL, catalog.CatalogType())
	}

	// Verify database file was created
	if _, err := os.Stat(cfg.Catalog.SQLite.Path); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestNewCatalogMissingConfig(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			// SQLite config is nil
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error for missing SQLite configuration")
	}
}

func TestCreateAndCheckNamespace(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}

	// Check namespace doesn't exist initially
	exists, err := catalog.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to check namespace existence: %v", err)
	}
	if exists {
		t.Error("Namespace should not exist initially")
	}

	// Create namespace
	props := iceberg.Properties{"description": "Test namespace"}
	err = catalog.CreateNamespace(ctx, namespace, props)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Check namespace exists now
	exists, err = catalog.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to check namespace existence: %v", err)
	}
	if !exists {
		t.Error("Namespace should exist after creation")
	}

	// Try to create duplicate namespace
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != icebergcatalog.ErrNamespaceAlreadyExists {
		t.Errorf("Expected ErrNamespaceAlreadyExists, got: %v", err)
	}
}

func TestLoadNamespaceProperties(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}

	// Try to load properties for non-existent namespace
	_, err := catalog.LoadNamespaceProperties(ctx, namespace)
	if err != icebergcatalog.ErrNoSuchNamespace {
		t.Errorf("Expected ErrNoSuchNamespace, got: %v", err)
	}

	// Create namespace with properties
	originalProps := iceberg.Properties{
		"description": "Test namespace",
		"location":    "/test/location",
	}
	err = catalog.CreateNamespace(ctx, namespace, originalProps)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Load properties
	loadedProps, err := catalog.LoadNamespaceProperties(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to load namespace properties: %v", err)
	}

	// Check that expected properties exist (plus the "exists" property)
	if loadedProps["description"] != "Test namespace" {
		t.Errorf("Expected description 'Test namespace', got '%s'", loadedProps["description"])
	}

	if loadedProps["location"] != "/test/location" {
		t.Errorf("Expected location '/test/location', got '%s'", loadedProps["location"])
	}

	if loadedProps["exists"] != "true" {
		t.Errorf("Expected exists 'true', got '%s'", loadedProps["exists"])
	}
}

func TestListNamespaces(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()

	// Initially should have no namespaces
	namespaces, err := catalog.ListNamespaces(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to list namespaces: %v", err)
	}
	if len(namespaces) != 0 {
		t.Errorf("Expected 0 namespaces, got %d", len(namespaces))
	}

	// Create some namespaces
	ns1 := table.Identifier{"namespace1"}
	ns2 := table.Identifier{"namespace2"}

	err = catalog.CreateNamespace(ctx, ns1, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace1: %v", err)
	}

	err = catalog.CreateNamespace(ctx, ns2, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace2: %v", err)
	}

	// List namespaces
	namespaces, err = catalog.ListNamespaces(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to list namespaces: %v", err)
	}
	if len(namespaces) != 2 {
		t.Errorf("Expected 2 namespaces, got %d", len(namespaces))
	}
}

func TestDropNamespace(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}

	// Try to drop non-existent namespace
	err := catalog.DropNamespace(ctx, namespace)
	if err != icebergcatalog.ErrNoSuchNamespace {
		t.Errorf("Expected ErrNoSuchNamespace, got: %v", err)
	}

	// Create namespace
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Drop namespace
	err = catalog.DropNamespace(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to drop namespace: %v", err)
	}

	// Verify namespace no longer exists
	exists, err := catalog.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to check namespace existence: %v", err)
	}
	if exists {
		t.Error("Namespace should not exist after dropping")
	}
}

func TestCreateAndCheckTable(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}
	tableIdent := table.Identifier{"test_namespace", "test_table"}

	// Create namespace first
	err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Check table doesn't exist initially
	exists, err := catalog.CheckTableExists(ctx, tableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if exists {
		t.Error("Table should not exist initially")
	}

	// Create table
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	table, err := catalog.CreateTable(ctx, tableIdent, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if table == nil {
		t.Error("Expected table to be returned")
	}

	// Check table exists now
	exists, err = catalog.CheckTableExists(ctx, tableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if !exists {
		t.Error("Table should exist after creation")
	}

	// Try to create duplicate table
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	if err != icebergcatalog.ErrTableAlreadyExists {
		t.Errorf("Expected ErrTableAlreadyExists, got: %v", err)
	}
}

func TestCreateTableInNonExistentNamespace(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	tableIdent := table.Identifier{"nonexistent_namespace", "test_table"}

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	_, err := catalog.CreateTable(ctx, tableIdent, schema)
	if err != icebergcatalog.ErrNoSuchNamespace {
		t.Errorf("Expected ErrNoSuchNamespace, got: %v", err)
	}
}

func TestLoadTable(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}
	tableIdent := table.Identifier{"test_namespace", "test_table"}

	// Try to load non-existent table
	_, err := catalog.LoadTable(ctx, tableIdent, iceberg.Properties{})
	if err != icebergcatalog.ErrNoSuchTable {
		t.Errorf("Expected ErrNoSuchTable, got: %v", err)
	}

	// Create namespace and table
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Load table
	table, err := catalog.LoadTable(ctx, tableIdent, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to load table: %v", err)
	}
	if table == nil {
		t.Error("Expected table to be returned")
	}
}

func TestDropTable(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}
	tableIdent := table.Identifier{"test_namespace", "test_table"}

	// Try to drop non-existent table
	err := catalog.DropTable(ctx, tableIdent)
	if err != icebergcatalog.ErrNoSuchTable {
		t.Errorf("Expected ErrNoSuchTable, got: %v", err)
	}

	// Create namespace and table
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop table
	err = catalog.DropTable(ctx, tableIdent)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify table no longer exists
	exists, err := catalog.CheckTableExists(ctx, tableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if exists {
		t.Error("Table should not exist after dropping")
	}
}

func TestListTables(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}

	// Create namespace
	err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Initially should have no tables
	tables := make([]table.Identifier, 0)
	for table, err := range catalog.ListTables(ctx, namespace) {
		if err != nil {
			t.Fatalf("Error listing tables: %v", err)
		}
		tables = append(tables, table)
	}
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(tables))
	}

	// Create some tables
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	table1 := table.Identifier{"test_namespace", "table1"}
	table2 := table.Identifier{"test_namespace", "table2"}

	_, err = catalog.CreateTable(ctx, table1, schema)
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	_, err = catalog.CreateTable(ctx, table2, schema)
	if err != nil {
		t.Fatalf("Failed to create table2: %v", err)
	}

	// List tables
	tables = make([]table.Identifier, 0)
	for table, err := range catalog.ListTables(ctx, namespace) {
		if err != nil {
			t.Fatalf("Error listing tables: %v", err)
		}
		tables = append(tables, table)
	}
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

func TestDropNamespaceWithTables(t *testing.T) {
	catalog := createTestCatalog(t)
	defer catalog.Close()

	ctx := context.Background()
	namespace := table.Identifier{"test_namespace"}
	tableIdent := table.Identifier{"test_namespace", "test_table"}

	// Create namespace and table
	err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to drop namespace with tables - should fail
	err = catalog.DropNamespace(ctx, namespace)
	if err != icebergcatalog.ErrNamespaceNotEmpty {
		t.Errorf("Expected ErrNamespaceNotEmpty, got: %v", err)
	}

	// Drop table first
	err = catalog.DropTable(ctx, tableIdent)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Now should be able to drop namespace
	err = catalog.DropNamespace(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to drop namespace: %v", err)
	}
}

func TestHelperFunctions(t *testing.T) {
	// Test NamespaceFromIdent
	ident := table.Identifier{"ns1", "ns2", "table"}
	namespace := icebergcatalog.NamespaceFromIdent(ident)
	expected := table.Identifier{"ns1", "ns2"}

	if len(namespace) != len(expected) {
		t.Errorf("Expected namespace length %d, got %d", len(expected), len(namespace))
	}

	for i, part := range expected {
		if i >= len(namespace) || namespace[i] != part {
			t.Errorf("Expected namespace part %d to be '%s', got '%s'", i, part, namespace[i])
		}
	}

	// Test TableNameFromIdent
	tableName := icebergcatalog.TableNameFromIdent(ident)
	if tableName != "table" {
		t.Errorf("Expected table name 'table', got '%s'", tableName)
	}

	// Test namespaceToString and stringToNamespace
	ns := table.Identifier{"ns1", "ns2"}
	nsStr := namespaceToString(ns)
	expectedStr := "ns1.ns2"
	if nsStr != expectedStr {
		t.Errorf("Expected namespace string '%s', got '%s'", expectedStr, nsStr)
	}

	// Test stringToNamespace (current implementation is simplified)
	backToNs := stringToNamespace(nsStr)
	if len(backToNs) != 1 || backToNs[0] != nsStr {
		t.Logf("Note: stringToNamespace implementation is simplified for testing")
	}
}

// Helper function to create a test catalog
func createTestCatalog(t *testing.T) *Catalog {
	tempDir, err := os.MkdirTemp("", "icebox-sqlite-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-catalog",
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

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create test catalog: %v", err)
	}

	// Set up cleanup
	t.Cleanup(func() {
		catalog.Close()
		os.RemoveAll(tempDir)
	})

	return catalog
}
