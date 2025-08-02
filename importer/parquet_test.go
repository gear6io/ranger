package importer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go/table"
)

func TestNewParquetImporter(t *testing.T) {
	cfg := createTestConfig(t)

	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	if importer.config != cfg {
		t.Error("Expected config to be set")
	}

	if importer.catalog == nil {
		t.Error("Expected catalog to be initialized")
	}
}

func TestInferSchema(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Use the real titanic.parquet file
	titanicPath := filepath.Join("..", "testdata", "titanic.parquet")
	if _, err := os.Stat(titanicPath); os.IsNotExist(err) {
		t.Skip("Titanic test data not available, skipping test")
	}

	schema, stats, err := importer.InferSchema(titanicPath)
	if err != nil {
		t.Fatalf("Failed to infer schema from titanic.parquet: %v", err)
	}

	if schema == nil {
		t.Error("Expected schema to be inferred")
		return
	}

	if stats == nil || stats.RecordCount <= 0 {
		t.Error("Expected positive record count")
		return
	}

	t.Logf("Inferred schema with %d fields and %d rows", len(schema.Fields), stats.RecordCount)
}

func TestInferSchemaUserFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Test with a user-provided file path (using titanic.parquet as example)
	titanicPath := filepath.Join("..", "testdata", "titanic.parquet")
	if _, err := os.Stat(titanicPath); os.IsNotExist(err) {
		t.Skip("Titanic test data not available, skipping test")
	}

	schema, stats, err := importer.InferSchema(titanicPath)
	if err != nil {
		t.Fatalf("Failed to infer schema from user file: %v", err)
	}

	if schema == nil {
		t.Error("Expected schema to be inferred from user file")
		return
	}

	if stats == nil || stats.RecordCount <= 0 {
		t.Error("Expected positive record count from user file")
		return
	}

	// Titanic dataset should have specific columns
	hasPassengerIdField := false
	hasNameField := false
	for _, field := range schema.Fields {
		if field.Name == "PassengerId" {
			hasPassengerIdField = true
		}
		if field.Name == "Name" {
			hasNameField = true
		}
	}

	if !hasPassengerIdField {
		t.Error("Expected titanic dataset to have PassengerId field")
	}
	if !hasNameField {
		t.Error("Expected titanic dataset to have Name field")
	}
}

func TestInferSchemaGenericFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Test with different parquet files in testdata
	testFiles := []string{
		filepath.Join("..", "testdata", "decimals.parquet"),
		filepath.Join("..", "testdata", "date.parquet"),
	}

	for _, testFile := range testFiles {
		if _, err := os.Stat(testFile); os.IsNotExist(err) {
			t.Logf("Test file %s not available, skipping", testFile)
			continue
		}

		schema, stats, err := importer.InferSchema(testFile)
		if err != nil {
			t.Errorf("Failed to infer schema from %s: %v", testFile, err)
			continue
		}

		if schema == nil {
			t.Errorf("Expected schema to be inferred from %s", testFile)
			continue
		}

		if stats == nil || stats.RecordCount < 0 {
			t.Errorf("Expected non-negative record count from %s", testFile)
		}

		t.Logf("File %s: %d fields, %d rows", filepath.Base(testFile), len(schema.Fields), stats.RecordCount)
	}
}

func TestGetTableLocation(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	location := importer.GetTableLocation(tableIdent)

	expectedPrefix := "file://"
	if !filepath.IsAbs(location) && !strings.HasPrefix(location, expectedPrefix) {
		t.Errorf("Expected location to start with %s or be absolute, got: %s", expectedPrefix, location)
	}

	// Should contain namespace and table name
	if !strings.Contains(location, "test_namespace") {
		t.Errorf("Expected location to contain namespace, got: %s", location)
	}

	if !strings.Contains(location, "test_table") {
		t.Errorf("Expected location to contain table name, got: %s", location)
	}
}

func TestImportTable(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Use the real titanic.parquet file
	titanicPath := filepath.Join("..", "testdata", "titanic.parquet")
	if _, err := os.Stat(titanicPath); os.IsNotExist(err) {
		t.Skip("Titanic test data not available, skipping test")
	}

	ctx := context.Background()
	req := ImportRequest{
		ParquetFile:    titanicPath,
		TableIdent:     table.Identifier{"test", "titanic"},
		NamespaceIdent: table.Identifier{"test"},
		Overwrite:      false,
	}

	result, err := importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import titanic table: %v", err)
	}

	if result == nil {
		t.Fatal("Expected import result")
	}

	// Verify the table was created
	exists, err := importer.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if !exists {
		t.Error("Expected table to exist after import")
	}

	t.Logf("Imported table with %d records", result.RecordCount)
}

func TestImportTableWithExistingNamespace(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	ctx := context.Background()

	// Create namespace first
	namespace := table.Identifier{"existing_ns"}
	err = importer.catalog.CreateNamespace(ctx, namespace, nil)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Use the real titanic.parquet file
	titanicPath := filepath.Join("..", "testdata", "titanic.parquet")
	if _, err := os.Stat(titanicPath); os.IsNotExist(err) {
		t.Skip("Titanic test data not available, skipping test")
	}

	req := ImportRequest{
		ParquetFile:    titanicPath,
		TableIdent:     table.Identifier{"existing_ns", "titanic"},
		NamespaceIdent: table.Identifier{"existing_ns"},
		Overwrite:      false,
	}

	result, err := importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table with existing namespace: %v", err)
	}

	if result == nil {
		t.Fatal("Expected import result")
	}

	// Verify the table was created
	exists, err := importer.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if !exists {
		t.Error("Expected table to exist after import with existing namespace")
	}
}

func TestImportTableOverwrite(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	ctx := context.Background()

	// Use the real titanic.parquet file
	titanicPath := filepath.Join("..", "testdata", "titanic.parquet")
	if _, err := os.Stat(titanicPath); os.IsNotExist(err) {
		t.Skip("Titanic test data not available, skipping test")
	}

	req := ImportRequest{
		ParquetFile:    titanicPath,
		TableIdent:     table.Identifier{"test", "titanic_overwrite"},
		NamespaceIdent: table.Identifier{"test"},
		Overwrite:      false,
	}

	// Import once
	_, err = importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table first time: %v", err)
	}

	// Import again with overwrite
	req.Overwrite = true
	result, err := importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table with overwrite: %v", err)
	}

	if result == nil {
		t.Fatal("Expected import result")
	}

	// Verify the table still exists
	exists, err := importer.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if !exists {
		t.Error("Expected table to exist after overwrite")
	}
}

func TestInferSchemaNonExistentFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	_, _, err = importer.InferSchema("/non/existent/file.parquet")
	if err == nil {
		t.Error("Expected error when inferring schema from non-existent file")
	}
}

// Helper functions

func createTestConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-importer-test")
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

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return cfg
}
