package metadata

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gear6io/ranger/server/catalog/json"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/paths"
	"github.com/rs/zerolog"
)

func TestCreateTableWithSchemaEnhanced(t *testing.T) {
	// Create temporary directory for test
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create JSON catalog for testing

	// Create a minimal config for the catalog
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir

	// Create path manager
	pathManager := paths.NewManager(tempDir)

	catalog, err := json.NewCatalog(cfg, pathManager)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Create metadata manager
	manager, err := NewMetadataManager(catalog, dbPath, tempDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("Failed to create metadata manager: %v", err)
	}
	defer manager.Close()

	// Start the manager
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start metadata manager: %v", err)
	}
	defer manager.Stop(ctx)

	// Create test database first
	if err := manager.CreateDatabase(ctx, "testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Run("Valid Iceberg Types", func(t *testing.T) {
		table := &regtypes.Table{
			Name:        "users",
			DisplayName: "Users Table",
			Description: "Test table with valid Iceberg types",
			TableType:   "user",
			IsTemporary: false,
			IsExternal:  false,
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				IsNullable:      false,
				IsPrimary:       true,
				OrdinalPosition: 1,
			},
			{
				ColumnName:      "name",
				DataType:        "string",
				IsNullable:      false,
				OrdinalPosition: 2,
			},
			{
				ColumnName:      "email",
				DataType:        "string",
				IsNullable:      true,
				OrdinalPosition: 3,
			},
			{
				ColumnName:      "active",
				DataType:        "boolean",
				IsNullable:      false,
				DefaultValue:    "true",
				OrdinalPosition: 4,
			},
			{
				ColumnName:      "score",
				DataType:        "float64",
				IsNullable:      true,
				OrdinalPosition: 5,
			},
		}

		tableID, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}

		if tableID <= 0 {
			t.Fatalf("Expected positive table ID, got %d", tableID)
		}

		// Verify table exists
		if !manager.TableExists(ctx, "testdb", "users") {
			t.Fatal("Table should exist after creation")
		}
	})

	t.Run("Complex Iceberg Types", func(t *testing.T) {
		table := &regtypes.Table{
			Name:        "complex_data",
			DisplayName: "Complex Data Table",
			Description: "Test table with complex Iceberg types",
			TableType:   "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				IsNullable:      false,
				OrdinalPosition: 1,
			},
			{
				ColumnName:      "price",
				DataType:        "decimal(10,2)",
				IsNullable:      false,
				Precision:       10,
				Scale:           2,
				OrdinalPosition: 2,
			},
			{
				ColumnName:      "tags",
				DataType:        "list<string>",
				IsNullable:      true,
				OrdinalPosition: 3,
			},
			{
				ColumnName:      "metadata",
				DataType:        "map<string,string>",
				IsNullable:      true,
				OrdinalPosition: 4,
			},
		}

		tableID, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err != nil {
			t.Fatalf("Expected success with complex types, got error: %v", err)
		}

		if tableID <= 0 {
			t.Fatalf("Expected positive table ID, got %d", tableID)
		}
	})

	t.Run("Invalid Iceberg Type", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "invalid_table",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "VARCHAR", // Invalid - should be string
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for invalid type, got success")
		}

		if !contains(err.Error(), "unsupported SQL type 'VARCHAR'") {
			t.Fatalf("Expected SQL type migration error, got: %v", err)
		}
	})

	t.Run("Duplicate Column Names", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "duplicate_cols",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				OrdinalPosition: 1,
			},
			{
				ColumnName:      "id", // Duplicate
				DataType:        "string",
				OrdinalPosition: 2,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for duplicate column names, got success")
		}

		if !contains(err.Error(), "duplicate column name 'id'") {
			t.Fatalf("Expected duplicate column error, got: %v", err)
		}
	})

	t.Run("Invalid Decimal Precision", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "invalid_decimal",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "price",
				DataType:        "decimal(10,2)",
				Precision:       0, // Invalid - should be positive
				Scale:           2,
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for invalid decimal precision, got success")
		}

		if !contains(err.Error(), "registry precision (0) does not match type precision (10)") {
			t.Fatalf("Expected decimal precision mismatch error, got: %v", err)
		}
	})

	t.Run("Invalid Table Type", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "invalid_type",
			TableType: "invalid_type", // Invalid table type
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for invalid table type, got success")
		}

		if !contains(err.Error(), "invalid table type 'invalid_type'") {
			t.Fatalf("Expected table type error, got: %v", err)
		}
	})

	t.Run("Empty Table Name", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "", // Empty name
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for empty table name, got success")
		}

		if !contains(err.Error(), "table name cannot be empty") {
			t.Fatalf("Expected empty table name error, got: %v", err)
		}
	})

	t.Run("No Columns", func(t *testing.T) {
		table := &regtypes.Table{
			Name:      "no_columns",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{} // Empty columns

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error for no columns, got success")
		}

		if !contains(err.Error(), "table must have at least one column") {
			t.Fatalf("Expected no columns error, got: %v", err)
		}
	})

	t.Run("Duplicate Table Name", func(t *testing.T) {
		// First, create a table
		table1 := &regtypes.Table{
			Name:      "duplicate_test",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table1, columns)
		if err != nil {
			t.Fatalf("Failed to create first table: %v", err)
		}

		// Try to create another table with the same name
		table2 := &regtypes.Table{
			Name:      "duplicate_test", // Same name
			TableType: "user",
		}

		_, err = manager.CreateTableWithSchema(ctx, "testdb", table2, columns)
		if err == nil {
			t.Fatal("Expected error for duplicate table name, got success")
		}

		// Should get a table exists error
		if !contains(err.Error(), "already exists") {
			t.Fatalf("Expected table exists error, got: %v", err)
		}
	})

	t.Run("Manager Not Running", func(t *testing.T) {
		// Stop the manager
		if err := manager.Stop(ctx); err != nil {
			t.Fatalf("Failed to stop manager: %v", err)
		}

		table := &regtypes.Table{
			Name:      "test_stopped",
			TableType: "user",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int64",
				OrdinalPosition: 1,
			},
		}

		_, err := manager.CreateTableWithSchema(ctx, "testdb", table, columns)
		if err == nil {
			t.Fatal("Expected error when manager is not running, got success")
		}

		if !contains(err.Error(), "metadata manager is not running") {
			t.Fatalf("Expected manager not running error, got: %v", err)
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 1; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}())))
}
