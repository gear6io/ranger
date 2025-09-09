package registry

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
)

func TestSystemDatabaseManager(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "system_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	basePath := filepath.Join(tempDir, "data")

	// Create a test store
	store, err := NewStore(dbPath, basePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Get system database manager
	systemMgr := store.GetSystemManager()

	ctx := context.Background()

	t.Run("SystemDatabaseInitialized", func(t *testing.T) {
		// System database is now initialized by the Store during creation
		// Just verify it's available
		if systemMgr == nil {
			t.Errorf("System manager should be available")
		}
	})

	t.Run("CreateTestDatabase", func(t *testing.T) {
		err := store.CreateDatabase(ctx, "testdb")
		if err != nil {
			t.Errorf("Failed to create test database: %v", err)
		}
	})

	t.Run("CreateTestTable", func(t *testing.T) {
		// Create a simple table with columns
		table := &regtypes.Table{
			Name: "users",
		}

		columns := []*regtypes.TableColumn{
			{
				ColumnName:      "id",
				DataType:        "int",
				IsNullable:      false,
				IsPrimary:       true,
				OrdinalPosition: 1,
			},
			{
				ColumnName:      "name",
				DataType:        "string",
				IsNullable:      true,
				IsPrimary:       false,
				OrdinalPosition: 2,
			},
		}

		_, err := store.CreateTableWithColumns(ctx, "testdb", table, columns)
		if err != nil {
			t.Errorf("Failed to create test table: %v", err)
		}
	})

	t.Run("GetSystemDatabases", func(t *testing.T) {
		databases, err := systemMgr.GetSystemDatabases(ctx)
		if err != nil {
			t.Errorf("Failed to get system databases: %v", err)
		}

		if len(databases) < 2 { // system + testdb
			t.Errorf("Expected at least 2 databases, got %d", len(databases))
		}

		// Check if testdb exists
		found := false
		for _, db := range databases {
			if db.Name == "testdb" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find testdb in system databases")
		}
	})

	t.Run("GetSystemTables", func(t *testing.T) {
		tables, err := systemMgr.GetSystemTables(ctx, "testdb")
		if err != nil {
			t.Errorf("Failed to get system tables: %v", err)
		}

		if len(tables) != 1 {
			t.Errorf("Expected 1 table, got %d", len(tables))
		}

		if len(tables) > 0 && tables[0].TableName != "users" {
			t.Errorf("Expected table name 'users', got %s", tables[0].TableName)
		}
	})

	t.Run("GetSystemColumns", func(t *testing.T) {
		columns, err := systemMgr.GetSystemColumns(ctx, "testdb", "users")
		if err != nil {
			t.Errorf("Failed to get system columns: %v", err)
		}

		if len(columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(columns))
		}

		// Check first column (id)
		if len(columns) > 0 {
			if columns[0].ColumnName != "id" {
				t.Errorf("Expected first column 'id', got %s", columns[0].ColumnName)
			}
			if columns[0].DataType != "int" {
				t.Errorf("Expected first column type 'int', got %s", columns[0].DataType)
			}
			if !columns[0].IsPrimary {
				t.Errorf("Expected first column to be primary key")
			}
		}

		// Check second column (name)
		if len(columns) > 1 {
			if columns[1].ColumnName != "name" {
				t.Errorf("Expected second column 'name', got %s", columns[1].ColumnName)
			}
			if columns[1].DataType != "string" {
				t.Errorf("Expected second column type 'string', got %s", columns[1].DataType)
			}
			if columns[1].IsPrimary {
				t.Errorf("Expected second column to not be primary key")
			}
		}
	})

	t.Run("GenerateCreateTableDDL", func(t *testing.T) {
		ddl, err := systemMgr.GenerateCreateTableDDL(ctx, "testdb", "users")
		if err != nil {
			t.Errorf("Failed to generate DDL: %v", err)
		}

		expectedStart := "CREATE TABLE testdb.users ("
		if !contains(ddl, expectedStart) {
			t.Errorf("Expected DDL to start with %s, got: %s", expectedStart, ddl)
		}

		if !contains(ddl, "id int NOT NULL PRIMARY KEY") {
			t.Errorf("Expected DDL to contain id column definition")
		}

		if !contains(ddl, "name string") {
			t.Errorf("Expected DDL to contain name column definition")
		}
	})

	t.Run("QuerySystemDatabase", func(t *testing.T) {
		query := "SELECT * FROM system.columns WHERE table_name = 'users' AND database_name = 'testdb'"
		result, err := systemMgr.Query(ctx, query)
		if err != nil {
			t.Errorf("Failed to query system database: %v", err)
		}

		if result.RowCount != 2 {
			t.Errorf("Expected 2 rows, got %d", result.RowCount)
		}

		if len(result.Columns) == 0 {
			t.Errorf("Expected columns in result")
		}
	})

	t.Run("IsSystemDatabaseQuery", func(t *testing.T) {
		tests := []struct {
			query    string
			expected bool
		}{
			{"SELECT * FROM system.columns", true},
			{"SELECT * FROM system.tables", true},
			{"SELECT * FROM system.databases", true},
			{"SELECT * FROM users", false},
			{"SELECT * FROM testdb.users", false},
			{"SELECT * FROM system.columns WHERE table_name = 'users'", true},
		}

		for _, tt := range tests {
			result := systemMgr.IsSystemDatabaseQuery(tt.query)
			if result != tt.expected {
				t.Errorf("Query '%s': expected %v, got %v", tt.query, tt.expected, result)
			}
		}
	})
}

func TestSystemDatabaseManagerErrors(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "system_db_error_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	basePath := filepath.Join(tempDir, "data")

	// Create a test store
	store, err := NewStore(dbPath, basePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Get system database manager
	systemMgr := store.GetSystemManager()
	ctx := context.Background()

	// System database is now initialized by the Store during creation
	// Just verify it's available
	if systemMgr == nil {
		t.Fatalf("System manager should be available")
	}

	t.Run("GetSystemColumns_NonExistentTable", func(t *testing.T) {
		_, err := systemMgr.GetSystemColumns(ctx, "nonexistent", "nonexistent")
		if err == nil {
			t.Errorf("Expected error for non-existent table")
		}
	})

	t.Run("GenerateCreateTableDDL_NonExistentTable", func(t *testing.T) {
		_, err := systemMgr.GenerateCreateTableDDL(ctx, "nonexistent", "nonexistent")
		if err == nil {
			t.Errorf("Expected error for non-existent table")
		}
	})

	t.Run("QuerySystemDatabase_InvalidQuery", func(t *testing.T) {
		_, err := systemMgr.Query(ctx, "SELECT * FROM users")
		if err == nil {
			t.Errorf("Expected error for non-system database query")
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
