package query

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/storage"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
)

func TestSystemDatabaseBasic(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "system_db_basic_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	basePath := filepath.Join(tempDir, "data")

	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create metadata manager
	dbPath := filepath.Join(tempDir, "test.db")
	metaMgr, err := metadata.NewMetadataManager(nil, dbPath, basePath, logger)
	if err != nil {
		t.Fatalf("Failed to create metadata manager: %v", err)
	}

	// Create storage manager
	cfg := &config.Config{}
	storageMgr, err := storage.NewManager(cfg, logger, metaMgr)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	if err != nil {
		t.Fatalf("Failed to create query engine: %v", err)
	}
	defer engine.Shutdown(context.Background())

	ctx := context.Background()

	// Create test database and table
	err = storageMgr.CreateDatabase(ctx, "testdb")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create a test table with columns
	table := &regtypes.Table{
		Name:        "users",
		DisplayName: "users",
		Description: "Test users table",
		TableType:   "user",
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

	_, err = storageMgr.GetMetadataManager().GetStorage().CreateTableWithColumns(ctx, "testdb", table, columns)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	t.Run("SHOW_DATABASES", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW DATABASES;",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SHOW DATABASES: %v", err)
			return
		}

		if result.RowCount < 2 { // system + testdb
			t.Errorf("Expected at least 2 databases, got %d", result.RowCount)
		}
	})

	t.Run("SHOW_TABLES_FROM", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW TABLES FROM testdb;",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SHOW TABLES FROM testdb: %v", err)
			return
		}

		if result.RowCount != 1 {
			t.Errorf("Expected 1 table, got %d", result.RowCount)
		}
	})

	t.Run("SHOW_COLUMNS_FROM", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW COLUMNS FROM testdb.users;",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SHOW COLUMNS FROM testdb.users: %v", err)
			return
		}

		if result.RowCount != 2 {
			t.Errorf("Expected 2 columns, got %d", result.RowCount)
		}
	})

	t.Run("SHOW_CREATE_TABLE", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW CREATE TABLE testdb.users;",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SHOW CREATE TABLE testdb.users: %v", err)
			return
		}

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}

		// Check that DDL contains expected elements
		if data, ok := result.Data.([][]interface{}); ok && len(data) > 0 && len(data[0]) > 1 {
			ddl := data[0][1].(string)
			if !strings.Contains(ddl, "CREATE TABLE testdb.users") {
				t.Errorf("Expected DDL to contain 'CREATE TABLE testdb.users', got: %s", ddl)
			}
		}
	})

	t.Run("SELECT_FROM_SYSTEM_COLUMNS", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SELECT * FROM system.columns WHERE table_name = 'users' AND database_name = 'testdb';",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SELECT FROM system.columns: %v", err)
			return
		}

		if result.RowCount != 2 {
			t.Errorf("Expected 2 rows, got %d", result.RowCount)
		}
	})

	t.Run("SELECT_FROM_SYSTEM_TABLES", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SELECT * FROM system.tables WHERE database_name = 'testdb';",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SELECT FROM system.tables: %v", err)
			return
		}

		if result.RowCount != 1 {
			t.Errorf("Expected 1 row, got %d", result.RowCount)
		}
	})

	t.Run("SELECT_FROM_SYSTEM_DATABASES", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SELECT * FROM system.databases;",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Failed to execute SELECT FROM system.databases: %v", err)
			return
		}

		if result.RowCount < 2 { // system + testdb
			t.Errorf("Expected at least 2 rows, got %d", result.RowCount)
		}
	})
}

func TestSystemDatabaseErrorHandling(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "system_db_error_basic_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	basePath := filepath.Join(tempDir, "data")

	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create metadata manager
	dbPath := filepath.Join(tempDir, "test.db")
	metaMgr, err := metadata.NewMetadataManager(nil, dbPath, basePath, logger)
	if err != nil {
		t.Fatalf("Failed to create metadata manager: %v", err)
	}

	// Create storage manager
	cfg := &config.Config{}
	storageMgr, err := storage.NewManager(cfg, logger, metaMgr)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	if err != nil {
		t.Fatalf("Failed to create query engine: %v", err)
	}
	defer engine.Shutdown(context.Background())

	ctx := context.Background()

	t.Run("SHOW_COLUMNS_NonExistentTable", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW COLUMNS FROM nonexistent.nonexistent;",
			User:  "test",
		}

		_, err := engine.ExecuteQuery(ctx, queryCtx)
		if err == nil {
			t.Errorf("Expected error for non-existent table")
		}
	})

	t.Run("SHOW_CREATE_TABLE_NonExistentTable", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SHOW CREATE TABLE nonexistent.nonexistent;",
			User:  "test",
		}

		_, err := engine.ExecuteQuery(ctx, queryCtx)
		if err == nil {
			t.Errorf("Expected error for non-existent table")
		}
	})

	t.Run("SELECT_FROM_SYSTEM_NonExistentTable", func(t *testing.T) {
		queryCtx := &types.QueryContext{
			Query: "SELECT * FROM system.columns WHERE table_name = 'nonexistent';",
			User:  "test",
		}

		result, err := engine.ExecuteQuery(ctx, queryCtx)
		if err != nil {
			t.Errorf("Unexpected error for system query: %v", err)
		}

		// Should return 0 rows for non-existent table
		if result.RowCount != 0 {
			t.Errorf("Expected 0 rows for non-existent table, got %d", result.RowCount)
		}
	})
}
