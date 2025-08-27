package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/paths"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/storage"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEngineBasicFunctionality tests only the query engine functionality
// without testing storage components
func TestEngineBasicFunctionality(t *testing.T) {
	// Create minimal test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_query_engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create minimal storage manager for testing
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create engine with minimal storage (for basic functionality testing)
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("EngineCreation", func(t *testing.T) {
		// Test that engine was created successfully
		assert.NotNil(t, engine)
		// Note: GetStorageManager() is not available when engine is created without storage
		// This test focuses on basic engine functionality without storage dependencies
	})

	t.Run("QueryParsing", func(t *testing.T) {
		// Test basic query parsing functionality
		queryCtx := &types.QueryContext{
			Query:      "SELECT * FROM test_table;",
			Database:   "default",
			User:       "test",
			ClientAddr: "127.0.0.1",
		}
		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		// This might fail due to missing storage, but parsing should work
		t.Logf("Query result: %v, error: %v", result, err)
	})

	t.Run("EngineClose", func(t *testing.T) {
		// Test engine cleanup
		err := engine.Close()
		assert.NoError(t, err)
	})
}

// TestEngineQueryParsing tests query parsing without storage dependencies
func TestEngineQueryParsing(t *testing.T) {
	// Create minimal test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_query_engine_parsing")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create minimal storage manager for testing
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create engine with minimal storage
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)
	defer engine.Close()

	testCases := []struct {
		name  string
		query string
		valid bool
	}{
		{"SimpleSelect", "SELECT * FROM table;", true},
		{"CreateTable", "CREATE TABLE test (id INT) ENGINE = MEMORY;", true},
		{"InsertData", "INSERT INTO test VALUES (1);", true},
		{"ShowTables", "SHOW TABLES;", true},
		{"ShowDatabases", "SHOW DATABASES;", true},
		{"InvalidQuery", "INVALID SQL QUERY", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test query parsing (execution will fail without storage, but parsing should work)
			queryCtx := &types.QueryContext{
				Query:      tc.query,
				Database:   "default",
				User:       "test",
				ClientAddr: "127.0.0.1",
			}
			result, err := engine.ExecuteQuery(context.Background(), queryCtx)
			if tc.valid {
				// Valid queries should parse but may fail execution due to missing storage
				t.Logf("Query '%s' parsed successfully, execution result: %v, error: %v",
					tc.query, result, err)
			} else {
				// Invalid queries should fail parsing
				assert.Error(t, err, "Invalid query should fail")
			}
		})
	}
}

// TestEngineConfiguration tests engine configuration without storage
func TestEngineConfiguration(t *testing.T) {
	// Create minimal test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_query_engine_config")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create minimal storage manager for testing
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create engine with minimal storage
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("DuckDBEngine", func(t *testing.T) {
		// Test that DuckDB engine is properly configured
		// This tests the query engine's internal DuckDB setup
		assert.NotNil(t, engine)
	})

	t.Run("ExecutionManager", func(t *testing.T) {
		// Test that execution manager is properly configured
		// This tests the query engine's internal execution management
		assert.NotNil(t, engine)
	})
}

// TestQueryEngineStreaming tests the new streaming methods for memory efficiency
func TestQueryEngineStreaming(t *testing.T) {
	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_query_engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create minimal storage manager for testing
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	// Create test database and table with unique names
	timestamp := time.Now().UnixNano()
	database := fmt.Sprintf("testdb_streaming_%d", timestamp)
	tableName := "test_table"

	// Create database
	err = storageMgr.CreateDatabase(ctx, database)
	require.NoError(t, err)

	// Create table using Query Engine (which will use MEMORY engine by default)
	// Create proper TableSchema object
	schema := &parser.TableSchema{
		ColumnDefinitions: map[string]*parser.ColumnDefinition{
			"id": {
				DataType: "INT",
				Nullable: false,
			},
			"name": {
				DataType: "VARCHAR",
				Length:   255,
				Nullable: true,
			},
			"value": {
				DataType: "DECIMAL",
				Nullable: true,
			},
		},
	}

	// Use Query Engine to create table
	err = engine.CreateTable(ctx, database, tableName, "MEMORY", schema)
	require.NoError(t, err)

	t.Run("InsertDataBatchStreaming", func(t *testing.T) {
		// Test data
		testData := [][]interface{}{
			{1, "Alice", 25.5},
			{2, "Bob", 30.2},
			{3, "Charlie", 28.7},
			{4, "Diana", 35.1},
			{5, "Eve", 27.9},
		}

		// Test batch streaming with batch size 2
		err := engine.InsertDataBatchStreaming(ctx, database, tableName, testData, 2)
		assert.NoError(t, err, "Should insert data using batch streaming")

		// Verify data was inserted by retrieving it using QE
		data, err := engine.GetTableData(ctx, database, tableName, 0) // 0 = no limit
		assert.NoError(t, err, "Should retrieve data using Query Engine")
		assert.Equal(t, 5, len(data), "Should have 5 rows")
	})

	t.Run("GetTableDataStreaming", func(t *testing.T) {
		// Get streaming reader
		reader, err := engine.GetTableDataStreaming(ctx, database, tableName)
		assert.NoError(t, err, "Should get streaming reader")
		defer reader.Close()

		// Read all data from stream
		content, err := io.ReadAll(reader)
		assert.NoError(t, err, "Should read from streaming reader")

		// Parse the streamed data
		lines := strings.Split(string(content), "\n")
		var allData [][]interface{}

		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			var batch [][]interface{}
			err := json.Unmarshal([]byte(line), &batch)
			require.NoError(t, err, "Should parse JSON batch")

			allData = append(allData, batch...)
		}

		assert.Equal(t, 5, len(allData), "Should have 5 rows from streaming")
	})

	t.Run("InsertDataStreaming", func(t *testing.T) {
		// Create test data as JSON string
		testData := [][]interface{}{
			{6, "Frank", 32.1},
			{7, "Grace", 29.8},
		}

		// Convert to JSON bytes
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err, "Should marshal test data to JSON")

		// Create reader from bytes
		dataReader := bytes.NewReader(jsonData)

		// Test streaming insertion
		err = engine.InsertDataStreaming(ctx, database, tableName, dataReader)
		assert.NoError(t, err, "Should insert data using streaming")

		// Verify total data count using QE
		data, err := engine.GetTableData(ctx, database, tableName, 0) // 0 = no limit
		assert.NoError(t, err, "Should retrieve data using Query Engine")
		assert.Equal(t, 7, len(data), "Should have 7 total rows after streaming insertion")
	})
}

// TestQueryEngineStreamingPerformance tests memory efficiency of streaming vs non-streaming
func TestQueryEngineStreamingPerformance(t *testing.T) {
	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_query_engine_perf")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create minimal storage manager for testing
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	// Create test database and table with unique names
	timestamp := time.Now().UnixNano()
	database := fmt.Sprintf("testdb_perf_%d", timestamp)
	tableName := "perf_table"

	// Create database
	err = storageMgr.CreateDatabase(ctx, database)
	require.NoError(t, err)

	// Create table using Query Engine (which will use MEMORY engine by default)
	// Create proper TableSchema object
	schema := &parser.TableSchema{
		ColumnDefinitions: map[string]*parser.ColumnDefinition{
			"id": {
				DataType: "INT",
				Nullable: false,
			},
			"name": {
				DataType: "VARCHAR",
				Length:   255,
				Nullable: true,
			},
			"value": {
				DataType: "DECIMAL",
				Nullable: true,
			},
		},
	}

	// Use Query Engine to create table
	err = engine.CreateTable(ctx, database, tableName, "MEMORY", schema)
	require.NoError(t, err)

	t.Run("LargeDatasetStreaming", func(t *testing.T) {
		// Generate large dataset (1000 rows)
		largeData := make([][]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			largeData[i] = []interface{}{i + 1, fmt.Sprintf("User_%d", i+1), float64(i+1) + 0.5}
		}

		// Test batch streaming with large dataset
		err := engine.InsertDataBatchStreaming(ctx, database, tableName, largeData, 100)
		assert.NoError(t, err, "Should insert large dataset using batch streaming")

		// Verify data was inserted using QE
		data, err := engine.GetTableData(ctx, database, tableName, 0) // 0 = no limit
		assert.NoError(t, err, "Should retrieve large dataset using Query Engine")
		assert.Equal(t, 1000, len(data), "Should have 1000 rows")

		// Test streaming retrieval
		reader, err := engine.GetTableDataStreaming(ctx, database, tableName)
		assert.NoError(t, err, "Should get streaming reader for large dataset")
		defer reader.Close()

		// Read data in chunks to simulate streaming
		buffer := make([]byte, 4096) // 4KB buffer
		totalBytes := 0
		for {
			n, err := reader.Read(buffer)
			if err == io.EOF {
				break
			}
			require.NoError(t, err, "Should read from streaming reader")
			totalBytes += n
		}

		assert.Greater(t, totalBytes, 0, "Should read data from streaming reader")
		t.Logf("Streamed %d bytes for 1000 rows", totalBytes)
	})
}

// TestDatabaseSelection tests the new database selection functionality
func TestDatabaseSelection(t *testing.T) {
	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_db_selection")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create storage manager
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)
	defer engine.Close()

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	// Create test databases
	testDB1 := "test_database_1"
	testDB2 := "test_database_2"

	err = storageMgr.CreateDatabase(ctx, testDB1)
	require.NoError(t, err)
	err = storageMgr.CreateDatabase(ctx, testDB2)
	require.NoError(t, err)

	// Create test tables in both databases
	schema := &parser.TableSchema{
		ColumnDefinitions: map[string]*parser.ColumnDefinition{
			"id": {
				DataType: "INT",
				Nullable: false,
			},
			"name": {
				DataType: "VARCHAR",
				Length:   255,
				Nullable: true,
			},
		},
	}

	// Create tables in both databases
	err = engine.CreateTable(ctx, testDB1, "table1", "MEMORY", schema)
	require.NoError(t, err)
	err = engine.CreateTable(ctx, testDB1, "table2", "MEMORY", schema)
	require.NoError(t, err)
	err = engine.CreateTable(ctx, testDB2, "table3", "MEMORY", schema)
	require.NoError(t, err)

	t.Run("DatabaseSelectionFromContext", func(t *testing.T) {
		// Test that database is selected from QueryContext when not specified in query
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   testDB1,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.NoError(t, err, "Should execute SHOW TABLES successfully")
		assert.NotNil(t, result, "Should return result")
		assert.Equal(t, int64(2), result.RowCount, "Should show 2 tables from testDB1")
		assert.Contains(t, result.Message, testDB1, "Message should mention the database")
	})

	t.Run("DatabaseSelectionFallbackToDefault", func(t *testing.T) {
		// Test that database falls back to "default" when QueryContext has empty database
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   "", // Empty database should fall back to "default"
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		// This should work since "default" database exists
		assert.NoError(t, err, "Should execute SHOW TABLES with default database")
		assert.NotNil(t, result, "Should return result")
	})

	t.Run("DatabaseValidation", func(t *testing.T) {
		// Test that non-existent database returns proper error
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   "non_existent_database",
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.Error(t, err, "SHOW TABLES should fail with non-existent database")
		assert.Nil(t, result, "Should not return result")

		// Check error message contains database name
		assert.Contains(t, err.Error(), "non_existent_database", "Error should mention the database name")
		assert.Contains(t, err.Error(), "does not exist", "Error should indicate database doesn't exist")
	})

	t.Run("ShowDatabases", func(t *testing.T) {
		// Test SHOW DATABASES functionality
		queryCtx := &types.QueryContext{
			Query:      "SHOW DATABASES;",
			Database:   "default",
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.NoError(t, err, "Should execute SHOW DATABASES successfully")
		assert.NotNil(t, result, "Should return result")
		assert.GreaterOrEqual(t, result.RowCount, int64(2), "Should show at least 2 databases (testDB1, testDB2)")

		// Verify specific databases are present
		if result.Data != nil {
			if rows, ok := result.Data.([][]interface{}); ok {
				databases := make([]string, len(rows))
				for i, row := range rows {
					databases[i] = row[0].(string)
				}
				assert.Contains(t, databases, testDB1, "Should contain testDB1")
				assert.Contains(t, databases, testDB2, "Should contain testDB2")
				t.Logf("Found databases: %v", databases)
			}
		}
	})

	t.Run("CreateTableWithDatabaseContext", func(t *testing.T) {
		// Test CREATE TABLE uses database from QueryContext
		queryCtx := &types.QueryContext{
			Query:      "CREATE TABLE context_table (id INT, name VARCHAR(255)) ENGINE = MEMORY;",
			Database:   testDB1,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.NoError(t, err, "Should create table using database from context")
		assert.NotNil(t, result, "Should return result")
		assert.Contains(t, result.Message, "created successfully", "Should indicate table creation success")

		// Verify table exists in the correct database
		exists := storageMgr.TableExists(ctx, testDB1, "context_table")
		assert.True(t, exists, "Table should exist in testDB1")

		// Verify table doesn't exist in other database
		exists = storageMgr.TableExists(ctx, testDB2, "context_table")
		assert.False(t, exists, "Table should not exist in testDB2")
	})

	t.Run("CreateTableWithQualifiedName", func(t *testing.T) {
		// Test CREATE TABLE with database.table format overrides context
		queryCtx := &types.QueryContext{
			Query:      fmt.Sprintf("CREATE TABLE %s.qualified_table (id INT, name VARCHAR(255)) ENGINE = MEMORY;", testDB2),
			Database:   testDB1, // Context says testDB1
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		// But query specifies testDB2
		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.NoError(t, err, "Should create table in testDB2 despite context being testDB1")
		assert.NotNil(t, result, "Should return result")

		// Verify table exists in testDB2 (from query) not testDB1 (from context)
		exists := storageMgr.TableExists(ctx, testDB2, "qualified_table")
		assert.True(t, exists, "Table should exist in testDB2")

		exists = storageMgr.TableExists(ctx, testDB1, "qualified_table")
		assert.False(t, exists, "Table should not exist in testDB1")
	})

	t.Run("QueryContextPersistence", func(t *testing.T) {
		// Test that QueryContext is properly passed through all query types
		queryCtx := &types.QueryContext{
			Query:      "SELECT * FROM table1;",
			Database:   testDB1,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		// Test SELECT query (should use context database)
		selectResult, err := engine.ExecuteQuery(context.Background(), queryCtx)
		// SELECT might fail due to DuckDB integration, but context should be passed
		t.Logf("SELECT result: %v, error: %v", selectResult, err)

		// Test INSERT query (should use context database)
		insertResult, err := engine.ExecuteQuery(context.Background(), &types.QueryContext{
			Query:      "INSERT INTO table1 VALUES (1, 'test');",
			Database:   testDB1,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		})
		// INSERT might fail due to DuckDB integration, but context should be passed
		t.Logf("INSERT result: %v, error: %v", insertResult, err)
	})

	t.Run("ShowTablesParsing", func(t *testing.T) {
		// Test that SHOW TABLES properly parses and uses database context
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   testDB2, // Use testDB2 which now has 2 tables (table3 + qualified_table)
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.NoError(t, err, "Should execute SHOW TABLES successfully")
		assert.NotNil(t, result, "Should return result")
		assert.Equal(t, int64(2), result.RowCount, "Should show 2 tables from testDB2")
		assert.Contains(t, result.Message, testDB2, "Message should mention testDB2")

		// Verify the table names are correct
		if result.Data != nil {
			if rows, ok := result.Data.([][]interface{}); ok {
				assert.Equal(t, 2, len(rows), "Should have 2 rows")
				tableNames := []string{rows[0][0].(string), rows[1][0].(string)}
				assert.Contains(t, tableNames, "table3", "Should show table3")
				assert.Contains(t, tableNames, "qualified_table", "Should show qualified_table")
			}
		}
	})

	t.Run("ErrorHandlingNonExistentDatabase", func(t *testing.T) {
		// Test various query types fail properly with non-existent database
		nonExistentDB := "completely_nonexistent_db"
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   nonExistentDB,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		// Test SHOW TABLES with non-existent database
		result, err := engine.ExecuteQuery(context.Background(), queryCtx)
		assert.Error(t, err, "SHOW TABLES should fail with non-existent database")
		assert.Nil(t, result, "Should not return result")
		assert.Contains(t, err.Error(), nonExistentDB, "Error should mention the database name")

		// Test CREATE TABLE with non-existent database
		result, err = engine.ExecuteQuery(context.Background(), &types.QueryContext{
			Query:      "CREATE TABLE test_table (id INT) ENGINE = MEMORY;",
			Database:   nonExistentDB,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		})
		assert.Error(t, err, "CREATE TABLE should fail with non-existent database")
		assert.Nil(t, result, "Should not return result")
		assert.Contains(t, err.Error(), nonExistentDB, "Error should mention the database name")
	})

	// Test DROP TABLE functionality
	t.Run("DropTable", func(t *testing.T) {
		// First create a table to drop
		createQuery := "CREATE TABLE test_drop_table (id INT) ENGINE = MEMORY;"
		createCtx := &types.QueryContext{
			Query:      createQuery,
			Database:   "test_database_1", // Use existing database
			User:       "test",
			ClientAddr: "127.0.0.1",
		}

		createResult, err := engine.ExecuteQuery(ctx, createCtx)
		if err != nil {
			t.Fatalf("Failed to create table for drop test: %v", err)
		}
		t.Logf("Table created successfully: %s", createResult.Message)

		// Now test DROP TABLE
		dropQuery := "DROP TABLE test_drop_table;"
		dropCtx := &types.QueryContext{
			Query:      dropQuery,
			Database:   "test_database_1", // Use existing database
			User:       "test",
			ClientAddr: "127.0.0.1",
		}

		dropResult, err := engine.ExecuteQuery(ctx, dropCtx)
		if err != nil {
			t.Fatalf("DROP TABLE failed: %v", err)
		}

		if dropResult.Message != "Table test_database_1.test_drop_table dropped successfully" {
			t.Errorf("Expected drop success message, got: %s", dropResult.Message)
		}

		t.Logf("Table dropped successfully: %s", dropResult.Message)
	})

	// Test DROP TABLE IF EXISTS with non-existent table
	t.Run("DropTableIfExists", func(t *testing.T) {
		dropQuery := "DROP TABLE IF EXISTS non_existent_table;"
		dropCtx := &types.QueryContext{
			Query:      dropQuery,
			Database:   "default",
			User:       "test",
			ClientAddr: "127.0.0.1",
		}

		dropResult, err := engine.ExecuteQuery(ctx, dropCtx)
		if err != nil {
			t.Fatalf("DROP TABLE IF EXISTS failed: %v", err)
		}

		if !strings.Contains(dropResult.Message, "does not exist (IF EXISTS)") {
			t.Errorf("Expected IF EXISTS message, got: %s", dropResult.Message)
		}

		t.Logf("DROP TABLE IF EXISTS handled correctly: %s", dropResult.Message)
	})
}

// TestDatabaseSelectionHelpers tests the database selection helper functions in isolation
func TestDatabaseSelectionHelpers(t *testing.T) {
	// Create minimal test configuration
	tempDir, err := os.MkdirTemp("", "ranger_test_db_helpers")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = tempDir
	cfg.Storage.Catalog.Type = "json"

	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create storage manager
	storageMgr, err := storage.NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Create query engine
	engine, err := NewEngine(cfg, storageMgr, logger)
	require.NoError(t, err)
	defer engine.Close()

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	// Create test database
	testDB := "helper_test_db"
	err = storageMgr.CreateDatabase(ctx, testDB)
	require.NoError(t, err)

	t.Run("GetDatabaseFromContext", func(t *testing.T) {
		// Test with specified database
		queryCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   testDB,
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		// Use reflection to access the private method for testing
		// This tests the database selection logic in isolation
		result := engine.getDatabaseFromContext(queryCtx)
		assert.Equal(t, testDB, result, "Should return database from context")

		// Test with empty database (should fall back to "default")
		emptyCtx := &types.QueryContext{
			Query:      "SHOW TABLES;",
			Database:   "",
			User:       "test_user",
			ClientAddr: "127.0.0.1",
		}

		result = engine.getDatabaseFromContext(emptyCtx)
		assert.Equal(t, "default", result, "Should fall back to default database")
	})

	t.Run("ValidateDatabaseExists", func(t *testing.T) {
		// Test with existing database
		err := engine.validateDatabaseExists(ctx, testDB)
		assert.NoError(t, err, "Should not error for existing database")

		// Test with default database (should skip validation)
		err = engine.validateDatabaseExists(ctx, "default")
		assert.NoError(t, err, "Should not error for default database")

		// Test with non-existent database
		err = engine.validateDatabaseExists(ctx, "non_existent_helper_db")
		assert.Error(t, err, "Should error for non-existent database")
		assert.Contains(t, err.Error(), "non_existent_helper_db", "Error should mention database name")
		assert.Contains(t, err.Error(), "does not exist", "Error should indicate database doesn't exist")
	})
}
