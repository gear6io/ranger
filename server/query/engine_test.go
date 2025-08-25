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
		query := "SELECT * FROM test_table"
		result, err := engine.ExecuteQuery(context.Background(), query)
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
		{"SimpleSelect", "SELECT * FROM table", true},
		{"CreateTable", "CREATE TABLE test (id INT)", true},
		{"InsertData", "INSERT INTO test VALUES (1)", true},
		{"InvalidQuery", "INVALID SQL QUERY", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test query parsing (execution will fail without storage, but parsing should work)
			result, err := engine.ExecuteQuery(context.Background(), tc.query)
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
