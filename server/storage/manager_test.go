package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/paths"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageManagerCreateTableWithEngines(t *testing.T) {
	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_storage_manager")
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

	// Create storage manager
	storageMgr, err := NewManager(cfg, logger, metadataMgr)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	// Use unique database names to avoid conflicts
	timestamp := time.Now().UnixNano()

	t.Run("CreateDatabase", func(t *testing.T) {
		// Create a test database using metadata manager with unique name
		dbName := fmt.Sprintf("testdb_create_%d", timestamp)
		// Test database creation
		err := storageMgr.CreateDatabase(ctx, dbName)
		assert.NoError(t, err)

		// Test database existence
		exists := storageMgr.DatabaseExists(ctx, dbName)
		assert.True(t, exists)

		// Test duplicate database creation (should fail)
		err = storageMgr.CreateDatabase(ctx, dbName)
		assert.Error(t, err)
	})

	t.Run("CreateTableWithMemoryEngine", func(t *testing.T) {
		// Create database first
		dbName := fmt.Sprintf("testdb_memory_%d", timestamp)
		err := storageMgr.CreateDatabase(ctx, dbName)
		require.NoError(t, err, "Should create database successfully")

		// Create table with MEMORY storage engine
		// Serialize schema (simplified for testing)
		schemaData := []byte(`{"columns":{"id":{"type":"INT","nullable":false},"name":{"type":"VARCHAR","length":255,"nullable":true},"value":{"type":"DECIMAL","nullable":true}}}`)

		err = storageMgr.CreateTable(ctx, dbName, "my_memory_table", schemaData, "MEMORY", nil)
		assert.NoError(t, err, "Should create table with MEMORY engine")

		// Verify table was created
		tables, err := storageMgr.ListTablesForDatabase(ctx, dbName)
		assert.NoError(t, err)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, "my_memory_table")
	})

	t.Run("CreateTableWithFilesystemEngine", func(t *testing.T) {
		// Create database first
		dbName := fmt.Sprintf("testdb_filesystem_%d", timestamp)
		err := storageMgr.CreateDatabase(ctx, dbName)
		require.NoError(t, err, "Should create database successfully")

		// Create table with FILESYSTEM storage engine
		// Serialize schema (simplified for testing)
		schemaData := []byte(`{"columns":{"id":{"type":"INT","nullable":false},"description":{"type":"TEXT","nullable":true},"amount":{"type":"DOUBLE","nullable":true}}}`)

		err = storageMgr.CreateTable(ctx, dbName, "my_filesystem_table", schemaData, "FILESYSTEM", nil)
		assert.NoError(t, err, "Should create table with FILESYSTEM engine")

		// Verify table was created
		tables, err := storageMgr.ListTablesForDatabase(ctx, dbName)
		assert.NoError(t, err)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, "my_filesystem_table")
	})

	t.Run("TestTableDataOperations", func(t *testing.T) {
		// Create database and table for data operations test
		dbName := fmt.Sprintf("testdb_data_%d", timestamp)
		err := storageMgr.CreateDatabase(ctx, dbName)
		require.NoError(t, err, "Should create database successfully")

		// Create table with MEMORY storage engine for data ops test
		schemaData := []byte(`{"columns":{"id":{"type":"INT","nullable":false},"name":{"type":"VARCHAR","length":255,"nullable":true},"value":{"type":"DECIMAL","nullable":true}}}`)
		err = storageMgr.CreateTable(ctx, dbName, "my_memory_table", schemaData, "MEMORY", nil)
		require.NoError(t, err, "Should create table with MEMORY engine")

		// Test inserting data into memory table
		data := [][]interface{}{
			{1, "Test User 1", 100.50},
			{2, "Test User 2", 200.75},
		}

		err = storageMgr.InsertData(ctx, dbName, "my_memory_table", data)
		assert.NoError(t, err, "Should insert data into memory table")

		// Test querying data from memory table
		retrievedData, err := storageMgr.GetTableData(ctx, dbName, "my_memory_table")
		assert.NoError(t, err, "Should get data from memory table")
		assert.NotNil(t, retrievedData, "Data should not be nil")
		assert.GreaterOrEqual(t, len(retrievedData), 2, "Should have at least 2 rows")

		// Verify the data (JSON unmarshaling converts integers to float64)
		if len(retrievedData) >= 2 {
			assert.Equal(t, float64(1), retrievedData[0][0], "First row ID should be 1")
			assert.Equal(t, "Test User 1", retrievedData[0][1], "First row name should match")
			assert.Equal(t, float64(2), retrievedData[1][0], "Second row ID should be 2")
			assert.Equal(t, "Test User 2", retrievedData[1][1], "Second row name should match")
		}
	})

	t.Run("VerifyStorageEngineTypes", func(t *testing.T) {
		// Get storage manager to verify engine types
		engineRegistry := storageMgr.GetEngineRegistry()
		require.NotNil(t, engineRegistry, "Engine registry should not be nil")

		// List available engines
		engines := engineRegistry.ListEngines()
		t.Logf("Available storage engines: %v", engines)

		// Verify we have the expected engines
		assert.Contains(t, engines, "MEMORY", "MEMORY engine should be available")
		assert.Contains(t, engines, "FILESYSTEM", "FILESYSTEM engine should be available")

		// Test getting specific engines
		memoryEngine, err := engineRegistry.GetEngine("MEMORY")
		assert.NoError(t, err, "Should get MEMORY engine")

		// Use type assertion to access concrete method
		if memEngine, ok := memoryEngine.(interface{ GetStorageType() string }); ok {
			assert.Equal(t, "MEMORY", memEngine.GetStorageType(), "Engine type should match")
		}

		filesystemEngine, err := engineRegistry.GetEngine("FILESYSTEM")
		assert.NoError(t, err, "Should get FILESYSTEM engine")

		// Use type assertion to access concrete method
		if fsEngine, ok := filesystemEngine.(interface{ GetStorageType() string }); ok {
			assert.Equal(t, "FILESYSTEM", fsEngine.GetStorageType(), "Engine type should match")
		}
	})
}
