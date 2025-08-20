package storage

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageManagerCreateTableWithEngines(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/icebox_test_storage_manager"
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create storage manager
	storageMgr, err := NewManager(cfg, logger)
	require.NoError(t, err)
	defer storageMgr.Close()

	// Initialize storage
	ctx := context.Background()
	err = storageMgr.Initialize(ctx)
	require.NoError(t, err)

	t.Run("CreateDatabase", func(t *testing.T) {
		// Create a test database using metadata manager
		err := storageMgr.GetMetadataManager().CreateDatabase(ctx, "testdb")
		assert.NoError(t, err, "Should create database successfully")

		// Verify database was created
		exists := storageMgr.GetMetadataManager().DatabaseExists(ctx, "testdb")
		assert.True(t, exists, "Database should exist after creation")
	})

	t.Run("CreateTableWithMemoryEngine", func(t *testing.T) {
		// Create table with MEMORY storage engine
		// Serialize schema (simplified for testing)
		schemaData := []byte(`{"columns":{"id":{"type":"INT","nullable":false},"name":{"type":"VARCHAR","length":255,"nullable":true},"value":{"type":"DECIMAL","nullable":true}}}`)

		err := storageMgr.CreateTable(ctx, "testdb", "my_memory_table", schemaData, "MEMORY", nil)
		assert.NoError(t, err, "Should create table with MEMORY engine")

		// Verify table was created
		tables, err := storageMgr.GetMetadataManager().ListTables(ctx, "testdb")
		assert.NoError(t, err, "Should list tables successfully")
		assert.Contains(t, tables, "my_memory_table", "Table should exist in metadata")
	})

	t.Run("CreateTableWithFilesystemEngine", func(t *testing.T) {
		// Create table with FILESYSTEM storage engine
		// Serialize schema (simplified for testing)
		schemaData := []byte(`{"columns":{"id":{"type":"INT","nullable":false},"description":{"type":"TEXT","nullable":true},"amount":{"type":"DOUBLE","nullable":true}}}`)

		err := storageMgr.CreateTable(ctx, "testdb", "my_filesystem_table", schemaData, "FILESYSTEM", nil)
		assert.NoError(t, err, "Should create table with FILESYSTEM engine")

		// Verify table was created
		tables, err := storageMgr.GetMetadataManager().ListTables(ctx, "testdb")
		assert.NoError(t, err, "Should list tables successfully")
		assert.Contains(t, tables, "my_filesystem_table", "Table should exist in metadata")
	})

	t.Run("TestTableDataOperations", func(t *testing.T) {
		// Test inserting data into memory table
		data := [][]interface{}{
			{1, "Test User 1", 100.50},
			{2, "Test User 2", 200.75},
		}

		err := storageMgr.InsertData(ctx, "testdb", "my_memory_table", data)
		assert.NoError(t, err, "Should insert data into memory table")

		// Test querying data from memory table
		retrievedData, err := storageMgr.GetTableData(ctx, "testdb", "my_memory_table")
		assert.NoError(t, err, "Should get data from memory table")
		assert.NotNil(t, retrievedData, "Data should not be nil")
		assert.GreaterOrEqual(t, len(retrievedData), 2, "Should have at least 2 rows")

		// Verify the data
		if len(retrievedData) >= 2 {
			assert.Equal(t, 1, retrievedData[0][0], "First row ID should be 1")
			assert.Equal(t, "Test User 1", retrievedData[0][1], "First row name should match")
			assert.Equal(t, 2, retrievedData[1][0], "Second row ID should be 2")
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
