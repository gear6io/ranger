package query

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/query/parser"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineWithStorage(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.Config.Type = "memory" // Use memory storage for testing
	cfg.Storage.Catalog.Type = "json"  // Use JSON catalog for testing

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create engine with storage
	engine, err := NewEngine(cfg, logger)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	t.Run("CreateTable", func(t *testing.T) {
		// Create a simple table schema
		schema := &parser.TableSchema{
			ColumnDefinitions: map[string]*parser.ColumnDefinition{
				"id": {
					DataType: "INTEGER",
					Nullable: false,
				},
				"name": {
					DataType: "VARCHAR",
					Length:   100,
					Nullable: true,
				},
				"age": {
					DataType: "INTEGER",
					Nullable: true,
				},
			},
		}

		// Create table
		err := engine.CreateTable(ctx, "test_users", schema)
		assert.NoError(t, err)

		// Verify table was created by checking schema
		retrievedSchema, err := engine.GetTableSchema(ctx, "test_users")
		assert.NoError(t, err)
		assert.NotNil(t, retrievedSchema)
		assert.Len(t, retrievedSchema.ColumnDefinitions, 3)
	})

	t.Run("InsertData", func(t *testing.T) {
		// Insert test data
		data := [][]interface{}{
			{1, "Alice", 25},
			{2, "Bob", 30},
			{3, "Charlie", 35},
		}

		err := engine.InsertData(ctx, "test_users", data)
		assert.NoError(t, err)
	})

	t.Run("GetTableData", func(t *testing.T) {
		// Retrieve data
		data, err := engine.GetTableData(ctx, "test_users", 10)
		assert.NoError(t, err)
		assert.NotNil(t, data)
		assert.Len(t, data, 3) // Should have 3 rows

		// Verify first row
		if len(data) > 0 {
			assert.Equal(t, 1, data[0][0])
			assert.Equal(t, "Alice", data[0][1])
			assert.Equal(t, 25, data[0][2])
		}
	})

	t.Run("StorageManagerAccess", func(t *testing.T) {
		// Test direct access to storage manager
		storageMgr := engine.GetStorageManager()
		assert.NotNil(t, storageMgr)

		// Check storage status
		status := storageMgr.GetStatus()
		assert.Equal(t, "memory", status["type"])
	})
}

func TestEngineClose(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.Config.Type = "memory"
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create engine
	engine, err := NewEngine(cfg, logger)
	require.NoError(t, err)

	// Close engine
	err = engine.Close()
	assert.NoError(t, err)
}
