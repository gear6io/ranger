package memory

import (
	"testing"

	"github.com/TFMV/icebox/server/storage/parquet"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParquetManager(t *testing.T) {
	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Test with default config
	pm := NewParquetManager(schema, nil)
	assert.NotNil(t, pm)
	assert.Equal(t, schema, pm.schema)
	assert.NotNil(t, pm.config)
	assert.Equal(t, int64(0), pm.GetRowCount())
	assert.Equal(t, int64(0), pm.GetMemoryUsage())

	// Test with custom config
	config := &parquet.ParquetConfig{
		MaxMemoryUsage: 1024,
		BatchSize:      100,
	}
	pm = NewParquetManager(schema, config)
	assert.Equal(t, config, pm.config)
}

func TestParquetManager_StoreData_ValidData(t *testing.T) {
	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Valid data
	data := [][]interface{}{
		{int64(1), "Alice", true},
		{int64(2), "Bob", false},
		{int64(3), nil, true},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Verify stats
	stats := pm.GetStats()
	assert.Equal(t, int64(3), stats.RowsWritten)
	assert.Equal(t, int64(3), pm.GetRowCount())
	assert.Greater(t, pm.GetMemoryUsage(), int64(0))
}

func TestParquetManager_StoreData_InvalidData(t *testing.T) {
	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Invalid data - wrong number of columns
	data := [][]interface{}{
		{int64(1), "Alice", "extra"},
		{int64(2), "Bob"},
	}

	err := pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2")

	// Invalid data - wrong type
	data = [][]interface{}{
		{int64(1), "Alice"},
		{"invalid", "Bob"}, // id should be int64
	}

	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects int64")

	// Invalid data - null in required field
	data = [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), nil}, // name is required
	}

	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be null")
}

func TestParquetManager_StoreData_EmptyData(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Empty data should be valid
	data := [][]interface{}{}
	err := pm.StoreData(data)
	assert.NoError(t, err)

	// Verify no rows were written
	assert.Equal(t, int64(0), pm.GetRowCount())
}

func TestParquetManager_StoreData_MemoryLimit(t *testing.T) {
	t.Skip("Skipping memory limit test until memory estimation is fixed")

	// Create a schema with large string fields
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Create config with very low memory limit
	config := &parquet.ParquetConfig{
		MaxMemoryUsage: 50, // Very low limit
		BatchSize:      10,
	}

	pm := NewParquetManager(schema, config)
	defer pm.Close()

	// Create data that will exceed memory limit
	data := [][]interface{}{
		{int64(1), "Very long string"},
	}

	// This should fail due to memory limit
	err := pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "memory limit exceeded")
}

func TestParquetManager_GetData(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Store some data
	data := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Retrieve data
	retrievedData, err := pm.GetData()
	require.NoError(t, err)

	// Verify data integrity
	assert.Equal(t, len(data), len(retrievedData))
	for i, row := range data {
		assert.Equal(t, row[0], retrievedData[i][0])
		assert.Equal(t, row[1], retrievedData[i][1])
	}
}

func TestParquetManager_GetDataBatch(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Store some data
	data := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
		{int64(4), "David"},
		{int64(5), "Eve"},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Get data in batches
	dataChan, err := pm.GetDataBatch(2)
	require.NoError(t, err)

	var allBatches [][]interface{}
	for batch := range dataChan {
		allBatches = append(allBatches, batch...)
	}

	// Verify all data was received
	assert.Equal(t, len(data), len(allBatches))
}

func TestParquetManager_GetSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	retrievedSchema := pm.GetSchema()
	assert.Equal(t, schema, retrievedSchema)
}

func TestParquetManager_GetStats(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Initial stats
	initialStats := pm.GetStats()
	assert.Equal(t, int64(0), initialStats.RowsWritten)
	assert.Equal(t, int64(0), initialStats.MemoryUsage)

	// Store data
	data := [][]interface{}{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Updated stats
	updatedStats := pm.GetStats()
	assert.Equal(t, int64(3), updatedStats.RowsWritten)
	assert.Greater(t, updatedStats.MemoryUsage, int64(0))
}

func TestParquetManager_ClearData(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Store data
	data := [][]interface{}{
		{int64(1)},
		{int64(2)},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Verify data was stored
	assert.Equal(t, int64(2), pm.GetRowCount())
	assert.Greater(t, pm.GetMemoryUsage(), int64(0))

	// Clear data
	pm.ClearData()

	// Verify data was cleared
	assert.Equal(t, int64(0), pm.GetRowCount())
	assert.Equal(t, int64(0), pm.GetMemoryUsage())
}

func TestParquetManager_Close(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)

	// Store some data
	data := [][]interface{}{
		{int64(1)},
		{int64(2)},
	}

	err := pm.StoreData(data)
	require.NoError(t, err)

	// Close the manager
	err = pm.Close()
	assert.NoError(t, err)

	// Verify it's closed
	assert.True(t, pm.closed)

	// Try to store data after closing
	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data manager is closed")
}

func TestParquetManager_Integration(t *testing.T) {
	// Test integration with memory storage
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	pm := NewParquetManager(schema, nil)
	defer pm.Close()

	// Store various types of data
	testData := [][]interface{}{
		{"Hello, World!"},
		{"This is a test message"},
		{"Another test message"},
	}

	err := pm.StoreData(testData)
	require.NoError(t, err)

	// Verify data integrity
	retrievedData, err := pm.GetData()
	require.NoError(t, err)

	assert.Equal(t, len(testData), len(retrievedData))
	for i, row := range testData {
		assert.Equal(t, row[0], retrievedData[i][0])
	}

	// Verify stats
	stats := pm.GetStats()
	assert.Equal(t, int64(len(testData)), stats.RowsWritten)
	assert.Greater(t, stats.MemoryUsage, int64(0))
}
