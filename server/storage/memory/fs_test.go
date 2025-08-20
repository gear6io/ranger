package memory

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemoryStorage(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)
	assert.NotNil(t, ms)
	assert.Equal(t, "MEMORY", ms.GetStorageType())
	assert.NotNil(t, ms.tables)
}

func TestMemoryStorage_PrepareTableEnvironment(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test successful table environment creation
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	assert.NoError(t, err)

	// Verify table was created
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData, exists := ms.tables[tableKey]
	assert.True(t, exists)
	assert.Equal(t, "testdb", tableData.Database)
	assert.Equal(t, "testtable", tableData.TableName)

	// Test duplicate table creation
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestMemoryStorage_StoreTableData(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Prepare table environment first
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	// Test storing data
	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	assert.NoError(t, err)

	// Verify data was stored
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData := ms.tables[tableKey]
	assert.NotNil(t, tableData.ParquetManager)
	assert.NotNil(t, tableData.Schema)

	// Test storing data to non-existent table
	err = ms.StoreTableData("testdb", "nonexistent", testData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestMemoryStorage_GetTableData(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting data from non-existent table
	data, err := ms.GetTableData("testdb", "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, []byte("[]"), data)

	// Prepare table and store data
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	require.NoError(t, err)

	// Get data back
	retrievedData, err := ms.GetTableData("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedData)
}

func TestMemoryStorage_RemoveTableEnvironment(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Prepare table and store data
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	require.NoError(t, err)

	// Verify table exists
	tableKey := ms.getTableKey("testdb", "testtable")
	assert.Contains(t, ms.tables, tableKey)

	// Remove table environment
	err = ms.RemoveTableEnvironment("testdb", "testtable")
	assert.NoError(t, err)

	// Verify table was removed
	assert.NotContains(t, ms.tables, tableKey)

	// Test removing non-existent table
	err = ms.RemoveTableEnvironment("testdb", "nonexistent")
	assert.NoError(t, err) // Should not error
}

func TestMemoryStorage_SetTableSchema(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Prepare table environment
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	// Create Iceberg schema
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Set table schema
	err = ms.SetTableSchema("testdb", "testtable", icebergSchema)
	assert.NoError(t, err)

	// Verify schema was set
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData := ms.tables[tableKey]
	assert.Equal(t, icebergSchema, tableData.IcebergSchema)
	assert.NotNil(t, tableData.Schema)
	assert.NotNil(t, tableData.ParquetManager)

	// Test setting schema for non-existent table
	err = ms.SetTableSchema("testdb", "nonexistent", icebergSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestMemoryStorage_GetTableStats(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting stats from non-existent table
	stats, err := ms.GetTableStats("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Prepare table and store data
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	require.NoError(t, err)

	// Get stats
	stats, err = ms.GetTableStats("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Greater(t, stats.RowsWritten, int64(0))
}

func TestMemoryStorage_GetTableRowCount(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting row count from non-existent table
	rowCount, err := ms.GetTableRowCount("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Prepare table and store data
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	require.NoError(t, err)

	// Get row count
	rowCount, err = ms.GetTableRowCount("testdb", "testtable")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowCount)
}

func TestMemoryStorage_GetTableMemoryUsage(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting memory usage from non-existent table
	memoryUsage, err := ms.GetTableMemoryUsage("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Prepare table and store data
	err = ms.PrepareTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	testData := []byte("test data")
	err = ms.StoreTableData("testdb", "testtable", testData)
	require.NoError(t, err)

	// Get memory usage
	memoryUsage, err = ms.GetTableMemoryUsage("testdb", "testtable")
	assert.NoError(t, err)
	assert.Greater(t, memoryUsage, int64(0))
}

func TestMemoryStorage_HelperMethods(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getTableKey
	tableKey := ms.getTableKey("testdb", "testtable")
	assert.Equal(t, "testdb.testtable", tableKey)

	// Test createDefaultSchema
	schema, err := ms.createDefaultSchema([]byte("test"))
	assert.NoError(t, err)
	assert.NotNil(t, schema)
	assert.Equal(t, 1, len(schema.Fields()))
	assert.Equal(t, "data", schema.Field(0).Name)

	// Test convertBytesToInterface
	interfaceData, err := ms.convertBytesToInterface([]byte("test data"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(interfaceData))
	assert.Equal(t, 1, len(interfaceData[0]))
	assert.Equal(t, "test data", interfaceData[0][0])

	// Test convertInterfaceToBytes
	bytesData, err := ms.convertInterfaceToBytes([][]interface{}{
		{"row1"},
		{"row2"},
	})
	assert.NoError(t, err)
	assert.Contains(t, string(bytesData), "row1")
	assert.Contains(t, string(bytesData), "row2")
}

func TestMemoryStorage_Integration(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Complete workflow test
	database := "testdb"
	tableName := "users"

	// 1. Prepare table environment
	err = ms.PrepareTableEnvironment(database, tableName)
	require.NoError(t, err)

	// 2. Set schema - use single column to match our current data conversion
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	err = ms.SetTableSchema(database, tableName, icebergSchema)
	require.NoError(t, err)

	// 3. Store data multiple times
	// Note: Our current implementation has a simple data conversion
	// that creates single-column data, so we'll test with that for now
	testData1 := []byte("user1 data")
	testData2 := []byte("user2 data")

	err = ms.StoreTableData(database, tableName, testData1)
	require.NoError(t, err)

	err = ms.StoreTableData(database, tableName, testData2)
	require.NoError(t, err)

	// 4. Verify data
	rowCount, err := ms.GetTableRowCount(database, tableName)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)

	memoryUsage, err := ms.GetTableMemoryUsage(database, tableName)
	assert.NoError(t, err)
	assert.Greater(t, memoryUsage, int64(0))

	// 5. Clean up
	err = ms.RemoveTableEnvironment(database, tableName)
	assert.NoError(t, err)
}
