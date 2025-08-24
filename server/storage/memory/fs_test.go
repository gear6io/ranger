package memory

import (
	"encoding/json"
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
}

func TestMemoryStorage_SetupTable(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test successful table environment creation
	err = ms.SetupTable("testdb", "testtable")
	assert.NoError(t, err)

	// Verify table was created
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData, exists := ms.tables[tableKey]
	assert.True(t, exists)
	assert.Equal(t, "testdb", tableData.Database)
	assert.Equal(t, "testtable", tableData.TableName)

	// Test duplicate table creation (should not error, just return success)
	err = ms.SetupTable("testdb", "testtable")
	assert.NoError(t, err)
}

func TestMemoryStorage_OpenTableForWrite(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test opening a table for writing
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)
	defer writer.Close()

	// Write some test data
	testData := []byte("test data")
	n, err := writer.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// Verify table was created
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData, exists := ms.tables[tableKey]
	assert.True(t, exists)
	assert.Equal(t, "testdb", tableData.Database)
	assert.Equal(t, "testtable", tableData.TableName)
}

func TestMemoryStorage_OpenTableForRead(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// First create a table and write some data
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)

	// Write proper JSON data that the memory storage can parse
	testData := [][]interface{}{
		{1, "test data", 42.5},
		{2, "more data", 100.0},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	writer.Write(jsonData)
	writer.Close()

	// Now test opening for reading
	reader, err := ms.OpenTableForRead("testdb", "testtable")
	require.NoError(t, err)
	defer reader.Close()

	// Read the data
	buf := make([]byte, len(jsonData))
	n, err := reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(jsonData), n)
	// The data might be formatted differently when read back, so just check we got something
	assert.Greater(t, n, 0)
}

func TestMemoryStorage_RemoveTableEnvironment(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Prepare table and store data
	err = ms.SetupTable("testdb", "testtable")
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

	// Prepare table environment first
	err = ms.SetupTable("testdb", "testtable")
	require.NoError(t, err)

	// Test setting schema
	schema := &iceberg.Schema{}
	err = ms.SetTableSchema("testdb", "testtable", schema)
	assert.NoError(t, err)

	// Verify schema was set
	tableKey := ms.getTableKey("testdb", "testtable")
	tableData := ms.tables[tableKey]
	assert.NotNil(t, tableData.IcebergSchema)
}

func TestMemoryStorage_GetTableStats(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting stats from non-existent table
	stats, err := ms.GetTableStats("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Nil(t, stats)

	// Prepare table and write some data
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)

	// Write proper JSON data that the memory storage can parse
	testData := [][]interface{}{
		{1, "test data", 42.5},
		{2, "more data", 100.0},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	writer.Write(jsonData)
	writer.Close()

	// Get stats
	stats, err = ms.GetTableStats("testdb", "testtable")
	assert.NoError(t, err)
	assert.NotNil(t, stats)
}

func TestMemoryStorage_GetTableRowCount(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting row count from non-existent table
	count, err := ms.GetTableRowCount("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)

	// Prepare table and write some data
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)

	// Write proper JSON data that the memory storage can parse
	testData := [][]interface{}{
		{1, "test data", 42.5},
		{2, "more data", 100.0},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	writer.Write(jsonData)
	writer.Close()

	// Get row count
	count, err = ms.GetTableRowCount("testdb", "testtable")
	assert.NoError(t, err)
	assert.Greater(t, count, int64(0))
}

func TestMemoryStorage_GetTableMemoryUsage(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getting memory usage from non-existent table
	usage, err := ms.GetTableMemoryUsage("testdb", "nonexistent")
	assert.Error(t, err)
	assert.Equal(t, int64(0), usage)

	// Prepare table and write some data
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)

	// Write proper JSON data that the memory storage can parse
	testData := [][]interface{}{
		{1, "test data", 42.5},
		{2, "more data", 100.0},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	writer.Write(jsonData)
	writer.Close()

	// Get memory usage
	usage, err = ms.GetTableMemoryUsage("testdb", "testtable")
	assert.NoError(t, err)
	assert.Greater(t, usage, int64(0))
}

func TestMemoryStorage_HelperMethods(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test getTableKey
	key := ms.getTableKey("testdb", "testtable")
	expectedKey := "testdb.testtable"
	assert.Equal(t, expectedKey, key)

	// Test table key with special characters
	key = ms.getTableKey("test-db", "test_table")
	expectedKey = "test-db.test_table"
	assert.Equal(t, expectedKey, key)
}

func TestMemoryStorage_Integration(t *testing.T) {
	ms, err := NewMemoryStorage()
	require.NoError(t, err)

	// Test complete workflow: setup -> write -> read -> remove
	err = ms.SetupTable("testdb", "testtable")
	require.NoError(t, err)

	// Write data
	writer, err := ms.OpenTableForWrite("testdb", "testtable")
	require.NoError(t, err)

	// Write proper JSON data that the memory storage can parse
	testData := [][]interface{}{
		{1, "integration test data", 42.5},
		{2, "more integration data", 100.0},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	n, err := writer.Write(jsonData)
	require.NoError(t, err)
	assert.Equal(t, len(jsonData), n)
	writer.Close()

	// Read data
	reader, err := ms.OpenTableForRead("testdb", "testtable")
	require.NoError(t, err)
	buf := make([]byte, len(jsonData))
	n, err = reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(jsonData), n)
	// The data might be formatted differently when read back, so just check we got something
	assert.Greater(t, n, 0)
	reader.Close()

	// Get stats
	stats, err := ms.GetTableStats("testdb", "testtable")
	require.NoError(t, err)
	assert.NotNil(t, stats)

	// Remove table
	err = ms.RemoveTableEnvironment("testdb", "testtable")
	require.NoError(t, err)

	// Verify table was removed
	tableKey := ms.getTableKey("testdb", "testtable")
	assert.NotContains(t, ms.tables, tableKey)
}
