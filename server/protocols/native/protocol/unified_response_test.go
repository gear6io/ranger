package protocol

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedResponse_BasicUsage(t *testing.T) {
	// Build a response with sample data
	builder := NewResponseBuilder()

	// Set column metadata
	builder.SetColumns(
		[]string{"id", "name", "age", "salary"},
		[]string{"int64", "string", "int32", "float64"},
	)

	// Add sample rows
	builder.AddRow([]interface{}{int64(1), "Alice", int32(30), 75000.50})
	builder.AddRow([]interface{}{int64(2), "Bob", int32(25), 65000.00})
	builder.AddRow([]interface{}{int64(3), "Charlie", int32(35), 85000.75})

	// Build the final response
	response := builder.Build("query_123", "OK", nil)

	// Verify metadata
	assert.Equal(t, int64(3), response.RowCount)
	assert.Equal(t, int32(4), response.ColumnCount)
	assert.Equal(t, "query_123", response.QueryID)
	assert.Equal(t, "OK", response.Message)
	assert.Nil(t, response.Error)

	// Verify columns
	names, types, err := response.GetColumns()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "age", "salary"}, names)
	assert.Equal(t, []string{"int64", "string", "int32", "float64"}, types)

	// Verify row iteration
	iterator := response.GetRowIterator()
	rowCount := 0

	expectedRows := []struct {
		id     int64
		name   string
		age    int32
		salary float64
	}{
		{1, "Alice", 30, 75000.50},
		{2, "Bob", 25, 65000.00},
		{3, "Charlie", 35, 85000.75},
	}

	for iterator.Next() {
		var id int64
		var name string
		var age int32
		var salary float64

		err := iterator.ScanRow(&id, &name, &age, &salary)
		require.NoError(t, err)

		expected := expectedRows[rowCount]
		assert.Equal(t, expected.id, id)
		assert.Equal(t, expected.name, name)
		assert.Equal(t, expected.age, age)
		assert.Equal(t, expected.salary, salary)

		rowCount++
	}

	assert.Equal(t, 3, rowCount)
}

func TestUnifiedResponse_EmptyResult(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns([]string{"col1"}, []string{"string"})

	response := builder.Build("empty_query", "No results", nil)

	assert.Equal(t, int64(0), response.RowCount)
	assert.Equal(t, int32(1), response.ColumnCount)

	names, types, err := response.GetColumns()
	require.NoError(t, err)
	assert.Equal(t, []string{"col1"}, names)
	assert.Equal(t, []string{"string"}, types)

	iterator := response.GetRowIterator()
	assert.False(t, iterator.Next())
}

func TestUnifiedResponse_NullValues(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"id", "optional_name"},
		[]string{"int64", "string"},
	)

	builder.AddRow([]interface{}{int64(1), "Alice"})
	builder.AddRow([]interface{}{int64(2), nil})
	builder.AddRow([]interface{}{int64(3), "Charlie"})

	response := builder.Build("null_test", "OK", nil)

	iterator := response.GetRowIterator()

	// First row - normal values
	require.True(t, iterator.Next())
	var id1 int64
	var name1 string
	err := iterator.ScanRow(&id1, &name1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), id1)
	assert.Equal(t, "Alice", name1)

	// Second row - null name
	require.True(t, iterator.Next())
	var id2 int64
	var name2 string
	err = iterator.ScanRow(&id2, &name2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), id2)
	assert.Equal(t, "", name2) // null becomes empty string

	// Third row - normal values
	require.True(t, iterator.Next())
	var id3 int64
	var name3 string
	err = iterator.ScanRow(&id3, &name3)
	require.NoError(t, err)
	assert.Equal(t, int64(3), id3)
	assert.Equal(t, "Charlie", name3)

	assert.False(t, iterator.Next())
}

func TestUnifiedResponse_AllDataTypes(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"str", "i64", "i32", "f64", "f32", "bool"},
		[]string{"string", "int64", "int32", "float64", "float32", "bool"},
	)

	builder.AddRow([]interface{}{
		"test_string",
		int64(9223372036854775807), // max int64
		int32(2147483647),          // max int32
		float64(3.141592653589793),
		float32(2.718281828),
		true,
	})

	builder.AddRow([]interface{}{
		"",
		int64(-9223372036854775808),       // min int64
		int32(-2147483648),                // min int32
		float64(-1.7976931348623157e+308), // close to min float64
		float32(-3.4028235e+38),           // close to min float32
		false,
	})

	response := builder.Build("types_test", "OK", nil)

	iterator := response.GetRowIterator()

	// First row - max values
	require.True(t, iterator.Next())
	var str1 string
	var i64_1 int64
	var i32_1 int32
	var f64_1 float64
	var f32_1 float32
	var bool1 bool

	err := iterator.ScanRow(&str1, &i64_1, &i32_1, &f64_1, &f32_1, &bool1)
	require.NoError(t, err)

	assert.Equal(t, "test_string", str1)
	assert.Equal(t, int64(9223372036854775807), i64_1)
	assert.Equal(t, int32(2147483647), i32_1)
	assert.Equal(t, float64(3.141592653589793), f64_1)
	assert.Equal(t, float32(2.718281828), f32_1)
	assert.Equal(t, true, bool1)

	// Second row - min values
	require.True(t, iterator.Next())
	var str2 string
	var i64_2 int64
	var i32_2 int32
	var f64_2 float64
	var f32_2 float32
	var bool2 bool

	err = iterator.ScanRow(&str2, &i64_2, &i32_2, &f64_2, &f32_2, &bool2)
	require.NoError(t, err)

	assert.Equal(t, "", str2)
	assert.Equal(t, int64(-9223372036854775808), i64_2)
	assert.Equal(t, int32(-2147483648), i32_2)
	assert.Equal(t, float64(-1.7976931348623157e+308), f64_2)
	assert.Equal(t, float32(-3.4028235e+38), f32_2)
	assert.Equal(t, false, bool2)

	assert.False(t, iterator.Next())
}

func TestUnifiedResponse_MemoryEfficiency(t *testing.T) {
	const numRows = 1000
	const numCols = 10

	// Build a large response
	builder := NewResponseBuilder()

	columns := make([]string, numCols)
	types := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		columns[i] = "col_" + string(rune('0'+i))
		types[i] = "string"
	}
	builder.SetColumns(columns, types)

	// Add many rows
	for i := 0; i < numRows; i++ {
		row := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			row[j] = "value_" + string(rune('0'+i%10)) + "_" + string(rune('0'+j))
		}
		builder.AddRow(row)
	}

	response := builder.Build("memory_test", "Large dataset", nil)

	// Verify the response works correctly
	assert.Equal(t, int64(numRows), response.RowCount)
	assert.Equal(t, int32(numCols), response.ColumnCount)

	// Verify we can iterate through all rows
	iterator := response.GetRowIterator()
	actualRows := 0
	for iterator.Next() {
		// Create destinations for all columns
		dests := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			var s string
			dests[j] = &s
		}

		err := iterator.ScanRow(dests...)
		require.NoError(t, err)
		actualRows++
	}

	assert.Equal(t, numRows, actualRows)

	// The response should be significantly smaller than traditional [][]interface{}
	// Each interface{} is ~16 bytes overhead, so traditional would be:
	// numRows * numCols * (16 + average_string_size)
	traditionalEstimate := numRows * numCols * (16 + 20) // 16 bytes interface{} + ~20 bytes string

	t.Logf("UnifiedResponse size: %d bytes", response.Size())
	t.Logf("Traditional estimate: %d bytes", traditionalEstimate)
	t.Logf("Memory savings: %.1f%%", float64(traditionalEstimate-response.Size())/float64(traditionalEstimate)*100)

	// We should see significant memory savings
	assert.Less(t, response.Size(), traditionalEstimate/2, "UnifiedResponse should use less than half the memory")
}

func TestUnifiedResponse_ToLegacyRows(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"id", "name", "active"},
		[]string{"int64", "string", "bool"},
	)

	builder.AddRow([]interface{}{int64(1), "Alice", true})
	builder.AddRow([]interface{}{int64(2), "Bob", false})

	response := builder.Build("legacy_test", "OK", nil)

	// Convert to legacy format
	rows, err := response.ToLegacyRows()
	require.NoError(t, err)

	expectedRows := [][]interface{}{
		{int64(1), "Alice", true},
		{int64(2), "Bob", false},
	}

	assert.Equal(t, expectedRows, rows)
}

func TestUnifiedResponse_ErrorHandling(t *testing.T) {
	// Test with corrupted data
	response := &UnifiedResponse{
		Data:        []byte{0x01, 0x02}, // Invalid data
		RowCount:    1,
		ColumnCount: 1,
	}

	// Should handle corrupted column data gracefully
	_, _, err := response.GetColumns()
	assert.Error(t, err)

	// Should handle corrupted row data gracefully
	iterator := response.GetRowIterator()
	assert.False(t, iterator.Next()) // Should fail safely
}

func TestUnifiedResponse_ScanRowTypeMismatch(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns([]string{"number"}, []string{"int64"})
	builder.AddRow([]interface{}{int64(42)})

	response := builder.Build("type_test", "OK", nil)
	iterator := response.GetRowIterator()

	require.True(t, iterator.Next())

	// Try to scan int64 into string pointer - should fail
	var wrongType string
	err := iterator.ScanRow(&wrongType)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "destination is not *int64")
}

func TestUnifiedResponse_BufferSizeEstimation(t *testing.T) {
	builder := NewResponseBuilder()

	// Test buffer size estimation with various data types
	builder.SetColumns(
		[]string{"short", "long_column_name", "medium"},
		[]string{"string", "string", "int64"},
	)

	// Add rows with varying sizes
	builder.AddRow([]interface{}{"a", "very_long_string_value_here", int64(123)})
	builder.AddRow([]interface{}{"", "another_long_value", int64(456)})

	// The estimation should work without panicking
	size := builder.calculateBufferSize()
	assert.Greater(t, size, 0)

	response := builder.Build("estimation_test", "OK", nil)

	// The actual size should be reasonable compared to estimation
	actualSize := len(response.Data)
	assert.Greater(t, actualSize, 0)
	assert.Less(t, actualSize, size*2) // Should not be wildly off
}

func TestUnifiedResponse_UnsafePointerUsage(t *testing.T) {
	// Test that unsafe pointer operations work correctly for float types
	builder := NewResponseBuilder()
	builder.SetColumns([]string{"f32", "f64"}, []string{"float32", "float64"})

	testFloat32 := float32(3.14159)
	testFloat64 := float64(2.718281828459045)

	builder.AddRow([]interface{}{testFloat32, testFloat64})

	response := builder.Build("unsafe_test", "OK", nil)
	iterator := response.GetRowIterator()

	require.True(t, iterator.Next())

	var f32 float32
	var f64 float64

	err := iterator.ScanRow(&f32, &f64)
	require.NoError(t, err)

	// Float32 precision might be slightly different due to binary representation
	assert.InDelta(t, testFloat32, f32, 0.0001)
	assert.Equal(t, testFloat64, f64)
}

func TestUnifiedResponse_MemoryFootprint(t *testing.T) {
	builder := NewResponseBuilder()
	builder.SetColumns([]string{"test"}, []string{"string"})
	builder.AddRow([]interface{}{"hello"})

	response := builder.Build("footprint_test", "OK", nil)

	size := response.Size()

	// Size should include:
	// - Data slice length
	// - QueryID string length
	// - Message string length
	// - Struct size
	expectedMinSize := len(response.Data) +
		len(response.QueryID) +
		len(response.Message) +
		int(unsafe.Sizeof(*response))

	assert.Equal(t, expectedMinSize, size)
	assert.Greater(t, size, 0)
}
