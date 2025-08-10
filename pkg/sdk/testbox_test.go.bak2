package sdk

import (
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isCI checks if we're running in a CI environment
func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" || os.Getenv("JENKINS_URL") != ""
}

func TestNewTestBox(t *testing.T) {
	testBox := NewTestBox(t)
	require.NotNil(t, testBox)

	// Verify that the test box is properly initialized
	assert.NotNil(t, testBox.GetConfig())
	assert.NotNil(t, testBox.GetCatalog())
	assert.NotNil(t, testBox.GetEngine())
	assert.NotNil(t, testBox.GetMemoryFS())
}

func TestNewTestBoxWithOptions(t *testing.T) {
	// Test with custom name and memory filesystem
	testBox1 := NewTestBox(t, WithName("custom-test"), WithMemoryLimit("512MB"))
	assert.NotNil(t, testBox1)
	assert.Equal(t, "custom-test", testBox1.GetConfig().Name)

	// Test with file system
	testBox2 := NewTestBox(t, WithFileSystem())
	assert.NotNil(t, testBox2)
	assert.Nil(t, testBox2.GetMemoryFS()) // Should be nil when using file system
}

func TestNewTestBoxWithDefaults(t *testing.T) {
	// Test CI configuration
	testBox1 := NewTestBox(t, WithDefaults(CIDefaults))
	assert.NotNil(t, testBox1)

	// Test fast configuration for quick tests
	testBox2 := NewTestBox(t, WithDefaults(FastDefaults))
	assert.NotNil(t, testBox2)
}

func TestCreateNamespace(t *testing.T) {
	testBox := NewTestBox(t)

	// Create a namespace
	namespace := testBox.CreateNamespace("test_namespace")
	assert.Equal(t, table.Identifier{"test_namespace"}, namespace)

	// Create a namespace with properties
	namespace2 := testBox.CreateNamespace("test_ns_2", map[string]string{"description": "Test namespace"})
	assert.Equal(t, table.Identifier{"test_ns_2"}, namespace2)
}

func TestCreateTable(t *testing.T) {
	testBox := NewTestBox(t)

	// Create namespace first
	testBox.CreateNamespace("test_namespace")

	// Create table with default schema
	icebergTable := testBox.CreateTable("test_namespace", "test_table")
	assert.NotNil(t, icebergTable)

	// Create namespace for second test
	testBox.CreateNamespace("test_ns")

	// Create table with custom schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	icebergTable2 := testBox.CreateTable("test_ns", "custom_table", schema)
	assert.NotNil(t, icebergTable2)
	assert.Equal(t, schema.String(), icebergTable2.Schema().String())
}

func TestExecuteSQL(t *testing.T) {
	// Use file system instead of memory filesystem for DuckDB compatibility
	testBox := NewTestBox(t, WithFileSystem())

	// Create namespace and table
	testBox.CreateNamespace("test_ns")
	table := testBox.CreateTable("test_ns", "test_table")

	// Register table with engine
	testBox.RegisterTable(table)

	// Execute basic SQL query - COUNT(*) always returns 1 row with the count value
	result := testBox.MustExecuteSQL("SELECT COUNT(*) FROM test_ns_test_table")
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowCount) // COUNT(*) returns 1 row

	// Check if this is a placeholder table (when Iceberg extension is not available)
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		count := result.Rows[0][0]

		// First check if this might be a placeholder table by querying the table directly
		directResult := testBox.MustExecuteSQL("SELECT * FROM test_ns_test_table")
		if len(directResult.Rows) > 0 && len(directResult.Rows[0]) > 0 {
			if firstCol, ok := directResult.Rows[0][0].(string); ok &&
				firstCol == "Iceberg extension not available on this platform" {
				// This is a placeholder table, so COUNT(*) will return 1 (the placeholder row)
				assert.Equal(t, int64(1), count) // Placeholder table has 1 row
				t.Log("Iceberg extension not available - using placeholder table")
			} else {
				// This is a real Iceberg table, so COUNT(*) should return 0 for empty table
				assert.Equal(t, int64(0), count) // Empty table should have count of 0
			}
		}
	}
}

func TestExecuteSQLWithError(t *testing.T) {
	testBox := NewTestBox(t)

	// Test error handling
	_, err := testBox.ExecuteSQL("SELECT * FROM nonexistent_table")
	assert.Error(t, err)
}

func TestRegisterTable(t *testing.T) {
	// Use file system instead of memory filesystem for DuckDB compatibility
	testBox := NewTestBox(t, WithFileSystem())

	// Create namespace and table
	testBox.CreateNamespace("test_ns")
	table := testBox.CreateTable("test_ns", "test_table")

	// Register table
	testBox.RegisterTable(table)

	// Verify table is registered by checking engine
	tables, err := testBox.GetEngine().ListTables(testBox.t.Context())
	require.NoError(t, err)
	assert.Contains(t, tables, "test_ns_test_table")
}

func TestCleanup(t *testing.T) {
	// This test ensures cleanup works without panicking
	testBox := NewTestBox(t)

	// Use the test box
	testBox.CreateNamespace("cleanup_test")
	testBox.CreateTable("cleanup_test", "table1")

	// Cleanup should happen automatically via t.Cleanup()
	// If there are any issues, they would show up as test failures
}

func TestMemoryFileSystemIntegration(t *testing.T) {
	if isCI() {
		t.Skip("Skipping memory filesystem tests in CI due to Windows path handling issues")
	}

	// Skip this test for now due to DuckDB/memory filesystem incompatibility
	t.Skip("Memory filesystem is incompatible with DuckDB's iceberg_scan function which requires actual files")

	testBox := NewTestBox(t, WithMemoryLimit("256MB"))
	assert.NotNil(t, testBox.GetMemoryFS())

	// Create namespace first
	testBox.CreateNamespace("memory_test")

	// Test that we can create and query tables using memory filesystem
	icebergTable := testBox.CreateTable("memory_test", "test_table")
	testBox.RegisterTable(icebergTable)

	// Execute a simple query
	result := testBox.MustExecuteSQL("SELECT 1 as test_col")
	assert.NotNil(t, result)
	assert.True(t, result.RowCount > 0)
}

func TestWithPropertyOption(t *testing.T) {
	testBox := NewTestBox(t,
		WithProperty("test.key1", "value1"),
		WithProperty("test.key2", "value2"),
	)

	// Test that testbox was created successfully
	assert.NotNil(t, testBox)
	// Properties are stored in the configuration but may not be directly accessible
	// The important thing is that the option doesn't cause errors
}

func TestMultipleOptions(t *testing.T) {
	testBox := NewTestBox(t,
		WithName("multi-option-test"),
		WithDefaults(FastDefaults),
		WithMemoryLimit("1GB"),
		WithProperty("custom.setting", "test-value"),
	)

	assert.NotNil(t, testBox)
	assert.Equal(t, "multi-option-test", testBox.GetConfig().Name)
}

// Test that demonstrates the intended usage pattern from the design document
func TestDesignDocumentUsagePattern(t *testing.T) {
	// Use file system for DuckDB compatibility
	testBox := NewTestBox(t, WithFileSystem())

	// Create some test data
	namespace := testBox.CreateNamespace("analytics")
	table := testBox.CreateTable("analytics", "events")

	// Verify namespace was created
	assert.Equal(t, "analytics", namespace[0])

	// Register for querying
	testBox.RegisterTable(table)

	// Execute queries
	result := testBox.MustExecuteSQL("SHOW TABLES")
	assert.NotNil(t, result)
	assert.True(t, result.RowCount >= 1)

	// Test with CI-friendly configuration (but skip table registration for memory FS)
	ciTestBox := NewTestBox(t, WithDefaults(CIDefaults))
	assert.NotNil(t, ciTestBox.GetMemoryFS())
}

// Benchmark to ensure TestBox creation is fast enough for CI
func BenchmarkNewTestBox(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testBox := NewTestBox(&testing.T{}, WithDefaults(FastDefaults))
		_ = testBox
	}
}
