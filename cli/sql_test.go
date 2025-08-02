package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLCommandIntegration(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "icebox-sql-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create project structure
	projectDir := filepath.Join(tempDir, "test-project")
	err = os.MkdirAll(filepath.Join(projectDir, ".icebox", "catalog"), 0755)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(projectDir, ".icebox", "data"), 0755)
	require.NoError(t, err)

	// Create configuration
	configFile := filepath.Join(projectDir, ".icebox.yml")
	configContent := `name: test-sql-catalog
catalog:
  sqlite:
    path: .icebox/catalog/catalog.db
storage:
  filesystem:
    root_path: .icebox/data`

	err = os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change directory for config discovery
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Logf("Failed to restore original directory: %v", err)
		}
	}()

	err = os.Chdir(projectDir)
	require.NoError(t, err)

	// Test basic SQL query functionality
	// Note: This is an integration test that would require the full command setup
	// For now, we test the components individually

	// Load configuration
	configPath, cfg, err := config.FindConfig()
	require.NoError(t, err)
	assert.Equal(t, ".icebox.yml", filepath.Base(configPath))

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Create engine
	engine, err := duckdb.NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	// Test basic query
	result, err := engine.ExecuteQuery(context.Background(), "SELECT 42 as answer")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowCount)
	assert.Equal(t, []string{"answer"}, result.Columns)
}

func TestAutoRegisterTables(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "icebox-auto-register-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test configuration with a real file database
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(tempDir, "data"),
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := duckdb.NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Create namespace and table
	namespace := table.Identifier{"test_namespace"}
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"description": "Test namespace"})
	require.NoError(t, err)

	// Create a test schema
	fields := []iceberg.NestedField{
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	// Create table
	tableIdent := table.Identifier{"test_namespace", "test_table"}
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Test auto-registration
	err = autoRegisterTables(ctx, engine, catalog)
	require.NoError(t, err)

	// Verify table was registered
	tables, err := engine.ListTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "test_namespace_test_table")
}

func TestDisplayTableFormat(t *testing.T) {
	columns := []string{"id", "name", "amount"}
	rows := [][]interface{}{
		{1, "Alice", 100.50},
		{2, "Bob", 250.00},
		{3, "Charlie", 75.25},
	}

	// This should not panic or error
	err := displayTableFormat(columns, rows)
	assert.NoError(t, err)
}

func TestDisplayCSVFormat(t *testing.T) {
	columns := []string{"id", "name", "amount"}
	rows := [][]interface{}{
		{1, "Alice", 100.50},
		{2, "Bob", 250.00},
		{3, "Charlie", 75.25},
	}

	// This should not panic or error
	err := displayCSVFormat(columns, rows)
	assert.NoError(t, err)
}

func TestDisplayJSONFormat(t *testing.T) {
	columns := []string{"id", "name", "amount"}
	rows := [][]interface{}{
		{1, "Alice", 100.50},
		{2, "Bob", 250.00},
		{3, "Charlie", 75.25},
	}

	// This should not panic or error
	err := displayJSONFormat(columns, rows)
	assert.NoError(t, err)
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{42, "42"},
		{"hello", "hello"},
		{3.14, "3.14"},
		{true, "true"},
	}

	for _, test := range tests {
		result := formatValue(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestFormatValueCSV(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, ""},
		{42, "42"},
		{"hello", "hello"},
		{"hello,world", `"hello,world"`},
		{"say \"hello\"", `"say ""hello"""`},
		{"line1\nline2", `"line1\nline2"`},
	}

	for _, test := range tests {
		result := formatValueCSV(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"short", 3, "sho"},
		{"", 5, ""},
		{"exact", 5, "exact"},
	}

	for _, test := range tests {
		result := truncateString(test.input, test.maxLen)
		assert.Equal(t, test.expected, result)
	}
}

func TestSQLOptionsValidation(t *testing.T) {
	// Test that SQL options have reasonable defaults
	assert.Equal(t, "table", sqlOpts.format)
	assert.Equal(t, 1000, sqlOpts.maxRows)
	assert.False(t, sqlOpts.showSchema)
	assert.True(t, sqlOpts.timing)
	assert.True(t, sqlOpts.autoRegister)
}

// Test that demonstrates the SQL command structure is in place
func TestSQLCommandStructure(t *testing.T) {
	// Verify the command is properly configured
	assert.NotNil(t, sqlCmd)
	assert.Equal(t, "sql [query]", sqlCmd.Use)
	assert.NotNil(t, sqlCmd.RunE)

	// Verify flags are configured
	flag := sqlCmd.Flag("format")
	assert.NotNil(t, flag)
	assert.Equal(t, "table", flag.DefValue)

	flag = sqlCmd.Flag("max-rows")
	assert.NotNil(t, flag)
	assert.Equal(t, "1000", flag.DefValue)

	flag = sqlCmd.Flag("show-schema")
	assert.NotNil(t, flag)
	assert.Equal(t, "false", flag.DefValue)

	flag = sqlCmd.Flag("timing")
	assert.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)

	flag = sqlCmd.Flag("auto-register")
	assert.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)
}
