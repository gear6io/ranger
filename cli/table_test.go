package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper functions

func setupTestCatalog(t *testing.T) (catalog.CatalogInterface, func()) {
	tempDir := t.TempDir()
	warehouseDir := filepath.Join(tempDir, "warehouse")
	err := os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	cfg := &config.Config{
		Name: "test-table",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			Type: "filesystem",
			FileSystem: &config.FileSystemConfig{
				RootPath: warehouseDir,
			},
		},
	}

	cat, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)

	return cat, func() { cat.Close() }
}

func createTestTable(t *testing.T, cat catalog.CatalogInterface, tableIdent table.Identifier) *table.Table {
	ctx := context.Background()

	// Create namespace if it doesn't exist
	namespaceIdent := tableIdent[:len(tableIdent)-1]
	if len(namespaceIdent) == 0 {
		namespaceIdent = table.Identifier{"default"}
	}

	exists, err := cat.CheckNamespaceExists(ctx, namespaceIdent)
	require.NoError(t, err)
	if !exists {
		err = cat.CreateNamespace(ctx, namespaceIdent, iceberg.Properties{})
		require.NoError(t, err)
	}

	// Create schema
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
	)

	// Create table
	tbl, err := cat.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	return tbl
}

func captureOutput(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r) // Ignore error for test output capture
	return buf.String()
}

// Test parseIcebergType function

func TestParseIcebergType(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		expected iceberg.Type
		wantErr  bool
	}{
		{
			name:     "boolean",
			typeStr:  "boolean",
			expected: iceberg.PrimitiveTypes.Bool,
			wantErr:  false,
		},
		{
			name:     "bool",
			typeStr:  "bool",
			expected: iceberg.PrimitiveTypes.Bool,
			wantErr:  false,
		},
		{
			name:     "int",
			typeStr:  "int",
			expected: iceberg.PrimitiveTypes.Int32,
			wantErr:  false,
		},
		{
			name:     "integer",
			typeStr:  "integer",
			expected: iceberg.PrimitiveTypes.Int32,
			wantErr:  false,
		},
		{
			name:     "int32",
			typeStr:  "int32",
			expected: iceberg.PrimitiveTypes.Int32,
			wantErr:  false,
		},
		{
			name:     "long",
			typeStr:  "long",
			expected: iceberg.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "int64",
			typeStr:  "int64",
			expected: iceberg.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "bigint",
			typeStr:  "bigint",
			expected: iceberg.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "float",
			typeStr:  "float",
			expected: iceberg.PrimitiveTypes.Float32,
			wantErr:  false,
		},
		{
			name:     "float32",
			typeStr:  "float32",
			expected: iceberg.PrimitiveTypes.Float32,
			wantErr:  false,
		},
		{
			name:     "double",
			typeStr:  "double",
			expected: iceberg.PrimitiveTypes.Float64,
			wantErr:  false,
		},
		{
			name:     "float64",
			typeStr:  "float64",
			expected: iceberg.PrimitiveTypes.Float64,
			wantErr:  false,
		},
		{
			name:     "decimal",
			typeStr:  "decimal",
			expected: iceberg.DecimalTypeOf(38, 18),
			wantErr:  false,
		},
		{
			name:     "decimal with precision and scale",
			typeStr:  "decimal(10,2)",
			expected: iceberg.DecimalTypeOf(10, 2),
			wantErr:  false,
		},
		{
			name:     "date",
			typeStr:  "date",
			expected: iceberg.PrimitiveTypes.Date,
			wantErr:  false,
		},
		{
			name:     "time",
			typeStr:  "time",
			expected: iceberg.PrimitiveTypes.Time,
			wantErr:  false,
		},
		{
			name:     "timestamp",
			typeStr:  "timestamp",
			expected: iceberg.PrimitiveTypes.Timestamp,
			wantErr:  false,
		},
		{
			name:     "timestamptz",
			typeStr:  "timestamptz",
			expected: iceberg.PrimitiveTypes.TimestampTz,
			wantErr:  false,
		},
		{
			name:     "timestamp_tz",
			typeStr:  "timestamp_tz",
			expected: iceberg.PrimitiveTypes.TimestampTz,
			wantErr:  false,
		},
		{
			name:     "string",
			typeStr:  "string",
			expected: iceberg.PrimitiveTypes.String,
			wantErr:  false,
		},
		{
			name:     "varchar",
			typeStr:  "varchar",
			expected: iceberg.PrimitiveTypes.String,
			wantErr:  false,
		},
		{
			name:     "text",
			typeStr:  "text",
			expected: iceberg.PrimitiveTypes.String,
			wantErr:  false,
		},
		{
			name:     "uuid",
			typeStr:  "uuid",
			expected: iceberg.PrimitiveTypes.UUID,
			wantErr:  false,
		},
		{
			name:     "fixed",
			typeStr:  "fixed",
			expected: iceberg.FixedTypeOf(16),
			wantErr:  false,
		},
		{
			name:     "fixed with length",
			typeStr:  "fixed(8)",
			expected: iceberg.FixedTypeOf(8),
			wantErr:  false,
		},
		{
			name:     "binary",
			typeStr:  "binary",
			expected: iceberg.PrimitiveTypes.Binary,
			wantErr:  false,
		},
		{
			name:     "unsupported type",
			typeStr:  "unsupported",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "invalid decimal format",
			typeStr:  "decimal(invalid)",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "invalid fixed format",
			typeStr:  "fixed(invalid)",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseIcebergType(tt.typeStr)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test parseSchemaFromJSON function

func TestParseSchemaFromJSON(t *testing.T) {
	t.Run("ValidSchema", func(t *testing.T) {
		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "type": "int64", "required": true},
				{"id": 2, "name": "name", "type": "string", "required": false},
				{"id": 3, "name": "price", "type": "decimal(10,2)", "required": true}
			]
		}`

		schema, err := parseSchemaFromJSON(schemaJSON)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 3, len(schema.Fields()))

		fields := schema.Fields()
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[0].Type)
		assert.True(t, fields[0].Required)

		assert.Equal(t, "name", fields[1].Name)
		assert.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)
		assert.False(t, fields[1].Required)

		assert.Equal(t, "price", fields[2].Name)
		assert.Equal(t, iceberg.DecimalTypeOf(10, 2), fields[2].Type)
		assert.True(t, fields[2].Required)
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		schemaJSON := `{"invalid": json}`

		_, err := parseSchemaFromJSON(schemaJSON)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse JSON schema")
	})

	t.Run("InvalidSchemaType", func(t *testing.T) {
		schemaJSON := `{
			"type": "array",
			"fields": []
		}`

		_, err := parseSchemaFromJSON(schemaJSON)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema type must be 'struct'")
	})

	t.Run("InvalidFieldType", func(t *testing.T) {
		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "type": "invalid_type", "required": true}
			]
		}`

		_, err := parseSchemaFromJSON(schemaJSON)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse type for field 'id'")
	})
}

// Test readSchemaFromFile function

func TestReadSchemaFromFile(t *testing.T) {
	t.Run("ValidSchemaFile", func(t *testing.T) {
		tempDir := t.TempDir()
		schemaFile := filepath.Join(tempDir, "schema.json")

		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "type": "int64", "required": true},
				{"id": 2, "name": "name", "type": "string", "required": false}
			]
		}`

		err := os.WriteFile(schemaFile, []byte(schemaJSON), 0644)
		require.NoError(t, err)

		schema, err := readSchemaFromFile(schemaFile)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 2, len(schema.Fields()))
	})

	t.Run("NonexistentFile", func(t *testing.T) {
		_, err := readSchemaFromFile("/nonexistent/file.json")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read schema file")
	})

	t.Run("InvalidSchemaFile", func(t *testing.T) {
		tempDir := t.TempDir()
		schemaFile := filepath.Join(tempDir, "invalid_schema.json")

		err := os.WriteFile(schemaFile, []byte("invalid json"), 0644)
		require.NoError(t, err)

		_, err = readSchemaFromFile(schemaFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse JSON schema")
	})
}

// Test getTableSchema function

func TestGetTableSchema(t *testing.T) {
	t.Run("SchemaFromFile", func(t *testing.T) {
		tempDir := t.TempDir()
		schemaFile := filepath.Join(tempDir, "schema.json")

		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "type": "int64", "required": true}
			]
		}`

		err := os.WriteFile(schemaFile, []byte(schemaJSON), 0644)
		require.NoError(t, err)

		opts := &tableCreateOptions{
			schemaFile: schemaFile,
		}

		schema, err := getTableSchema(opts)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 1, len(schema.Fields()))
	})

	t.Run("SchemaFromJSON", func(t *testing.T) {
		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "id", "type": "int64", "required": true}
			]
		}`

		opts := &tableCreateOptions{
			schemaJSON: schemaJSON,
		}

		schema, err := getTableSchema(opts)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 1, len(schema.Fields()))
	})

	t.Run("DefaultSchema", func(t *testing.T) {
		opts := &tableCreateOptions{}

		schema, err := getTableSchema(opts)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 3, len(schema.Fields())) // Default schema has 3 fields
	})

	t.Run("SchemaFilePriority", func(t *testing.T) {
		tempDir := t.TempDir()
		schemaFile := filepath.Join(tempDir, "schema.json")

		schemaJSON := `{
			"type": "struct",
			"fields": [
				{"id": 1, "name": "file_field", "type": "int64", "required": true}
			]
		}`

		err := os.WriteFile(schemaFile, []byte(schemaJSON), 0644)
		require.NoError(t, err)

		opts := &tableCreateOptions{
			schemaFile: schemaFile,
			schemaJSON: `{"type": "struct", "fields": [{"id": 1, "name": "json_field", "type": "string", "required": true}]}`,
		}

		schema, err := getTableSchema(opts)
		require.NoError(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, 1, len(schema.Fields()))
		assert.Equal(t, "file_field", schema.Fields()[0].Name) // File takes priority
	})
}

// Test createDefaultSchema function

func TestCreateDefaultSchema(t *testing.T) {
	schema := createDefaultSchema()
	assert.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))

	fields := schema.Fields()
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[0].Type)
	assert.True(t, fields[0].Required)

	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)
	assert.False(t, fields[1].Required)

	assert.Equal(t, "created_at", fields[2].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.TimestampTz, fields[2].Type)
	assert.False(t, fields[2].Required)
}

// Test createPartitionSpec function

func TestCreatePartitionSpec(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	t.Run("NoPartitionColumns", func(t *testing.T) {
		spec, err := createPartitionSpec(schema, []string{})
		require.NoError(t, err)
		assert.NotNil(t, spec)
		assert.True(t, spec.IsUnpartitioned())
	})

	t.Run("ValidPartitionColumns", func(t *testing.T) {
		spec, err := createPartitionSpec(schema, []string{"date", "name"})
		require.NoError(t, err)
		assert.NotNil(t, spec)
		assert.False(t, spec.IsUnpartitioned())

		var fieldCount int
		for range spec.Fields() {
			fieldCount++
		}
		assert.Equal(t, 2, fieldCount)
	})

	t.Run("InvalidPartitionColumn", func(t *testing.T) {
		_, err := createPartitionSpec(schema, []string{"nonexistent"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "partition column 'nonexistent' not found in schema")
	})
}

// Test createSortOrder function

func TestCreateSortOrder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	t.Run("NoSortColumns", func(t *testing.T) {
		sortOrder, err := createSortOrder(schema, []string{})
		require.NoError(t, err)
		assert.NotNil(t, sortOrder)
		assert.Equal(t, 0, len(sortOrder.Fields))
	})

	t.Run("ValidSortColumns", func(t *testing.T) {
		sortOrder, err := createSortOrder(schema, []string{"id", "name DESC"})
		require.NoError(t, err)
		assert.NotNil(t, sortOrder)
		assert.Equal(t, 2, len(sortOrder.Fields))

		// Check first field (ASC by default)
		assert.Equal(t, 1, sortOrder.Fields[0].SourceID)
		assert.Equal(t, table.SortASC, sortOrder.Fields[0].Direction)
		assert.Equal(t, table.NullsFirst, sortOrder.Fields[0].NullOrder)

		// Check second field (DESC specified)
		assert.Equal(t, 2, sortOrder.Fields[1].SourceID)
		assert.Equal(t, table.SortDESC, sortOrder.Fields[1].Direction)
		assert.Equal(t, table.NullsLast, sortOrder.Fields[1].NullOrder)
	})

	t.Run("InvalidSortColumn", func(t *testing.T) {
		_, err := createSortOrder(schema, []string{"nonexistent"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sort column 'nonexistent' not found in schema")
	})

	t.Run("SortColumnWithASC", func(t *testing.T) {
		sortOrder, err := createSortOrder(schema, []string{"id ASC"})
		require.NoError(t, err)
		assert.NotNil(t, sortOrder)
		assert.Equal(t, 1, len(sortOrder.Fields))
		assert.Equal(t, table.SortASC, sortOrder.Fields[0].Direction)
	})
}

// Test display functions

func TestDisplayTableList(t *testing.T) {
	tables := []table.Identifier{
		{"default", "table1"},
		{"analytics", "events"},
		{"warehouse", "products"},
	}

	t.Run("TableFormat", func(t *testing.T) {
		tableListOpts.format = "table"
		output := captureOutput(func() {
			err := displayTableList(tables, table.Identifier{"default"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Tables in namespace 'default'")
		assert.Contains(t, output, "table1")
	})

	t.Run("CSVFormat", func(t *testing.T) {
		tableListOpts.format = "csv"
		output := captureOutput(func() {
			err := displayTableList(tables, nil)
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "namespace,table")
		assert.Contains(t, output, "default,table1")
	})

	t.Run("JSONFormat", func(t *testing.T) {
		tableListOpts.format = "json"
		output := captureOutput(func() {
			err := displayTableList(tables, nil)
			assert.NoError(t, err)
		})
		assert.Contains(t, output, `"namespace": "default"`)
		assert.Contains(t, output, `"table": "table1"`)
	})

	t.Run("EmptyTableList", func(t *testing.T) {
		tableListOpts.format = "table"
		output := captureOutput(func() {
			err := displayTableList([]table.Identifier{}, table.Identifier{"empty"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "No tables found in namespace 'empty'")
	})

	t.Run("UnsupportedFormat", func(t *testing.T) {
		tableListOpts.format = "unsupported"
		err := displayTableList(tables, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported format: unsupported")
	})
}

func TestDisplayTableListTable(t *testing.T) {
	tables := []table.Identifier{
		{"default", "table1"},
		{"analytics", "events"},
	}

	t.Run("WithNamespace", func(t *testing.T) {
		output := captureOutput(func() {
			err := displayTableListTable(tables, table.Identifier{"default"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Tables in namespace 'default'")
		assert.Contains(t, output, "table1")
		assert.Contains(t, output, "events")
	})

	t.Run("AllNamespaces", func(t *testing.T) {
		output := captureOutput(func() {
			err := displayTableListTable(tables, nil)
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "All Tables")
		assert.Contains(t, output, "table1")
		assert.Contains(t, output, "events")
	})
}

func TestDisplayTableListCSV(t *testing.T) {
	tables := []table.Identifier{
		{"default", "table1"},
		{"analytics", "events"},
	}

	output := captureOutput(func() {
		err := displayTableListCSV(tables)
		assert.NoError(t, err)
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Equal(t, "namespace,table", lines[0])
	assert.Equal(t, "default,table1", lines[1])
	assert.Equal(t, "analytics,events", lines[2])
}

func TestDisplayTableListJSON(t *testing.T) {
	tables := []table.Identifier{
		{"default", "table1"},
		{"analytics", "events"},
	}

	output := captureOutput(func() {
		err := displayTableListJSON(tables)
		assert.NoError(t, err)
	})

	// Verify it's valid JSON
	var result []map[string]string
	err := json.Unmarshal([]byte(output), &result)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "default", result[0]["namespace"])
	assert.Equal(t, "table1", result[0]["table"])
	assert.Equal(t, "analytics", result[1]["namespace"])
	assert.Equal(t, "events", result[1]["table"])
}

// Test command functions with mocked catalog

func TestRunTableList(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, ".icebox.yml")
	warehouseDir := filepath.Join(tempDir, "warehouse")
	dbFile := filepath.Join(tempDir, "catalog.db")
	err := os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	configContent := fmt.Sprintf(`
name: test-project
catalog:
  type: sqlite
  sqlite:
    path: %s
storage:
  type: filesystem
  filesystem:
    root_path: %s
`, dbFile, warehouseDir)
	err = os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change to temp directory
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(oldDir) // Ignore error in test cleanup
	}()
	err = os.Chdir(tempDir)
	require.NoError(t, err)

	// Reset options
	tableListOpts = &tableListOptions{
		namespace: "default",
		format:    "table",
	}

	// Create command
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	t.Run("SuccessfulList", func(t *testing.T) {
		// First create the default namespace since it doesn't exist in a fresh catalog
		cfg, err := config.ReadConfig(configFile)
		require.NoError(t, err)
		cat, err := catalog.NewCatalog(cfg)
		require.NoError(t, err)
		defer cat.Close()

		err = cat.CreateNamespace(context.Background(), table.Identifier{"default"}, iceberg.Properties{})
		require.NoError(t, err)

		// Test that the function succeeds (regardless of whether there are tables or not)
		err = runTableList(cmd, []string{})
		assert.NoError(t, err)
	})

	t.Run("NonexistentNamespace", func(t *testing.T) {
		tableListOpts.namespace = "nonexistent"
		err := runTableList(cmd, []string{})
		assert.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "namespace")
		assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
	})
}

func TestRunTableDescribe(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, ".icebox.yml")
	warehouseDir := filepath.Join(tempDir, "warehouse")
	err := os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	configContent := fmt.Sprintf(`
name: test-project
catalog:
  type: sqlite
  sqlite:
    path: ":memory:"
storage:
  type: filesystem
  filesystem:
    root_path: %s
`, warehouseDir)
	err = os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change to temp directory
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(oldDir) // Ignore error in test cleanup
	}()
	err = os.Chdir(tempDir)
	require.NoError(t, err)

	// Reset options
	tableDescribeOpts = &tableDescribeOptions{
		format:         "table",
		showProperties: true,
	}

	// Create command
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	t.Run("NonexistentTable", func(t *testing.T) {
		err := runTableDescribe(cmd, []string{"nonexistent"})
		assert.Error(t, err)
		// Test that we get an error about table not existing, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "table")
		assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
	})
}

func TestRunTableCreate(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, ".icebox.yml")
	warehouseDir := filepath.Join(tempDir, "warehouse")
	err := os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	configContent := fmt.Sprintf(`
name: test-project
catalog:
  type: sqlite
  sqlite:
    path: ":memory:"
storage:
  type: filesystem
  filesystem:
    root_path: %s
`, warehouseDir)
	err = os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change to temp directory
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(oldDir) // Ignore error in test cleanup
	}()
	err = os.Chdir(tempDir)
	require.NoError(t, err)

	// Reset options
	tableCreateOpts = &tableCreateOptions{
		properties: make(map[string]string),
	}

	// Create command
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	t.Run("SuccessfulCreate", func(t *testing.T) {
		output := captureOutput(func() {
			err := runTableCreate(cmd, []string{"test_table"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Successfully created table!")
		assert.Contains(t, output, "test_table")
	})

	t.Run("CreateWithPartition", func(t *testing.T) {
		tableCreateOpts.partitionBy = []string{"created_at"}
		output := captureOutput(func() {
			err := runTableCreate(cmd, []string{"partitioned_table"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Successfully created table!")
		assert.Contains(t, output, "Partitioned by: created_at")
	})

	t.Run("CreateWithSort", func(t *testing.T) {
		tableCreateOpts.partitionBy = nil
		tableCreateOpts.sortBy = []string{"id", "name DESC"}
		output := captureOutput(func() {
			err := runTableCreate(cmd, []string{"sorted_table"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Successfully created table!")
		assert.Contains(t, output, "Sorted by: id, name DESC")
	})

	t.Run("CreateWithProperties", func(t *testing.T) {
		tableCreateOpts.sortBy = nil
		tableCreateOpts.properties = map[string]string{
			"key1": "value1",
			"key2": "value2",
		}
		output := captureOutput(func() {
			err := runTableCreate(cmd, []string{"props_table"})
			assert.NoError(t, err)
		})
		assert.Contains(t, output, "Successfully created table!")
		assert.Contains(t, output, "Properties:")
		assert.Contains(t, output, "key1: value1")
		assert.Contains(t, output, "key2: value2")
	})

	t.Run("CreateWithInvalidPartition", func(t *testing.T) {
		tableCreateOpts.properties = make(map[string]string)
		tableCreateOpts.partitionBy = []string{"nonexistent_column"}
		err := runTableCreate(cmd, []string{"invalid_partition_table"})
		assert.Error(t, err)
		// Test that we get an error about partition specification, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "partition")
		assert.Contains(t, strings.ToLower(err.Error()), "not found")
	})

	t.Run("CreateWithInvalidSort", func(t *testing.T) {
		tableCreateOpts.partitionBy = nil
		tableCreateOpts.sortBy = []string{"nonexistent_column"}
		err := runTableCreate(cmd, []string{"invalid_sort_table"})
		assert.Error(t, err)
		// Test that we get an error about sort order, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "sort")
		assert.Contains(t, strings.ToLower(err.Error()), "not found")
	})
}

func TestRunTableDrop(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, ".icebox.yml")
	warehouseDir := filepath.Join(tempDir, "warehouse")
	err := os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	configContent := fmt.Sprintf(`
name: test-project
catalog:
  type: sqlite
  sqlite:
    path: ":memory:"
storage:
  type: filesystem
  filesystem:
    root_path: %s
`, warehouseDir)
	err = os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Change to temp directory
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(oldDir) // Ignore error in test cleanup
	}()
	err = os.Chdir(tempDir)
	require.NoError(t, err)

	// Create command
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	t.Run("NonexistentTable", func(t *testing.T) {
		err := runTableDrop(cmd, []string{"nonexistent"})
		assert.Error(t, err)
		// Test that we get an error about table not existing, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "table")
		assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
	})
}

// Test edge cases and error conditions

func TestTableCommandsWithoutConfig(t *testing.T) {
	// Change to a directory without config
	tempDir := t.TempDir()
	oldDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(oldDir) // Ignore error in test cleanup
	}()
	err = os.Chdir(tempDir)
	require.NoError(t, err)

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	t.Run("ListWithoutConfig", func(t *testing.T) {
		err := runTableList(cmd, []string{})
		assert.Error(t, err)
		// Test that we get a configuration-related error, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "icebox")
		assert.Contains(t, strings.ToLower(err.Error()), "found")
	})

	t.Run("DescribeWithoutConfig", func(t *testing.T) {
		err := runTableDescribe(cmd, []string{"test"})
		assert.Error(t, err)
		// Test that we get a configuration-related error, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "icebox")
		assert.Contains(t, strings.ToLower(err.Error()), "found")
	})

	t.Run("CreateWithoutConfig", func(t *testing.T) {
		err := runTableCreate(cmd, []string{"test"})
		assert.Error(t, err)
		// Test that we get a configuration-related error, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "icebox")
		assert.Contains(t, strings.ToLower(err.Error()), "found")
	})

	t.Run("DropWithoutConfig", func(t *testing.T) {
		err := runTableDrop(cmd, []string{"test"})
		assert.Error(t, err)
		// Test that we get a configuration-related error, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "icebox")
		assert.Contains(t, strings.ToLower(err.Error()), "found")
	})

	t.Run("HistoryWithoutConfig", func(t *testing.T) {
		err := runTableHistory(cmd, []string{"test"})
		assert.Error(t, err)
		// Test that we get a configuration-related error, regardless of exact format
		assert.Contains(t, strings.ToLower(err.Error()), "icebox")
		assert.Contains(t, strings.ToLower(err.Error()), "found")
	})
}

// Test utility functions

func TestTableTruncateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "short string",
			input:    "hello",
			maxLen:   10,
			expected: "hello",
		},
		{
			name:     "exact length",
			input:    "hello",
			maxLen:   5,
			expected: "hello",
		},
		{
			name:     "long string",
			input:    "this is a very long string",
			maxLen:   10,
			expected: "this is...",
		},
		{
			name:     "very short max length",
			input:    "hello",
			maxLen:   3,
			expected: "hel",
		},
		{
			name:     "max length 1",
			input:    "hello",
			maxLen:   1,
			expected: "h",
		},
		{
			name:     "empty string",
			input:    "",
			maxLen:   5,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test complex scenarios

func TestCreateTableWithOptions(t *testing.T) {
	cat, cleanup := setupTestCatalog(t)
	defer cleanup()

	ctx := context.Background()
	tableIdent := table.Identifier{"default", "test_table"}

	// Create namespace
	err := cat.CreateNamespace(ctx, table.Identifier{"default"}, iceberg.Properties{})
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	t.Run("CreateWithAllOptions", func(t *testing.T) {
		partitionSpec, err := createPartitionSpec(schema, []string{"date"})
		require.NoError(t, err)

		sortOrder, err := createSortOrder(schema, []string{"id", "name DESC"})
		require.NoError(t, err)

		properties := iceberg.Properties{
			"test.property": "test.value",
		}

		// Test that the function succeeds and returns a valid table
		tbl, err := createTableWithOptions(ctx, cat, tableIdent, schema, partitionSpec, sortOrder, properties)
		assert.NoError(t, err)
		assert.NotNil(t, tbl)

		// Test that the table has the expected properties
		assert.NotNil(t, tbl.Schema())
		assert.Equal(t, 3, len(tbl.Schema().Fields()))
	})

	t.Run("CreateWithUnpartitioned", func(t *testing.T) {
		tableIdent2 := table.Identifier{"default", "test_table2"}
		partitionSpec := iceberg.NewPartitionSpec()

		tbl, err := createTableWithOptions(ctx, cat, tableIdent2, schema, &partitionSpec, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
	})
}

func TestDisplayTableHistoryDetailed(t *testing.T) {
	cat, cleanup := setupTestCatalog(t)
	defer cleanup()

	tableIdent := table.Identifier{"default", "test_table"}
	tbl := createTestTable(t, cat, tableIdent)

	t.Run("TableFormat", func(t *testing.T) {
		opts := &tableHistoryOptions{
			format:       "table",
			maxSnapshots: 10,
			reverse:      false,
		}

		// Test that the function succeeds
		err := displayTableHistoryDetailed(tbl, opts)
		assert.NoError(t, err)
	})

	t.Run("JSONFormat", func(t *testing.T) {
		opts := &tableHistoryOptions{
			format:       "json",
			maxSnapshots: 10,
			reverse:      false,
		}

		// Test that the function succeeds
		err := displayTableHistoryDetailed(tbl, opts)
		assert.NoError(t, err)
	})

	t.Run("UnsupportedFormat", func(t *testing.T) {
		opts := &tableHistoryOptions{
			format: "unsupported",
		}

		err := displayTableHistoryDetailed(tbl, opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported format: unsupported")
	})
}

func TestDisplayTableDescription(t *testing.T) {
	cat, cleanup := setupTestCatalog(t)
	defer cleanup()

	tableIdent := table.Identifier{"default", "test_table"}
	tbl := createTestTable(t, cat, tableIdent)

	t.Run("WithProperties", func(t *testing.T) {
		opts := &tableDescribeOptions{
			showProperties: true,
			showStats:      false,
		}

		output := captureOutput(func() {
			err := displayTableDescription(tbl, opts)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Table:")
		assert.Contains(t, output, "Location:")
		assert.Contains(t, output, "Schema")
		assert.Contains(t, output, "id")
		assert.Contains(t, output, "name")
		assert.Contains(t, output, "created_at")
	})

	t.Run("WithoutProperties", func(t *testing.T) {
		opts := &tableDescribeOptions{
			showProperties: false,
			showStats:      false,
		}

		output := captureOutput(func() {
			err := displayTableDescription(tbl, opts)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Table:")
		assert.Contains(t, output, "Schema")
	})
}

// Test initialization

func TestInit(t *testing.T) {
	// Test that commands are properly initialized
	assert.NotNil(t, tableCmd)
	assert.NotNil(t, tableListCmd)
	assert.NotNil(t, tableDescribeCmd)
	assert.NotNil(t, tableHistoryCmd)
	assert.NotNil(t, tableCreateCmd)
	assert.NotNil(t, tableDropCmd)

	// Test that options are initialized
	assert.NotNil(t, tableListOpts)
	assert.NotNil(t, tableDescribeOpts)
	assert.NotNil(t, tableHistoryOpts)
	assert.NotNil(t, tableCreateOpts)
	assert.NotNil(t, tableDropOpts)
}

// Benchmark tests

func BenchmarkParseIcebergType(b *testing.B) {
	types := []string{"int64", "string", "decimal(10,2)", "timestamp", "boolean"}

	for i := 0; i < b.N; i++ {
		for _, typeStr := range types {
			_, _ = parseIcebergType(typeStr)
		}
	}
}

func BenchmarkCreatePartitionSpec(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	partitionColumns := []string{"date", "name"}

	for i := 0; i < b.N; i++ {
		_, _ = createPartitionSpec(schema, partitionColumns)
	}
}

func BenchmarkTruncateString(b *testing.B) {
	longString := strings.Repeat("a", 1000)

	for i := 0; i < b.N; i++ {
		_ = truncateString(longString, 50)
	}
}
