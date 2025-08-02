package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAvroImporter(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	require.NotNil(t, importer)
	defer importer.Close()

	assert.Equal(t, cfg, importer.config)
	assert.NotNil(t, importer.catalog)
	assert.NotNil(t, importer.allocator)
	assert.NotNil(t, importer.writer)
}

func TestAvroImporter_Close(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	require.NotNil(t, importer)

	err = importer.Close()
	assert.NoError(t, err)
}

func TestAvroImporter_InferSchema(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Create a temporary Avro file for testing
	tempDir := t.TempDir()
	avroFile := filepath.Join(tempDir, "test.avro")
	createTestAvroFile(t, avroFile)

	// Test schema inference
	schema, stats, err := importer.InferSchema(avroFile)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.NotNil(t, stats)

	// Verify schema fields (simple users should have 7 fields)
	assert.Equal(t, 7, len(schema.Fields), "Schema should have 7 fields")

	// Check specific field names and types
	expectedFields := map[string]string{
		"id":       "long",
		"name":     "string",
		"email":    "string",
		"age":      "int",
		"active":   "boolean",
		"score":    "double",
		"metadata": "string",
	}

	for _, field := range schema.Fields {
		expectedType, exists := expectedFields[field.Name]
		assert.True(t, exists, "Field %s should exist in expected fields", field.Name)
		assert.Equal(t, expectedType, field.Type, "Field %s should have type %s", field.Name, expectedType)
	}

	// Verify stats
	assert.Equal(t, int64(5), stats.RecordCount, "Should have 5 records")
	assert.Equal(t, 7, stats.ColumnCount, "Should have 7 columns")
	assert.Greater(t, stats.FileSize, int64(0), "File should have non-zero size")
}

func TestAvroImporter_GetTableLocation(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	location := importer.GetTableLocation(tableIdent)

	expected := "file:///tmp/test-warehouse/test_namespace/test_table"
	assert.Equal(t, expected, location)
}

func TestAvroImporter_ImportTable(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Create a temporary Avro file for testing
	tempDir := t.TempDir()
	avroFile := filepath.Join(tempDir, "test.avro")
	createTestAvroFile(t, avroFile)

	// Test import
	ctx := context.Background()
	req := ImportRequest{
		ParquetFile:    avroFile, // Reusing ParquetFile field for Avro file path
		TableIdent:     table.Identifier{"test_namespace", "test_table"},
		NamespaceIdent: table.Identifier{"test_namespace"},
		Overwrite:      false,
	}

	result, err := importer.ImportTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify result
	assert.Equal(t, req.TableIdent, result.TableIdent)
	assert.Equal(t, int64(5), result.RecordCount, "Should have imported 5 records")
	assert.Greater(t, result.DataSize, int64(0), "Should have non-zero data size")
	assert.Contains(t, result.TableLocation, "test_namespace/test_table")
}

func TestAvroImporter_ArrowTypeConversions(t *testing.T) {
	cfg := &config.Config{
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	tests := []struct {
		name           string
		arrowType      arrow.DataType
		expectedSimple string
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, "boolean"},
		{"int32", arrow.PrimitiveTypes.Int32, "int"},
		{"int64", arrow.PrimitiveTypes.Int64, "long"},
		{"float32", arrow.PrimitiveTypes.Float32, "float"},
		{"float64", arrow.PrimitiveTypes.Float64, "double"},
		{"string", arrow.BinaryTypes.String, "string"},
		{"binary", arrow.BinaryTypes.Binary, "binary"},
		{"date32", arrow.FixedWidthTypes.Date32, "date"},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_ns, "timestamp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			simpleType := importer.arrowTypeToSimpleType(tt.arrowType)
			assert.Equal(t, tt.expectedSimple, simpleType)
		})
	}
}

func TestAvroImporter_ConvertArrowSchemaToSimple(t *testing.T) {
	cfg := &config.Config{
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Create a test Arrow schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	// Convert to simple schema
	schema := importer.convertArrowSchemaToSimple(arrowSchema)

	require.Len(t, schema.Fields, 4)

	assert.Equal(t, "id", schema.Fields[0].Name)
	assert.Equal(t, "long", schema.Fields[0].Type)
	assert.False(t, schema.Fields[0].Nullable)

	assert.Equal(t, "name", schema.Fields[1].Name)
	assert.Equal(t, "string", schema.Fields[1].Type)
	assert.True(t, schema.Fields[1].Nullable)

	assert.Equal(t, "score", schema.Fields[2].Name)
	assert.Equal(t, "double", schema.Fields[2].Type)
	assert.True(t, schema.Fields[2].Nullable)

	assert.Equal(t, "active", schema.Fields[3].Name)
	assert.Equal(t, "boolean", schema.Fields[3].Type)
	assert.False(t, schema.Fields[3].Nullable)
}

func TestAvroImporter_ConvertArrowSchemaToIceberg(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Create test Arrow schema
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	// Convert to Iceberg schema
	icebergSchema, err := importer.convertArrowSchemaToIceberg(arrowSchema)
	require.NoError(t, err)
	require.NotNil(t, icebergSchema)

	// Verify schema
	fields := icebergSchema.Fields()
	assert.Len(t, fields, 3)
	assert.Equal(t, "id", fields[0].Name)
	assert.True(t, fields[0].Required)
	assert.Equal(t, "name", fields[1].Name)
	assert.False(t, fields[1].Required)
}

// createTestAvroFile creates a test Avro file for testing
func createTestAvroFile(t *testing.T, filePath string) {
	// Use the simple Avro file we generated
	simpleAvroFile := "../testdata/simple_users.avro"

	// Check if the simple file exists
	if _, err := os.Stat(simpleAvroFile); os.IsNotExist(err) {
		t.Skipf("Simple Avro test file not found at %s", simpleAvroFile)
		return
	}

	// Copy the simple Avro file to the test location
	sourceFile, err := os.Open(simpleAvroFile)
	require.NoError(t, err)
	defer sourceFile.Close()

	destFile, err := os.Create(filePath)
	require.NoError(t, err)
	defer destFile.Close()

	_, err = destFile.ReadFrom(sourceFile)
	require.NoError(t, err)
}

func TestAvroImporter_ReadAvroFile(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Test reading non-existent file
	ctx := context.Background()
	_, err = importer.readAvroFile(ctx, "/nonexistent/file.avro")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestAvroImporter_ImportTable_Integration(t *testing.T) {
	// Skip integration test if not in integration mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "avro_import_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	warehouseDir := filepath.Join(tempDir, "warehouse")
	err = os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	// Create test Avro file
	avroFile := filepath.Join(tempDir, "test.avro")
	createTestAvroFile(t, avroFile)

	cfg := &config.Config{
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: warehouseDir,
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Test table import
	ctx := context.Background()
	req := ImportRequest{
		ParquetFile:    avroFile, // Note: reusing ParquetFile field for Avro file
		TableIdent:     table.Identifier{"test_namespace", "test_table"},
		NamespaceIdent: table.Identifier{"test_namespace"},
		Overwrite:      false,
	}

	result, err := importer.ImportTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check result
	assert.Equal(t, req.TableIdent, result.TableIdent)
	assert.Equal(t, int64(5), result.RecordCount)
	assert.Greater(t, result.DataSize, int64(0))
	assert.Contains(t, result.TableLocation, "test_namespace/test_table")

	// Verify table exists in catalog
	exists, err := importer.catalog.CheckTableExists(ctx, req.TableIdent)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestAvroImporter_ImportTable_Overwrite(t *testing.T) {
	// Skip integration test if not in integration mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "avro_overwrite_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	warehouseDir := filepath.Join(tempDir, "warehouse")
	err = os.MkdirAll(warehouseDir, 0755)
	require.NoError(t, err)

	// Create test Avro file
	avroFile := filepath.Join(tempDir, "test.avro")
	createTestAvroFile(t, avroFile)

	cfg := &config.Config{
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: warehouseDir,
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	ctx := context.Background()
	req := ImportRequest{
		ParquetFile:    avroFile,
		TableIdent:     table.Identifier{"test_namespace", "test_table"},
		NamespaceIdent: table.Identifier{"test_namespace"},
		Overwrite:      false,
	}

	// First import
	result1, err := importer.ImportTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result1)

	// Second import without overwrite should fail
	_, err = importer.ImportTable(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Second import with overwrite should succeed
	req.Overwrite = true
	result2, err := importer.ImportTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result2)

	assert.Equal(t, result1.TableIdent, result2.TableIdent)
	assert.Equal(t, result1.RecordCount, result2.RecordCount)
}

func TestAvroImporter_ErrorHandling(t *testing.T) {
	cfg := &config.Config{
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	importer, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer importer.Close()

	// Test with non-existent file
	_, _, err = importer.InferSchema("/non/existent/file.avro")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to stat file")

	// Test reading non-existent file
	ctx := context.Background()
	_, err = importer.readAvroFile(ctx, "/non/existent/file.avro")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}
