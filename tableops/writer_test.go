package tableops

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWriter(t *testing.T) {
	// Create test configuration
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

	// Create catalog using factory
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)
	defer cat.Close()

	// Create writer
	writer := NewWriter(cat)
	assert.NotNil(t, writer)
	assert.Equal(t, cat, writer.catalog)
	assert.NotNil(t, writer.allocator)
}

func TestDefaultWriteOptions(t *testing.T) {
	opts := DefaultWriteOptions()
	assert.NotNil(t, opts)
	assert.NotNil(t, opts.SnapshotProperties)
	assert.Equal(t, int64(1000), opts.BatchSize)
	assert.False(t, opts.Overwrite)
}

func TestWriteOptionsSetTimestamp(t *testing.T) {
	opts := DefaultWriteOptions()

	// Initially no timestamp
	_, exists := opts.SnapshotProperties["icebox.write.timestamp"]
	assert.False(t, exists)
}

func TestReadParquetFileNotExists(t *testing.T) {
	// Create test configuration
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

	// Create catalog and writer
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)
	defer cat.Close()

	writer := NewWriter(cat)

	// Test reading non-existent file
	ctx := context.Background()
	_, err = writer.readParquetFile(ctx, "/nonexistent/file.parquet")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTableWriter(t *testing.T) {
	// Create test configuration
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

	// Create catalog and writer
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)
	defer cat.Close()

	writer := NewWriter(cat)

	// Test that GetTableWriter returns error for non-existent table
	ctx := context.Background()
	identifier := table.Identifier{"test", "table"}

	_, err = writer.GetTableWriter(ctx, identifier)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load table")
}

// Test that demonstrates the structure is in place for future full implementation
func TestWriterStructure(t *testing.T) {
	// Create test configuration
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

	// Create catalog and writer
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)
	defer cat.Close()

	writer := NewWriter(cat)

	// Verify all expected methods exist (this will compile if the structure is correct)
	assert.NotNil(t, writer.WriteArrowTable)
	assert.NotNil(t, writer.WriteRecordReader)
	assert.NotNil(t, writer.WriteParquetFile)
	assert.NotNil(t, writer.GetTableWriter)

	// Test write options
	opts := &WriteOptions{
		SnapshotProperties: make(iceberg.Properties),
		BatchSize:          500,
		Overwrite:          true,
	}

	assert.NotNil(t, opts.SnapshotProperties)
	assert.Equal(t, int64(500), opts.BatchSize)
	assert.True(t, opts.Overwrite)
}

func TestWriteOverwriteLimitations(t *testing.T) {
	// Create test configuration
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

	// Create catalog and writer
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)
	defer cat.Close()

	writer := NewWriter(cat)
	ctx := context.Background()

	// Create test arrow table
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	builder.AppendValues([]int64{1, 2, 3}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: "test", Type: arrow.PrimitiveTypes.Int64}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumn(field, arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr}))
	columns := []arrow.Column{*col}
	arrowTable := array.NewTable(schema, columns, -1)
	defer arrowTable.Release()
	defer col.Release()

	// Test record reader overwrite limitation
	reader := array.NewTableReader(arrowTable, 1000)
	defer reader.Release()

	// Create write options with overwrite enabled
	opts := &WriteOptions{
		SnapshotProperties: make(iceberg.Properties),
		BatchSize:          1000,
		Overwrite:          true,
	}

	// This should return an error because overwrite for RecordReader is not implemented
	err = writer.WriteRecordReader(ctx, nil, reader, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overwrite mode for RecordReader is not yet fully implemented")

	// Test parquet file not exists
	err = writer.WriteParquetFile(ctx, nil, "/test/file.parquet", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}
