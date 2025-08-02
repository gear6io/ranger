package importer

import (
	"testing"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewImporterFactory(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	factory := NewImporterFactory(cfg)
	require.NotNil(t, factory)
	assert.Equal(t, cfg, factory.config)
}

func TestImporterFactory_CreateImporter(t *testing.T) {
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

	factory := NewImporterFactory(cfg)

	// Test Parquet importer creation
	parquetImporter, importerType, err := factory.CreateImporter("test.parquet")
	require.NoError(t, err)
	require.NotNil(t, parquetImporter)
	assert.Equal(t, ImporterTypeParquet, importerType)
	defer parquetImporter.Close()

	// Test Avro importer creation
	avroImporter, importerType, err := factory.CreateImporter("test.avro")
	require.NoError(t, err)
	require.NotNil(t, avroImporter)
	assert.Equal(t, ImporterTypeAvro, importerType)
	defer avroImporter.Close()

	// Test unsupported importer type
	_, _, err = factory.CreateImporter("test.csv")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported file format")
}

func TestImporterFactory_CreateImporterByType(t *testing.T) {
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

	factory := NewImporterFactory(cfg)

	// Test Parquet importer creation by type
	parquetImporter, err := factory.CreateImporterByType(ImporterTypeParquet)
	require.NoError(t, err)
	require.NotNil(t, parquetImporter)
	defer parquetImporter.Close()

	// Test Avro importer creation by type
	avroImporter, err := factory.CreateImporterByType(ImporterTypeAvro)
	require.NoError(t, err)
	require.NotNil(t, avroImporter)
	defer avroImporter.Close()

	// Test unsupported importer type
	_, err = factory.CreateImporterByType("unsupported")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported importer type")
}

func TestImporterFactory_GetImporterForFile(t *testing.T) {
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

	factory := NewImporterFactory(cfg)

	tests := []struct {
		filename     string
		expectedType ImporterType
		shouldError  bool
	}{
		{"data.parquet", ImporterTypeParquet, false},
		{"data.PARQUET", ImporterTypeParquet, false},
		{"data.avro", ImporterTypeAvro, false},
		{"data.AVRO", ImporterTypeAvro, false},
		{"data.txt", "", true},
		{"data", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			importer, importerType, err := factory.CreateImporter(tt.filename)
			if tt.shouldError {
				assert.Error(t, err)
				assert.Nil(t, importer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, importer)
				assert.Equal(t, tt.expectedType, importerType)
				defer importer.Close()
			}
		})
	}
}

func TestImporterFactory_DetectFileType(t *testing.T) {
	factory := &ImporterFactory{}

	tests := []struct {
		filename     string
		expectedType ImporterType
		shouldError  bool
	}{
		{"test.parquet", ImporterTypeParquet, false},
		{"test.PARQUET", ImporterTypeParquet, false},
		{"test.avro", ImporterTypeAvro, false},
		{"test.AVRO", ImporterTypeAvro, false},
		{"test.txt", "", true},
		{"test", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			fileType, err := factory.DetectFileType(tt.filename)
			if tt.shouldError {
				assert.Error(t, err)
				assert.Equal(t, ImporterType(""), fileType)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedType, fileType)
			}
		})
	}
}

func TestImporterFactory_GetSupportedFormats(t *testing.T) {
	cfg := &config.Config{}
	factory := NewImporterFactory(cfg)

	formats := factory.GetSupportedFormats()

	assert.Len(t, formats, 2)
	assert.Contains(t, formats, ".parquet")
	assert.Contains(t, formats, ".avro")
}

func TestImporterTypes(t *testing.T) {
	// Test that the constants are defined correctly
	assert.Equal(t, ImporterType("parquet"), ImporterTypeParquet)
	assert.Equal(t, ImporterType("avro"), ImporterTypeAvro)
}

func TestImporterInterface(t *testing.T) {
	// Test that both importers implement the Importer interface
	cfg := &config.Config{
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

	// Test ParquetImporter implements Importer
	parquetImporter, err := NewParquetImporter(cfg)
	require.NoError(t, err)
	defer parquetImporter.Close()

	var _ Importer = parquetImporter

	// Test AvroImporter implements Importer
	avroImporter, err := NewAvroImporter(cfg)
	require.NoError(t, err)
	defer avroImporter.Close()

	var _ Importer = avroImporter
}
