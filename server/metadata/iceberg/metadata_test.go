package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataGenerator_GenerateManifest(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "iceberg_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create mock path manager
	pathManager := &MockPathManager{
		BasePath: tempDir,
	}

	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create metadata generator
	generator := NewMetadataGenerator(pathManager, logger)

	// Create test batch
	batch := BatchInfo{
		ID: "test-batch-1",
		Files: []*regtypes.TableFile{
			{
				ID:                   1,
				TableID:              1,
				FileName:             "test1.parquet",
				FilePath:             "/data/test1.parquet",
				FileSize:             1024,
				FileType:             "PARQUET",
				RowCount:             100,
				IcebergMetadataState: "pending",
			},
			{
				ID:                   2,
				TableID:              1,
				FileName:             "test2.parquet",
				FilePath:             "/data/test2.parquet",
				FileSize:             2048,
				FileType:             "PARQUET",
				RowCount:             200,
				IcebergMetadataState: "pending",
			},
		},
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	// Create test table info
	tableInfo := &registry.CompleteTableInfo{
		Database: "default",
		Table: &regtypes.Table{
			ID:   1,
			Name: "test_table",
		},
	}

	// Generate manifest
	manifestPath, err := generator.GenerateManifest(context.Background(), batch, tableInfo)
	require.NoError(t, err)
	assert.NotEmpty(t, manifestPath)

	// Verify manifest file exists
	assert.FileExists(t, manifestPath)

	// Verify manifest content
	manifestBytes, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	assert.Contains(t, string(manifestBytes), "manifest_type")
	assert.Contains(t, string(manifestBytes), "test1.parquet")
	assert.Contains(t, string(manifestBytes), "test2.parquet")
}

func TestMetadataGenerator_UpdateMetadataFile(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "iceberg_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create mock path manager
	pathManager := &MockPathManager{
		BasePath: tempDir,
	}

	// Create logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create metadata generator
	generator := NewMetadataGenerator(pathManager, logger)

	// Create test batch
	batch := BatchInfo{
		ID: "test-batch-1",
		Files: []*regtypes.TableFile{
			{
				ID:                   1,
				TableID:              1,
				FileName:             "test1.parquet",
				FilePath:             "/data/test1.parquet",
				FileSize:             1024,
				FileType:             "PARQUET",
				RowCount:             100,
				IcebergMetadataState: "pending",
			},
		},
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	// Create test table info
	tableInfo := &registry.CompleteTableInfo{
		Database: "default",
		Table: &regtypes.Table{
			ID:   1,
			Name: "test_table",
		},
	}

	// Update metadata file
	err = generator.UpdateMetadataFile(context.Background(), batch, "/manifests/test.avro", tableInfo)
	require.NoError(t, err)

	// Verify metadata directory was created
	metadataDir := filepath.Join(tempDir, "tables", "default", "test_table", "metadata")
	assert.DirExists(t, metadataDir)

	// Verify metadata file exists
	files, err := os.ReadDir(metadataDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 0)

	// Verify metadata content
	metadataFile := filepath.Join(metadataDir, files[0].Name())
	metadataBytes, err := os.ReadFile(metadataFile)
	require.NoError(t, err)
	assert.Contains(t, string(metadataBytes), "snapshot_id")
	assert.Contains(t, string(metadataBytes), "operation")
	assert.Contains(t, string(metadataBytes), "append")
}

// MockPathManager implements paths.PathManager for testing
type MockPathManager struct {
	BasePath string
}

func (m *MockPathManager) GetBasePath() string {
	return m.BasePath
}

func (m *MockPathManager) GetCatalogPath() string {
	return filepath.Join(m.BasePath, "catalog")
}

func (m *MockPathManager) GetDataPath() string {
	return filepath.Join(m.BasePath, "data")
}

func (m *MockPathManager) GetInternalMetadataPath() string {
	return filepath.Join(m.BasePath, "internal")
}

func (m *MockPathManager) GetInternalMetadataDBPath() string {
	return filepath.Join(m.BasePath, "internal", "metadata.db")
}

func (m *MockPathManager) GetMigrationsPath() string {
	return filepath.Join(m.BasePath, "migrations")
}

func (m *MockPathManager) GetCatalogURI(catalogType string) string {
	return filepath.Join(m.BasePath, "catalog", catalogType)
}

func (m *MockPathManager) GetTablePath(database, tableName string) string {
	return filepath.Join(m.BasePath, "tables", database, tableName)
}

func (m *MockPathManager) GetTableDataPath(namespace []string, tableName string) string {
	nsPath := filepath.Join(namespace...)
	return filepath.Join(m.BasePath, "tables", nsPath, tableName, "data")
}

func (m *MockPathManager) GetTableMetadataPath(namespace []string, tableName string) string {
	nsPath := filepath.Join(namespace...)
	return filepath.Join(m.BasePath, "tables", nsPath, tableName, "metadata")
}

func (m *MockPathManager) GetTableMetadataFile(database, tableName string, version int) string {
	return filepath.Join(m.BasePath, "tables", database, tableName, "metadata", fmt.Sprintf("metadata-%d.json", version))
}

func (m *MockPathManager) GetViewMetadataPath(namespace []string, viewName string) string {
	nsPath := filepath.Join(namespace...)
	return filepath.Join(m.BasePath, "views", nsPath, viewName)
}

func (m *MockPathManager) GetParquetDataPath(database, tableName string) string {
	return filepath.Join(m.BasePath, "tables", database, tableName, "data", "parquet")
}

func (m *MockPathManager) GetParquetFileName(timestamp string, fileCount int) string {
	return fmt.Sprintf("%s_%d.parquet", timestamp, fileCount)
}

func (m *MockPathManager) GetParquetFilePath(database, tableName, filename string) string {
	return filepath.Join(m.BasePath, "tables", database, tableName, "data", "parquet", filename)
}

func (m *MockPathManager) GetParquetFilePattern(database, tableName string) string {
	return filepath.Join(m.BasePath, "tables", database, tableName, "data", "parquet", "*.parquet")
}

func (m *MockPathManager) GetNamespacePath(namespace []string) string {
	return filepath.Join(m.BasePath, "namespaces", filepath.Join(namespace...))
}

func (m *MockPathManager) GetMetadataDir() string {
	return filepath.Join(m.BasePath, "metadata")
}

func (m *MockPathManager) GetDataDir() string {
	return filepath.Join(m.BasePath, "data")
}

func (m *MockPathManager) EnsureDirectoryStructure() error {
	return os.MkdirAll(m.BasePath, 0755)
}

func (m *MockPathManager) GetTableManifestPath(namespace []string, tableName string) string {
	nsPath := filepath.Join(namespace...)
	return filepath.Join(m.BasePath, "tables", nsPath, tableName, "manifests")
}
