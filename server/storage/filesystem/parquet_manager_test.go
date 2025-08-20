package filesystem

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/icebox/server/storage/parquet"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParquetManager(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Test with default config
	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	assert.NotNil(t, pm)
	assert.Equal(t, schema, pm.schema)
	assert.NotNil(t, pm.config)
	assert.Equal(t, tempDir, pm.basePath)
	assert.Equal(t, int64(0), pm.GetRowCount())

	// Test with custom config
	config := &parquet.ParquetConfig{
		MaxFileSize:     1024,
		RotationTimeout: 60,
	}
	pm2, err := NewParquetManager(schema, config, tempDir)
	require.NoError(t, err)
	defer pm2.Close()

	assert.Equal(t, config, pm2.config)
}

func TestParquetManager_StoreData_ValidData(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Valid data
	data := [][]interface{}{
		{int64(1), "Alice", true},
		{int64(2), "Bob", false},
		{int64(3), nil, true},
	}

	err = pm.StoreData(data)
	require.NoError(t, err)

	// Verify stats
	stats := pm.GetStats()
	assert.Equal(t, int64(3), stats.RowsWritten)
	assert.Equal(t, int64(3), pm.GetRowCount())

	// Verify file was created
	files, err := pm.GetFiles()
	require.NoError(t, err)
	assert.Len(t, files, 1)
	assert.Greater(t, files[0].Size, int64(0))
}

func TestParquetManager_StoreData_InvalidData(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Invalid data - wrong number of columns
	data := [][]interface{}{
		{int64(1), "Alice", "extra"},
		{int64(2), "Bob"},
	}

	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 2")

	// Invalid data - wrong type
	data = [][]interface{}{
		{int64(1), "Alice"},
		{"invalid", "Bob"}, // id should be int64
	}

	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects int64")

	// Invalid data - null in required field
	data = [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), nil}, // name is required
	}

	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be null")
}

func TestParquetManager_StoreData_EmptyData(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Empty data should be valid
	data := [][]interface{}{}
	err = pm.StoreData(data)
	assert.NoError(t, err)

	// Verify no rows were written
	assert.Equal(t, int64(0), pm.GetRowCount())
}

func TestParquetManager_FileRotation_SizeLimit(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Create config with very small file size limit
	config := &parquet.ParquetConfig{
		MaxFileSize:       100, // Very small limit to force rotation
		RotationTimeout:   300,
		Compression:       "snappy", // Add compression to avoid validation error
		ColumnCompression: make(map[string]string),
	}

	pm, err := NewParquetManager(schema, config, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Add data multiple times to trigger file rotation
	for i := 0; i < 10; i++ {
		data := [][]interface{}{
			{int64(i), "This is a longer string to increase file size"},
		}
		err = pm.StoreData(data)
		require.NoError(t, err)
	}

	// Verify multiple files were created
	files, err := pm.GetFiles()
	require.NoError(t, err)
	assert.Greater(t, len(files), 1, "Expected multiple files due to rotation")
}

func TestParquetManager_FileRotation_TimeoutLimit(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Create config with very short timeout
	config := &parquet.ParquetConfig{
		MaxFileSize:       50 << 30, // 50GB (won't be reached)
		RotationTimeout:   1,        // 1 second timeout
		Compression:       "snappy", // Add compression to avoid validation error
		ColumnCompression: make(map[string]string),
	}

	pm, err := NewParquetManager(schema, config, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Add some data
	data := [][]interface{}{
		{int64(1)},
	}
	err = pm.StoreData(data)
	require.NoError(t, err)

	// Wait for timeout
	time.Sleep(2 * time.Second)

	// Add more data to trigger timeout rotation check
	data = [][]interface{}{
		{int64(2)},
	}
	err = pm.StoreData(data)
	require.NoError(t, err)

	// Verify files were created
	files, err := pm.GetFiles()
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "Expected files to be created")
}

func TestParquetManager_GetFiles(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Initially no files
	files, err := pm.GetFiles()
	require.NoError(t, err)
	assert.Len(t, files, 0)

	// Store some data
	data := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
	}

	err = pm.StoreData(data)
	require.NoError(t, err)

	// Verify file info
	files, err = pm.GetFiles()
	require.NoError(t, err)
	assert.Len(t, files, 1)

	fileInfo := files[0]
	assert.Greater(t, fileInfo.Size, int64(0))
	assert.NotEmpty(t, fileInfo.Path)
	assert.Greater(t, fileInfo.Created, int64(0))
	assert.Equal(t, schema, fileInfo.Schema)
}

func TestParquetManager_GetStats(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Initial stats
	initialStats := pm.GetStats()
	assert.Equal(t, int64(0), initialStats.RowsWritten)
	assert.Equal(t, int64(0), initialStats.BytesWritten)

	// Store data
	data := [][]interface{}{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	}

	err = pm.StoreData(data)
	require.NoError(t, err)

	// Updated stats
	updatedStats := pm.GetStats()
	assert.Equal(t, int64(3), updatedStats.RowsWritten)
	assert.Greater(t, updatedStats.WriteDuration, int64(0))
}

func TestParquetManager_Close(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)

	// Store some data
	data := [][]interface{}{
		{int64(1)},
		{int64(2)},
	}

	err = pm.StoreData(data)
	require.NoError(t, err)

	// Close the manager
	err = pm.Close()
	assert.NoError(t, err)

	// Verify it's closed
	assert.True(t, pm.closed)

	// Try to store data after closing
	err = pm.StoreData(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parquet manager is closed")

	// Verify files were properly closed and written
	files, err := filepath.Glob(filepath.Join(tempDir, "*.parquet"))
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "Expected Parquet files to be written")

	// Verify file is not empty
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0), "Parquet file should not be empty")
	}
}

func TestParquetManager_Integration(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Test integration with filesystem storage
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "message", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	pm, err := NewParquetManager(schema, nil, tempDir)
	require.NoError(t, err)
	defer pm.Close()

	// Store various types of data
	testData := [][]interface{}{
		{int64(1), "Hello, World!", 95.5},
		{int64(2), "This is a test message", 87.2},
		{int64(3), "Another test message", nil},
		{int64(4), "Final test message", 100.0},
	}

	err = pm.StoreData(testData)
	require.NoError(t, err)

	// Verify stats
	stats := pm.GetStats()
	assert.Equal(t, int64(len(testData)), stats.RowsWritten)
	assert.Greater(t, stats.WriteDuration, int64(0))

	// Verify file was created
	files, err := pm.GetFiles()
	require.NoError(t, err)
	assert.Len(t, files, 1)
	assert.Greater(t, files[0].Size, int64(0))

	// Close and verify final state
	err = pm.Close()
	require.NoError(t, err)

	// Verify the Parquet file exists and has content
	parquetFiles, err := filepath.Glob(filepath.Join(tempDir, "*.parquet"))
	require.NoError(t, err)
	assert.Len(t, parquetFiles, 1)

	fileInfo, err := os.Stat(parquetFiles[0])
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(100), "Parquet file should have reasonable size")
}

func TestParquetManager_Compression(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := ioutil.TempDir("", "parquet_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Test different compression algorithms
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	compressionTypes := []string{"snappy", "gzip", "brotli", "lz4", "zstd"}

	for _, compression := range compressionTypes {
		t.Run(compression, func(t *testing.T) {
			subDir := filepath.Join(tempDir, compression)
			err := os.MkdirAll(subDir, 0755)
			require.NoError(t, err)

			// Create config with specific compression
			config := &parquet.ParquetConfig{
				MaxFileSize:      50 << 30, // 50GB
				RotationTimeout:  300,      // 5 minutes
				Compression:      compression,
				CompressionLevel: 3,
				ColumnCompression: map[string]string{
					"text": "snappy", // Use snappy for text column
				},
				EnableStats: true,
			}

			pm, err := NewParquetManager(schema, config, subDir)
			require.NoError(t, err)
			defer pm.Close()

			// Store test data with repeated text to test compression
			testData := [][]interface{}{
				{int64(1), "This is a repeated message for compression testing", "Additional data"},
				{int64(2), "This is a repeated message for compression testing", "More additional data"},
				{int64(3), "This is a repeated message for compression testing", "Even more data"},
				{int64(4), "This is a repeated message for compression testing", nil},
			}

			err = pm.StoreData(testData)
			require.NoError(t, err)

			// Verify stats
			stats := pm.GetStats()
			assert.Equal(t, int64(len(testData)), stats.RowsWritten)
			assert.Greater(t, stats.WriteDuration, int64(0))

			// Verify compression ratio is set
			expectedRatio := parquet.GetCompressionRatio(compression)
			assert.Equal(t, expectedRatio, stats.CompressionRatio)

			// Verify file was created
			files, err := pm.GetFiles()
			require.NoError(t, err)
			assert.Len(t, files, 1)
			assert.Greater(t, files[0].Size, int64(0))

			// Close and verify final state
			err = pm.Close()
			require.NoError(t, err)

			// Verify the Parquet file exists
			parquetFiles, err := filepath.Glob(filepath.Join(subDir, "*.parquet"))
			require.NoError(t, err)
			assert.Len(t, parquetFiles, 1)

			fileInfo, err := os.Stat(parquetFiles[0])
			require.NoError(t, err)
			assert.Greater(t, fileInfo.Size(), int64(100), "Parquet file should have reasonable size")
		})
	}
}
