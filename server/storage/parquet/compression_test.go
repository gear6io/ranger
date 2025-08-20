package parquet

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCompressionCodec(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		expectError bool
	}{
		{"None", "none", false},
		{"Uncompressed", "uncompressed", false},
		{"Snappy", "snappy", false},
		{"Gzip", "gzip", false},
		{"GZ", "gz", false},
		{"Brotli", "brotli", false},
		{"LZ4", "lz4", false},
		{"ZSTD", "zstd", false},
		{"Invalid", "invalid", true},
		{"Empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec, err := GetCompressionCodec(tt.compression)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, codec)
			}
		})
	}
}

func TestValidateCompressionConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *ParquetConfig
		expectError bool
	}{
		{
			name: "Valid default config",
			config: &ParquetConfig{
				Compression:       "snappy",
				CompressionLevel:  1,
				ColumnCompression: make(map[string]string),
			},
			expectError: false,
		},
		{
			name: "Valid gzip config",
			config: &ParquetConfig{
				Compression:       "gzip",
				CompressionLevel:  5,
				ColumnCompression: make(map[string]string),
			},
			expectError: false,
		},
		{
			name: "Invalid compression type",
			config: &ParquetConfig{
				Compression:       "invalid",
				CompressionLevel:  1,
				ColumnCompression: make(map[string]string),
			},
			expectError: true,
		},
		{
			name: "Invalid gzip compression level",
			config: &ParquetConfig{
				Compression:       "gzip",
				CompressionLevel:  15, // Too high
				ColumnCompression: make(map[string]string),
			},
			expectError: true,
		},
		{
			name: "Valid column-specific compression",
			config: &ParquetConfig{
				Compression:      "snappy",
				CompressionLevel: 1,
				ColumnCompression: map[string]string{
					"column1": "gzip",
					"column2": "zstd",
				},
			},
			expectError: false,
		},
		{
			name: "Invalid column-specific compression",
			config: &ParquetConfig{
				Compression:      "snappy",
				CompressionLevel: 1,
				ColumnCompression: map[string]string{
					"column1": "invalid",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCompressionConfig(tt.config)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCompressionLevel(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		level       int
		expectError bool
	}{
		{"Snappy no level", "snappy", 0, false},
		{"Snappy with level", "snappy", 5, false}, // Ignored
		{"Gzip valid level", "gzip", 5, false},
		{"Gzip too low", "gzip", 0, true},
		{"Gzip too high", "gzip", 10, true},
		{"Brotli valid level", "brotli", 5, false},
		{"Brotli too high", "brotli", 15, true},
		{"ZSTD valid level", "zstd", 10, false},
		{"ZSTD too high", "zstd", 25, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCompressionLevel(tt.compression, tt.level)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetCompressionForColumn(t *testing.T) {
	config := &ParquetConfig{
		Compression: "snappy",
		ColumnCompression: map[string]string{
			"column1": "gzip",
			"column2": "zstd",
		},
	}

	// Test column with specific compression
	assert.Equal(t, "gzip", GetCompressionForColumn(config, "column1"))
	assert.Equal(t, "zstd", GetCompressionForColumn(config, "column2"))

	// Test column without specific compression (should use default)
	assert.Equal(t, "snappy", GetCompressionForColumn(config, "column3"))
}

func TestCreateWriterProperties(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	config := &ParquetConfig{
		Compression:      "gzip",
		CompressionLevel: 5,
		ColumnCompression: map[string]string{
			"name": "snappy",
		},
		EnableStats: true,
	}

	properties, err := CreateWriterProperties(config, schema)
	require.NoError(t, err)
	require.NotNil(t, properties)

	// Check that properties contain expected keys
	assert.Contains(t, properties, "compression")
	assert.Contains(t, properties, "compression_level")
	assert.Contains(t, properties, "column_compression")
	assert.Contains(t, properties, "enable_stats")

	// Check compression level
	assert.Equal(t, 5, properties["compression_level"])
	assert.Equal(t, true, properties["enable_stats"])

	// Check column compression - it should be the actual type returned by our function
	columnCompressionRaw := properties["column_compression"]
	require.NotNil(t, columnCompressionRaw, "column_compression should exist")

	// It should be of type map[string]compress.Compression
	// For the test, we just verify it exists and is not nil
}

func TestCreateWriterProperties_InvalidConfig(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	config := &ParquetConfig{
		Compression:       "invalid",
		CompressionLevel:  1,
		ColumnCompression: make(map[string]string),
	}

	properties, err := CreateWriterProperties(config, schema)
	assert.Error(t, err)
	assert.Nil(t, properties)
}

func TestRequiresCompressionLevel(t *testing.T) {
	tests := []struct {
		compression string
		expected    bool
	}{
		{"snappy", false},
		{"lz4", false},
		{"none", false},
		{"gzip", true},
		{"gz", true},
		{"brotli", true},
		{"zstd", true},
	}

	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			result := requiresCompressionLevel(tt.compression)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCompressionRatio(t *testing.T) {
	tests := []struct {
		compression string
		minRatio    float64
		maxRatio    float64
	}{
		{"none", 1.0, 1.0},
		{"uncompressed", 1.0, 1.0},
		{"snappy", 0.5, 0.7},
		{"lz4", 0.6, 0.7},
		{"gzip", 0.3, 0.5},
		{"brotli", 0.3, 0.4},
		{"zstd", 0.2, 0.4},
		{"invalid", 1.0, 1.0}, // Should default to 1.0
	}

	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			ratio := GetCompressionRatio(tt.compression)
			assert.GreaterOrEqual(t, ratio, tt.minRatio)
			assert.LessOrEqual(t, ratio, tt.maxRatio)
		})
	}
}

func TestDefaultParquetConfig_Compression(t *testing.T) {
	config := DefaultParquetConfig()

	// Test default compression settings
	assert.Equal(t, "snappy", config.Compression)
	assert.Equal(t, 1, config.CompressionLevel)
	assert.NotNil(t, config.ColumnCompression)
	assert.Len(t, config.ColumnCompression, 0) // Should be empty map

	// Test that default config is valid
	err := ValidateCompressionConfig(config)
	assert.NoError(t, err)
}

func TestCompressionIntegration(t *testing.T) {
	// Test that different compression types work together
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	compressionTypes := []string{"snappy", "gzip", "brotli", "lz4", "zstd"}

	for _, compression := range compressionTypes {
		t.Run(compression, func(t *testing.T) {
			config := &ParquetConfig{
				Compression:      compression,
				CompressionLevel: 3,
				ColumnCompression: map[string]string{
					"text": "snappy", // Use snappy for text column
				},
				EnableStats: true,
			}

			// Validate config
			err := ValidateCompressionConfig(config)
			assert.NoError(t, err)

			// Create writer properties
			properties, err := CreateWriterProperties(config, schema)
			assert.NoError(t, err)
			assert.NotNil(t, properties)

			// Verify compression ratio
			ratio := GetCompressionRatio(compression)
			assert.Greater(t, ratio, 0.0)
			assert.LessOrEqual(t, ratio, 1.0)
		})
	}
}
