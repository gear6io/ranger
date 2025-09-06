package parquet

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/gear6io/ranger/pkg/errors"
)

// Package-specific error codes for parquet compression
var (
	ParquetCompressionUnsupportedType = errors.MustNewCode("parquet.compression_unsupported_type")
	ParquetCompressionInvalidType     = errors.MustNewCode("parquet.compression_invalid_type")
	ParquetCompressionInvalidLevel    = errors.MustNewCode("parquet.compression_invalid_level")
	ParquetCompressionFailed          = errors.MustNewCode("parquet.compression_failed")
)

// CompressionType represents supported compression algorithms
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionSnappy CompressionType = "snappy"
	CompressionGzip   CompressionType = "gzip"
	CompressionBrotli CompressionType = "brotli"
	CompressionLZ4    CompressionType = "lz4"
	CompressionZSTD   CompressionType = "zstd"
)

// CompressionConfig holds compression settings for Parquet writing
type CompressionConfig struct {
	Algorithm CompressionType
	Level     int
}

// GetCompressionCodec converts compression string to Parquet compression codec
func GetCompressionCodec(compression string) (compress.Compression, error) {
	switch strings.ToLower(compression) {
	case "none", "uncompressed":
		return compress.Codecs.Uncompressed, nil
	case "snappy":
		return compress.Codecs.Snappy, nil
	case "gzip", "gz":
		return compress.Codecs.Gzip, nil
	case "brotli":
		return compress.Codecs.Brotli, nil
	case "lz4":
		return compress.Codecs.Lz4, nil
	case "zstd":
		return compress.Codecs.Zstd, nil
	default:
		return compress.Codecs.Uncompressed, errors.New(ParquetCompressionUnsupportedType, "unsupported compression type", nil).AddContext("compression", compression)
	}
}

// ValidateCompressionConfig validates compression configuration
func ValidateCompressionConfig(config *ParquetConfig) error {
	// Validate main compression
	if _, err := GetCompressionCodec(config.Compression); err != nil {
		return err
	}

	// Validate compression level
	if err := validateCompressionLevel(config.Compression, config.CompressionLevel); err != nil {
		return err
	}

	// Validate column-specific compression
	for column, compression := range config.ColumnCompression {
		if _, err := GetCompressionCodec(compression); err != nil {
			return errors.AddContext(err, "column", column)
		}
	}

	return nil
}

// validateCompressionLevel checks if compression level is valid for the algorithm
func validateCompressionLevel(compression string, level int) error {
	switch strings.ToLower(compression) {
	case "none", "uncompressed", "snappy", "lz4":
		// These don't use compression levels
		return nil
	case "gzip", "gz":
		if level < 1 || level > 9 {
			return errors.New(ParquetCompressionInvalidLevel, "gzip compression level must be between 1 and 9", nil).AddContext("level", fmt.Sprintf("%d", level)).AddContext("compression", "gzip")
		}
	case "brotli":
		if level < 1 || level > 11 {
			return errors.New(ParquetCompressionInvalidLevel, "brotli compression level must be between 1 and 11", nil).AddContext("level", fmt.Sprintf("%d", level)).AddContext("compression", "brotli")
		}
	case "zstd":
		if level < 1 || level > 22 {
			return errors.New(ParquetCompressionInvalidLevel, "zstd compression level must be between 1 and 22", nil).AddContext("level", fmt.Sprintf("%d", level)).AddContext("compression", "zstd")
		}
	}
	return nil
}

// GetCompressionForColumn returns the compression type for a specific column
func GetCompressionForColumn(config *ParquetConfig, columnName string) string {
	if columnCompression, exists := config.ColumnCompression[columnName]; exists {
		return columnCompression
	}
	return config.Compression
}

// CreateWriterProperties creates Parquet writer properties with compression settings
func CreateWriterProperties(config *ParquetConfig, schema *arrow.Schema) (map[string]interface{}, error) {
	// Validate compression config first
	if err := ValidateCompressionConfig(config); err != nil {
		return nil, err
	}

	// For now, return a simple map with compression settings
	// This will be used by the ParquetManager to configure the writer
	properties := make(map[string]interface{})

	// Set default compression
	defaultCodec, err := GetCompressionCodec(config.Compression)
	if err != nil {
		return nil, err
	}
	properties["compression"] = defaultCodec

	// Set compression level if applicable
	if config.CompressionLevel > 0 {
		properties["compression_level"] = config.CompressionLevel
	}

	// Set column-specific compression
	columnCompression := make(map[string]compress.Compression)
	for _, field := range schema.Fields() {
		columnName := field.Name
		compression := GetCompressionForColumn(config, columnName)

		codec, err := GetCompressionCodec(compression)
		if err != nil {
			return nil, err
		}

		columnCompression[columnName] = codec
	}
	properties["column_compression"] = columnCompression

	// Additional performance settings
	properties["enable_stats"] = config.EnableStats

	return properties, nil
}

// requiresCompressionLevel checks if a compression algorithm uses compression levels
func requiresCompressionLevel(compression string) bool {
	switch strings.ToLower(compression) {
	case "gzip", "gz", "brotli", "zstd":
		return true
	default:
		return false
	}
}

// GetCompressionRatio estimates compression ratio for different algorithms
func GetCompressionRatio(compression string) float64 {
	switch strings.ToLower(compression) {
	case "none", "uncompressed":
		return 1.0
	case "snappy":
		return 0.6 // ~40% compression
	case "lz4":
		return 0.65 // ~35% compression
	case "gzip", "gz":
		return 0.4 // ~60% compression
	case "brotli":
		return 0.35 // ~65% compression
	case "zstd":
		return 0.3 // ~70% compression
	default:
		return 1.0
	}
}
