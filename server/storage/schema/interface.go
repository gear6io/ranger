package schema

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// ParquetWriter defines the interface for writing Parquet data
type ParquetWriter interface {
	// WriteBatch writes a batch of data to Parquet format
	WriteBatch(data [][]interface{}, schema *arrow.Schema) error

	// WriteStream writes streaming data to Parquet format
	WriteStream(data <-chan [][]interface{}, schema *arrow.Schema) error

	// Close closes the writer and flushes any remaining data
	Close() error

	// GetStats returns writing statistics
	GetStats() *WriteStats
}

// SchemaValidator defines the interface for validating data against schemas
type SchemaValidator interface {
	// ValidateData validates data against a given schema
	ValidateData(data [][]interface{}, schema *arrow.Schema) error

	// ValidateBatch validates a single batch of data
	ValidateBatch(batch [][]interface{}, schema *arrow.Schema) error

	// GetValidationErrors returns detailed validation errors
	GetValidationErrors() []ValidationError
}

// FileManager defines the interface for managing Parquet files
type FileManager interface {
	// CreateFile creates a new Parquet file
	CreateFile(path string, schema *arrow.Schema) (ParquetWriter, error)

	// OpenFile opens an existing Parquet file for reading
	OpenFile(path string) (ParquetReader, error)

	// GetFileInfo returns information about a Parquet file
	GetFileInfo(path string) (*FileInfo, error)

	// ListFiles lists all Parquet files in a directory
	ListFiles(directory string) ([]*FileInfo, error)
}

// ParquetReader defines the interface for reading Parquet data
type ParquetReader interface {
	// ReadBatch reads a batch of data from Parquet format
	ReadBatch(batchSize int) ([][]interface{}, error)

	// ReadAll reads all data from the Parquet file
	ReadAll() ([][]interface{}, error)

	// GetSchema returns the schema of the Parquet file
	GetSchema() *arrow.Schema

	// Close closes the reader
	Close() error
}

// WriteStats contains statistics about Parquet writing operations
type WriteStats struct {
	RowsWritten      int64
	BytesWritten     int64
	WriteDuration    int64 // nanoseconds
	CompressionRatio float64
	MemoryUsage      int64 // bytes
}

// FileInfo contains information about a Parquet file
type FileInfo struct {
	Path        string
	Size        int64
	RowCount    int64
	Schema      *arrow.Schema
	Created     int64 // Unix timestamp
	Modified    int64 // Unix timestamp
	Compression string
	Version     string
}

// ValidationError represents a validation error
type ValidationError struct {
	RowIndex     int
	ColumnIndex  int
	ColumnName   string
	Value        interface{}
	ExpectedType string
	Message      string
}

// Error implements the error interface
func (ve ValidationError) Error() string {
	return ve.Message
}

// ParquetConfig holds configuration for Parquet operations
type ParquetConfig struct {
	// Memory settings
	MaxMemoryUsage   int64 // bytes
	BatchSize        int
	StreamingEnabled bool
	ChunkSize        int

	// Compression settings
	Compression      string // none, snappy, gzip, brotli, lz4, zstd
	CompressionLevel int    // compression level (1-9 for gzip, 1-22 for zstd)

	// Column-specific compression (map of column name to compression type)
	ColumnCompression map[string]string

	// Performance settings
	EnableStats    bool
	MemoryPoolSize int64 // bytes

	// File settings
	MaxFileSize     int64 // bytes
	RotationTimeout int64 // seconds
}

// DefaultParquetConfig returns default configuration
func DefaultParquetConfig() *ParquetConfig {
	return &ParquetConfig{
		MaxMemoryUsage:    1 << 30, // 1GB
		BatchSize:         10000,
		StreamingEnabled:  true,
		ChunkSize:         1000,
		Compression:       "snappy",
		CompressionLevel:  1,
		ColumnCompression: make(map[string]string),
		EnableStats:       true,
		MemoryPoolSize:    100 << 20, // 100MB
		MaxFileSize:       50 << 30,  // 50GB
		RotationTimeout:   300,       // 5 minutes
	}
}
