package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/paths"
	parquet "github.com/gear6io/ranger/server/storage/schema"
)

// Package-specific error codes for filesystem parquet manager
var (
	FilesystemParquetCreateDirFailed        = errors.MustNewCode("filesystem_parquet.create_dir_failed")
	FilesystemParquetManagerClosed          = errors.MustNewCode("filesystem_parquet.manager_closed")
	FilesystemParquetValidationFailed       = errors.MustNewCode("filesystem_parquet.validation_failed")
	FilesystemParquetConversionFailed       = errors.MustNewCode("filesystem_parquet.conversion_failed")
	FilesystemParquetEnsureFileFailed       = errors.MustNewCode("filesystem_parquet.ensure_file_failed")
	FilesystemParquetWriteFailed            = errors.MustNewCode("filesystem_parquet.write_failed")
	FilesystemParquetRotationFailed         = errors.MustNewCode("filesystem_parquet.rotation_failed")
	FilesystemParquetCreateFileFailed       = errors.MustNewCode("filesystem_parquet.create_file_failed")
	FilesystemParquetCreatePropertiesFailed = errors.MustNewCode("filesystem_parquet.create_properties_failed")
	FilesystemParquetCreateWriterFailed     = errors.MustNewCode("filesystem_parquet.create_writer_failed")
	FilesystemParquetGetSizeFailed          = errors.MustNewCode("filesystem_parquet.get_size_failed")
	FilesystemParquetCloseFailed            = errors.MustNewCode("filesystem_parquet.close_failed")
	FilesystemParquetListFilesFailed        = errors.MustNewCode("filesystem_parquet.list_files_failed")
	FilesystemParquetSchemaIsNil            = errors.MustNewCode("filesystem_parquet.schema_is_nil")
	FilesystemParquetColumnConversionFailed = errors.MustNewCode("filesystem_parquet.column_conversion_failed")
	FilesystemParquetInsufficientColumns    = errors.MustNewCode("filesystem_parquet.insufficient_columns")
	FilesystemParquetAppendValueFailed      = errors.MustNewCode("filesystem_parquet.append_value_failed")
	FilesystemParquetTypeMismatch           = errors.MustNewCode("filesystem_parquet.type_mismatch")
	FilesystemParquetUnsupportedType        = errors.MustNewCode("filesystem_parquet.unsupported_type")
)

// ParquetManager manages Parquet data operations for filesystem storage
type ParquetManager struct {
	schema      *arrow.Schema
	config      *parquet.ParquetConfig
	memoryPool  memory.Allocator
	pathManager paths.PathManager
	database    string
	tableName   string
	currentFile *ParquetFile
	fileCount   int
	stats       *parquet.WriteStats
	mu          sync.RWMutex
	closed      bool
}

// ParquetFile represents an active Parquet file
type ParquetFile struct {
	Path      string
	Writer    *pqarrow.FileWriter
	File      *os.File
	RowCount  int64
	FileSize  int64
	CreatedAt time.Time
	LastWrite time.Time
}

// NewParquetManager creates a new filesystem Parquet manager
func NewParquetManager(schema *arrow.Schema, config *parquet.ParquetConfig, pathManager paths.PathManager, database, tableName string) (*ParquetManager, error) {
	if config == nil {
		config = parquet.DefaultParquetConfig()
	}

	// Create base directory using PathManager if it doesn't exist
	dataPath := pathManager.GetParquetDataPath(database, tableName)
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return nil, errors.New(FilesystemParquetCreateDirFailed, "failed to create data directory", err).AddContext("path", dataPath)
	}

	return &ParquetManager{
		schema:      schema,
		config:      config,
		memoryPool:  memory.NewGoAllocator(),
		pathManager: pathManager,
		database:    database,
		tableName:   tableName,
		stats: &parquet.WriteStats{
			RowsWritten:      0,
			BytesWritten:     0,
			WriteDuration:    0,
			CompressionRatio: 1.0,
			MemoryUsage:      0,
		},
	}, nil
}

// StoreData stores data as Parquet files on disk
func (fm *ParquetManager) StoreData(data [][]interface{}) error {
	if fm.closed {
		return errors.New(FilesystemParquetManagerClosed, "parquet manager is closed", nil)
	}

	if len(data) == 0 {
		return nil
	}

	startTime := time.Now()

	// Validate data against schema
	Manager := parquet.NewManager(fm.config)
	if err := Manager.ValidateData(data, fm.schema); err != nil {
		return err
	}

	// Convert data to Arrow record
	arrays, err := fm.convertDataToArrays(data)
	if err != nil {
		return err
	}

	record := array.NewRecord(fm.schema, arrays, int64(len(data)))
	defer record.Release()

	// Ensure we have an active file
	if err := fm.ensureActiveFile(); err != nil {
		return err
	}

	// Write the record to the current file
	if err := fm.currentFile.Writer.Write(record); err != nil {
		return errors.New(FilesystemParquetWriteFailed, "failed to write record to Parquet file", err).AddContext("filesystem_parquet", "write_operation_failed")
	}

	// Update statistics
	fm.mu.Lock()
	fm.currentFile.RowCount += int64(len(data))
	fm.currentFile.LastWrite = time.Now()
	fm.stats.RowsWritten += int64(len(data))
	fm.stats.WriteDuration = time.Since(startTime).Nanoseconds()
	fm.mu.Unlock()

	// Check if we need to rotate files
	if err := fm.checkFileRotation(); err != nil {
		return err
	}

	return nil
}

// ensureActiveFile ensures there's an active Parquet file for writing
func (fm *ParquetManager) ensureActiveFile() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.currentFile != nil {
		return nil
	}

	// Generate file path using PathManager
	timestamp := time.Now().Format("20060102_150405")
	filename := fm.pathManager.GetParquetFileName(timestamp, fm.fileCount)
	filePath := fm.pathManager.GetParquetFilePath(fm.database, fm.tableName, filename)

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return errors.New(FilesystemParquetCreateFileFailed, "failed to create Parquet file", err).AddContext("path", filePath)
	}

	// Create Parquet writer with compression settings
	_, err = parquet.CreateWriterProperties(fm.config, fm.schema)
	if err != nil {
		file.Close()
		return err
	}

	// For now, use default writer properties
	// TODO: Apply compression settings from CreateWriterProperties
	writer, err := pqarrow.NewFileWriter(fm.schema, file, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		return errors.New(FilesystemParquetCreateWriterFailed, "failed to create Parquet writer", err).AddContext("filesystem_parquet", "external_library_call_failed")
	}

	// Store compression info for stats
	fm.stats.CompressionRatio = parquet.GetCompressionRatio(fm.config.Compression)

	fm.currentFile = &ParquetFile{
		Path:      filePath,
		Writer:    writer,
		File:      file,
		RowCount:  0,
		FileSize:  0,
		CreatedAt: time.Now(),
		LastWrite: time.Now(),
	}

	fm.fileCount++
	return nil
}

// checkFileRotation checks if the current file should be rotated
func (fm *ParquetManager) checkFileRotation() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.currentFile == nil {
		return nil
	}

	// Check file size
	fileInfo, err := fm.currentFile.File.Stat()
	if err != nil {
		return errors.New(FilesystemParquetGetSizeFailed, "failed to get file size", err).AddContext("filesystem_parquet", "external_library_call_failed")
	}

	fm.currentFile.FileSize = fileInfo.Size()

	// Rotate if file is too large
	if fm.currentFile.FileSize >= fm.config.MaxFileSize {
		return fm.rotateFile("size limit reached")
	}

	// Rotate if timeout reached (only if there's data and we haven't written recently)
	if fm.currentFile.RowCount > 0 {
		timeSinceLastWrite := time.Since(fm.currentFile.LastWrite)
		if timeSinceLastWrite.Seconds() >= float64(fm.config.RotationTimeout) {
			return fm.rotateFile("timeout reached")
		}
	}

	return nil
}

// rotateFile closes the current file and prepares for a new one
func (fm *ParquetManager) rotateFile(reason string) error {
	if fm.currentFile == nil {
		return nil
	}

	// Close the Parquet writer (this also closes the underlying file)
	if err := fm.currentFile.Writer.Close(); err != nil {
		return errors.New(FilesystemParquetCloseFailed, "failed to close Parquet writer", err).AddContext("filesystem_parquet", "external_library_call_failed")
	}

	// Update final file size
	fileInfo, err := os.Stat(fm.currentFile.Path)
	if err == nil {
		fm.currentFile.FileSize = fileInfo.Size()
		fm.stats.BytesWritten += fm.currentFile.FileSize
	}

	// Log rotation (could be replaced with proper logging)
	fmt.Printf("Rotated Parquet file %s (reason: %s, rows: %d, size: %d bytes)\n",
		fm.currentFile.Path, reason, fm.currentFile.RowCount, fm.currentFile.FileSize)

	// Clear current file to force creation of new one
	fm.currentFile = nil

	return nil
}

// GetFiles returns information about all Parquet files
func (fm *ParquetManager) GetFiles() ([]*parquet.FileInfo, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	pattern := fm.pathManager.GetParquetFilePattern(fm.database, fm.tableName)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var fileInfos []*parquet.FileInfo
	for _, filePath := range files {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue // Skip files we can't stat
		}

		fileInfos = append(fileInfos, &parquet.FileInfo{
			Path:     filePath,
			Size:     fileInfo.Size(),
			Created:  fileInfo.ModTime().Unix(),
			Modified: fileInfo.ModTime().Unix(),
			Schema:   fm.schema,
		})
	}

	return fileInfos, nil
}

// GetStats returns writing statistics
func (fm *ParquetManager) GetStats() *parquet.WriteStats {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	// Create a copy to avoid race conditions
	stats := *fm.stats
	return &stats
}

// GetRowCount returns the total number of rows written
func (fm *ParquetManager) GetRowCount() int64 {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.stats.RowsWritten
}

// GetMemoryUsage returns current memory usage (minimal for filesystem)
func (fm *ParquetManager) GetMemoryUsage() int64 {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.stats.MemoryUsage
}

// Close closes the Parquet manager and any open files
func (fm *ParquetManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.closed {
		return nil
	}

	fm.closed = true

	// Close current file if any
	if fm.currentFile != nil {
		if err := fm.rotateFile("manager closing"); err != nil {
			return err
		}
	}

	return nil
}

// convertDataToArrays converts data to Arrow arrays (same as memory implementation)
func (fm *ParquetManager) convertDataToArrays(data [][]interface{}) ([]arrow.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if fm.schema == nil {
		return nil, errors.New(FilesystemParquetSchemaIsNil, "schema is nil", nil)
	}

	numCols := len(fm.schema.Fields())
	arrays := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := fm.schema.Field(colIdx)

		array, err := fm.convertColumnToArray(data, colIdx, field)
		if err != nil {
			return nil, err
		}
		arrays[colIdx] = array
	}

	return arrays, nil
}

// convertColumnToArray converts a single column to Arrow array
func (fm *ParquetManager) convertColumnToArray(data [][]interface{}, colIdx int, field arrow.Field) (arrow.Array, error) {
	numRows := len(data)

	// Create array builder
	builder := array.NewBuilder(fm.memoryPool, field.Type)
	defer builder.Release()

	// Convert each value in the column
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if colIdx >= len(data[rowIdx]) {
			return nil, errors.New(FilesystemParquetInsufficientColumns, "row has insufficient columns", nil).AddContext("row_index", fmt.Sprintf("%d", rowIdx))
		}

		value := data[rowIdx][colIdx]
		if err := fm.appendValueToBuilder(builder, value, field.Type); err != nil {
			return nil, err
		}
	}

	// Build the array
	array := builder.NewArray()
	return array, nil
}

// appendValueToBuilder appends a value to an Arrow array builder
func (fm *ParquetManager) appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch dataType.(type) {
	case *arrow.BooleanType:
		if boolVal, ok := value.(bool); ok {
			builder.(*array.BooleanBuilder).Append(boolVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected bool", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Int32Type:
		if intVal, ok := fm.convertToInt32(value); ok {
			builder.(*array.Int32Builder).Append(intVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected int32", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Int64Type:
		if intVal, ok := fm.convertToInt64(value); ok {
			builder.(*array.Int64Builder).Append(intVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected int64", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Float32Type:
		if floatVal, ok := fm.convertToFloat32(value); ok {
			builder.(*array.Float32Builder).Append(floatVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected float32", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Float64Type:
		if floatVal, ok := fm.convertToFloat64(value); ok {
			builder.(*array.Float64Builder).Append(floatVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected float64", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.StringType:
		if strVal, ok := value.(string); ok {
			builder.(*array.StringBuilder).Append(strVal)
		} else {
			return errors.New(FilesystemParquetTypeMismatch, "expected string", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	default:
		return errors.New(FilesystemParquetUnsupportedType, "unsupported data type", nil).AddContext("data_type", fmt.Sprintf("%T", dataType))
	}

	return nil
}

// Type conversion helpers (same as memory implementation)
func (fm *ParquetManager) convertToInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case int32:
		return v, true
	case int:
		return int32(v), true
	case int64:
		return int32(v), true
	case float32:
		return int32(v), true
	case float64:
		return int32(v), true
	default:
		return 0, false
	}
}

func (fm *ParquetManager) convertToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func (fm *ParquetManager) convertToFloat32(value interface{}) (float32, bool) {
	switch v := value.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case int:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	default:
		return 0, false
	}
}

func (fm *ParquetManager) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}
