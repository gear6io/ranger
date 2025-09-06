package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gear6io/ranger/pkg/errors"
	parquet "github.com/gear6io/ranger/server/storage/parquet"
)

// ParquetManager manages Parquet data operations in memory
type ParquetManager struct {
	schema        *arrow.Schema
	config        *parquet.ParquetConfig
	memoryPool    memory.Allocator
	recordBatches []arrow.Record
	stats         *parquet.WriteStats
	mu            sync.RWMutex
	closed        bool
}

// NewParquetManager creates a new Parquet data manager
func NewParquetManager(schema *arrow.Schema, config *parquet.ParquetConfig) *ParquetManager {
	if config == nil {
		config = parquet.DefaultParquetConfig()
	}

	return &ParquetManager{
		schema:        schema,
		config:        config,
		memoryPool:    memory.NewGoAllocator(),
		recordBatches: make([]arrow.Record, 0),
		stats: &parquet.WriteStats{
			RowsWritten:      0,
			BytesWritten:     0,
			WriteDuration:    0,
			CompressionRatio: 1.0,
			MemoryUsage:      0,
		},
	}
}

// StoreData stores data in memory using Parquet format
func (dm *ParquetManager) StoreData(data [][]interface{}) error {
	if dm.closed {
		return errors.New(ErrDataManagerClosed, "data manager is closed", nil)
	}

	if len(data) == 0 {
		return nil
	}

	startTime := time.Now()

	// Validate data against schema
	if err := parquet.ValidateData(data, dm.schema); err != nil {
		return errors.New(ErrDataValidationFailed, "data validation failed", err)
	}

	// Convert data to Arrow arrays
	arrays, err := dm.convertDataToArrays(data)
	if err != nil {
		return err
	}

	// Create Arrow record
	record := array.NewRecord(dm.schema, arrays, int64(len(data)))

	// Check memory usage
	if err := dm.checkMemoryUsage(record); err != nil {
		record.Release()
		return err
	}

	// Store the record
	dm.mu.Lock()
	dm.recordBatches = append(dm.recordBatches, record)
	dm.stats.RowsWritten += int64(len(data))
	dm.stats.MemoryUsage = dm.calculateMemoryUsage()
	dm.mu.Unlock()

	// Update stats
	duration := time.Since(startTime).Nanoseconds()
	dm.stats.WriteDuration = duration

	return nil
}

// GetData retrieves all stored data as interface slices
func (dm *ParquetManager) GetData() ([][]interface{}, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.closed {
		return nil, errors.New(ErrDataManagerClosed, "data manager is closed", nil)
	}

	var allData [][]interface{}

	for _, record := range dm.recordBatches {
		data, err := dm.convertRecordToData(record)
		if err != nil {
			return nil, err
		}
		allData = append(allData, data...)
	}

	return allData, nil
}

// GetDataBatch provides streaming data retrieval
func (dm *ParquetManager) GetDataBatch(batchSize int) (<-chan [][]interface{}, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.closed {
		return nil, errors.New(ErrDataManagerClosed, "data manager is closed", nil)
	}

	dataChan := make(chan [][]interface{}, batchSize)

	go func() {
		defer close(dataChan)

		for _, record := range dm.recordBatches {
			data, err := dm.convertRecordToData(record)
			if err != nil {
				// Log error but continue with other records
				fmt.Printf("Error converting record: %v\n", err)
				continue
			}

			// Send data in batches
			for i := 0; i < len(data); i += batchSize {
				end := i + batchSize
				if end > len(data) {
					end = len(data)
				}
				dataChan <- data[i:end]
			}
		}
	}()

	return dataChan, nil
}

// GetSchema returns the current schema
func (dm *ParquetManager) GetSchema() *arrow.Schema {
	return dm.schema
}

// GetStats returns writing statistics
func (dm *ParquetManager) GetStats() *parquet.WriteStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.stats
}

// GetRowCount returns the total number of rows stored
func (dm *ParquetManager) GetRowCount() int64 {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.stats.RowsWritten
}

// GetMemoryUsage returns current memory usage
func (dm *ParquetManager) GetMemoryUsage() int64 {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.stats.MemoryUsage
}

// ClearData clears all stored data
func (dm *ParquetManager) ClearData() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Release all records
	for _, record := range dm.recordBatches {
		record.Release()
	}

	dm.recordBatches = make([]arrow.Record, 0)
	dm.stats.RowsWritten = 0
	dm.stats.MemoryUsage = 0
}

// Close releases resources
func (dm *ParquetManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.closed {
		return nil
	}

	dm.closed = true

	// Release all records
	for _, record := range dm.recordBatches {
		record.Release()
	}

	dm.recordBatches = nil
	dm.memoryPool = nil

	return nil
}

// Helper methods

// convertDataToArrays converts data to Arrow arrays
func (dm *ParquetManager) convertDataToArrays(data [][]interface{}) ([]arrow.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if dm.schema == nil {
		return nil, errors.New(ErrSchemaIsNil, "schema is nil", nil)
	}

	numCols := len(dm.schema.Fields())
	arrays := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := dm.schema.Field(colIdx)

		array, err := dm.convertColumnToArray(data, colIdx, field)
		if err != nil {
			return nil, err
		}
		arrays[colIdx] = array
	}

	return arrays, nil
}

// convertColumnToArray converts a single column to Arrow array
func (dm *ParquetManager) convertColumnToArray(data [][]interface{}, colIdx int, field arrow.Field) (arrow.Array, error) {
	numRows := len(data)

	// Create array builder
	builder := array.NewBuilder(dm.memoryPool, field.Type)
	defer builder.Release()

	// Convert each value in the column
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if colIdx >= len(data[rowIdx]) {
			return nil, errors.New(ErrInsufficientColumns, "row has insufficient columns", nil).AddContext("row_index", fmt.Sprintf("%d", rowIdx))
		}

		value := data[rowIdx][colIdx]
		if err := dm.appendValueToBuilder(builder, value, field.Type); err != nil {
			return nil, errors.New(ErrValueAppendFailed, "failed to append value at row", err).AddContext("row_index", fmt.Sprintf("%d", rowIdx))
		}
	}

	// Build the array
	array := builder.NewArray()
	return array, nil
}

// appendValueToBuilder appends a value to an Arrow array builder
func (dm *ParquetManager) appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch dataType.(type) {
	case *arrow.BooleanType:
		if boolVal, ok := value.(bool); ok {
			builder.(*array.BooleanBuilder).Append(boolVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected bool", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Int32Type:
		if intVal, ok := dm.convertToInt32(value); ok {
			builder.(*array.Int32Builder).Append(intVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected int32", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Int64Type:
		if intVal, ok := dm.convertToInt64(value); ok {
			builder.(*array.Int64Builder).Append(intVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected int64", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Float32Type:
		if floatVal, ok := dm.convertToFloat32(value); ok {
			builder.(*array.Float32Builder).Append(floatVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected float32", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.Float64Type:
		if floatVal, ok := dm.convertToFloat64(value); ok {
			builder.(*array.Float64Builder).Append(floatVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected float64", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	case *arrow.StringType:
		if strVal, ok := value.(string); ok {
			builder.(*array.StringBuilder).Append(strVal)
		} else {
			return errors.New(ErrTypeMismatch, "expected string", nil).AddContext("actual_type", fmt.Sprintf("%T", value))
		}

	default:
		return errors.New(ErrUnsupportedDataType, "unsupported data type", nil).AddContext("data_type", fmt.Sprintf("%T", dataType))
	}

	return nil
}

// convertRecordToData converts Arrow record to interface data
func (dm *ParquetManager) convertRecordToData(record arrow.Record) ([][]interface{}, error) {
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	data := make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = make([]interface{}, numCols)
	}

	for colIdx := 0; colIdx < numCols; colIdx++ {
		col := record.Column(colIdx)
		field := record.Schema().Field(colIdx)

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if col.IsNull(rowIdx) {
				data[rowIdx][colIdx] = nil
				continue
			}

			value, err := dm.extractValue(col, rowIdx, field.Type)
			if err != nil {
				return nil, errors.New(ErrValueExtractionFailed, "failed to extract value", err).AddContext("row_index", fmt.Sprintf("%d", rowIdx)).AddContext("col_index", fmt.Sprintf("%d", colIdx))
			}
			data[rowIdx][colIdx] = value
		}
	}

	return data, nil
}

// extractValue extracts a value from an Arrow array
func (dm *ParquetManager) extractValue(col arrow.Array, rowIdx int, dataType arrow.DataType) (interface{}, error) {
	switch dt := dataType.(type) {
	case *arrow.BooleanType:
		return col.(*array.Boolean).Value(rowIdx), nil
	case *arrow.Int32Type:
		return col.(*array.Int32).Value(rowIdx), nil
	case *arrow.Int64Type:
		return col.(*array.Int64).Value(rowIdx), nil
	case *arrow.Float32Type:
		return col.(*array.Float32).Value(rowIdx), nil
	case *arrow.Float64Type:
		return col.(*array.Float64).Value(rowIdx), nil
	case *arrow.StringType:
		return col.(*array.String).Value(rowIdx), nil
	default:
		return nil, errors.New(ErrUnsupportedDataType, "unsupported data type", nil).AddContext("data_type", fmt.Sprintf("%T", dt))
	}
}

// checkMemoryUsage checks if adding a record would exceed memory limits
func (dm *ParquetManager) checkMemoryUsage(record arrow.Record) error {
	estimatedSize := dm.estimateRecordSize(record)

	dm.mu.RLock()
	currentUsage := dm.stats.MemoryUsage
	dm.mu.RUnlock()

	if currentUsage+estimatedSize > dm.config.MaxMemoryUsage {
		return errors.New(ErrMemoryLimitExceeded, "memory limit exceeded", nil).AddContext("current_usage", fmt.Sprintf("%d", currentUsage)).AddContext("estimated_size", fmt.Sprintf("%d", estimatedSize)).AddContext("limit", fmt.Sprintf("%d", dm.config.MaxMemoryUsage))
	}

	return nil
}

// calculateMemoryUsage calculates total memory usage
func (dm *ParquetManager) calculateMemoryUsage() int64 {
	var totalSize int64
	for _, record := range dm.recordBatches {
		totalSize += dm.estimateRecordSize(record)
	}
	return totalSize
}

// estimateRecordSize estimates the memory size of a record
func (dm *ParquetManager) estimateRecordSize(record arrow.Record) int64 {
	// Rough estimation: sum of all array sizes
	var totalSize int64
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		if col == nil || col.Data() == nil {
			continue
		}

		// Estimate based on data type and length
		switch col.DataType().(type) {
		case *arrow.BooleanType:
			totalSize += int64(col.Data().Len()) * 1
		case *arrow.Int32Type, *arrow.Float32Type:
			totalSize += int64(col.Data().Len()) * 4
		case *arrow.Int64Type, *arrow.Float64Type:
			totalSize += int64(col.Data().Len()) * 8
		case *arrow.StringType, *arrow.BinaryType:
			// For strings, estimate average length + overhead
			totalSize += int64(col.Data().Len()) * 16
		default:
			totalSize += int64(col.Data().Len()) * 8
		}
	}
	return totalSize
}

// Type conversion helpers
func (dm *ParquetManager) convertToInt32(value interface{}) (int32, bool) {
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

func (dm *ParquetManager) convertToInt64(value interface{}) (int64, bool) {
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

func (dm *ParquetManager) convertToFloat32(value interface{}) (float32, bool) {
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

func (dm *ParquetManager) convertToFloat64(value interface{}) (float64, bool) {
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
