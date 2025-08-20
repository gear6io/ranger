package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/storage/parquet"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// StoreData stores data in memory as Arrow records
func (pm *ParquetManager) StoreData(data [][]interface{}) error {
	if pm.closed {
		return fmt.Errorf("data manager is closed")
	}

	if len(data) == 0 {
		return nil
	}

	startTime := time.Now()

	// Validate data against schema
	schemaManager := parquet.NewSchemaManager(pm.config)
	if err := schemaManager.ValidateData(data, pm.schema); err != nil {
		return fmt.Errorf("data validation failed: %w", err)
	}

	// Convert data to Arrow arrays
	arrays, err := pm.convertDataToArrays(data)
	if err != nil {
		return fmt.Errorf("failed to convert data to arrays: %w", err)
	}

	// Create Arrow record
	record := array.NewRecord(pm.schema, arrays, int64(len(data)))

	// Check memory usage
	if err := pm.checkMemoryUsage(record); err != nil {
		record.Release()
		return fmt.Errorf("memory limit exceeded: %w", err)
	}

	// Store the record
	pm.mu.Lock()
	pm.recordBatches = append(pm.recordBatches, record)
	pm.stats.RowsWritten += int64(len(data))
	pm.stats.MemoryUsage = pm.calculateMemoryUsage()
	pm.mu.Unlock()

	// Update stats
	duration := time.Since(startTime).Nanoseconds()
	pm.stats.WriteDuration = duration

	return nil
}

// GetData retrieves all stored data as interface slices
func (pm *ParquetManager) GetData() ([][]interface{}, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.closed {
		return nil, fmt.Errorf("data manager is closed")
	}

	var allData [][]interface{}

	for _, record := range pm.recordBatches {
		data, err := pm.convertRecordToData(record)
		if err != nil {
			return nil, fmt.Errorf("failed to convert record: %w", err)
		}
		allData = append(allData, data...)
	}

	return allData, nil
}

// GetDataBatch provides streaming data retrieval
func (pm *ParquetManager) GetDataBatch(batchSize int) (<-chan [][]interface{}, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.closed {
		return nil, fmt.Errorf("data manager is closed")
	}

	dataChan := make(chan [][]interface{}, batchSize)

	go func() {
		defer close(dataChan)

		for _, record := range pm.recordBatches {
			data, err := pm.convertRecordToData(record)
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
func (pm *ParquetManager) GetSchema() *arrow.Schema {
	return pm.schema
}

// GetStats returns writing statistics
func (pm *ParquetManager) GetStats() *parquet.WriteStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.stats
}

// GetRowCount returns the total number of rows stored
func (pm *ParquetManager) GetRowCount() int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.stats.RowsWritten
}

// GetMemoryUsage returns current memory usage
func (pm *ParquetManager) GetMemoryUsage() int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.stats.MemoryUsage
}

// ClearData clears all stored data
func (pm *ParquetManager) ClearData() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Release all records
	for _, record := range pm.recordBatches {
		record.Release()
	}

	pm.recordBatches = make([]arrow.Record, 0)
	pm.stats.RowsWritten = 0
	pm.stats.MemoryUsage = 0
}

// Close releases resources
func (pm *ParquetManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.closed {
		return nil
	}

	pm.closed = true

	// Release all records
	for _, record := range pm.recordBatches {
		record.Release()
	}

	pm.recordBatches = nil
	pm.memoryPool = nil

	return nil
}

// Helper methods

// convertDataToArrays converts data to Arrow arrays
func (pm *ParquetManager) convertDataToArrays(data [][]interface{}) ([]arrow.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if pm.schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	numCols := len(pm.schema.Fields())
	arrays := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := pm.schema.Field(colIdx)

		array, err := pm.convertColumnToArray(data, colIdx, field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", field.Name, err)
		}
		arrays[colIdx] = array
	}

	return arrays, nil
}

// convertColumnToArray converts a single column to Arrow array
func (pm *ParquetManager) convertColumnToArray(data [][]interface{}, colIdx int, field arrow.Field) (arrow.Array, error) {
	numRows := len(data)

	// Create array builder
	builder := array.NewBuilder(pm.memoryPool, field.Type)
	defer builder.Release()

	// Convert each value in the column
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if colIdx >= len(data[rowIdx]) {
			return nil, fmt.Errorf("row %d has insufficient columns", rowIdx)
		}

		value := data[rowIdx][colIdx]
		if err := pm.appendValueToBuilder(builder, value, field.Type); err != nil {
			return nil, fmt.Errorf("failed to append value at row %d: %w", rowIdx, err)
		}
	}

	// Build the array
	array := builder.NewArray()
	return array, nil
}

// appendValueToBuilder appends a value to an Arrow array builder
func (pm *ParquetManager) appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch dataType.(type) {
	case *arrow.BooleanType:
		if boolVal, ok := value.(bool); ok {
			builder.(*array.BooleanBuilder).Append(boolVal)
		} else {
			return fmt.Errorf("expected bool, got %T", value)
		}

	case *arrow.Int32Type:
		if intVal, ok := pm.convertToInt32(value); ok {
			builder.(*array.Int32Builder).Append(intVal)
		} else {
			return fmt.Errorf("expected int32, got %T", value)
		}

	case *arrow.Int64Type:
		if intVal, ok := pm.convertToInt64(value); ok {
			builder.(*array.Int64Builder).Append(intVal)
		} else {
			return fmt.Errorf("expected int64, got %T", value)
		}

	case *arrow.Float32Type:
		if floatVal, ok := pm.convertToFloat32(value); ok {
			builder.(*array.Float32Builder).Append(floatVal)
		} else {
			return fmt.Errorf("expected float32, got %T", value)
		}

	case *arrow.Float64Type:
		if floatVal, ok := pm.convertToFloat64(value); ok {
			builder.(*array.Float64Builder).Append(floatVal)
		} else {
			return fmt.Errorf("expected float64, got %T", value)
		}

	case *arrow.StringType:
		if strVal, ok := value.(string); ok {
			builder.(*array.StringBuilder).Append(strVal)
		} else {
			return fmt.Errorf("expected string, got %T", value)
		}

	default:
		return fmt.Errorf("unsupported data type: %T", dataType)
	}

	return nil
}

// convertRecordToData converts Arrow record to interface data
func (pm *ParquetManager) convertRecordToData(record arrow.Record) ([][]interface{}, error) {
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

			value, err := pm.extractValue(col, rowIdx, field.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to extract value at row %d, col %d: %w", rowIdx, colIdx, err)
			}
			data[rowIdx][colIdx] = value
		}
	}

	return data, nil
}

// extractValue extracts a value from an Arrow array
func (pm *ParquetManager) extractValue(col arrow.Array, rowIdx int, dataType arrow.DataType) (interface{}, error) {
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
		return nil, fmt.Errorf("unsupported data type: %T", dt)
	}
}

// checkMemoryUsage checks if adding a record would exceed memory limits
func (pm *ParquetManager) checkMemoryUsage(record arrow.Record) error {
	estimatedSize := pm.estimateRecordSize(record)

	pm.mu.RLock()
	currentUsage := pm.stats.MemoryUsage
	pm.mu.RUnlock()

	if currentUsage+estimatedSize > pm.config.MaxMemoryUsage {
		return fmt.Errorf("memory limit exceeded: current=%d, estimated=%d, limit=%d",
			currentUsage, estimatedSize, pm.config.MaxMemoryUsage)
	}

	return nil
}

// calculateMemoryUsage calculates total memory usage
func (pm *ParquetManager) calculateMemoryUsage() int64 {
	var totalSize int64
	for _, record := range pm.recordBatches {
		totalSize += pm.estimateRecordSize(record)
	}
	return totalSize
}

// estimateRecordSize estimates the memory size of a record
func (pm *ParquetManager) estimateRecordSize(record arrow.Record) int64 {
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
func (pm *ParquetManager) convertToInt32(value interface{}) (int32, bool) {
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

func (pm *ParquetManager) convertToInt64(value interface{}) (int64, bool) {
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

func (pm *ParquetManager) convertToFloat32(value interface{}) (float32, bool) {
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

func (pm *ParquetManager) convertToFloat64(value interface{}) (float64, bool) {
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
