package memory

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/storage/schema"
	parquet "github.com/gear6io/ranger/server/storage/schema"
)

// StorageType constant for this storage engine
const Type = "MEMORY"

// MemoryStorage implements memory storage using Parquet format
type MemoryStorage struct {
	data   map[string][]byte
	tables map[string]*TableData
	mu     sync.RWMutex
}

// TableData represents a table in memory storage
type TableData struct {
	Schema         *arrow.Schema
	ParquetManager *ParquetManager
	IcebergSchema  *iceberg.Schema
	Database       string
	TableName      string
}

// NewMemoryStorage creates a new memory storage
func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		data:   make(map[string][]byte),
		tables: make(map[string]*TableData),
	}, nil
}

// GetStorageType returns the storage type identifier
func (ms *MemoryStorage) GetStorageType() string {
	return Type
}

// SetupTable creates the storage environment for a table
func (ms *MemoryStorage) SetupTable(database, tableName string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table already exists (this is fine, just return success)
	if _, exists := ms.tables[tableKey]; exists {
		return nil
	}

	// Create a placeholder table entry
	// The actual schema will be set when StoreTableData is called
	ms.tables[tableKey] = &TableData{
		Database:  database,
		TableName: tableName,
	}

	return nil
}

// OpenTableForWrite opens a table for streaming write operations
func (ms *MemoryStorage) OpenTableForWrite(database, tableName string) (io.WriteCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Ensure table environment exists (avoid calling SetupTable to prevent deadlock)
	if _, exists := ms.tables[tableKey]; !exists {
		// Create a placeholder table entry
		ms.tables[tableKey] = &TableData{
			Database:  database,
			TableName: tableName,
		}
	}

	return &memoryTableWriter{
		storage:   ms,
		database:  database,
		tableName: tableName,
		tableKey:  tableKey,
		buffer:    make([]byte, 0),
	}, nil
}

// OpenTableForRead opens a table for streaming read operations
func (ms *MemoryStorage) OpenTableForRead(database, tableName string) (io.ReadCloser, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	if _, exists := ms.tables[tableKey]; !exists {
		return nil, errors.New(ErrTableNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// For memory storage, we'll return a reader that provides data from the Parquet manager
	if tableData, exists := ms.tables[tableKey]; exists && tableData.ParquetManager != nil {
		return &memoryTableReader{
			storage:   ms,
			database:  database,
			tableName: tableName,
			tableKey:  tableKey,
		}, nil
	}

	// Return empty reader if no data
	return &emptyReader{}, nil
}

// RemoveTableEnvironment removes the storage environment for a table
func (ms *MemoryStorage) RemoveTableEnvironment(database, tableName string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return nil // Table doesn't exist, nothing to remove
	}

	// Close Parquet manager if it exists
	if tableData.ParquetManager != nil {
		if err := tableData.ParquetManager.Close(); err != nil {
			// Log warning but continue
			fmt.Printf("Warning: failed to close Parquet manager for %s.%s: %v\n", database, tableName, err)
		}
	}

	// Remove table entry
	delete(ms.tables, tableKey)

	return nil
}

// GetTableStats returns statistics for a table
func (ms *MemoryStorage) GetTableStats(database, tableName string) (*parquet.WriteStats, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return nil, errors.New(ErrTableNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// If no Parquet manager exists, return empty stats
	if tableData.ParquetManager == nil {
		return &parquet.WriteStats{}, nil
	}

	return tableData.ParquetManager.GetStats(), nil
}

// GetTableRowCount returns the number of rows in a table
func (ms *MemoryStorage) GetTableRowCount(database, tableName string) (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return 0, errors.New(ErrTableNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// If no Parquet manager exists, return 0
	if tableData.ParquetManager == nil {
		return 0, nil
	}

	return tableData.ParquetManager.GetRowCount(), nil
}

// GetTableMemoryUsage returns the memory usage of a table
func (ms *MemoryStorage) GetTableMemoryUsage(database, tableName string) (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return 0, errors.New(ErrTableNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// If no Parquet manager exists, return 0
	if tableData.ParquetManager == nil {
		return 0, nil
	}

	return tableData.ParquetManager.GetMemoryUsage(), nil
}

// SetTableSchema sets the schema for a table
func (ms *MemoryStorage) SetTableSchema(database, tableName string, icebergSchema *iceberg.Schema) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return errors.New(ErrTableNotFound, "table does not exist", nil).AddContext("database", database).AddContext("table", tableName)
	}

	// Convert Iceberg schema to Arrow schema
	Manager := schema.NewManager(schema.DefaultParquetConfig())
	arrowSchema, err := Manager.ConvertIcebergToArrowSchema(icebergSchema)
	if err != nil {
		return errors.New(ErrSchemaConversionFailed, "failed to convert Iceberg schema to Arrow schema", err)
	}

	// Update table schema
	tableData.IcebergSchema = icebergSchema
	tableData.Schema = arrowSchema

	// If Parquet manager exists, close it and create a new one with the new schema
	if tableData.ParquetManager != nil {
		if err := tableData.ParquetManager.Close(); err != nil {
			// Log warning but continue
			fmt.Printf("Warning: failed to close existing Parquet manager: %v\n", err)
		}
	}

	// Create new Parquet manager with the new schema
	config := schema.DefaultParquetConfig()
	tableData.ParquetManager = NewParquetManager(arrowSchema, config)

	return nil
}

// Exists checks if a file or directory exists
func (ms *MemoryStorage) Exists(path string) (bool, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// For memory storage, we only track tables, not individual files
	// This is a simplified implementation
	return false, nil
}

// Remove removes a file or directory
func (ms *MemoryStorage) Remove(path string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// For memory storage, we only track tables, not individual files
	// This is a simplified implementation
	return nil
}

// MkdirAll creates directories recursively
func (ms *MemoryStorage) MkdirAll(path string) error {
	// For memory storage, directories are virtual and always succeed
	return nil
}

// OpenForRead opens a file for streaming read
func (ms *MemoryStorage) OpenForRead(path string) (io.ReadCloser, error) {
	// For memory storage, we only support table operations
	// This is a simplified implementation
	return &emptyReader{}, nil
}

// OpenForWrite opens a file for streaming write
func (ms *MemoryStorage) OpenForWrite(path string) (io.WriteCloser, error) {
	// For memory storage, we only support table operations
	// This is a simplified implementation
	return &emptyWriter{}, nil
}

// Helper methods

// getTableKey creates a unique key for a table
func (ms *MemoryStorage) getTableKey(database, tableName string) string {
	return fmt.Sprintf("%s.%s", database, tableName)
}

// createDefaultSchema creates a default schema based on parsed data
func (ms *MemoryStorage) createDefaultSchema(data []byte) (*arrow.Schema, error) {
	// Parse the JSON to understand the data structure
	var parsedData [][]interface{}
	if err := json.Unmarshal(data, &parsedData); err != nil {
		return nil, errors.New(ErrSchemaInferenceFailed, "failed to parse data for schema inference", err)
	}

	if len(parsedData) == 0 {
		return nil, errors.New(ErrEmptyDataForSchema, "cannot infer schema from empty data", nil)
	}

	// Infer schema from the first row
	firstRow := parsedData[0]
	var fields []arrow.Field

	for i, value := range firstRow {
		// Use meaningful column names if available, otherwise generate descriptive names
		var fieldName string
		switch i {
		case 0:
			fieldName = "id"
		case 1:
			fieldName = "name"
		case 2:
			fieldName = "value"
		default:
			fieldName = fmt.Sprintf("column_%d", i+1)
		}

		var fieldType arrow.DataType

		// Simple type inference based on the first row
		switch value.(type) {
		case int, int32, int64:
			fieldType = arrow.PrimitiveTypes.Int64
		case float32, float64:
			fieldType = arrow.PrimitiveTypes.Float64
		case string:
			fieldType = arrow.BinaryTypes.String
		case bool:
			fieldType = arrow.FixedWidthTypes.Boolean
		default:
			fieldType = arrow.BinaryTypes.String // fallback to string
		}

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     fieldType,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertBytesToInterface converts JSON bytes back to interface format
func (ms *MemoryStorage) convertBytesToInterface(data []byte) ([][]interface{}, error) {
	// Parse JSON back to [][]interface{}
	var result [][]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, errors.New(ErrDataUnmarshalFailed, "failed to unmarshal JSON data", err)
	}
	return result, nil
}

// convertInterfaceToBytes converts interface format back to bytes
// This is a simplified conversion - in practice, you'd want more sophisticated serialization
func (ms *MemoryStorage) convertInterfaceToBytes(data [][]interface{}) ([]byte, error) {
	// For now, just create a simple conversion
	// In practice, you'd serialize the data appropriately
	if len(data) == 0 {
		return []byte("[]"), nil
	}

	// Convert to a simple format for now
	result := "["
	for i, row := range data {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%v", row)
	}
	result += "]"

	return []byte(result), nil
}

// memoryTableWriter implements io.WriteCloser for memory storage
type memoryTableWriter struct {
	storage   *MemoryStorage
	database  string
	tableName string
	tableKey  string
	buffer    []byte
	closed    bool
}

func (mtw *memoryTableWriter) Write(p []byte) (n int, err error) {
	if mtw.closed {
		return 0, errors.New(ErrWriterClosed, "write on closed writer", nil)
	}
	mtw.buffer = append(mtw.buffer, p...)
	return len(p), nil
}

func (mtw *memoryTableWriter) Close() error {
	if mtw.closed {
		return nil
	}
	mtw.closed = true

	// Process the buffered data and store it
	if len(mtw.buffer) > 0 {
		// Convert data to interface format and store using Parquet manager
		interfaceData, err := mtw.storage.convertBytesToInterface(mtw.buffer)
		if err != nil {
			return err
		}

		mtw.storage.mu.Lock()
		defer mtw.storage.mu.Unlock()

		tableData := mtw.storage.tables[mtw.tableKey]
		if tableData.ParquetManager == nil {
			// Create default schema and Parquet manager
			schema, err := mtw.storage.createDefaultSchema(mtw.buffer)
			if err != nil {
				return err
			}
			tableData.Schema = schema

			config := parquet.DefaultParquetConfig()
			tableData.ParquetManager = NewParquetManager(tableData.Schema, config)
		}

		// Store data using Parquet manager
		if err := tableData.ParquetManager.StoreData(interfaceData); err != nil {
			return err
		}
	}

	return nil
}

// memoryTableReader implements io.ReadCloser for memory storage
type memoryTableReader struct {
	storage   *MemoryStorage
	database  string
	tableName string
	tableKey  string
	position  int
	closed    bool
}

func (mtr *memoryTableReader) Read(p []byte) (n int, err error) {
	if mtr.closed {
		return 0, io.EOF
	}

	// Get data from Parquet manager
	mtr.storage.mu.RLock()
	tableData, exists := mtr.storage.tables[mtr.tableKey]
	mtr.storage.mu.RUnlock()

	if !exists || tableData.ParquetManager == nil {
		return 0, io.EOF
	}

	// For now, return data in a simple format
	// In a real implementation, you'd stream from the Parquet manager
	data, err := tableData.ParquetManager.GetData()
	if err != nil {
		return 0, err
	}

	// Convert to JSON for streaming
	jsonData, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}

	if mtr.position >= len(jsonData) {
		return 0, io.EOF
	}

	n = copy(p, jsonData[mtr.position:])
	mtr.position += n
	return n, nil
}

func (mtr *memoryTableReader) Close() error {
	mtr.closed = true
	return nil
}

// emptyReader implements io.ReadCloser for empty tables
type emptyReader struct{}

func (er *emptyReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (er *emptyReader) Close() error {
	return nil
}

// emptyWriter implements io.WriteCloser for unsupported operations
type emptyWriter struct{}

func (ew *emptyWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New(ErrWriteNotSupported, "write not supported for memory storage file operations", nil)
}

func (ew *emptyWriter) Close() error {
	return nil
}
