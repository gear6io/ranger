package memory

import (
	"fmt"
	"sync"

	"github.com/TFMV/icebox/server/storage/parquet"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
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

// PrepareTableEnvironment creates the storage environment for a table
func (ms *MemoryStorage) PrepareTableEnvironment(database, tableName string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table already exists
	if _, exists := ms.tables[tableKey]; exists {
		return fmt.Errorf("table %s.%s already exists", database, tableName)
	}

	// Create a placeholder table entry
	// The actual schema will be set when StoreTableData is called
	ms.tables[tableKey] = &TableData{
		Database:  database,
		TableName: tableName,
	}

	return nil
}

// StoreTableData stores data for a table in Parquet format
func (ms *MemoryStorage) StoreTableData(database, tableName string, data []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		return fmt.Errorf("table %s.%s does not exist", database, tableName)
	}

	// For now, we'll create a simple schema if none exists
	// In the future, this should come from the metadata registry
	if tableData.Schema == nil {
		schema, err := ms.createDefaultSchema(data)
		if err != nil {
			return fmt.Errorf("failed to create default schema: %w", err)
		}
		tableData.Schema = schema
	}

	// Create Parquet data manager if it doesn't exist
	if tableData.ParquetManager == nil {
		config := parquet.DefaultParquetConfig()
		tableData.ParquetManager = NewParquetManager(tableData.Schema, config)
	}

	// Convert data to interface format
	// This is a simplified conversion - in practice, you'd want more sophisticated parsing
	interfaceData, err := ms.convertBytesToInterface(data)
	if err != nil {
		return fmt.Errorf("failed to convert data: %w", err)
	}

	// Store data using Parquet manager
	if err := tableData.ParquetManager.StoreData(interfaceData); err != nil {
		return fmt.Errorf("failed to store data in Parquet manager: %w", err)
	}

	return nil
}

// GetTableData retrieves data for a table in Parquet format
func (ms *MemoryStorage) GetTableData(database, tableName string) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	tableKey := ms.getTableKey(database, tableName)

	// Check if table exists
	tableData, exists := ms.tables[tableKey]
	if !exists {
		// Return empty data if table doesn't exist
		return []byte("[]"), nil
	}

	// If no Parquet manager exists, return empty data
	if tableData.ParquetManager == nil {
		return []byte("[]"), nil
	}

	// Get data from Parquet manager
	interfaceData, err := tableData.ParquetManager.GetData()
	if err != nil {
		return nil, fmt.Errorf("failed to get data from Parquet manager: %w", err)
	}

	// Convert back to bytes (simplified for now)
	// In practice, you might want to return this in a different format
	dataBytes, err := ms.convertInterfaceToBytes(interfaceData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data to bytes: %w", err)
	}

	return dataBytes, nil
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
		return nil, fmt.Errorf("table %s.%s does not exist", database, tableName)
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
		return 0, fmt.Errorf("table %s.%s does not exist", database, tableName)
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
		return 0, fmt.Errorf("table %s.%s does not exist", database, tableName)
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
		return fmt.Errorf("table %s.%s does not exist", database, tableName)
	}

	// Convert Iceberg schema to Arrow schema
	schemaManager := parquet.NewSchemaManager(parquet.DefaultParquetConfig())
	arrowSchema, err := schemaManager.ConvertIcebergToArrowSchema(icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert Iceberg schema to Arrow schema: %w", err)
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
	config := parquet.DefaultParquetConfig()
	tableData.ParquetManager = NewParquetManager(arrowSchema, config)

	return nil
}

// Helper methods

// getTableKey creates a unique key for a table
func (ms *MemoryStorage) getTableKey(database, tableName string) string {
	return fmt.Sprintf("%s.%s", database, tableName)
}

// createDefaultSchema creates a default schema based on data
// This is a simplified implementation - in practice, you'd want more sophisticated schema inference
func (ms *MemoryStorage) createDefaultSchema(data []byte) (*arrow.Schema, error) {
	// For now, create a simple schema with string columns
	// In practice, this should come from the metadata registry
	fields := []arrow.Field{
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertBytesToInterface converts bytes to interface format
// This is a simplified conversion - in practice, you'd want more sophisticated parsing
func (ms *MemoryStorage) convertBytesToInterface(data []byte) ([][]interface{}, error) {
	// For now, just create a simple conversion
	// In practice, you'd parse the data format and convert appropriately
	return [][]interface{}{
		{string(data)},
	}, nil
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
