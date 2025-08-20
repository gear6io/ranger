# Storage Manager - Streaming Architecture

## Overview

The Storage Manager has been refactored to implement a **streaming-first architecture** that eliminates memory inefficiencies in data writing operations. This replaces the previous buffering approach with direct streaming from Query Engine to storage.

## Key Improvements

### 1. **Memory Efficiency**
- **Before**: Data was copied multiple times: QE → JSON bytes → storage buffer → final file
- **After**: Direct streaming with minimal memory footprint using batch processing

### 2. **Streaming Interface**
```go
type FileSystem interface {
    // Core streaming operations
    OpenForRead(path string) (io.ReadCloser, error)
    OpenForWrite(path string) (io.WriteCloser, error)
    
    // New streaming table operations
    OpenTableForWrite(database, tableName string) (io.WriteCloser, error)
    OpenTableForRead(database, tableName string) (io.ReadCloser, error)
    
    // Utility operations
    Remove(path string) error
    Exists(path string) (bool, error)
    MkdirAll(path string) error
    
    // Storage environment preparation
    PrepareTableEnvironment(database, tableName string) error
    RemoveTableEnvironment(database, tableName string) error
}
```

### 3. **Rollback Support**
- Automatic rollback on failed streaming operations
- Cleanup of partially written data
- Transaction-like behavior for data consistency

### 4. **Batch Processing**
- Configurable batch size (default: 1000 rows)
- Memory-efficient processing of large datasets
- JSON serialization per batch to avoid memory buildup

## Implementation Details

### Filesystem Storage
- Direct file I/O without intermediate buffering
- Uses `os.Create()` and `os.Open()` for true streaming
- Automatic cleanup of empty files

### Memory Storage
- Maintains efficient Arrow/Parquet format
- Streaming wrapper around existing Parquet manager
- No data duplication

### S3 Storage
- Streaming uploads with configurable buffer sizes
- Chunked reading for large objects
- Efficient object management

## Usage Example

```go
// Streaming data insertion
func (m *Manager) InsertData(ctx context.Context, database, tableName string, data [][]interface{}) error {
    // Open streaming writer
    writer, err := engine.OpenTableForWrite(database, tableName)
    if err != nil {
        return err
    }
    defer writer.Close()

    // Stream data in batches
    batchSize := 1000
    for i := 0; i < len(data); i += batchSize {
        end := i + batchSize
        if end > len(data) {
            end = len(data)
        }
        
        batch := data[i:end]
        batchBytes, _ := json.Marshal(batch)
        
        // Write directly to storage
        writer.Write(batchBytes)
        writer.Write([]byte("\n"))
    }
    
    return nil
}
```

## Performance Benefits

1. **Memory Usage**: Reduced from O(n) to O(batch_size) where n = total rows
2. **Scalability**: Handles datasets of any size without memory constraints
3. **Throughput**: Direct streaming eliminates buffer copy overhead
4. **Reliability**: Rollback support ensures data consistency

## Migration Notes

- **No backward compatibility**: Old `WriteFile`, `ReadFile`, `StoreTableData`, `GetTableData` methods removed
- **Interface changes**: All storage engines must implement new streaming methods
- **Testing required**: Verify streaming behavior with large datasets

## Future Enhancements

1. **True streaming**: Eliminate JSON serialization overhead
2. **Compression**: Add streaming compression support
3. **Parallel processing**: Multi-threaded batch processing
4. **Metrics**: Add streaming performance metrics
