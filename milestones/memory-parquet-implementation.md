# 🧊 Memory-Based Parquet Implementation - TODO Tracker

**Status**: 🚧 In Progress  
**Priority**: 🔴 High  
**Estimated Effort**: 2-3 weeks  
**Dependencies**: Apache Arrow Go, Metadata Registry  

## 🎯 Overview

Implement a memory-based Parquet storage system that will serve as the foundation for the full filesystem implementation. This focuses on core Parquet writing, schema validation, and memory-efficient processing.

## 📋 Requirements Summary

- ✅ **Schema storage**: In metadata registry when table is created
- ✅ **Schema format**: Apache Iceberg schema format
- ✅ **Validation strategy**: Fail on first error, reject entire batch
- ✅ **Memory management**: Use streaming, global dynamic limits
- ✅ **Integration**: Remove JSON storage completely
- ✅ **Testing**: Unit tests with small datasets
- 🔄 **Integration testing**: DuckDB integration testing (moved to Phase 2 - Filesystem)

## 🏗️ Implementation Tasks

### **Phase 1: Core Foundation (Week 1)**

#### **1.1 Create Parquet Package Structure**
- [x] **Create `server/storage/parquet/` directory**
- [x] **Create `server/storage/parquet/schema.go`**
  - [x] Define `SchemaManager` interface
  - [x] Implement Iceberg to Arrow schema conversion
  - [x] Add comprehensive schema validation
- [x] **Create `server/storage/parquet/data_manager.go`**
  - [x] Implement `ParquetDataManager` for in-memory storage
  - [x] Add memory management and limits
  - [x] Add streaming data operations

#### **1.2 Implement Schema Management**
- [x] **Research existing metadata registry schema storage**
  - [x] Analyze `server/metadata/registry/sqlite.go` table creation
  - [x] Document current schema storage format
  - [x] Identify integration points
- [x] **Implement Iceberg to Arrow schema conversion**
  - [x] Create `ConvertIcebergToArrowSchema(schema *iceberg.Schema) (*arrow.Schema, error)`
  - [x] Handle all Iceberg primitive types (int, long, float, double, string, boolean, date, timestamp)
  - [x] Handle Iceberg complex types (list, map, struct)
  - [x] Support nullable vs non-nullable field requirements
- [x] **Add schema validation**
  - [x] Implement `ValidateDataAgainstSchema(data [][]interface{}, schema *arrow.Schema) error`
  - [x] Fail on first validation error
  - [x] Reject entire batch if validation fails

#### **1.3 Extend Memory Storage**
- [x] **Add Parquet fields to MemoryStorage**
  - [x] Add `parquetManager *ParquetDataManager` field
  - [x] Add `schemaManager *SchemaManager` field
  - [x] Add `tables map[string]*TableData` field
- [x] **Create TableData struct**
  - [x] Store Arrow arrays instead of raw data
  - [x] Track schema information
  - [x] Track file metadata (size, row count, creation time)
- [x] **Implement new Parquet methods**
  - [x] `StoreTableData(database, tableName string, data []byte) error`
  - [x] `GetTableData(database, tableName string) ([]byte, error)`
  - [x] `SetTableSchema(database, tableName string, icebergSchema *iceberg.Schema) error`

### 🎯 **Phase 1 Results**
- ✅ **All Parquet package tests passing**: 16/16 tests
- ✅ **All Memory storage tests passing**: 12/12 tests  
- ✅ **Complete integration**: Parquet foundation + Memory storage
- ✅ **JSON storage removed**: Completely replaced with Parquet
- ✅ **File consolidation**: Clean, maintainable codebase

### **Phase 2: Filesystem Storage & DuckDB Integration (Week 2)**

#### **2.1 Apache Arrow Integration**
- [ ] **Research Arrow Parquet writing**
  - [ ] Study `github.com/apache/arrow-go/v18` Parquet writer APIs
  - [ ] Test memory usage patterns with small datasets
  - [ ] Document Arrow Parquet writer usage patterns
- [ ] **Implement Arrow array building**
  - [ ] Create `ArrowArrayBuilder` for efficient array construction
  - [ ] Implement streaming array building to minimize memory usage
  - [ ] Add type conversion from Go types to Arrow types
  - [ ] Handle null values properly

#### **2.2 Parquet Writer Implementation**
- [ ] **Create core Parquet writer**
  - [ ] Implement `ParquetWriter` struct with configurable options
  - [ ] Add `WriteBatch(data [][]interface{}, schema *arrow.Schema) error`
  - [ ] Implement streaming write support for large datasets
  - [ ] Add proper error handling and cleanup
- [ ] **Implement memory-efficient processing**
  - [ ] Use Arrow memory allocator for efficient allocation
  - [ ] Implement batch processing with configurable sizes
  - [ ] Add memory usage monitoring and limits
  - [ ] Implement garbage collection optimization

#### **2.3 Memory Management**
- [ ] **Implement global memory limits**
  - [ ] Create configurable memory limits (default: 1GB)
  - [ ] Add dynamic memory limit adjustment based on available system memory
  - [ ] Implement memory pressure handling and backpressure
  - [ ] Add memory usage monitoring and alerts
- [ ] **Add streaming operations**
  - [ ] Implement chunked data processing
  - [ ] Add streaming Parquet writer interface
  - [ ] Implement backpressure handling for memory pressure

### **Phase 3: Integration & Testing (Week 3)**

#### **3.1 Storage Manager Integration**
- [ ] **Remove JSON storage methods**
  - [ ] Remove `StoreTableData` JSON implementation
  - [ ] Remove `GetTableData` JSON implementation
  - [ ] Update all references to use Parquet methods
- [ ] **Update Storage Manager**
  - [ ] Modify `InsertData` to use Parquet storage
  - [ ] Modify `GetTableData` to use Parquet storage
  - [ ] Update table creation to store schemas in metadata registry
  - [ ] Ensure backward compatibility for existing interfaces

#### **3.2 Testing Implementation**
- [ ] **Create unit tests**
  - [ ] Test schema conversion with small datasets
  - [ ] Test data validation with various data types
  - [ ] Test Parquet writing with small batches
  - [ ] Test memory management and limits
- [ ] **Create integration tests**
  - [ ] Test with DuckDB for Parquet file validation
  - [ ] Test end-to-end data flow
  - [ ] Test performance with larger datasets
  - [ ] Test memory usage under load

#### **3.3 File Management Foundation**
- [ ] **Implement file rotation concepts**
  - [ ] Add file size tracking (prepare for 50GB limit)
  - [ ] Add timeout handling concepts (prepare for 5-minute timeout)
  - [ ] Create file naming conventions for future filesystem use
  - [ ] Add file metadata tracking
- [ ] **Create abstraction layer**
  - [ ] Prepare interfaces for filesystem backend
  - [ ] Document implementation patterns
  - [ ] Create migration path documentation

## 🔧 Technical Specifications

### **Schema Storage in Metadata Registry**
```sql
-- Add to table_metadata table
ALTER TABLE table_metadata ADD COLUMN parquet_schema BLOB;
ALTER TABLE table_metadata ADD COLUMN schema_version INTEGER DEFAULT 1;
```

### **ParquetTableData Structure**
```go
type ParquetTableData struct {
    Schema      *arrow.Schema
    Arrays      []arrow.Array
    RowCount    int64
    FileSize    int64
    Created     time.Time
    Modified    time.Time
    Metadata    map[string]interface{}
}
```

### **Memory Configuration**
```yaml
storage:
  memory:
    parquet:
      max_memory_usage: 1GB
      batch_size: 10000
      streaming:
        enabled: true
        chunk_size: 1000
      memory_pool:
        initial_size: 100MB
        max_size: 500MB
```

### **New Memory Storage Methods**
```go
// Add to MemoryStorage interface
func (ms *MemoryStorage) WriteParquetData(database, tableName string, data [][]interface{}, schema []byte) error
func (ms *MemoryStorage) ReadParquetData(database, tableName string) ([][]interface{}, error)
func (ms *MemoryStorage) ValidateDataAgainstSchema(data [][]interface{}, schema []byte) error
func (ms *MemoryStorage) GetParquetSchema(database, tableName string) (*arrow.Schema, error)
```

## ✅ Acceptance Criteria

### **Phase 1 Completion**
- [ ] Parquet package structure created
- [ ] Schema conversion working for all Iceberg types
- [ ] Data validation failing on first error
- [ ] Memory storage extended with Parquet fields

### **Phase 2 Completion**
- [ ] Arrow integration working with Parquet writing
- [ ] Memory-efficient processing implemented
- [ ] Global memory limits working dynamically
- [ ] Streaming operations functional

### **Phase 3 Completion**
- [ ] JSON storage completely removed
- [ ] Storage Manager using Parquet methods
- [ ] All unit tests passing
- [ ] DuckDB integration tests passing
- [ ] Foundation ready for filesystem implementation

## 🚧 Dependencies

### **External Dependencies**
- [x] `github.com/apache/arrow-go/v18 v18.3.0` - Available
- [x] `github.com/apache/iceberg-go v0.3.0` - Available

### **Internal Dependencies**
- [x] Memory Storage Engine - Available
- [x] Storage Manager - Available
- [x] Metadata Registry - Available
- [x] Path Management - Available

## 📊 Success Metrics

### **Performance Improvements**
- **Memory Usage**: Reduce by 60%+ compared to JSON storage
- **Processing Speed**: 2x+ faster than current JSON storage
- **Scalability**: Support for datasets 5x+ larger than current

### **Quality Metrics**
- **Test Coverage**: 90%+ for new Parquet functionality
- **Validation Accuracy**: 100% data validation accuracy
- **Memory Stability**: <10% memory usage variance under load
- **Error Handling**: Proper error propagation and batch rejection

## 🚨 Risks & Mitigation

### **High Risk Items**
1. **Arrow Integration Complexity**
   - *Risk*: Complex Arrow API leading to implementation issues
   - *Mitigation*: Start with simple examples, extensive testing

2. **Memory Management**
   - *Risk*: Memory leaks or excessive usage
   - *Mitigation*: Memory profiling, gradual rollout, monitoring

### **Medium Risk Items**
1. **Schema Conversion**
   - *Risk*: Incorrect type mapping between Iceberg and Arrow
   - *Mitigation*: Comprehensive testing, DuckDB validation

2. **Integration Complexity**
   - *Risk*: Breaking existing functionality during JSON removal
   - *Mitigation*: Incremental changes, extensive testing

## 📅 Timeline

### **Week 1: Foundation**
- Parquet package structure
- Schema management
- Memory storage extension

### **Week 2: Core Implementation**
- Arrow integration
- Parquet writing
- Memory management

### **Week 3: Integration & Testing**
- Storage manager integration
- Testing implementation
- Foundation for filesystem

## 🔄 Post-Implementation Tasks

### **Immediate (Week 4)**
- Performance benchmarking
- Memory usage optimization
- Documentation updates

### **Short-term (Month 2)**
- Filesystem backend implementation
- Advanced memory optimization
- Production deployment preparation

---

**Last Updated**: 2025-01-27  
**Next Review**: 2025-02-03  
**Owner**: Development Team  
**Stakeholders**: Product, Operations, QA
