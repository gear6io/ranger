# 🧊 Parquet Storage Implementation Milestone

**Status**: 🚧 In Planning  
**Priority**: 🔴 High  
**Estimated Effort**: 4-6 weeks  
**Dependencies**: Apache Arrow Go, Apache Iceberg Go  

## 🎯 Overview

Transform the data lakehouse platform from JSON-based storage to a fully Apache Iceberg native system that writes Parquet files on disk with minimal memory usage, proper file rotation, and complete Iceberg compliance.

## 📋 Requirements Summary

- ❌ **No backward compatibility** - The data lakehouse being Apache Iceberg native is supposed to write only Parquet files
- ❌ **No schema inference** - Table schema will be fixed from user's standpoint, we would only like to validate the data against that schema
- ❌ **Minimal memory usage** - Data shouldn't be copied in RAM, or to be copied least
- ❌ **File rotation** - 50GB max for each file, SDK native batch = single file or split if crossed 50GB max
- ❌ **Timeout handling** - Wait for timeout to close and rotate files in case User is ingesting small batches with delays
- ❌ **Partitioning** - Will come from Table Settings, requires assessing and building feature in registry
- ❌ **Compression** - Part of Table Settings, extend compression features to column level if possible
- ❌ **Apache Iceberg compliance** - These will be Apache Iceberg files

## 🏗️ Implementation Phases

### **Phase 1: Core Parquet Writing Infrastructure** 
*Estimated: 2-3 weeks*

#### **1.1 Arrow Schema Integration**
- [ ] **Research existing Iceberg schema implementation**
  - [ ] Analyze `server/catalog/json/catalog.go` schema handling
  - [ ] Review `server/storage/manager.go` schema usage
  - [ ] Document current schema structure and limitations
  - [ ] Identify gaps between current and required schema handling

- [ ] **Implement Iceberg to Arrow schema conversion**
  - [ ] Create `server/storage/parquet/schema_converter.go`
  - [ ] Implement `ConvertIcebergToArrowSchema(schema *iceberg.Schema) (*arrow.Schema, error)`
  - [ ] Handle all Iceberg primitive types (int, long, float, double, string, boolean, date, timestamp)
  - [ ] Handle Iceberg complex types (list, map, struct)
  - [ ] Support nullable vs non-nullable field requirements
  - [ ] Add comprehensive unit tests for type conversions

- [ ] **Create schema validation system**
  - [ ] Implement `ValidateDataAgainstSchema(data [][]interface{}, schema *arrow.Schema) error`
  - [ ] Add type checking for each column
  - [ ] Handle null value validation
  - [ ] Implement custom validation rules for complex types
  - [ ] Add performance benchmarks for validation

#### **1.2 Parquet Writer Implementation**
- [ ] **Research Apache Arrow Parquet writing capabilities**
  - [ ] Review `github.com/apache/arrow-go/v18` Parquet writer APIs
  - [ ] Test memory usage patterns with large datasets
  - [ ] Benchmark write performance with different batch sizes
  - [ ] Document Arrow Parquet writer limitations and workarounds

- [ ] **Create core Parquet writer**
  - [ ] Implement `server/storage/parquet/writer.go`
  - [ ] Create `ParquetWriter` struct with configurable options
  - [ ] Implement `WriteBatch(data [][]interface{}, schema *arrow.Schema) error`
  - [ ] Add streaming write support for large datasets
  - [ ] Implement proper error handling and cleanup

- [ ] **Implement memory-efficient data processing**
  - [ ] Create `ArrowArrayBuilder` for efficient array construction
  - [ ] Implement batch processing with configurable sizes
  - [ ] Add memory pool management using Arrow's memory allocator
  - [ ] Implement garbage collection optimization
  - [ ] Add memory usage monitoring and metrics

#### **1.3 File Management System**
- [ ] **Design file rotation strategy**
  - [ ] Implement 50GB file size limit enforcement
  - [ ] Create file naming convention: `{table}_data_{YYYYMMDD}_{ulid}.parquet`
  - [ ] Add file size tracking and monitoring
  - [ ] Implement automatic file rotation triggers

- [ ] **Implement timeout-based file rotation**
  - [ ] Create configurable timeout settings (default: 5 minutes)
  - [ ] Implement background goroutine for timeout monitoring
  - [ ] Add graceful file closing and rotation
  - [ ] Handle edge cases (very small batches, long delays)

- [ ] **Create file lifecycle management**
  - [ ] Implement file open/close state tracking
  - [ ] Add file metadata tracking (size, row count, creation time)
  - [ ] Implement file cleanup and garbage collection
  - [ ] Add file integrity checks and validation

### **Phase 2: Apache Iceberg Compliance**
*Estimated: 2-3 weeks*

#### **2.1 Snapshot Management**
- [ ] **Research Iceberg snapshot requirements**
  - [ ] Study Apache Iceberg specification for snapshots
  - [ ] Review existing Iceberg Go implementation
  - [ ] Document snapshot metadata structure
  - [ ] Identify required manifest file formats

- [ ] **Implement snapshot creation system**
  - [ ] Create `server/storage/iceberg/snapshot.go`
  - [ ] Implement `CreateSnapshot(tableName string, files []FileInfo) (*Snapshot, error)`
  - [ ] Generate unique snapshot IDs using ULID
  - [ ] Track snapshot metadata (timestamp, file list, statistics)
  - [ ] Add snapshot versioning and rollback support

- [ ] **Create manifest generation**
  - [ ] Implement `GenerateManifest(snapshot *Snapshot) (*Manifest, error)`
  - [ ] Create proper Iceberg manifest file format
  - [ ] Include file metadata (path, size, row count, partition info)
  - [ ] Add manifest checksums and validation
  - [ ] Implement manifest list generation

#### **2.2 Partition Support Implementation**
- [ ] **Research existing partition infrastructure**
  - [ ] Analyze `server/metadata/registry/migrations/001_start.go` partition tables
  - [ ] Review `server/metadata/registry/sqlite.go` partition handling
  - [ ] Document current partition specification structure
  - [ ] Identify gaps in partition implementation

- [ ] **Implement partition specification handling**
  - [ ] Create `server/storage/iceberg/partition.go`
  - [ ] Implement `PartitionSpec` struct with Iceberg compliance
  - [ ] Add support for identity, bucket, truncate, and year/month/day partitioning
  - [ ] Create partition value extraction from data
  - [ ] Implement partition directory structure creation

- [ ] **Create partition-aware file organization**
  - [ ] Implement partition-based file placement
  - [ ] Add partition metadata tracking
  - [ ] Create partition statistics (min/max values, null counts)
  - [ ] Implement partition pruning for queries
  - [ ] Add partition file listing and management

#### **2.3 Sort Order Implementation**
- [ ] **Research Iceberg sort order requirements**
  - [ ] Study Apache Iceberg sort order specification
  - [ ] Review existing sort order implementations
  - [ ] Document sort order metadata structure
  - [ ] Identify performance implications of different sort strategies

- [ ] **Implement sort order handling**
  - [ ] Create `server/storage/iceberg/sort_order.go`
  - [ ] Implement `SortOrder` struct with Iceberg compliance
  - [ ] Add support for ascending/descending sort orders
  - [ ] Implement nulls first/last handling
  - [ ] Create sort order validation and enforcement

- [ ] **Create data sorting system**
  - [ ] Implement in-memory sorting for small datasets
  - [ ] Add external sorting for large datasets
  - [ ] Implement streaming sort for very large datasets
  - [ ] Add sort performance optimization and caching
  - [ ] Create sort order metadata tracking

### **Phase 3: Performance & Optimization**
*Estimated: 1-2 weeks*

#### **3.1 Memory Management Optimization**
- [ ] **Implement streaming operations**
  - [ ] Create streaming Parquet writer interface
  - [ ] Implement chunked data processing
  - [ ] Add memory usage monitoring and alerts
  - [ ] Implement adaptive batch sizing based on available memory
  - [ ] Add memory pressure handling and backpressure

- [ ] **Optimize Arrow memory usage**
  - [ ] Implement custom memory allocator for the data lakehouse
  - [ ] Add memory pool management and reuse
  - [ ] Implement memory defragmentation strategies
  - [ ] Add memory usage profiling and optimization
  - [ ] Create memory usage benchmarks and monitoring

#### **3.2 Compression & Column-Level Features**
- [ ] **Research column-level compression**
  - [ ] Study Parquet compression algorithms and their characteristics
  - [ ] Review column-specific compression strategies
  - [ ] Document compression performance trade-offs
  - [ ] Identify optimal compression for different data types

- [ ] **Implement compression system**
  - [ ] Create `server/storage/parquet/compression.go`
  - [ ] Implement snappy, gzip, brotli compression support
  - [ ] Add column-level compression selection
  - [ ] Implement compression ratio monitoring
  - [ ] Add compression performance benchmarking

- [ ] **Create compression metadata tracking**
  - [ ] Track compression statistics per column
  - [ ] Implement compression ratio reporting
  - [ ] Add compression performance metrics
  - [ ] Create compression optimization recommendations

## 🔧 Technical Specifications

### **File Structure**
```
table_location/
├── metadata/
│   ├── v1.metadata.json          # ✅ EXISTS (basic)
│   ├── v2.metadata.json          # ❌ TO IMPLEMENT
│   └── ...                       # ❌ TO IMPLEMENT
├── data/
│   ├── part-0.parquet            # ❌ TO IMPLEMENT
│   ├── part-1.parquet            # ❌ TO IMPLEMENT
│   └── ...                       # ❌ TO IMPLEMENT
├── snapshots/                    # ❌ TO IMPLEMENT
│   ├── 1.avro                   # ❌ TO IMPLEMENT
│   └── ...                       # ❌ TO IMPLEMENT
└── manifests/                    # ❌ TO IMPLEMENT
    ├── manifest-list-1.avro      # ❌ TO IMPLEMENT
    └── ...                       # ❌ TO IMPLEMENT
```

### **Data Flow Architecture**
```
CURRENT (JSON-based):
Data → JSON Marshal → Memory Copy → Write to disk

TARGET (Iceberg + Parquet):
Data → Schema Validation → Arrow Arrays → Parquet Writer → Direct to disk (minimal RAM)
```

### **Configuration Options**
```yaml
storage:
  parquet:
    max_file_size: 50GB
    rotation_timeout: 5m
    batch_size: 10000
    compression:
      default: snappy
      column_overrides:
        text: gzip
        numeric: brotli
    memory:
      max_usage: 1GB
      pool_size: 100MB
```

## ✅ Acceptance Criteria

### **Phase 1 Completion**
- [ ] All existing JSON storage methods replaced with Parquet equivalents
- [ ] Schema validation working for all supported data types
- [ ] File rotation working with 50GB limit and timeout handling
- [ ] Memory usage reduced by at least 80% compared to JSON storage
- [ ] All unit tests passing for new Parquet functionality

### **Phase 2 Completion**
- [ ] Full Apache Iceberg compliance achieved
- [ ] Snapshot management working with proper manifest generation
- [ ] Partition support working with table settings
- [ ] Sort order implementation working correctly
- [ ] Integration tests passing with external Iceberg tools

### **Phase 3 Completion**
- [ ] Performance benchmarks showing significant improvements
- [ ] Memory usage optimized and stable under load
- [ ] Compression working at column level with configurable options
- [ ] Production-ready with comprehensive monitoring and alerting

## 🚧 Dependencies

### **External Dependencies**
- [x] `github.com/apache/arrow-go/v18 v18.3.0` - Available
- [x] `github.com/apache/iceberg-go v0.3.0` - Available
- [x] `github.com/oklog/ulid/v2 v2.1.1` - Available

### **Internal Dependencies**
- [x] Storage Manager infrastructure - Available
- [x] Metadata Registry - Available
- [x] Path Management - Available
- [x] Iceberg Catalog System - Available

### **New Dependencies to Add**
- [ ] `github.com/apache/arrow-go/v18/parquet` - For Parquet writing
- [ ] `github.com/apache/arrow-go/v18/memory` - For memory management
- [ ] `github.com/apache/arrow-go/v18/array` - For array operations

## 📊 Success Metrics

### **Performance Improvements**
- **Memory Usage**: Reduce by 80%+ compared to JSON storage
- **Write Performance**: 2x+ faster than current JSON storage
- **File Size**: 3x+ smaller than JSON files (with compression)
- **Query Performance**: 5x+ faster for analytical queries

### **Quality Metrics**
- **Test Coverage**: 90%+ for new Parquet functionality
- **Iceberg Compliance**: 100% compliance with Iceberg specification
- **Error Rate**: <0.1% for data corruption or loss
- **Performance Regression**: 0% for existing functionality

## 🚨 Risks & Mitigation

### **High Risk Items**
1. **Memory Management Complexity**
   - *Risk*: Complex memory management leading to memory leaks
   - *Mitigation*: Extensive testing, memory profiling, gradual rollout

2. **Iceberg Compliance**
   - *Risk*: Missing Iceberg specification requirements
   - *Mitigation*: Comprehensive testing with Iceberg tools, community validation

3. **Performance Regression**
   - *Risk*: New system slower than existing JSON storage
   - *Mitigation*: Performance benchmarking, A/B testing, rollback plan

### **Medium Risk Items**
1. **File Rotation Edge Cases**
   - *Risk*: Complex scenarios causing file corruption
   - *Mitigation*: Extensive edge case testing, file integrity validation

2. **Schema Evolution**
   - *Risk*: Schema changes breaking existing functionality
   - *Mitigation*: Schema versioning, backward compatibility testing

## 📅 Timeline

### **Week 1-2: Phase 1 Foundation**
- Arrow schema integration
- Basic Parquet writer
- File management system

### **Week 3-4: Phase 2 Compliance**
- Snapshot management
- Partition support
- Sort order implementation

### **Week 5-6: Phase 3 Optimization**
- Performance optimization
- Compression implementation
- Testing and validation

### **Week 7: Integration & Deployment**
- End-to-end testing
- Performance benchmarking
- Documentation and handoff

## 🔄 Post-Implementation Tasks

### **Monitoring & Maintenance**
- [ ] Set up performance monitoring dashboards
- [ ] Implement automated alerting for issues
- [ ] Create maintenance procedures and runbooks
- [ ] Plan regular performance reviews and optimizations

### **Documentation & Training**
- [ ] Update user documentation with new Parquet capabilities
- [ ] Create developer guides for Parquet storage
- [ ] Provide training for operations team
- [ ] Create troubleshooting guides and FAQs

### **Future Enhancements**
- [ ] Plan for additional compression algorithms
- [ ] Research advanced partitioning strategies
- [ ] Consider integration with external query engines
- [ ] Plan for cloud storage optimization

---

**Last Updated**: 2025-01-27  
**Next Review**: 2025-02-03  
**Owner**: Development Team  
**Stakeholders**: Product, Operations, QA
