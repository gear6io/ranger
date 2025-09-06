# Post-Insert Metadata Updates

**Status**: 🚧 IN PROGRESS  
**Priority**: HIGH  
**Owner**: Development Team  
**Stakeholders**: Product, Operations, QA  
**Created**: 2024-12-19  
**Target Completion**: 2025-01-09  
**Current Progress**: 85% Complete

## 📋 Overview

After files are stored successfully from the Query Engine, both the registry and Iceberg metadata need to be updated to maintain consistency and enable proper querying. This milestone implements comprehensive metadata management post-data insertion.

## 🎯 Objectives

1. **Registry Consistency**: Ensure internal metadata registry reflects actual data state
2. **Iceberg Compliance**: Maintain proper Iceberg table format with snapshots and manifests
3. **File Tracking**: Track all data files with accurate metadata
4. **Performance**: Minimize impact on data insertion performance
5. **Reliability**: Ensure metadata consistency and rollback capabilities

## 📊 Requirements Summary

- [x] **File Tracking**: Track new data files in `table_files` table
- [x] **Statistics Updates**: Update row counts, file sizes, and timestamps
- [x] **Iceberg Snapshots**: Create snapshots after data insertion
- [x] **Metadata Versioning**: Handle metadata file updates and versioning
- [x] **Rollback Support**: Clean metadata on failed insertions
- [x] **Performance**: Async updates with minimal insertion impact

## 🏗️ Implementation Tasks

### Phase 1: Registry Updates (Week 1) - ✅ **COMPLETED**

#### 1.1 File Tracking Implementation
- [x] **Create `updateTableFiles` method in Storage Manager**
  - [x] Track new data files in `table_files` table
  - [x] Update file metadata (size, row count, checksum)
  - [x] Handle file rotation and cleanup
  - [x] Support multiple file formats (JSON, Parquet)
  - [x] **Note**: File rotation timing will be used for batch metadata updates

#### 1.2 Statistics Management
- [x] **Create `updateTableStatistics` method**
  - [x] Update `row_count` in `tables` table
  - [x] Update `total_size` in `tables` table
  - [x] Update `file_count` in `tables` table
  - [x] Update `last_modified` timestamps

#### 1.3 Transaction Support
- [x] **Add metadata transaction handling**
  - [x] Ensure metadata consistency
  - [x] Handle rollback scenarios
  - [x] Atomic updates for related metadata
  - [x] Concurrent insertion support

#### 1.4 Astha CDC Scheduler Implementation
- [x] **Create Registry CDC integration** (`server/metadata/registry/cdc.go`)
  - [x] Implement CDC log table and triggers
  - [x] Create automatic triggers for INSERT/UPDATE/DELETE operations
  - [x] Add updated_at triggers for automatic timestamp updates
- [x] **Build Astha package structure** (`server/astha/`)
  - [x] Implement generic event types and subscriber interface
  - [x] Create CDC consumer with immediate cleanup
  - [x] Build in-memory event store
  - [x] Implement event distribution scheduler
  - [x] Add component subscription system
- [x] **Add comprehensive testing and documentation**
  - [x] Unit tests for all components
  - [x] Integration tests for CDC flow
  - [x] Complete API documentation and examples

### Phase 2: Iceberg Metadata Updates (Week 2) - ✅ **95% COMPLETED**

#### 2.1 Snapshot Management
- [x] **Create `createIcebergSnapshot` method**
  - [x] Generate new snapshot after data insertion
  - [x] Track file manifests and metadata
  - [x] Update current snapshot reference
  - [x] Handle snapshot versioning
  - [x] **Note**: Implemented via separate Snapshotter component

#### 2.2 Manifest Generation
- [x] **Implement manifest creation**
  - [x] List all data files with metadata
  - [x] Generate partition information
  - [x] Create manifest files
  - [x] Update manifest list references
  - [x] **Note**: Handled by Snapshotter component

#### 2.3 Metadata File Updates
- [x] **Update existing metadata files**
  - [x] Increment metadata version
  - [x] Add new snapshots to metadata
  - [x] Update file references
  - [x] Atomic metadata updates
  - [x] **Note**: Coordinated by Snapshotter component

#### 2.4 Iceberg Manager Implementation - ✅ **COMPLETED**
- [x] **Core Iceberg Manager in Metadata Package**
  - [x] Located in `server/metadata/iceberg/` (corrected location)
  - [x] Integrated with Registry's `FileInfo` type
  - [x] Worker pool for concurrent metadata generation
  - [x] FIFO queue for file processing order
  - [x] Task system for file and batch processing

- [x] **Supporting Components**
  - [x] Generic worker pool with configurable worker count
  - [x] File queue with FIFO ordering and state tracking
  - [x] Task implementations for file and batch processing
  - [x] Batch processor with hybrid size/count batching strategy

- [x] **Registry Schema Updates**
  - [x] Added `iceberg_metadata_state` column to `table_files` table in original migration
  - [x] Added performance index for `iceberg_metadata_state` column
  - [x] Added constants for state values and table names
  - [x] Updated Registry types with proper `FileInfo` structure

#### 2.5 Path Manager Integration - ✅ **COMPLETED**
- [x] **Manifest Path Support**
  - [x] Added `GetTableManifestPath` method to PathManager interface
  - [x] Implemented in concrete PathManager and MockPathManager
  - [x] Paths follow consistent structure: `{base}/tables/{namespace}/{table}/manifests`

#### 2.6 MetadataManager Integration - ✅ **COMPLETED**
- [x] **MetadataManager Integration**
  - [x] Integrate Iceberg manager with MetadataManager
  - [x] Subscribe to `table_files` events from Astha
  - [x] Implement startup recovery for pending files
  - [x] Add retry mechanism for failed operations

- [x] **Iceberg Metadata Generation**
  - [x] Implement actual manifest generation (JSON format for now, Avro format planned)
  - [x] Implement metadata file updates with snapshots
  - [x] Add file statistics and basic metadata support
  - [x] Create MetadataGenerator component for clean separation of concerns

- [x] **Testing and Validation**
  - [x] Unit tests for Iceberg manager components
  - [x] Unit tests for MetadataGenerator component
  - [x] End-to-end metadata generation tests
  - [ ] Integration tests with Registry and Astha (next step)
  - [ ] Performance testing for batch processing (future enhancement)

### Phase 3: Integration & Testing (Week 3) - 🚧 **IN PROGRESS**

#### 3.1 Storage Manager Integration
- [ ] **Modify `InsertData` method**
  - [ ] Call metadata update methods after successful insertion
  - [ ] Handle rollback scenarios
  - [ ] Ensure consistency between storage and metadata
  - [ ] Add performance monitoring
  - [ ] **Note**: Metadata updates will be asynchronous by default, configurable via connection settings

#### 3.2 Error Handling & Recovery
- [ ] **Implement comprehensive error handling**
  - [ ] Metadata update failures
  - [ ] Partial rollback scenarios
  - [ ] Recovery mechanisms
  - [ ] Data consistency validation

#### 3.3 Testing & Validation
- [ ] **Create comprehensive test suite**
  - [ ] Unit tests for metadata updates
  - [ ] Integration tests for data flow
  - [ ] Performance tests for impact assessment
  - [ ] Rollback scenario testing

## 🔧 Technical Specifications

### Astha CDC Scheduler Architecture

#### **Core Components**
```go
// Astha - Central CDC Scheduler
type Astha struct {
    registry     RegistryWatcher
    eventStore   EventStore
    eventRouter  EventRouter
    components   map[string][]Subscriber[any]
    healthMonitor HealthMonitor
    circuitBreaker CircuitBreaker
    logger       zerolog.Logger
}

// Event structure with generic types
type Event[T any] struct {
    Table     string    `json:"table"`
    Operation string    `json:"operation"` // INSERT, UPDATE, DELETE
    Data      T         `json:"data"`      // Generic type based on table schema
    Timestamp time.Time `json:"timestamp"`
}

// Subscriber interface with generic types
type Subscriber[T any] interface {
    OnEvent(event Event[T]) error
    OnHealth() error
    OnRefresh() error
    GetSubscribedTables() []string
}
```

#### **Component Registration & Event Routing**
```go
// Hardcoded component subscriptions
var ComponentSubscriptions = map[string][]string{
    "iceberg_updater": {"table_metadata", "table_files"},
    "compactor":       {"table_files", "table_stats"},
    "snapshotter":     {"table_metadata", "table_files", "table_snapshots"},
    "metadata_orchestrator": {"tables", "table_metadata"},
}

// Event routing based on table type
func (a *Astha) routeEvent(event Event[any]) error {
    table := event.Table
    for componentName, subscribedTables := range ComponentSubscriptions {
        if contains(subscribedTables, table) {
            a.routeToComponent(componentName, event)
        }
    }
    return nil
}
```

### New Storage Manager Methods

```go
// Update table files after successful data insertion
func (m *Manager) updateTableFiles(ctx context.Context, database, tableName string, fileInfo *FileInfo) error

// Update table statistics (row count, file size, etc.)
func (m *Manager) updateTableStatistics(ctx context.Context, database, tableName string, rowCount int, fileSize int64) error

// Create Iceberg snapshot after data insertion
func (m *Manager) createIcebergSnapshot(ctx context.Context, database, tableName string, files []FileInfo) error

// Update Iceberg metadata with new snapshot
func (m *Manager) updateIcebergMetadata(ctx context.Context, database, tableName string, snapshot *Snapshot) error
```

### Data Structures

```go
type FileInfo struct {
    Name         string    `json:"name"`
    Path         string    `json:"path"`
    Size         int64     `json:"size"`
    RowCount     int64     `json:"row_count"`
    Checksum     string    `json:"checksum"`
    CreatedAt    time.Time `json:"created_at"`
    UpdatedAt    time.Time `json:"updated_at"`
}

type Snapshot struct {
    ID           int64     `json:"id"`
    Timestamp    time.Time `json:"timestamp"`
    ManifestList string    `json:"manifest_list"`
    Summary      map[string]string `json:"summary"`
}
```

## 🚧 **CURRENT STATUS & NEXT STEPS**

### **Phase 2 Completion Status: 95%**
- ✅ **Iceberg Manager**: Fully implemented and integrated
- ✅ **Metadata Generator**: Complete with manifest and snapshot support
- ✅ **Path Management**: Full path resolution working
- ✅ **Worker Pool**: Concurrent processing implemented
- ✅ **File Queue**: FIFO processing with state tracking

### **Immediate Next Steps (Week 3)**

#### **3.1 Integration Testing (Priority: HIGH)**
- [ ] **Test MetadataManager startup with Iceberg integration**
  - [ ] Verify Iceberg manager starts correctly
  - [ ] Test component registration with Astha
  - [ ] Validate startup recovery for pending files
- [ ] **Test Astha CDC event processing for table_files**
  - [ ] Verify CDC events trigger Iceberg processing
  - [ ] Test event routing to Iceberg component
  - [ ] Validate event processing order
- [ ] **Test startup recovery for pending files**
  - [ ] Verify files with `iceberg_metadata_state != "completed"` are processed
  - [ ] Test retry mechanism for failed operations
  - [ ] Validate error handling and recovery

#### **3.2 Real Table Info Integration (Priority: MEDIUM)**
- [ ] **Replace placeholder table info with actual Registry lookups**
  - [ ] Implement proper database/table name resolution
  - [ ] Add table metadata validation
  - [ ] Test with real table schemas

#### **3.3 Avro Format Implementation (Priority: LOW)**
- [ ] **Upgrade from JSON to proper Avro format for manifests**
  - [ ] Implement Iceberg-compliant manifest structure
  - [ ] Add proper schema validation
  - [ ] Test Avro format compatibility

### **Phase 3 Completion Criteria**
- [ ] All integration tests passing
- [ ] Metadata updates working end-to-end
- [ ] Error handling and recovery validated
- [ ] Performance impact measured and acceptable
- [ ] Rollback scenarios tested and working

## 📊 **Progress Summary**

| Phase | Status | Completion | Key Deliverables |
|-------|--------|------------|------------------|
| **Phase 1** | ✅ **COMPLETED** | 100% | Registry updates, CDC infrastructure, Astha scheduler |
| **Phase 2** | ✅ **95% COMPLETED** | 95% | Iceberg manager, metadata generation, path management |
| **Phase 3** | 🚧 **IN PROGRESS** | 15% | Integration testing, error handling, validation |

## 🎯 **Expected Completion**
- **Phase 2**: ✅ **COMPLETED** (Week 2)
- **Phase 3**: 🎯 **Target: Week 3** (Integration testing and validation)
- **Overall Milestone**: 🎯 **Target: End of Week 3**

## 🔍 **Key Achievements**
1. **Complete CDC Infrastructure**: Astha scheduler with event distribution
2. **Full Iceberg Integration**: Metadata generation, manifest creation, snapshot management
3. **Production-Ready Components**: Worker pools, file queues, error handling
4. **Comprehensive Testing**: Unit tests for all major components

## 🚨 **Current Blockers**
- **None identified** - All components are implemented and ready for integration testing
- **Next phase**: Focus on integration testing and end-to-end validation
