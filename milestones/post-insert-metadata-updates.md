# Post-Insert Metadata Updates

**Status**: 🚧 IN PROGRESS  
**Priority**: HIGH  
**Owner**: Development Team  
**Stakeholders**: Product, Operations, QA  
**Created**: 2024-12-19  
**Target Completion**: 2025-01-09  

## 📋 Overview

After files are stored successfully from the Query Engine, both the registry and Iceberg metadata need to be updated to maintain consistency and enable proper querying. This milestone implements comprehensive metadata management post-data insertion.

## 🎯 Objectives

1. **Registry Consistency**: Ensure internal metadata registry reflects actual data state
2. **Iceberg Compliance**: Maintain proper Iceberg table format with snapshots and manifests
3. **File Tracking**: Track all data files with accurate metadata
4. **Performance**: Minimize impact on data insertion performance
5. **Reliability**: Ensure metadata consistency and rollback capabilities

## 📊 Requirements Summary

- [ ] **File Tracking**: Track new data files in `table_files` table
- [ ] **Statistics Updates**: Update row counts, file sizes, and timestamps
- [ ] **Iceberg Snapshots**: Create snapshots after data insertion
- [ ] **Metadata Versioning**: Handle metadata file updates and versioning
- [ ] **Rollback Support**: Clean metadata on failed insertions
- [ ] **Performance**: Async updates with minimal insertion impact

## 🏗️ Implementation Tasks

### Phase 1: Registry Updates (Week 1) - COMPLETED ✅

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

### Phase 2: Iceberg Metadata Updates (Week 2)

#### 2.1 Snapshot Management
- [ ] **Create `createIcebergSnapshot` method**
  - [ ] Generate new snapshot after data insertion
  - [ ] Track file manifests and metadata
  - [ ] Update current snapshot reference
  - [ ] Handle snapshot versioning
  - [ ] **Note**: Will be replaced by separate Snapshotter component

#### 2.2 Manifest Generation
- [ ] **Implement manifest creation**
  - [ ] List all data files with metadata
  - [ ] Generate partition information
  - [ ] Create manifest files
  - [ ] Update manifest list references
  - [ ] **Note**: Will be handled by Snapshotter component

#### 2.3 Metadata File Updates
- [ ] **Update existing metadata files**
  - [ ] Increment metadata version
  - [ ] Add new snapshots to metadata
  - [ ] Update file references
  - [ ] Atomic metadata updates
  - [ ] **Note**: Will be coordinated by Snapshotter component

### Phase 3: Integration & Testing (Week 3)

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
    Created      time.Time `json:"created"`
    Modified     time.Time `json:"modified"`
    PartitionPath string   `json:"partition_path,omitempty"`
}

type Snapshot struct {
    SnapshotID   int64      `json:"timestamp_ms"`
    Timestamp    int64      `json:"timestamp_ms"`
    ManifestList string     `json:"manifest_list"`
    Summary      map[string]interface{} `json:"summary"`
    SchemaID     int        `json:"schema_id"`
}

// Registry table schema types for generic events
type TableMetadata struct {
    ID            int64  `json:"id"`
    TableID       int64  `json:"table_id"`
    SchemaVersion int    `json:"schema_version"`
    Schema        []byte `json:"schema"`
    StorageEngine string `json:"storage_engine"`
    EngineConfig  string `json:"engine_config"`
    Format        string `json:"format"`
    Compression   string `json:"compression"`
    PartitionBy   string `json:"partition_by"`
    SortBy        string `json:"sort_by"`
    Properties    string `json:"properties"`
    LastModified  string `json:"last_modified"`
    CreatedAt     string `json:"created_at"`
    UpdatedAt     string `json:"updated_at"`
}

type TableFiles struct {
    ID           int64  `json:"id"`
    TableID      int64  `json:"table_id"`
    FileName     string `json:"file_name"`
    FilePath     string `json:"file_path"`
    FileSize     int64  `json:"file_size"`
    FileType     string `json:"file_type"`
    PartitionPath string `json:"partition_path"`
    RowCount     int64  `json:"row_count"`
    Checksum     string `json:"checksum"`
    IsCompressed bool   `json:"is_compressed"`
    CreatedAt    string `json:"created_at"`
    ModifiedAt   string `json:"modified_at"`
}

type TableStats struct {
    ID           int64  `json:"id"`
    TableID      int64  `json:"table_id"`
    RowCount     int64  `json:"row_count"`
    FileCount    int    `json:"file_count"`
    TotalSize    int64  `json:"total_size"`
    LastModified string `json:"last_modified"`
    CreatedAt    string `json:"created_at"`
    UpdatedAt    string `json:"updated_at"`
}
```

### Database Schema Updates

The existing production schema already supports this implementation:
- `table_files` table for file tracking
- `tables` table for statistics
- `table_metadata` table for schema and engine info

## 📈 Success Metrics

- [ ] **Metadata Consistency**: 100% accuracy between actual data and metadata
- [ ] **Performance Impact**: <5% increase in insertion time
- [ ] **Reliability**: 99.9% successful metadata updates
- [ ] **Rollback Success**: 100% successful rollbacks on failures
- [ ] **File Tracking**: Complete coverage of all data files

## 🚨 Risks & Mitigation

### High Risk Items

1. **Metadata Consistency**
   - *Risk*: Metadata getting out of sync with actual data
   - *Mitigation*: Use transactions, atomic updates, validation checks

2. **Performance Impact**
   - *Risk*: Metadata updates slowing down data insertion
   - *Mitigation*: Async updates, batch processing, efficient queries

3. **Rollback Complexity**
   - *Risk*: Complex rollback scenarios with partial metadata updates
   - *Mitigation*: Comprehensive testing, staged rollbacks, recovery tools

### Medium Risk Items

4. **Concurrency Issues**
   - *Risk*: Race conditions during concurrent insertions
   - *Mitigation*: Proper locking, optimistic concurrency control

5. **Storage Engine Differences**
   - *Risk*: Different metadata requirements per storage engine
   - *Mitigation*: Engine-specific metadata handlers, abstraction layers

## 🔗 Dependencies

- [x] **Storage Manager**: Core storage operations (COMPLETED)
- [x] **Metadata Registry**: Production schema (COMPLETED)
- [x] **Query Engine Streaming**: Data insertion flow (COMPLETED)
- [ ] **Path Manager**: File path resolution (IN PROGRESS)
- [ ] **Storage Engines**: Engine-specific metadata handling (PENDING)

## 📅 Timeline

| Phase | Duration | Start Date | End Date | Deliverables |
|-------|----------|------------|----------|--------------|
| **Phase 1** | 1 week | 2024-12-23 | 2024-12-27 | Registry updates, file tracking |
| **Phase 2** | 1 week | 2024-12-30 | 2025-01-03 | Iceberg metadata, snapshots |
| **Phase 3** | 1 week | 2025-01-06 | 2025-01-09 | Integration, testing, optimization |

## 🧪 Testing Strategy

### Unit Testing
- [ ] Test individual metadata update methods
- [ ] Test file tracking accuracy
- [ ] Test statistics calculations
- [ ] Test rollback scenarios

### Integration Testing
- [ ] End-to-end data insertion flow
- [ ] Metadata consistency validation
- [ ] Performance impact assessment
- [ ] Error handling validation

### Performance Testing
- [ ] Baseline insertion performance
- [ ] Metadata update overhead measurement
- [ ] Concurrent insertion testing
- [ ] Large dataset handling

## 📚 Documentation

- [ ] **API Documentation**: New method signatures and usage
- [ ] **Architecture Guide**: Metadata update flow diagrams
- [ ] **Troubleshooting Guide**: Common issues and solutions
- [ ] **Performance Guide**: Optimization recommendations

## 🔄 Future Enhancements

- [ ] **Batch Metadata Updates**: Group multiple file updates
- [ ] **Metadata Compression**: Optimize metadata file sizes
- [ ] **Incremental Updates**: Only update changed metadata
- [ ] **Metadata Caching**: Cache frequently accessed metadata
- [ ] **Distributed Metadata**: Support for distributed storage

## 🏗️ **Future Architectural Components** (Separate Milestones)

### **Astha CDC Scheduler (Central Nervous System)**
- [ ] **Event-driven metadata distribution**
  - [ ] Subscribe to Registry update events (CRUD operations only)
  - [ ] Route events to appropriate components based on hardcoded subscriptions
  - [ ] Handle event ordering and delivery guarantees
  - [ ] Support for CRUD event types (INSERT, UPDATE, DELETE)
  - [ ] **Immediate push strategy** - no batching, real-time delivery
  - [ ] **Max 30 retries** - then drop event if failed
  - [ ] **Self-recovery mechanisms** - Astha handles its own recovery
  - [ ] **Circuit breaker pattern** - break subscriptions on health issues
  - [ ] **Component health monitoring** - detect unresponsive components
  - [ ] **Timeout-based recovery** - wait for components to consume events
  - [ ] **Direct SQLite refresh** - tell components to refresh from Registry if timeout
  - [ ] **Re-initialization** - Astha reinitializes itself after recovery

### **Component Architecture**
- [ ] **Event-driven component design**
  - [ ] Components bootstrap from Registry for initial state
  - [ ] Subscribe to Astha CDC scheduler for real-time updates
  - [ ] No direct inter-component communication
  - [ ] Event-driven state synchronization
  - [ ] Graceful degradation and recovery
  - [ ] **Hardcoded subscriptions** - no dynamic registration
  - [ ] **Generic type events** - no marshaling/unmarshaling needed
  - [ ] **Standardized Subscriber interface** - all components must implement
  - [ ] **Three core methods**: OnEvent(), OnHealth(), OnRefresh()
  - [ ] **Type-safe event handling** - generic types prevent runtime errors

### **Snapshotter Component**
- [ ] **Separate service for snapshot management**
  - [ ] Configurable snapshot triggers (time-based, size-based, manual)
  - [ ] Table-level snapshot settings
  - [ ] Query-triggered snapshots
  - [ ] Snapshot lifecycle management
  - [ ] **Integration**: Receives events from Astha CDC scheduler
  - [ ] **Subscribed tables**: table_metadata, table_files, table_snapshots
  - [ ] **Event types**: INSERT, UPDATE, DELETE on subscribed tables
  - [ ] **Generic event handling**: Event[TableMetadata], Event[TableFiles], Event[TableSnapshots]
  - [ ] **No marshaling overhead**: Direct access to typed data

### **Compaction Service**
- [ ] **Intelligent file compaction**
  - [ ] Merge small files into optimal sizes
  - [ ] Partition-aware compaction strategies
  - [ ] Background compaction workers
  - [ ] Compaction metrics and monitoring
  - [ ] **Integration**: Receives events from Astha CDC scheduler
  - [ ] **Subscribed tables**: table_files, table_stats
  - [ ] **Event types**: INSERT, UPDATE, DELETE on subscribed tables
  - [ ] **Generic event handling**: Event[TableFiles], Event[TableStats]
  - [ ] **No marshaling overhead**: Direct access to typed data

### **Iceberg Metadata Updater**
- [ ] **Dedicated Iceberg metadata management**
  - [ ] Handle metadata file updates
  - [ ] Manage manifest generation
  - [ ] Coordinate with snapshot creation
  - [ ] **Integration**: Receives events from Astha CDC scheduler
  - [ ] **Subscribed tables**: table_metadata, table_files
  - [ ] **Event types**: INSERT, UPDATE, DELETE on subscribed tables
  - [ ] **Generic event handling**: Event[TableMetadata], Event[TableFiles]
  - [ ] **No marshaling overhead**: Direct access to typed data

### **Metadata Update Orchestrator**
- [ ] **Centralized metadata update coordination**
  - [ ] Async metadata updates by default
  - [ ] Configurable sync/async modes per connection
  - [ ] Batch metadata operations
  - [ ] Retry mechanisms and error handling
  - [ ] Performance optimization strategies
  - [ ] **Integration**: Publishes events to Astha CDC scheduler
  - [ ] **Subscribed tables**: tables, table_metadata
  - [ ] **Event types**: INSERT, UPDATE, DELETE on subscribed tables
  - [ ] **Generic event handling**: Event[Tables], Event[TableMetadata]
  - [ ] **No marshaling overhead**: Direct access to typed data

## 📝 Notes

- This milestone builds upon the completed Query Engine streaming integration
- Focus on maintaining backward compatibility with existing metadata
- Consider performance impact on high-frequency insertions
- Ensure proper error handling for production environments

## 🎯 **Detailed Implementation Specifications**

### **Event Processing Strategy**
- **Immediate push**: No batching, real-time event delivery to components
- **Max 30 retries**: Drop event if delivery fails after 30 attempts
- **CRUD operations only**: Handle INSERT, UPDATE, DELETE events
- **No schema changes**: Only data changes, not structural changes
- **Table schema dependent**: Event filtering based on actual table structure

### **CDC Architecture (Based on sqlite-cdc)**
- **Registry Integration**: CDC setup becomes part of Registry's own setup
- **Trigger-Based Capture**: Automatic triggers on INSERT/UPDATE/DELETE operations
- **Change Log Table**: `__cdc_log` table stores all changes with before/after images
- **Immediate Cleanup**: Logs deleted immediately after successful processing (no TTL needed)
- **ACID Guarantees**: Cleanup happens in same transaction as processing
- **Ordered Processing**: Changes processed in strict ID order (ORDER BY id ASC)
- **Batch Processing**: Configurable batch sizes (default: 256 changes per batch)
- **No Data Loss**: Failed batches are not cleaned up, allowing retry
- **Crash Recovery**: Unprocessed logs preserved if Astha crashes

### **Component Interface Requirements**
- **Subscriber[T] interface**: All components must implement this interface
- **Generic type events**: Event[T] where T matches table schema type
- **Three required methods**: OnEvent(), OnHealth(), OnRefresh()
- **Type safety**: Compile-time type checking prevents runtime errors
- **No marshaling**: Components receive typed data directly

### **Registry CDC Integration**
- **CDC Table**: `__cdc_log` with columns: id, timestamp, tablename, operation, before, after, created_at
- **Automatic Triggers**: Created for each monitored table (INSERT, UPDATE, DELETE)
- **Updated_at Triggers**: Automatically update `updated_at` column on any table modification
- **JSON Storage**: Before/after images stored as JSON objects for complete change tracking
- **No TTL Required**: Immediate cleanup after successful processing eliminates need for time-based retention
- **Integrated Setup**: CDC setup happens during Registry initialization, not as separate service

### **Circuit Breaker Implementation**
- **Health monitoring**: Detect unresponsive components
- **Subscription breaking**: Break CDC subscriptions when health issues detected
- **Timeout waiting**: Wait for components to consume pending events
- **Direct refresh**: Tell components to refresh from Registry if timeout reached
- **Self-reinitialization**: Astha reinitializes itself after recovery

### **Astha CDC Consumer (Core Functionality)**
- **Change Detection**: Polls `__cdc_log` table for new changes at configurable intervals
- **Batch Processing**: Collects changes up to configurable batch size (default: 256)
- **Ordered Processing**: Processes changes in strict ID order (ORDER BY id ASC)
- **Event Conversion**: Converts CDC changes to Astha events with proper typing
- **Immediate Cleanup**: Deletes processed logs immediately after successful processing
- **ACID Transactions**: Cleanup happens in same transaction as event processing
- **Error Handling**: Failed batches are not cleaned up, allowing automatic retry
- **Crash Recovery**: Unprocessed logs preserved for recovery after restart
- **No External Dependencies**: Pure Go implementation integrated into Astha core

### **Event Routing Logic**
- **Hardcoded subscriptions**: Component-table mappings are static
- **Table-based routing**: Route events based on affected table
- **Component filtering**: Only send events to interested components
- **Event ordering**: Maintain event order within table scope
- **Delivery guarantees**: Ensure events reach all subscribed components

### **CDC Log Processing Flow**
- **Change Capture**: SQLite triggers automatically populate `__cdc_log` table
- **Change Detection**: Astha polls log table at configurable intervals (e.g., 100ms)
- **Batch Collection**: Collects changes up to batch size limit (default: 256)
- **Event Processing**: Converts CDC changes to typed Astha events
- **Component Delivery**: Routes events to subscribed components
- **Immediate Cleanup**: Deletes processed logs after successful delivery
- **Retry Logic**: Failed deliveries preserve logs for retry attempts
- **Order Preservation**: Maintains strict chronological order of changes

### **Performance Considerations**
- **No event persistence**: Events are fire-and-forget
- **Direct type access**: No JSON parsing overhead in components
- **Efficient routing**: O(1) lookup for component subscriptions
- **Minimal memory footprint**: Events processed immediately, not stored
- **Concurrent processing**: Handle multiple events simultaneously
- **Batch optimization**: Process up to 256 changes per batch for efficiency
- **Immediate cleanup**: CDC logs don't accumulate, keeping table size small
- **Ordered processing**: Sequential processing eliminates need for complex ordering logic
- **Configurable polling**: Adjustable polling intervals for performance tuning
- **Memory efficiency**: Process and discard changes immediately, no long-term storage

### **Error Handling & Recovery**
- **Component failures**: Detect and handle individual component failures
- **Event delivery failures**: Retry logic with exponential backoff
- **Circuit breaker**: Prevent cascade failures
- **State recovery**: Components can refresh from Registry
- **Astha recovery**: Self-healing mechanisms for scheduler
- **CDC processing failures**: Failed batches are not cleaned up, allowing retry
- **Log corruption handling**: Malformed CDC log entries are logged and skipped
- **Database connection failures**: Automatic reconnection with exponential backoff
- **Partial processing recovery**: Failed event conversions don't block entire batch
- **Crash recovery**: Unprocessed CDC logs preserved for recovery after restart

### **Integration Points**
- **Registry watcher**: Subscribe to SQLite table changes
- **Component lifecycle**: Bootstrap, subscribe, process, recover
- **Storage manager**: Coordinate with data insertion flow
- **Path manager**: Use existing path resolution logic
- **Existing metadata**: Leverage current Registry infrastructure
- **Registry CDC setup**: CDC triggers and log table created during Registry initialization
- **Storage manager integration**: Post-insert metadata updates trigger CDC events automatically
- **Astha core integration**: CDC consumer becomes part of Astha's main event processing loop
- **Component subscription**: Hardcoded table subscriptions determine event routing
- **Event store integration**: CDC events flow directly into Astha's event distribution system

## 🎯 **Architectural Decisions & Future Plans**

### **Metadata Update Strategy**
- **Default**: Asynchronous metadata updates for performance
- **Configurable**: Users can enable synchronous mode via connection settings
- **Safe**: Not just simple goroutines - proper orchestration needed

### **Astha CDC-Driven Architecture**
- **Central Scheduler**: Astha CDC scheduler subscribes to Registry updates
- **Event Distribution**: Routes updates to different components (Iceberg, Compactor, etc.)
- **Component Initialization**: Components bootstrap from Registry, then subscribe to Astha events
- **Decoupled Design**: Components don't directly interact, only through Astha events
- **Hardcoded Registration**: Component subscriptions are hardcoded, not dynamic

### **Architecture Flow**
```
Registry Updates → Astha CDC Scheduler → Component Distribution
     ↓                    ↓              ↓
SQLite Tables    Event Routing    Iceberg Updater
     ↓                    ↓              ↓
Initial State    Event Ordering   Compactor
     ↓                    ↓              ↓
Bootstrap        Delivery Guar.   Snapshotter
     ↓                    ↓              ↓
Subscribe        Health Monitor   Metadata Orchestrator
```

### **Event Types & Flow**
1. **Registry Event**: Table/file metadata changes (CRUD operations only)
2. **Astha Processing**: Event ordering, routing, delivery
3. **Component Action**: Each component processes relevant events
4. **State Sync**: Components stay in sync via events
5. **Recovery**: Components can refresh from Registry if needed

### **Snapshot Management**
- **Separate Component**: Snapshotter service will handle all snapshot operations
- **Triggers**: Time-based, size-based, manual, and query-triggered snapshots
- **Integration**: Will coordinate with metadata updates and compaction via CDC events

### **File Management**
- **File Rotation**: Timing-based file rotation will drive batch metadata updates
- **Compaction Service**: Separate service for intelligent file merging and optimization
- **Performance**: Batch operations for metadata updates, not individual file tracking

### **Implementation Priority**
1. **Phase 1**: Basic registry updates and file tracking
2. **Phase 2**: Simplified metadata updates (prepare for Snapshotter)
3. **Phase 3**: Integration and testing
4. **Future**: Snapshotter, Compaction Service, and Metadata Orchestrator

**Note**: This milestone focuses on foundational metadata updates. Advanced features like intelligent snapshots and compaction will be separate, more sophisticated components.

## ⚙️ **Configuration & Implementation Details**

### **Registry CDC Configuration**
```go
// Registry CDC setup options
type RegistryCDCOptions struct {
    LogTableName    string        // Default: "__cdc_log"
    BatchSize       int           // Default: 256
    PollInterval    time.Duration // Default: 100ms
    EnableBlobs     bool          // Default: false (performance)
    SubsecondTime   bool          // Default: true (precision)
}

// Default configuration
var DefaultRegistryCDCOptions = RegistryCDCOptions{
    LogTableName:    "__cdc_log",
    BatchSize:       256,
    PollInterval:    100 * time.Millisecond,
    EnableBlobs:     false,
    SubsecondTime:   true,
}
```

### **Monitored Tables Configuration**
```go
// Tables to monitor with CDC (hardcoded for Phase 1)
var MonitoredTables = []string{
    "tables",           // Table-level statistics
    "table_files",      // File tracking information
    "file_statistics",  // File-level statistics
    "table_metadata",   // Schema and engine info
}

// Component subscription mappings
var ComponentSubscriptions = map[string][]string{
    "iceberg_updater": {"tables", "table_files", "table_metadata"},
    "compactor":       {"tables", "table_files", "file_statistics"},
    "snapshotter":     {"tables", "table_files", "file_statistics", "table_metadata"},
    "metadata_orchestrator": {"tables", "table_metadata"},
}
```

### **Performance Tuning Parameters**
```go
// Performance configuration for CDC processing
type CDCPerformanceConfig struct {
    // Batch processing
    MaxBatchSize        int           // Maximum changes per batch
    MinBatchSize        int           // Minimum changes to trigger processing
    
    // Polling configuration
    PollInterval        time.Duration // How often to check for changes
    MaxPollDelay        time.Duration // Maximum delay between polls
    
    // Memory management
    MaxMemoryUsage      int64         // Maximum memory for CDC processing
    GarbageCollection   time.Duration // How often to run GC
    
    // Error handling
    MaxRetryAttempts    int           // Maximum retry attempts for failed batches
    RetryBackoff        time.Duration // Backoff delay between retries
}

// Default performance settings
var DefaultCDCPerformanceConfig = CDCPerformanceConfig{
    MaxBatchSize:     256,
    MinBatchSize:     1,
    PollInterval:     100 * time.Millisecond,
    MaxPollDelay:     1 * time.Second,
    MaxMemoryUsage:   100 * 1024 * 1024, // 100MB
    GarbageCollection: 5 * time.Minute,
    MaxRetryAttempts: 3,
    RetryBackoff:     1 * time.Second,
}
```

### **Error Handling & Logging**
```go
// CDC error types
type CDCError struct {
    Type        string    `json:"type"`
    Message     string    `json:"message"`
    TableName   string    `json:"table_name"`
    Operation   string    `json:"operation"`
    Timestamp   time.Time `json:"timestamp"`
    RetryCount  int       `json:"retry_count"`
}

// Error handling strategies
const (
    ErrorTypeProcessingFailed = "processing_failed"
    ErrorTypeConversionFailed = "conversion_failed"
    ErrorTypeDeliveryFailed   = "delivery_failed"
    ErrorTypeDatabaseError     = "database_error"
    ErrorTypeCorruption        = "data_corruption"
)

// Logging levels for CDC operations
const (
    LogLevelCDCInfo     = "info"      // General CDC information
    LogLevelCDCWarning  = "warning"   // Non-critical issues
    LogLevelCDCError    = "error"     // Processing failures
    LogLevelCDCDebug    = "debug"     // Detailed processing info
    LogLevelCDCTrace    = "trace"     // Full change details
)
```

### **Monitoring & Metrics**
```go
// CDC metrics for monitoring
type CDCMetrics struct {
    // Processing metrics
    ChangesProcessed    int64         // Total changes processed
    ChangesFailed       int64         // Total changes that failed
    ProcessingLatency   time.Duration // Average processing time
    
    // Batch metrics
    BatchesProcessed    int64         // Total batches processed
    AverageBatchSize    float64       // Average batch size
    MaxBatchSize        int64         // Largest batch processed
    
    // Cleanup metrics
    LogsCleaned         int64         // Total logs cleaned up
    CleanupLatency      time.Duration // Average cleanup time
    
    // Error metrics
    RetryCount          int64         // Total retry attempts
    ErrorRate           float64       // Error rate percentage
    LastErrorTime       time.Time     // Timestamp of last error
}

// Metrics collection interface
type MetricsCollector interface {
    RecordChangeProcessed(tableName string, operation string, duration time.Duration)
    RecordChangeFailed(tableName string, operation string, errorType string)
    RecordBatchProcessed(batchSize int, duration time.Duration)
    RecordLogCleanup(count int, duration time.Duration)
    GetMetrics() CDCMetrics
}
```

## 🔧 **Technical Implementation Details**

### **Registry CDC Table Schema**
```sql
-- CDC log table for capturing all table changes
CREATE TABLE __cdc_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,                    -- Change timestamp with subsecond precision
    tablename TEXT NOT NULL,                    -- Name of the table that changed
    operation TEXT NOT NULL,                    -- INSERT, UPDATE, DELETE
    before TEXT,                                -- JSON of OLD values (UPDATE/DELETE)
    after TEXT,                                 -- JSON of NEW values (INSERT/UPDATE)
    created_at TEXT NOT NULL                    -- When this log entry was created
);

-- Indexes for efficient CDC processing
CREATE INDEX idx_cdc_log_timestamp ON __cdc_log(timestamp);
CREATE INDEX idx_cdc_log_tablename ON __cdc_log(tablename);
CREATE INDEX idx_cdc_log_operation ON __cdc_log(operation);
CREATE INDEX idx_cdc_log_created ON __cdc_log(created_at);
```

### **CDC Trigger Examples**
```sql
-- Example: CDC triggers for 'tables' table
CREATE TRIGGER IF NOT EXISTS tables__cdc_insert 
AFTER INSERT ON tables 
BEGIN
    INSERT INTO __cdc_log (timestamp, tablename, operation, before, after, created_at) 
    VALUES (datetime('now', 'subsec'), 'tables', 'INSERT', NULL, 
            json_object('id', NEW.id, 'name', NEW.name, 'database_id', NEW.database_id), 
            datetime('now'));
END;

CREATE TRIGGER IF NOT EXISTS tables__cdc_update 
AFTER UPDATE ON tables 
BEGIN
    INSERT INTO __cdc_log (timestamp, tablename, operation, before, after, created_at) 
    VALUES (datetime('now', 'subsec'), 'tables', 'UPDATE', 
            json_object('id', OLD.id, 'name', OLD.name, 'database_id', OLD.database_id), 
            json_object('id', NEW.id, 'name', NEW.name, 'database_id', NEW.database_id), 
            datetime('now'));
END;

CREATE TRIGGER IF NOT EXISTS tables__cdc_delete 
AFTER DELETE ON tables 
BEGIN
    INSERT INTO __cdc_log (timestamp, tablename, operation, before, after, created_at) 
    VALUES (datetime('now', 'subsec'), 'tables', 'DELETE', 
            json_object('id', OLD.id, 'name', OLD.name, 'database_id', OLD.database_id), 
            NULL, datetime('now'));
END;

-- Updated_at trigger for automatic timestamp updates
CREATE TRIGGER IF NOT EXISTS tables__updated_at 
AFTER UPDATE ON tables 
BEGIN
    UPDATE tables SET updated_at = datetime('now') WHERE id = NEW.id;
END;
```

### **Astha CDC Consumer Implementation**
```go
// server/astha/cdc_consumer.go
type CDCConsumer struct {
    db          *sql.DB
    logTable    string
    batchSize   int                    // Default: 256
    pollInterval time.Duration         // Default: 100ms
    logger      zerolog.Logger
    eventStore  EventStore
}

// Main processing loop
func (c *CDCConsumer) Start(ctx context.Context) error {
    ticker := time.NewTicker(c.pollInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            if err := c.processChanges(ctx); err != nil {
                c.logger.Error().Err(err).Msg("Failed to process CDC changes")
            }
        }
    }
}

// Process changes with immediate cleanup
func (c *CDCConsumer) processChanges(ctx context.Context) error {
    for {
        // 1. Get unprocessed changes (ordered by ID, limited by batch size)
        changes, err := c.getUnprocessedChanges(ctx)
        if err != nil {
            return fmt.Errorf("failed to get unprocessed changes: %w", err)
        }
        
        if len(changes) == 0 {
            return nil // No changes to process
        }
        
        // 2. Process the batch
        if err := c.processBatch(ctx, changes); err != nil {
            return fmt.Errorf("failed to process batch: %w", err)
        }
        
        // 3. ONLY if processing succeeds, delete processed logs
        if err := c.deleteProcessedLogs(ctx, changes); err != nil {
            return fmt.Errorf("failed to delete processed logs: %w", err)
        }
    }
}

// Get unprocessed changes
func (c *CDCConsumer) getUnprocessedChanges(ctx context.Context) ([]CDCLogEntry, error) {
    query := fmt.Sprintf(`
        SELECT id, timestamp, tablename, operation, before, after, created_at 
        FROM %s 
        ORDER BY id ASC 
        LIMIT ?
    `, c.logTable)
    
    rows, err := c.db.QueryContext(ctx, query, c.batchSize)
    if err != nil {
        return nil, fmt.Errorf("failed to query CDC log: %w", err)
    }
    defer rows.Close()
    
    var changes []CDCLogEntry
    for rows.Next() {
        var change CDCLogEntry
        if err := rows.Scan(&change.ID, &change.Timestamp, &change.TableName, 
                           &change.Operation, &change.Before, &change.After, &change.CreatedAt); err != nil {
            return nil, fmt.Errorf("failed to scan CDC log entry: %w", err)
        }
        changes = append(changes, change)
    }
    
    return changes, nil
}

// Delete processed logs (immediate cleanup)
func (c *CDCConsumer) deleteProcessedLogs(ctx context.Context, changes []CDCLogEntry) error {
    if len(changes) == 0 {
        return nil
    }
    
    // Get the highest ID we processed
    maxID := changes[len(changes)-1].ID
    
    tx, err := c.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback()
    
    // Delete logs up to the highest processed ID
    deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id <= ?`, c.logTable)
    _, err = tx.ExecContext(ctx, deleteQuery, maxID)
    if err != nil {
        return fmt.Errorf("failed to delete processed logs: %w", err)
    }
    
    if err = tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit deletion: %w", err)
    }
    
    c.logger.Debug().Int64("max_id", maxID).Int("deleted_count", len(changes)).
        Msg("Deleted processed CDC logs")
    
    return nil
}
```

### **Event Conversion Logic**
```go
// Convert CDC changes to Astha events
func (c *CDCConsumer) convertChangeToEvent(change CDCLogEntry) (Event[any], error) {
    var event Event[any]
    
    // Parse timestamp
    timestamp, err := time.Parse("2006-01-02 15:04:05.999999999", change.Timestamp)
    if err != nil {
        return Event[any]{}, fmt.Errorf("failed to parse timestamp: %w", err)
    }
    
    // Parse table-specific data based on operation
    switch change.TableName {
    case "tables":
        event = c.convertTableChange(change, timestamp)
    case "table_files":
        event = c.convertTableFileChange(change, timestamp)
    case "file_statistics":
        event = c.convertFileStatisticsChange(change, timestamp)
    default:
        return Event[any]{}, fmt.Errorf("unknown table: %s", change.TableName)
    }
    
    return event, nil
}

// Convert table change to event
func (c *CDCConsumer) convertTableChange(change CDCLogEntry, timestamp time.Time) Event[any] {
    var data TableStats
    
    switch change.Operation {
    case "INSERT", "UPDATE":
        // Parse 'after' JSON to get current table stats
        if err := json.Unmarshal([]byte(change.After), &data); err != nil {
            c.logger.Error().Err(err).Msg("Failed to parse table change data")
        }
    case "DELETE":
        // Parse 'before' JSON to get deleted table stats
        if err := json.Unmarshal([]byte(change.Before), &data); err != nil {
            c.logger.Error().Err(err).Msg("Failed to parse table change data")
        }
    }
    
    return Event[any]{
        Table:     "tables",
        Operation: string(change.Operation),
        Data:      data,
        Timestamp: timestamp,
    }
}
```

## 🔧 **Component Implementation Examples**

### **Example: Iceberg Updater Component**
```go
type IcebergUpdater struct {
    logger zerolog.Logger
    // ... other fields
}

// Implement Subscriber interface for TableMetadata events
func (iu *IcebergUpdater) OnEvent(event Event[TableMetadata]) error {
    metadata := event.Data // Already typed as TableMetadata
    
    switch event.Operation {
    case "INSERT":
        return iu.handleMetadataInsert(metadata)
    case "UPDATE":
        return iu.handleMetadataUpdate(metadata)
    case "DELETE":
        return iu.handleMetadataDelete(metadata)
    default:
        return fmt.Errorf("unknown operation: %s", event.Operation)
    }
}

// Implement Subscriber interface for TableFiles events
func (iu *IcebergUpdater) OnEvent(event Event[TableFiles]) error {
    fileInfo := event.Data // Already typed as TableFiles
    
    switch event.Operation {
    case "INSERT":
        return iu.handleFileInsert(fileInfo)
    case "UPDATE":
        return iu.handleFileUpdate(fileInfo)
    case "DELETE":
        return iu.handleFileDelete(fileInfo)
    default:
        return fmt.Errorf("unknown operation: %s", event.Operation)
    }
}

func (iu *IcebergUpdater) OnHealth() error {
    // Return health status
    return nil
}

func (iu *IcebergUpdater) OnRefresh() error {
    // Refresh state from Registry
    return iu.refreshFromRegistry()
}

func (iu *IcebergUpdater) GetSubscribedTables() []string {
    return []string{"table_metadata", "table_files"}
}
```

### **Example: Compactor Component**
```go
type Compactor struct {
    logger zerolog.Logger
    // ... other fields
}

// Implement Subscriber interface for TableFiles events
func (c *Compactor) OnEvent(event Event[TableFiles]) error {
    fileInfo := event.Data // Already typed as TableFiles
    
    switch event.Operation {
    case "INSERT":
        return c.handleFileInsert(fileInfo)
    case "UPDATE":
        return c.handleFileUpdate(fileInfo)
    case "DELETE":
        return c.handleFileDelete(fileInfo)
    default:
        return fmt.Errorf("unknown operation: %s", event.Operation)
    }
}

// Implement Subscriber interface for TableStats events
func (c *Compactor) OnEvent(event Event[TableStats]) error {
    stats := event.Data // Already typed as TableStats
    
    switch event.Operation {
    case "INSERT":
        return c.handleStatsInsert(stats)
    case "UPDATE":
        return c.handleStatsUpdate(stats)
    case "DELETE":
        return c.handleStatsDelete(stats)
    default:
        return fmt.Errorf("unknown operation: %s", event.Operation)
    }
}

func (c *Compactor) OnHealth() error {
    return nil
}

func (c *Compactor) OnRefresh() error {
    return c.refreshFromRegistry()
}

func (c *Compactor) GetSubscribedTables() []string {
    return []string{"table_files", "table_stats"}
}
```

### **Example: Astha Event Routing**
```go
// Hardcoded component subscriptions
var ComponentSubscriptions = map[string][]string{
    "iceberg_updater": {"table_metadata", "table_files"},
    "compactor":       {"table_files", "table_stats"},
    "snapshotter":     {"table_metadata", "table_files", "table_snapshots"},
    "metadata_orchestrator": {"tables", "table_metadata"},
}

// Route events to appropriate components
func (a *Astha) routeEvent(event Event[any]) error {
    table := event.Table
    
    for componentName, subscribedTables := range ComponentSubscriptions {
        if contains(subscribedTables, table) {
            if err := a.routeToComponent(componentName, event); err != nil {
                a.logger.Error().Err(err).Str("component", componentName).Msg("Failed to route event")
            }
        }
    }
    return nil
}
```

## 🚧 Phase 2: Iceberg Metadata Updates - IN PROGRESS

### 2.1 Iceberg Manager Implementation - COMPLETED ✅

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

### 2.2 Path Manager Integration - COMPLETED ✅

- [x] **Manifest Path Support**
  - [x] Added `GetTableManifestPath` method to PathManager interface
  - [x] Implemented in concrete PathManager and MockPathManager
  - [x] Paths follow consistent structure: `{base}/tables/{namespace}/{table}/manifests`

### 2.3 MetadataManager Integration - COMPLETED ✅

- [x] **MetadataManager Integration - COMPLETED ✅**
  - [x] Integrate Iceberg manager with MetadataManager
  - [x] Subscribe to `table_files` events from Astha
  - [x] Implement startup recovery for pending files
  - [x] Add retry mechanism for failed operations

- [x] **Iceberg Metadata Generation - COMPLETED ✅**
  - [x] Implement actual manifest generation (JSON format for now, Avro format planned)
  - [x] Implement metadata file updates with snapshots
  - [x] Add file statistics and basic metadata support
  - [x] Create MetadataGenerator component for clean separation of concerns

- [x] **Testing and Validation - COMPLETED ✅**
  - [x] Unit tests for Iceberg manager components
  - [x] Unit tests for MetadataGenerator component
  - [x] End-to-end metadata generation tests
  - [ ] Integration tests with Registry and Astha (next step)
  - [ ] Performance testing for batch processing (future enhancement)

### 2.4 Next Steps - PENDING ⏳

- [ ] **Integration Testing**
  - [ ] Test MetadataManager startup with Iceberg integration
  - [ ] Test Astha CDC event processing for table_files
  - [ ] Test startup recovery for pending files
  - [ ] Test retry mechanism for failed operations

- [ ] **Real Table Info Integration**
  - [ ] Replace placeholder table info with actual Registry lookups
  - [ ] Implement proper database/table name resolution
  - [ ] Add table metadata validation

- [ ] **Avro Format Implementation**
  - [ ] Upgrade from JSON to proper Avro format for manifests
  - [ ] Implement Iceberg-compliant manifest structure
  - [ ] Add proper schema validation
