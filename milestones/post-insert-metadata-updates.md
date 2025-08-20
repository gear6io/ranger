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

### Phase 1: Registry Updates (Week 1)

#### 1.1 File Tracking Implementation
- [ ] **Create `updateTableFiles` method in Storage Manager**
  - [ ] Track new data files in `table_files` table
  - [ ] Update file metadata (size, row count, checksum)
  - [ ] Handle file rotation and cleanup
  - [ ] Support multiple file formats (JSON, Parquet)
  - [ ] **Note**: File rotation timing will be used for batch metadata updates

#### 1.2 Statistics Management
- [ ] **Create `updateTableStatistics` method**
  - [ ] Update `row_count` in `tables` table
  - [ ] Update `total_size` in `tables` table
  - [ ] Update `file_count` in `tables` table
  - [ ] Update `last_modified` timestamps

#### 1.3 Transaction Support
- [ ] **Add metadata transaction handling**
  - [ ] Ensure metadata consistency
  - [ ] Handle rollback scenarios
  - [ ] Atomic updates for related metadata
  - [ ] Concurrent insertion support

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

### **Component Interface Requirements**
- **Subscriber[T] interface**: All components must implement this interface
- **Generic type events**: Event[T] where T matches table schema type
- **Three required methods**: OnEvent(), OnHealth(), OnRefresh()
- **Type safety**: Compile-time type checking prevents runtime errors
- **No marshaling**: Components receive typed data directly

### **Circuit Breaker Implementation**
- **Health monitoring**: Detect unresponsive components
- **Subscription breaking**: Break CDC subscriptions when health issues detected
- **Timeout waiting**: Wait for components to consume pending events
- **Direct refresh**: Tell components to refresh from Registry if timeout reached
- **Self-reinitialization**: Astha reinitializes itself after recovery

### **Event Routing Logic**
- **Hardcoded subscriptions**: Component-table mappings are static
- **Table-based routing**: Route events based on affected table
- **Component filtering**: Only send events to interested components
- **Event ordering**: Maintain event order within table scope
- **Delivery guarantees**: Ensure events reach all subscribed components

### **Performance Considerations**
- **No event persistence**: Events are fire-and-forget
- **Direct type access**: No JSON parsing overhead in components
- **Efficient routing**: O(1) lookup for component subscriptions
- **Minimal memory footprint**: Events processed immediately, not stored
- **Concurrent processing**: Handle multiple events simultaneously

### **Error Handling & Recovery**
- **Component failures**: Detect and handle individual component failures
- **Event delivery failures**: Retry logic with exponential backoff
- **Circuit breaker**: Prevent cascade failures
- **State recovery**: Components can refresh from Registry
- **Astha recovery**: Self-healing mechanisms for scheduler

### **Integration Points**
- **Registry watcher**: Subscribe to SQLite table changes
- **Component lifecycle**: Bootstrap, subscribe, process, recover
- **Storage manager**: Coordinate with data insertion flow
- **Path manager**: Use existing path resolution logic
- **Existing metadata**: Leverage current Registry infrastructure

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
