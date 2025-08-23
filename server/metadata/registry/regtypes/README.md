# Registry Types (regtypes)

This package contains the core type definitions for the Icebox metadata registry system.

## Overview

The `regtypes` package provides strongly-typed structures that represent the database schema for the metadata registry. These types are used throughout the system for:

- Database operations with Bun ORM
- CDC (Change Data Capture) event processing
- API responses and data transfer
- Type-safe metadata handling

## Core Types

### Database Entities

- **`User`** - User account information
- **`Database`** - Database metadata and configuration
- **`Table`** - Table-level metadata and statistics
- **`TableMetadata`** - Schema and engine configuration
- **`TableFile`** - File tracking with Iceberg metadata state
- **`TableColumn`** - Column definitions and constraints
- **`TablePartition`** - Partition information and statistics
- **`TableIndex`** - Index definitions and metadata
- **`TableConstraint`** - Table constraints and relationships
- **`TableStatistic`** - Statistical information about tables

### Utility Types

- **`TableReference`** - Simple table identifier
- **`FileReference`** - File location and metadata
- **`CDCLogEntry`** - Change data capture log entries
- **`CDCSetup`** - CDC configuration and setup
- **`ManagerConfig`** - Registry manager configuration

## Phase 2.4.1 Implementation Status ✅

### Completed Features

1. **Registry Schema Integration**
   - All core table types are properly defined with Bun ORM tags
   - `iceberg_metadata_state` column added to `TableFile` for tracking
   - Proper relationships and foreign key constraints defined

2. **CDC Event Processing**
   - Generic event parsing for all table types
   - Type-safe conversion from CDC logs to Registry types
   - Support for INSERT, UPDATE, DELETE operations

3. **Astha Integration**
   - Component registration with type adapters
   - Event routing based on table subscriptions
   - Proper error handling and health monitoring

4. **Iceberg Metadata Manager**
   - File processing with state tracking
   - Startup recovery for pending files
   - Integration with Registry CDC events

### Key Improvements

- **Generic Parsing**: Single `parseTableData` function handles all table types
- **Type Safety**: Strong typing throughout the event processing pipeline
- **Error Handling**: Comprehensive error handling with proper logging
- **Performance**: Efficient CDC processing with batch operations

## Usage Examples

### Working with Table Files

```go
// Get pending files for Iceberg metadata generation
pendingFiles, err := storage.GetPendingFilesForIceberg(ctx)
if err != nil {
    return fmt.Errorf("failed to get pending files: %w", err)
}

// Process each file
for _, file := range pendingFiles {
    // File is already typed as *regtypes.TableFile
    if err := icebergManager.ProcessFile(file); err != nil {
        log.Printf("Failed to process file %d: %v", file.ID, err)
    }
}
```

### CDC Event Processing

```go
// CDC events are automatically parsed to appropriate types
func (c *Component) OnEvent(ctx context.Context, event astha.Event[any]) error {
    switch event.Table {
    case "table_files":
        if fileInfo, ok := event.Data.(*registry.FileInfo); ok {
            return c.handleFileEvent(fileInfo)
        }
    case "tables":
        if tableInfo, ok := event.Data.(*registry.TableInfo); ok {
            return c.handleTableEvent(tableInfo)
        }
    }
    return nil
}
```

### Component Registration

```go
// Register component with Astha scheduler
icebergComponent := iceberg.NewIcebergComponent(manager, logger)
if err := astha.RegisterComponentWithInstance(
    icebergComponent.GetComponentInfo(),
    icebergComponent.AsSubscriberAny(),
); err != nil {
    return fmt.Errorf("failed to register component: %w", err)
}
```

## Architecture Benefits

1. **Type Safety**: Compile-time type checking prevents runtime errors
2. **Maintainability**: Single source of truth for all Registry types
3. **Extensibility**: Easy to add new table types and relationships
4. **Performance**: Efficient JSON parsing and type conversion
5. **Reliability**: Comprehensive error handling and validation

## Future Enhancements

- **Schema Validation**: Runtime schema validation for CDC events
- **Type Registry**: Dynamic type registration for custom tables
- **Performance Optimization**: Caching for frequently accessed types
- **Monitoring**: Metrics and health checks for type processing

## Dependencies

- **Bun ORM**: Database operations and migrations
- **JSON**: CDC event serialization/deserialization
- **Time**: Timestamp handling and formatting
- **Zerolog**: Structured logging and error reporting

## Testing

The package includes comprehensive tests for:

- Type definitions and relationships
- JSON marshaling/unmarshaling
- CDC event parsing
- Type conversion utilities
- Error handling scenarios

Run tests with:

```bash
go test ./server/metadata/registry/regtypes/...
```

## Contributing

When adding new types or modifying existing ones:

1. Ensure proper Bun ORM tags are included
2. Add JSON tags for serialization
3. Include proper validation and constraints
4. Update tests to cover new functionality
5. Document any breaking changes

## Related Components

- **Registry Store**: Database operations and CDC setup
- **Astha Scheduler**: Event distribution and component management
- **Iceberg Manager**: Metadata generation and file processing
- **Metadata Manager**: High-level coordination and integration
