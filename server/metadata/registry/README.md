# Production-Ready Registry Schema

This directory contains a production-ready, normalized database schema for the Icebox metadata registry system.

## Overview

The new production schema replaces the old "menace" tables with a properly normalized, enterprise-grade design that includes:

- **Proper Foreign Keys**: All relationships properly defined with CASCADE/RESTRICT rules
- **Normalized Structure**: 3NF normalization with surrogate keys
- **Performance Indexes**: Strategic indexing for common query patterns
- **User Management**: Complete user access control system
- **Audit Trail**: Comprehensive logging and tracking
- **Partitioning Support**: Table partitioning metadata
- **Schema Versioning**: Complete schema evolution tracking

## Key Tables

### Core Tables
- **`users`**: User management and access control
- **`databases`**: Database metadata with ownership tracking
- **`tables`**: Table metadata with database relationships

### Metadata Tables
- **`table_metadata`**: Comprehensive table schema and engine info
- **`table_columns`**: Column-level metadata and constraints
- **`table_indexes`**: Index tracking and management
- **`table_constraints`**: Constraint management (PK, FK, etc.)

### File Management
- **`table_files`**: File tracking with partitioning support
- **`table_partitions`**: Partition management and statistics

### System Tables
- **`table_statistics`**: Performance analytics and statistics
- **`access_log`**: Complete audit trail
- **`schema_versions`**: Schema versioning and migration tracking

## Usage

### Basic Setup

```go
import "github.com/TFMV/icebox/server/metadata/registry"

// Create a new production store
store, err := registry.NewProductionStore(dbPath, basePath)
if err != nil {
    log.Fatal(err)
}

// Initialize the production schema (this will drop old tables and create new ones)
ctx := context.Background()
err = store.InitializeProductionSchema(ctx)
if err != nil {
    log.Fatal(err)
}
```

### Creating Databases and Tables

```go
// Create a database
err = store.CreateDatabase(ctx, "analytics")
if err != nil {
    log.Fatal(err)
}

// Create a table
err = store.CreateTable(ctx, "analytics", "user_events")
if err != nil {
    log.Fatal(err)
}

// List databases
databases, err := store.ListDatabases(ctx)
if err != nil {
    log.Fatal(err)
}

// List tables in a database
tables, err := store.ListTables(ctx, "analytics")
if err != nil {
    log.Fatal(err)
}
```

## Schema Design Principles

### 1. Normalization
- **3NF Design**: Eliminates transitive dependencies
- **Surrogate Keys**: Auto-incrementing IDs for all tables
- **Proper Relationships**: Foreign keys with appropriate CASCADE/RESTRICT rules

### 2. Performance
- **Strategic Indexing**: Indexes on commonly queried columns
- **Composite Indexes**: Multi-column indexes for complex queries
- **Unique Constraints**: Data integrity through unique indexes

### 3. Scalability
- **Partitioning Support**: Metadata for table partitioning
- **Statistics Tracking**: Performance analytics for optimization
- **Audit Logging**: Complete access trail for compliance

### 4. Maintainability
- **Clear Structure**: Logical table organization
- **Versioning**: Schema evolution tracking
- **Documentation**: Comprehensive field documentation

## Migration Strategy

Since this is an experimental stage product, the approach is:

1. **Clean Slate**: Drop all old tables completely
2. **Fresh Start**: Create new production schema from scratch
3. **No Backward Compatibility**: Focus on future-proof design
4. **Simple Integration**: Easy to integrate with existing code

## Benefits

### Data Integrity
- **Foreign Key Constraints**: Prevents orphaned records
- **Unique Constraints**: Ensures data uniqueness
- **Cascade Rules**: Proper cleanup on deletions

### Performance
- **Optimized Indexes**: Fast query execution
- **Efficient Joins**: Proper relationship design
- **Statistics**: Query optimization insights

### Enterprise Features
- **User Management**: Role-based access control
- **Audit Trail**: Complete operation logging
- **Partitioning**: Large dataset support
- **Versioning**: Schema evolution tracking

## Future Enhancements

The schema is designed to support future features:

- **Multi-tenancy**: User isolation and permissions
- **Advanced Partitioning**: Dynamic partition management
- **Performance Monitoring**: Real-time statistics and alerts
- **Schema Evolution**: Automated migration tools
- **Backup/Recovery**: Metadata backup strategies

## Testing

To test the new schema:

```go
// Create test store
store, err := registry.NewProductionStore(":memory:", "/tmp/test")
if err != nil {
    t.Fatal(err)
}

// Initialize schema
err = store.InitializeProductionSchema(ctx)
if err != nil {
    t.Fatal(err)
}

// Test operations
err = store.CreateDatabase(ctx, "testdb")
if err != nil {
    t.Fatal(err)
}

// Verify schema
tables, err := store.ListDatabases(ctx)
if err != nil {
    t.Fatal(err)
}

if len(tables) != 1 || tables[0] != "testdb" {
    t.Error("Database creation failed")
}
```

## Conclusion

This production schema transforms the registry from a "menace" of weak foreign keys and denormalized structure into a robust, enterprise-grade metadata management system that follows database design best practices and provides a solid foundation for future growth.
