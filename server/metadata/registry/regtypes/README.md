# Registry Types (regtypes)

This package contains the core type definitions for the Icebox metadata registry system, organized for clarity and maintainability.

## 🏗️ **Package Overview**

The `regtypes` package provides strongly-typed structures that represent the database schema for the metadata registry. These types are used throughout the system for:

- Database operations with Bun ORM
- CDC (Change Data Capture) event processing
- API responses and data transfer
- Type-safe metadata handling

## 📁 **Package Structure**

```
regtypes/
├── types.go      # Core database types and relationships
├── constants.go  # System constants and default values
└── README.md     # This documentation
```

## 🗄️ **Core Database Types**

### **Database Entities**
- **`User`** - User account information and authentication
- **`Database`** - Database metadata and organization
- **`Table`** - Table-level metadata and statistics

### **Table Schema & Metadata**
- **`TableMetadata`** - Schema and engine configuration
- **`TableColumn`** - Column definitions and constraints

### **Storage & Files**
- **`TableFile`** - File tracking with Iceberg metadata state

### **Table Optimization**
- **`TablePartition`** - Partition information and statistics
- **`TableIndex`** - Index definitions and metadata
- **`TableConstraint`** - Table constraints and relationships

### **Monitoring & Statistics**
- **`TableStatistic`** - Statistical information about tables

### **System Tables**
- **`AccessLog`** - Complete audit trail
- **`SchemaVersion`** - Schema versioning and migration tracking

## 🔧 **Type Organization**

### **Logical Grouping**
Types are organized into logical sections for better maintainability:

1. **Core Database Entities** - Users, databases, tables
2. **Table Metadata & Schema** - Schema and column information
3. **Table Files & Storage** - File tracking and storage
4. **Table Optimization & Indexing** - Partitions, indexes, constraints
5. **Table Statistics & Monitoring** - Performance metrics
6. **System & Audit Tables** - Logging and versioning

### **Relationship Mapping**
Each type includes proper Bun ORM relationship tags:
- `belongs-to` relationships for foreign keys
- Proper join conditions
- Clear relationship documentation

## 📊 **Constants & Defaults**

### **System Constants**
- **Iceberg Metadata States** - File processing status tracking
- **Table Types** - User, system, temporary, external
- **Storage Engines** - Filesystem, memory, S3
- **File Formats** - Parquet, JSON, CSV, Avro
- **Compression** - None, Gzip, Snappy, LZ4

### **Default Values**
- Schema version defaults
- Table type defaults
- Storage engine defaults
- File format defaults

## 🚀 **Usage Examples**

### **Basic Table Creation**
```go
import "github.com/TFMV/icebox/server/metadata/registry/regtypes"

// Create a new table
table := &regtypes.Table{
    DatabaseID: 1,
    Name:       "users",
    TableType:  regtypes.TableTypeUser,
    RowCount:   0,
    FileCount:  0,
    TotalSize:  0,
}
```

### **Table Metadata**
```go
// Create table metadata
metadata := &regtypes.TableMetadata{
    TableID:       1,
    SchemaVersion: regtypes.DefaultSchemaVersion,
    StorageEngine: regtypes.StorageEngineFilesystem,
    Format:        regtypes.FileTypeParquet,
    Compression:   regtypes.CompressionSnappy,
}
```

### **File Tracking**
```go
// Track a new file
file := &regtypes.TableFile{
    TableID:              1,
    FileName:             "part-0.parquet",
    FilePath:             "tables/users/part-0.parquet",
    FileSize:             1024,
    FileType:             regtypes.FileTypeParquet,
    IcebergMetadataState: regtypes.IcebergMetadataGenerationStatePending,
}
```

## 🔍 **Type Validation**

### **Bun ORM Tags**
All types include proper Bun ORM tags:
- `pk` for primary keys
- `autoincrement` for auto-incrementing fields
- `notnull` for required fields
- `default` for default values
- `unique` for unique constraints

### **JSON Tags**
All types include JSON tags for serialization:
- Proper field naming
- Omit empty fields where appropriate
- Consistent naming conventions

## 📈 **Performance Considerations**

### **Memory Efficiency**
- Proper use of pointers for optional relationships
- Efficient string handling
- Optimized time field types

### **Database Optimization**
- Proper indexing through relationship tags
- Efficient join conditions
- Optimized field types for storage

## 🔧 **Development Guidelines**

### **Adding New Types**
1. **Group logically** - Place in appropriate section
2. **Document relationships** - Add clear relationship comments
3. **Include tags** - Add proper Bun ORM and JSON tags
4. **Update README** - Document new types here

### **Modifying Existing Types**
1. **Maintain backward compatibility** - Don't break existing code
2. **Update documentation** - Keep README current
3. **Test thoroughly** - Ensure ORM operations still work
4. **Consider migrations** - Plan for schema changes

## 🎯 **Integration Points**

### **Registry Package**
- Types are used by the main registry package
- Support lazy loading and composite types
- Enable efficient metadata operations

### **Iceberg Manager**
- File tracking and metadata state
- Batch processing and optimization
- Performance monitoring

### **CDC System**
- Change detection and logging
- Event processing and routing
- Audit trail maintenance

## 📚 **Related Documentation**

- [Registry Package](../README.md) - Main registry implementation
- [Types Package](../types.go) - Composite types with lazy loading
- [Constants](../constants.go) - System constants and defaults
- [Bun ORM](https://bun.uptrace.dev/) - Database ORM documentation
