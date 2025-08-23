# Registry Types (regtypes)

The `regtypes` package is the **single source of truth** for all types used throughout the Icebox project. It provides a consistent, type-safe interface for working with database entities, business logic, and metadata.

## 🏗️ Architecture

### Core Bun Models
- **Direct database table representations** with bun ORM tags
- **Proper relations** between tables (e.g., Table belongs to Database)
- **Type-safe database operations** with bun ORM

### Utility Types
- **Helper structures** for common operations
- **Configuration types** for system setup
- **Event types** for CDC and messaging

### Supertypes (Future)
- **Will be built incrementally** based on actual business needs
- **Extended types** that wrap bun models for business logic
- **Additional methods** and functionality beyond basic database operations

## 📦 Package Structure

```
regtypes/
├── types.go          # Core bun models and utility types
├── constants.go      # Constants and configuration values
└── README.md         # This documentation
```

## 🚀 Usage

### Import the Package

```go
import "github.com/TFMV/icebox/server/metadata/registry/regtypes"
```

### Using Core Bun Models

```go
// Create a new table
table := &regtypes.Table{
    Name:        "users",
    DatabaseID:  1,
    TableType:   regtypes.TableTypeUser,
    IsTemporary: false,
}

// Use with bun ORM
err := db.NewInsert().Model(table).Exec(ctx)
```

### Using Bun Models Directly

```go
// Create a new table
table := &regtypes.Table{
    Name:        "users",
    DatabaseID:  1,
    TableType:   regtypes.TableTypeUser,
    IsTemporary: false,
}

// Use with bun ORM
err := db.NewInsert().Model(table).Exec(ctx)
```

### Using Constants

```go
// Use predefined constants
if table.TableType == regtypes.TableTypeUser {
    // Handle user table
}

// Use batch processing constants
if fileCount <= regtypes.MaxFilesPerBatch {
    // Process batch
}

// Use retry constants
for attempt := 1; attempt <= regtypes.MaxRetryAttempts; attempt++ {
    // Retry logic
}
```

## 🔄 Type Hierarchy

```
Bun Models (Database Tables)
├── User
├── Database
├── Table
├── TableMetadata
├── TableFile
├── TableColumn
├── TablePartition
├── TableIndex
├── TableConstraint
├── TableStatistic
├── AccessLog
└── SchemaVersion

Utility Types
├── TableReference
├── FileReference
├── CDCLogEntry
├── CDCSetup
└── ManagerConfig

Supertypes (Future - Built as Needed)
├── Will be created incrementally
├── Based on actual business requirements
└── Extend bun models with business logic
```

## 🎯 Key Benefits

### 1. **Single Source of Truth**
- All types defined in one place
- No duplicate type definitions
- Consistent across all packages

### 2. **Type Safety**
- Proper bun ORM integration
- Compile-time type checking
- IDE autocomplete support

### 3. **Incremental Growth**
- Supertypes built as needed
- No premature abstraction
- Focus on current requirements

### 4. **Performance**
- Direct bun ORM usage
- No unnecessary type conversions
- Efficient database operations

## 📝 Examples

### Creating a Table with Columns

```go
// Create base table
table := &regtypes.Table{
    Name:        "orders",
    DatabaseID:  dbID,
    TableType:   regtypes.TableTypeUser,
    IsTemporary: false,
}

// Create columns
column := &regtypes.TableColumn{
    ColumnName:      "order_id",
    DataType:        "INTEGER",
    IsPrimary:       true,
    OrdinalPosition: 1,
}

// Use with bun ORM
err := db.NewInsert().Model(table).Exec(ctx)
err = db.NewInsert().Model(column).Exec(ctx)
```

### Working with Files

```go
// Create table file
tableFile := &regtypes.TableFile{
    TableID:              1,
    FileName:             "orders_2024_01.parquet",
    FilePath:             "/data/orders/orders_2024_01.parquet",
    FileSize:             1024000,
    FileType:             regtypes.FileTypeParquet,
    IcebergMetadataState: regtypes.IcebergMetadataGenerationStatePending,
}

// Use directly with bun ORM
err := db.NewInsert().Model(tableFile).Exec(ctx)
```

### Database Operations

```go
// Query with bun ORM
var tables []*regtypes.Table
err := db.NewSelect().
    Model(&tables).
    Where("database_id = ?", dbID).
    Scan(ctx)

// Use tables directly
for _, table := range tables {
    // Access table properties directly
    tableName := table.Name
    tableType := table.TableType
    // ... use as needed
}
```

## 🔧 Migration from Old Types

### Before (Old Way)
```go
import "github.com/TFMV/icebox/server/metadata/registry"

// Using old types
fileInfo := registry.FileInfo{...}
tableInfo := registry.TableInfo{...}
```

### After (New Way)
```go
import "github.com/TFMV/icebox/server/metadata/registry/regtypes"

// Using new regtypes
fileInfo := regtypes.NewFileInfo(&regtypes.TableFile{...})
tableInfo := regtypes.NewTableInfo(&regtypes.Table{...})
```

## 🚨 Important Notes

1. **Always use regtypes** - Don't create duplicate types in other packages
2. **Use bun models directly** - They have proper database tags and relations
3. **Constants are your friend** - Use predefined constants instead of magic strings
4. **Build supertypes as needed** - Create them incrementally based on actual requirements
5. **Keep it simple** - Start with bun models, extend only when necessary

## 🔮 Future Enhancements

- **Validation methods** for type constraints
- **Serialization helpers** for JSON/Protobuf
- **Comparison methods** for sorting and equality
- **Builder patterns** for complex type construction
- **Caching mechanisms** for frequently accessed data

---

**Remember: `regtypes` is the single source of truth for all types in Icebox. Use it consistently across all packages! 🎯**
