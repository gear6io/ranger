# Icebox Storage Package

A standardized storage system that automatically creates a clean, organized directory structure for all Icebox components.

## **🏗️ Architecture**

### **PathManager (Centralized Path Logic)**
The `PathManager` is the **single source of truth** for all path construction in Icebox:

- **Base Paths**: `GetCatalogPath()`, `GetDataPath()`, `GetInternalMetadataPath()`, `GetMigrationsPath()`
- **Table Paths**: `GetTablePath()`, `GetTableDataPath()`, `GetTableMetadataPath()`
- **Catalog URIs**: `GetCatalogURI(catalogType)` - generates proper file:// URIs
- **Path Parsing**: `ParseTableIdentifier()` - handles `database.table` syntax
- **Directory Creation**: `EnsureDirectoryStructure()` - creates all necessary directories

### **Storage Manager**
The `StorageManager` orchestrates all storage operations and uses `PathManager` for all path-related decisions:

- **No path logic**: All paths come from PathManager
- **Clean separation**: Storage logic vs. path logic
- **Exposed PathManager**: `GetPathManager()` method for external access

## 🏗️ **Standardized Structure**

```
{data_path}/                           # Base directory (configurable)
├── .icebox/                           # Icebox internal metadata
│   ├── metadata.db                    # SQLite database for internal metadata
│   └── migrations/                    # Migration files
├── catalog/                           # Iceberg catalog files
│   ├── catalog.json                   # JSON catalog (or catalog.db for SQLite)
│   └── namespaces/                    # Namespace metadata
└── data/                              # Actual data files
    ├── default/                       # Database: default
    │   ├── users/                     # Table: users
    │   │   ├── data/                  # Data files (parquet, etc.)
    │   │   │   ├── part-0.parquet
    │   │   │   └── part-1.parquet
    │   │   └── metadata/              # Iceberg metadata
    │   │       ├── v1.metadata.json
    │   │       └── v2.metadata.json
    │   └── sales/                     # Table: sales
    │       ├── data/
    │       └── metadata/
    └── analytics/                     # Database: analytics
        └── events/                    # Table: events
            ├── data/
            └── metadata/
```

## 🚀 **Quick Start**

### **Configuration**

```yaml
# icebox-server.yml
storage:
  # Single data path for all storage (defaults to "./data" if not specified)
  data_path: "./data"
  
  # Catalog configuration (defaults to "json")
  catalog:
    type: "json"           # json, sqlite, rest
  
  # Data storage configuration (defaults to "filesystem")
  data:
    type: "filesystem"     # memory, filesystem, s3
  
  # Internal metadata is always SQLite (not configurable)
```

**Note**: If you don't specify `data_path`, it will default to `"./data"`. All other settings also have sensible defaults. Internal metadata is always stored in SQLite format.

### **Usage**

```go
import "github.com/TFMV/icebox/server/storage"

// Create storage manager
manager, err := storage.NewManager(cfg, logger)
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// Initialize storage (creates directory structure)
if err := manager.Initialize(ctx); err != nil {
    log.Fatal(err)
}

// Create table with database.table syntax
schema := []byte(`{"fields":[{"name":"id","type":"int"}]}`)
if err := manager.CreateTable("analytics.users", schema); err != nil {
    log.Fatal(err)
}

// Insert data
data := [][]interface{}{{1}, {2}, {3}}
if err := manager.InsertData("analytics.users", data); err != nil {
    log.Fatal(err)
}
```

## 🔧 **Key Features**

### **✅ Automatic Directory Creation**
- Creates all necessary directories on initialization
- Follows Iceberg specification for metadata structure
- Maintains clean separation between components

### **✅ Database.Table Support**
- Supports `database.table` syntax (e.g., `analytics.users`)
- Falls back to `default.table` for simple table names
- Creates proper namespace directories

### **✅ Proper Iceberg Metadata**
- Creates standard Iceberg metadata files
- Follows Iceberg v2 specification
- Includes proper schema, partitioning, and properties

### **✅ Multiple Storage Types**
- **Memory**: No files created on disk
- **Filesystem**: Local file storage
- **S3**: Cloud storage support

### **✅ Internal Metadata Management**
- SQLite database for Icebox-specific metadata
- Separate from Iceberg catalog metadata
- Handles migrations and schema changes

## 📁 **Path Management**

The `PathManager` handles all path construction:

```go
pm := NewPathManager("./data")

// Get various paths
catalogPath := pm.GetCatalogPath()           // "./data/catalog"
dataPath := pm.GetDataPath()                // "./data/data"
metadataPath := pm.GetInternalMetadataPath() // "./data/.icebox"

// Table-specific paths
tablePath := pm.GetTablePath("analytics", "users")           // "./data/data/analytics/users"
dataPath := pm.GetTableDataPath("analytics", "users")        // "./data/data/analytics/users/data"
metadataPath := pm.GetTableMetadataPath("analytics", "users") // "./data/data/analytics/users/metadata"

// Parse table identifiers
database, table := pm.ParseTableIdentifier("analytics.users") // "analytics", "users"
database, table := pm.ParseTableIdentifier("users")           // "default", "users"
```

## 🎯 **Table Operations**

### **Creating Tables**

```go
// Create table in specific database
err := manager.CreateTable("analytics.users", schema)

// Create table in default database
err := manager.CreateTable("users", schema)
```

### **Inserting Data**

```go
// Insert data into specific table
data := [][]interface{}{{1, "Alice"}, {2, "Bob"}}
err := manager.InsertData("analytics.users", data)
```

### **Querying Metadata**

```go
// Get table metadata
metadata, err := manager.GetTableMetadata("analytics.users")

// Check if table exists
exists := manager.TableExists("analytics.users")

// List all tables
tables, err := manager.ListTables()
```

## 🔄 **Storage Types**

### **Memory Storage**
- No files created on disk
- Perfect for testing and development
- Fast in-memory operations

### **Filesystem Storage**
- Creates proper directory structure
- Persists data and metadata
- Good for development and small deployments

### **S3 Storage**
- Cloud-based storage
- Requires proper AWS/MinIO configuration
- Good for production deployments

## 🧪 **Testing**

Run the storage tests:

```bash
cd server/storage
go test ./... -v
```

## 📚 **Examples**

See `example_usage.go` for a complete working example.

## 🔮 **Future Enhancements**

- [ ] **Partitioning Support**: Automatic partition management
- [ ] **Compaction**: Data file compaction and optimization
- [ ] **Versioning**: Advanced table versioning support
- [ ] **Backup/Restore**: Automated backup and restore procedures

## 🤝 **Contributing**

When adding new features:

1. **Follow the Path Structure**: Use `PathManager` for all path operations
2. **Support Database.Table**: Ensure new methods handle both formats
3. **Create Tests**: Add tests for new functionality
4. **Update Documentation**: Keep this README current

---

**🎯 The new standardized structure eliminates the hybrid mess and provides a clean, organized foundation for all Icebox storage operations!**
