# Storage Separation Fix

## Problem Identified
The original structure incorrectly combined two different concerns under `server/storage/`:
1. **Catalog Storage** (metadata management) - SQLite, JSON, REST catalogs for table metadata
2. **Data Storage** (actual data files) - Local filesystem, S3, memory for storing actual data files

## Solution Implemented

### 1. Separated Catalog Management
**Moved to:** `server/catalog/`
- `server/catalog/sqlite/` - SQLite catalog implementation
- `server/catalog/json/` - JSON catalog implementation  
- `server/catalog/rest/` - REST catalog implementation
- `server/catalog/factory.go` - Catalog factory
- `server/catalog/factory_test.go` - Catalog factory tests

**Purpose:** Manages table metadata, schemas, and catalog information

### 2. Separated Data Storage
**Remains in:** `server/storage/`
- `server/storage/local/` - Local filesystem implementation
- `server/storage/memory/` - In-memory filesystem implementation
- `server/storage/minio/` - S3/MinIO filesystem implementation (placeholder)
- `server/storage/manager.go` - Data storage manager

**Purpose:** Manages actual data files (Parquet, Avro, etc.)

## Key Changes Made

### Catalog Package (`server/catalog/`)
```go
package catalog

// CatalogInterface defines the common interface for all catalog implementations
type CatalogInterface interface {
    icebergcatalog.Catalog
    Name() string
    Close() error
}

// NewCatalog creates a new catalog based on the configuration type
func NewCatalog(cfg *config.Config) (CatalogInterface, error) {
    switch cfg.Catalog.Type {
    case "sqlite":
        return sqlite.NewCatalog(cfg)
    case "rest":
        return rest.NewCatalog(cfg)
    case "json":
        return json.NewCatalog(cfg)
    default:
        return nil, fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
    }
}
```

### Storage Package (`server/storage/`)
```go
package storage

// FileSystem interface for data storage operations
type FileSystem interface {
    Open(path string) (io.ReadCloser, error)
    Create(path string) (io.WriteCloser, error)
    Remove(path string) error
    Exists(path string) (bool, error)
}

// Manager represents a data storage manager for icebox
type Manager struct {
    config *Config
    logger zerolog.Logger
    fs     FileSystem
}

// NewManager creates a new data storage manager
func NewManager(cfg *Config, logger zerolog.Logger) (*Manager, error) {
    var fs FileSystem
    var err error

    switch cfg.Type {
    case "filesystem":
        fs, err = local.NewLocalFileSystem(cfg.Path)
    case "memory":
        fs, err = memory.NewFileSystemAdapter()
    case "s3":
        fs, err = minio.NewS3FileSystem(cfg)
    default:
        return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
    }
    // ...
}
```

## File System Implementations

### Local File System (`server/storage/local/fs.go`)
- Implements local filesystem operations
- Handles file:// URIs
- Manages directory creation and file operations

### Memory File System (`server/storage/memory/`)
- Uses existing comprehensive `MemoryFileSystem` implementation
- Added `adapter.go` to bridge to our `FileSystem` interface
- Supports in-memory operations for testing and development

### S3/MinIO File System (`server/storage/minio/fs.go`)
- Placeholder implementation
- Returns "not yet implemented" errors
- Ready for future S3/MinIO integration

## Benefits of This Separation

1. **Clear Separation of Concerns:**
   - Catalog management is separate from data storage
   - Each component has a single responsibility

2. **Better Architecture:**
   - Follows Apache Iceberg's design principles
   - Catalog manages metadata, storage manages data files

3. **Easier Maintenance:**
   - Catalog implementations can be updated independently
   - Storage implementations can be swapped without affecting catalogs

4. **Flexible Configuration:**
   - Can use SQLite catalog with S3 storage
   - Can use REST catalog with local filesystem
   - Any combination of catalog + storage is possible

## Usage Example

```go
// Create catalog (metadata management)
catalog, err := catalog.NewCatalog(config)
if err != nil {
    return err
}

// Create storage (data file management)
storage, err := storage.NewManager(storageConfig, logger)
if err != nil {
    return err
}

// Use catalog for table operations
table, err := catalog.CreateTable(ctx, tableIdent, schema)

// Use storage for data file operations
writer, err := storage.GetFileSystem().Create("data.parquet")
```

## Migration Notes

- All catalog-related files moved from `server/storage/` to `server/catalog/`
- Package names updated from `storage` to `catalog` where appropriate
- Storage manager now focuses purely on data file operations
- Existing functionality preserved through proper interfaces 