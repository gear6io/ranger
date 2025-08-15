# Icebox Metadata Package

A modern metadata management system that coordinates between Iceberg catalog operations and Icebox's personal metadata storage, using bun ORM for professional-grade migrations and deployment safety.

## 🏗️ **Architecture Overview**

```
MetadataManager (Coordinator)
├── Iceberg Catalog (External metadata following Iceberg spec)
└── Personal Metadata Storage (Internal metadata for convenience)
    └── Bun Migration System (Go-based, modern)
        ├── HybridDeploymentManager (Deployment safety)
        └── SQLite Storage (Persistent storage)
```

## 🚀 **Key Features**

### **✅ Modern Migration System**
- **Bun Migrations**: Professional Go-based migrations using bun ORM
- **Type Safety**: Full type safety with Go structs
- **Migration Tracking**: Built-in migration versioning and status

### **✅ Deployment Safety**
- **Fail-Fast Deployment**: Application startup fails if migrations fail
- **Schema Verification**: Automatic schema validation on startup
- **Rollback Support**: Built-in rollback capabilities for migrations

### **✅ Modern ORM Integration**
- **bun ORM**: Fast, type-safe database operations
- **SQLite Support**: Lightweight, embedded database
- **Query Builder**: Powerful query building with type safety

### **✅ Simple Configuration**
- **Single System**: No more choosing between migration systems
- **Configurable Paths**: Customizable database and data paths
- **Automatic Setup**: Migrations run automatically on startup

## 🔧 **Quick Start**

### **Basic Usage**

```go
import "github.com/TFMV/icebox/server/metadata"

// Create metadata manager with bun migrations
manager, err := metadata.NewMetadataManager(catalog, "/tmp/icebox.db", "/tmp/icebox/data")
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// Ensure deployment readiness
ctx := context.Background()
if err := manager.EnsureDeploymentReady(ctx); err != nil {
    log.Fatal("Deployment failed:", err)
}
```

### **Advanced Usage**

```go
// Get the hybrid deployment manager
hybridManager := manager.GetHybridManager()
if hybridManager != nil {
    // Use advanced deployment features
    status, err := hybridManager.GetDeploymentStatus(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Deployment status: %s\n", status.Status)
}

// Get the underlying bun DB for advanced operations
bunDB := manager.GetBunDB()
if bunDB != nil {
    // Use bun's powerful query builder
    var databases []struct {
        Name       string `bun:"name"`
        TableCount int    `bun:"table_count"`
    }
    
    err := bunDB.NewSelect().
        Model(&databases).
        Where("table_count > ?", 0).
        Order("name ASC").
        Scan(ctx)
}
```

### **Database Operations**

```go
ctx := context.Background()

// Create database
if err := manager.CreateDatabase(ctx, "mydb"); err != nil {
    log.Fatal(err)
}

// Create table
if err := manager.CreateTable(ctx, "mydb", "mytable"); err != nil {
    log.Fatal(err)
}

// List databases
databases, err := manager.ListDatabases(ctx)
if err != nil {
    log.Fatal(err)
}

// List tables in database
tables, err := manager.ListTables(ctx, "mydb")
if err != nil {
    log.Fatal(err)
}

// Check existence
if manager.DatabaseExists(ctx, "mydb") {
    fmt.Println("Database exists")
}

if manager.TableExists(ctx, "mydb", "mytable") {
    fmt.Println("Table exists")
}
```

## 🔄 **Migration System**

### **Bun Migrations (Go-based)**

The system uses Go functions and provides excellent type safety:

```go
// Get the bun migration manager
bunMigrator := manager.GetStorage().GetBunMigrationManager()

// Check status
status, err := bunMigrator.GetMigrationStatus(ctx)

// Run migrations
err = bunMigrator.MigrateToLatest(ctx)

// Get current version
version, err := bunMigrator.GetCurrentVersion(ctx)
```

### **Adding New Migrations**

1. Add migration functions to `internal/bun_migrations.go`
2. Update `internal/bun_migration.go` to register new migrations
3. Test with `go test ./internal/...`

## 🚀 **Deployment Safety**

### **Automatic Deployment Checks**

The system automatically runs deployment checks on startup:

```go
// This will fail fast if anything is wrong
if err := manager.EnsureDeploymentReady(ctx); err != nil {
    log.Fatal("Deployment failed:", err)
}
```

### **Manual Deployment Status**

Check deployment status manually:

```go
hybridManager := manager.GetHybridManager()
status, err := hybridManager.GetDeploymentStatus(ctx)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Status: %s\n", status.Status)
fmt.Printf("Current Version: %d\n", status.CurrentVersion)
fmt.Printf("Schema Valid: %t\n", status.SchemaValid)
```

## 🧪 **Testing**

### **Run All Tests**

```bash
cd server/metadata
go test ./...
```

### **Run Specific Tests**

```bash
# Test bun system
go test ./internal/... -run TestBunSystem

# Test deployment safety
go test ./internal/... -run TestBunSystem/HybridDeploymentManager
```

## 📁 **File Structure**

```
server/metadata/
├── README.md                 # This file
├── manager.go               # Main metadata coordinator
├── interface.go             # Public interfaces
├── example_usage.go         # Usage examples
├── manager_test.go          # Manager tests
└── internal/                # Internal implementation
    ├── sqlite.go            # SQLite storage implementation
    ├── bun_migration.go     # Bun migration system
    ├── bun_migrations.go    # Bun migration functions
    ├── hybrid_deployment.go # Hybrid deployment manager
    ├── cli.go              # CLI tools
    └── *_test.go           # Test files
```

## 🔧 **Configuration**

### **Environment Variables**

- `ICEBOX_DB_PATH`: Path to SQLite database file
- `ICEBOX_DATA_PATH`: Path to data storage directory

### **Configuration File**

```yaml
# icebox-server.yml
metadata:
  db_path: "/var/lib/icebox/metadata.db"
  data_path: "/var/lib/icebox/data"
  deployment:
    fail_fast: true
    verify_schema: true
```

## 🚨 **Troubleshooting**

### **Common Issues**

1. **Migration Failures**
   - Check database permissions
   - Verify SQLite is available
   - Check migration function syntax

2. **Schema Verification Failures**
   - Ensure all required tables exist
   - Check table structure matches expectations
   - Verify foreign key constraints

3. **Deployment Failures**
   - Check migration status
   - Verify schema integrity
   - Check database connectivity

### **Debug Mode**

Enable debug logging:

```go
import "log"

log.SetFlags(log.LstdFlags | log.Lshortfile)
log.Println("Debug mode enabled")
```

## 🔮 **Future Enhancements**

### **Planned Features**

- [ ] **Advanced Migration Types**: Complex schema transformations
- [ ] **Migration Dependencies**: Migration ordering and dependencies
- [ ] **Rollback Strategies**: Advanced rollback mechanisms
- [ ] **Performance Monitoring**: Migration performance metrics
- [ ] **Distributed Migrations**: Multi-node migration support

### **Migration Examples**

```go
// Future: Complex migration with data transformation
func migrateUserData(ctx context.Context, db *bun.DB) error {
    // Transform existing data
    _, err := db.NewUpdate().
        Model(&User{}).
        Set("email = LOWER(email)").
        Where("email IS NOT NULL").
        Exec(ctx)
    
    return err
}

// Future: Migration with validation
func migrateWithValidation(ctx context.Context, db *bun.DB) error {
    // Run migration
    if err := createNewTable(ctx, db); err != nil {
        return err
    }
    
    // Validate migration
    if err := validateDataIntegrity(ctx, db); err != nil {
        return err
    }
    
    return nil
}
```

## 🤝 **Contributing**

### **Development Workflow**

1. **Fork the repository**
2. **Create a feature branch**
3. **Write tests for new functionality**
4. **Ensure all tests pass**
5. **Submit a pull request**

### **Code Standards**

- **Go formatting**: Use `gofmt` or `goimports`
- **Testing**: Maintain >90% test coverage
- **Documentation**: Update README for new features
- **Error handling**: Use proper error wrapping with `fmt.Errorf`

### **Testing Guidelines**

- **Unit tests**: Test individual components
- **Integration tests**: Test component interactions
- **Migration tests**: Test both Up and Down migrations
- **Error tests**: Test error conditions and edge cases

## 📚 **References**

- [bun ORM Documentation](https://bun.uptrace.dev/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Go Testing Best Practices](https://golang.org/doc/code.html#Testing)

## 📄 **License**

This project is licensed under the same license as the main Icebox project.

---

**🎯 The system now uses only the modern bun migration system with your excellent deployment safety logic!**
