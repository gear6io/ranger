# Internal Errors Package Migration Todo

## Overview
This document tracks the migration of all packages from using Go's standard `fmt.Errorf` and `errors.New` to the internal `github.com/TFMV/icebox/pkg/errors` package. This migration is required to follow the workspace rule: "Never use fmt.Errorf or errors.New, must always use the internal errors package".

## Migration Status

### ✅ Already Using Internal Errors Package
- `server/catalog/shared` - Uses internal errors package
- `server/storage/manager` - Uses internal errors package  
- `server/catalog/json` - Uses internal errors package
- `server/storage/memory` - Uses internal errors package

### 🔄 Packages Requiring Migration

#### 1. Client Packages
- **Priority: High**
- **Location**: `client/`, `client/commands/`, `client/config/`
- **Current Usage**: Extensive use of `fmt.Errorf`
- **Files to Update**:
  - `client/client.go` - 12 instances of `fmt.Errorf`
  - `client/commands/query.go` - 6 instances of `fmt.Errorf`
  - `client/config/config.go` - 7 instances of `fmt.Errorf`

#### 2. SDK Packages
- **Priority: High**
- **Location**: `pkg/sdk/`
- **Current Usage**: Mix of `fmt.Errorf` and `errors.New`
- **Files to Update**:
  - `pkg/sdk/client.go` - 2 instances of `fmt.Errorf`, 8 instances of `errors.New`
  - `pkg/sdk/protocol.go` - 20+ instances of `fmt.Errorf`, 15+ instances of `errors.New`
  - `pkg/sdk/test_server.go` - 2 instances of `fmt.Errorf`

#### 3. Server Core Packages
- **Priority: High**
- **Location**: `server/`
- **Current Usage**: Extensive use of `fmt.Errorf`
- **Files to Update**:
  - `server/metadata/registry/sqlite.go` - 30+ instances of `fmt.Errorf`
  - `server/metadata/registry/cdc.go` - 15+ instances of `fmt.Errorf`
  - `server/metadata/registry/migrations/001_start.go` - 30+ instances of `fmt.Errorf`
  - `server/metadata/manager.go` - 20+ instances of `fmt.Errorf`
  - `server/astha/astha.go` - 3 instances of `fmt.Errorf`
  - `server/astha/cdc_consumer.go` - 20+ instances of `fmt.Errorf`
  - `server/astha/scheduler.go` - 8 instances of `fmt.Errorf`
  - `server/storage/manager.go` - 15+ instances of `fmt.Errorf` (mixed with internal errors)

#### 4. Ariasql Packages
- **Priority: Medium**
- **Location**: `ariasql/src/`
- **Current Usage**: Extensive use of `fmt.Errorf` and `errors.New`
- **Files to Update**:
  - `ariasql/src/catalog/catalog.go` - 50+ instances of `fmt.Errorf`, 15+ instances of `errors.New`
  - `ariasql/src/executor/executor.go` - 1 instance of `fmt.Errorf`, 30+ instances of `errors.New`
  - `ariasql/src/core/core.go` - 1 instance of `errors.New`
  - `ariasql/src/shared/shared.go` - 1 instance of `fmt.Errorf`

#### 5. Utility Packages
- **Priority: Low**
- **Location**: `utils/`
- **Current Usage**: No error handling found
- **Status**: No changes needed

## Migration Guidelines

### 1. Error Creation Rules
**Errors are ONLY created at the origin stage:**
- **External/Std Library Calls**: When calling external libraries or standard library functions
- **Custom Validation**: When the code itself performs business logic validation

**All other functions should either:**
- **Pass through errors as-is** when no additional context is needed
- **Wrap with `WithAdditional()`** when adding minimal, important context

### 2. Replace `fmt.Errorf` with `errors.New`
```go
// Before
return fmt.Errorf("failed to create database %s: %w", dbName, err)

// After - Origin stage (external library call)
return errors.New(errors.CommonInternal, "failed to create database").AddContext("database", dbName).WithCause(err)
```

### 3. Replace `errors.New` with `errors.New`
```go
// Before
return errors.New("invalid DSN format")

// After - Origin stage (custom validation)
return errors.New(errors.CommonInvalidArgument, "invalid DSN format")
```

### 4. Error Propagation Rules
```go
// Internal function returning internal error - just wrap if needed
if err := createTable(); err != nil {
    // Use WithAdditional for minimal context, avoid over-wrapping
    return errors.WithAdditional(err, "while creating table %s", tableName)
}

// Pass through when no context needed
if err := someOperation(); err != nil {
    return err // No logging, no wrapping
}
```

### 5. Use Appropriate Error Codes
- `errors.CommonInternal` - For internal server errors
- `errors.CommonNotFound` - For resource not found errors  
- `errors.CommonInvalidArgument` - For invalid input errors
- `errors.CommonUnauthorized` - For permission errors
- `errors.CommonConflict` - For resource conflict errors
- `errors.CommonAlreadyExists` - For duplicate resource errors
- `errors.CommonValidation` - For validation failures

### 6. Package-Specific Error Codes
Create package-specific error codes for business logic:
```go
// In registry package
var (
    RegistryTableNotFound = MustNewCode("registry.table_not_found")
    RegistryDatabaseExists = MustNewCode("registry.database_exists")
)

// Usage
return errors.New(RegistryTableNotFound, "table does not exist").AddContext("table", tableName)
```

### 7. Context Guidelines
- **Minimal context only** - avoid verbose error messages
- **Important identifiers only** - database, table, file names
- **Use `AddContext()` sparingly** - only when the information is crucial for debugging
- **Avoid over-wrapping** - don't add context at every layer

## Migration Order

1. **Phase 1**: Client and SDK packages (external-facing)
2. **Phase 2**: Server core packages (metadata, storage, astha)
3. **Phase 3**: Ariasql packages (legacy SQL engine)
4. **Phase 4**: Testing and validation

## Package Inspection Process

Each package must be thoroughly inspected individually following these steps:

### 1. **Identify Origin Points**
- Find all external library calls (`sql.Open`, `os.MkdirAll`, etc.)
- Find all custom validation logic (business rules, input validation)
- Mark these as the ONLY places where `errors.New()` should be used

### 2. **Identify Propagation Points**
- Find all internal function calls that return errors
- Determine if additional context is needed
- Use `WithAdditional()` sparingly, only for crucial context

### 3. **Remove Anti-Patterns**
- Remove `fmt.Errorf` usage
- Remove `errors.New` usage (except at origin points)
- Remove any logging of errors being returned
- Remove over-wrapping with excessive context

### 4. **Verify Error Flow**
- Ensure errors flow from origin to consumer without unnecessary wrapping
- Verify that error messages remain clear and actionable
- Check that package-specific error codes are used appropriately

## Testing Requirements

- All error messages should maintain the same information
- Error codes should be appropriate for the error type
- Context information should be preserved
- Error wrapping should maintain the original error chain
- **No logging of returned errors** - errors should only be logged when handled/consumed
- **Minimal context** - verify that error messages are not overly verbose
- **Proper error propagation** - test that errors flow through the call chain correctly

## Notes

- The `pkg/errors` package itself uses `fmt.Errorf` in some places - this is acceptable as it's the internal errors package
- Some packages like `server/storage/manager.go` already use the internal errors package but still have some `fmt.Errorf` instances that need cleanup
- The migration should be done incrementally to avoid breaking changes

## Common Patterns to Avoid

### ❌ Don't Create Errors at Every Layer
```go
// Wrong - creating new error at propagation layer
if err := createTable(); err != nil {
    return errors.New(errors.CommonInternal, "failed to create table") // Wrong!
}

// Correct - use WithAdditional for minimal context
if err := createTable(); err != nil {
    return errors.WithAdditional(err, "while creating table %s", tableName)
}
```

### ❌ Don't Over-Wrap with Context
```go
// Wrong - too much context at every layer
return errors.New(errors.CommonInternal, "failed to create table").
    AddContext("database", database).
    AddContext("table", tableName).
    AddContext("timestamp", time.Now().String()). // Too verbose!
    AddContext("user", username).
    WithCause(err)

// Correct - minimal, important context only
return errors.New(errors.CommonInternal, "failed to create table").
    AddContext("table", tableName).
    WithCause(err)
```

### ❌ Don't Log Errors Being Returned
```go
// Wrong - logging error that's being returned
if err := someOperation(); err != nil {
    log.Printf("Error occurred: %v", err) // Wrong!
    return err
}

// Correct - no logging, just return
if err := someOperation(); err != nil {
    return err
}
```

## Estimated Effort

- **Client Packages**: 2-3 days
- **SDK Packages**: 3-4 days  
- **Server Core Packages**: 4-5 days
- **Ariasql Packages**: 3-4 days
- **Testing and Validation**: 2-3 days

**Total Estimated Effort**: 14-19 days
