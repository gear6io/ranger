# Internal Errors Package Migration Todo

## Overview
This document tracks the migration of all packages from using Go's standard `fmt.Errorf` and `errors.New` to the internal `github.com/TFMV/icebox/pkg/errors` package. This migration is required to follow the workspace rule: "Never use fmt.Errorf or errors.New, must always use the internal errors package".

## Current Errors API Status
**⚠️ IMPORTANT: The errors API has been simplified and no longer includes `WithAdditional` or `WithCause` methods.**

**Available methods:**
- `errors.New(code Code, message string, cause error)` - Creates new errors with code, message, and optional cause
- `errors.Newf(code Code, format string, args ...interface{})` - Creates formatted errors
- `errors.AddContext(err error, key string, value interface{})` - External function to add context
- `error.AddContext(key string, value interface{})` - Method to add context to existing errors
- `error.Error()` - Returns error message string
- `error.Unwrap()` - Returns underlying cause error

## Migration Status

### ✅ Already Using Internal Errors Package
- `server/catalog/shared` - Uses internal errors package
- `server/storage/memory` - ⚠️ **API UPDATE NEEDED** - Uses internal errors but with old `.WithCause()` API
- `server/catalog/json` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `server/catalog/rest` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `server/catalog/sqlite` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `client/config` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `client/commands` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `client` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `server/gateway` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes
- `server/query` - ✅ **FULLY COMPLIANT** - Migrated to internal errors package with proper error codes

### 🔄 Packages Requiring Migration

#### 1. Server Core Packages
- **Priority: High**
- **Location**: `server/`
- **Current Usage**: Mix of `fmt.Errorf` and old internal errors API
- **Files to Update**:
  - `server/metadata/registry/sqlite.go` - ⚠️ **API UPDATE NEEDED** - Uses internal errors but with old `.WithCause()` API (30+ instances)
  - `server/metadata/registry/cdc.go` - ⚠️ **API UPDATE NEEDED** - Uses internal errors but with old `.WithCause()` API (15+ instances)
  - `server/metadata/registry/migrations/001_start.go` - ⚠️ **API UPDATE NEEDED** - Uses internal errors but with old `.WithCause()` API (30+ instances)
  - `server/metadata/manager.go` - 20+ instances of `fmt.Errorf`
  - `server/astha/cdc_consumer.go` - 20+ instances of `fmt.Errorf`
  - `server/astha/scheduler.go` - 8 instances of `fmt.Errorf`

#### 2. Ariasql Packages
- **Priority: Medium**
- **Location**: `ariasql/src/`
- **Current Usage**: Extensive use of `fmt.Errorf` and `errors.New`
- **Files to Update**:
  - `ariasql/src/catalog/catalog.go` - 50+ instances of `fmt.Errorf`, 15+ instances of `errors.New`
  - `ariasql/src/executor/executor.go` - 1 instance of `fmt.Errorf`, 30+ instances of `errors.New`
  - `ariasql/src/core/core.go` - 1 instance of `errors.New`
  - `ariasql/src/shared/shared.go` - 1 instance of `fmt.Errorf`
  - `ariasql/src/wal/wal.go` - 1 instance of `errors.New`

#### 3. Integration Tests
- **Priority: Medium**
- **Location**: `integration_tests/`
- **Current Usage**: Extensive use of `fmt.Errorf`
- **Files to Update**:
  - `integration_tests/native_test.go` - 50+ instances of `fmt.Errorf`

#### 4. SDK Packages
- **Priority: Low** - **NOT REQUIRED**
- **Location**: `pkg/sdk/`
- **Current Usage**: Uses `github.com/go-faster/errors` (external library)
- **Status**: **NO CHANGES NEEDED** - SDK packages should use external error libraries, not internal server errors

## Migration Guidelines

### ⚠️ **CRITICAL: Correct Error Handling Pattern**
**The key insight is to minimize error wrapping and only add context when truly necessary:**

- **`errors.New()`** = Create new errors ONLY at origin points:
  - External library calls (`os.MkdirAll`, `sql.Open`, etc.)
  - Custom validation logic (business rules, input validation)
  - When the error originates in this function

- **`errors.AddContext()`** = Add context ONLY when the caller doesn't have access to important information:
  - Adding request IDs, user context, etc.
  - When the error comes from a different package and needs local context

- **`return err`** = The DEFAULT approach for error propagation:
  - Most internal function calls should just propagate errors as-is
  - Don't wrap errors unless you're adding genuinely useful context
  - Avoid verbose "operation" context that doesn't help debugging

### 1. Error Creation Rules
**Errors are ONLY created at the origin stage:**
- **External/Std Library Calls**: When calling external libraries or standard library functions
- **Custom Validation**: When the code itself performs business logic validation

**All other functions should:**
- **Pass through errors as-is** (default behavior)
- **Use `AddContext()` sparingly** only when adding crucial context the caller doesn't have

### 2. Replace `fmt.Errorf` with `errors.New`
```go
// Before
return fmt.Errorf("failed to create database %s: %w", dbName, err)

// After - Origin stage (external library call)
return errors.New(ErrDatabaseCreationFailed, "failed to create database", err)
```

### 3. Replace `errors.New` with `errors.New`
```go
// Before
return errors.New("invalid DSN format")

// After - Origin stage (custom validation)
return errors.New(ErrInvalidDSN, "invalid DSN format", nil)
```

### 4. Error Propagation Rules
```go
// ✅ CORRECT: Just propagate the error (most common case)
if err := createTable(); err != nil {
    return err // No wrapping needed
}

// ✅ CORRECT: Add context only when caller doesn't have access to it
if err := validateUser(user); err != nil {
    return errors.AddContext(err, "request_id", requestID)
}

// ❌ WRONG: Don't add verbose "operation" context
if err := createTable(); err != nil {
    return errors.AddContext(err, "operation", "create table") // Too verbose!
}
```

### 5. Use Appropriate Error Codes
- `errors.CommonInternal` - For internal server errors
- `errors.CommonNotFound` - For resource not found errors  
- `errors.CommonInvalidInput` - For invalid input errors
- `errors.CommonUnauthorized` - For permission errors
- `errors.CommonConflict` - For resource conflict errors
- `errors.CommonAlreadyExists` - For duplicate resource errors
- `errors.CommonValidation` - For validation failures

### 6. Package-Specific Error Codes
Create package-specific error codes for business logic:
```go
// In registry package
var (
    RegistryTableNotFound = errors.MustNewCode("registry.table_not_found")
    RegistryDatabaseExists = errors.MustNewCode("registry.database_exists")
)

// Usage
return errors.New(RegistryTableNotFound, "table does not exist", nil).AddContext("table", tableName)
```

### 7. Context Guidelines
- **Minimal context only** - avoid verbose error messages
- **Important identifiers only** - database, table, file names
- **Use `AddContext()` sparingly** - only when the information is crucial for debugging
- **Default to `return err`** - most errors should just propagate without wrapping

## Migration Order

1. **Phase 1**: Client and SDK packages (external-facing)
2. **Phase 2**: Server core packages (gateway, query, metadata, astha)
3. **Phase 3**: Ariasql packages (legacy SQL engine)
4. **Phase 4**: Integration tests
5. **Phase 5**: Testing and validation

## Package Inspection Process

Each package must be thoroughly inspected individually following these steps:

### 1. **Identify Origin Points**
- Find all external library calls (`sql.Open`, `os.MkdirAll`, etc.)
- Find all custom validation logic (business rules, input validation)
- Mark these as the ONLY places where `errors.New()` should be used

### 2. **Identify Propagation Points**
- Find all internal function calls that return errors
- Determine if additional context is needed
- Use `AddContext()` sparingly, only for crucial context

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
- The simplified API requires more immediate context addition - cannot chain context later
- Some packages like `server/storage/memory` already use the internal errors package correctly

## Common Patterns to Avoid

### ❌ Don't Create Errors at Every Layer
```go
// Wrong - creating new error at propagation layer
if err := createTable(); err != nil {
    return errors.New(errors.CommonInternal, "failed to create table", nil) // Wrong!
}

// Correct - just propagate the error
if err := createTable(); err != nil {
    return err
}
```

### ❌ Don't Over-Wrap with Context
```go
// Wrong - too much context at every layer
return errors.New(errors.CommonInternal, "failed to create table", err).
    AddContext("database", database).
    AddContext("table", tableName).
    AddContext("timestamp", time.Now().String()). // Too verbose!
    AddContext("user", username)

// Correct - minimal, important context only
return errors.New(errors.CommonInternal, "failed to create table", err).
    AddContext("table", tableName)
```

### ❌ Don't Add Verbose "Operation" Context
```go
// Wrong - adding context that doesn't help debugging
if err := createTable(); err != nil {
    return errors.AddContext(err, "operation", "create table") // Too verbose!
}

// Correct - only add context the caller doesn't have
if err := validateUser(user); err != nil {
    return errors.AddContext(err, "request_id", requestID) // Useful context
}
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

### ✅ Do Use Simple Error Propagation
```go
// Correct - most errors should just propagate
if err := someOperation(); err != nil {
    return err
}

// Correct - add context only when truly needed
if err := externalCall(); err != nil {
    return errors.AddContext(err, "user_id", userID)
}
```

## Estimated Effort

- **Client Packages**: 2-3 days
- **SDK Packages**: 3-4 days  
- **Server Core Packages**: 4-5 days
- **Ariasql Packages**: 3-4 days
- **Integration Tests**: 2-3 days
- **Testing and Validation**: 2-3 days

**Total Estimated Effort**: 16-22 days
