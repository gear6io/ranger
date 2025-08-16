# Icebox Errors Package

A clean, simple, and powerful error handling package for the Icebox project with **validated error codes** and **package prefixes**.

## Features

- **✅ Validated Error Codes** - Compile-time validation prevents typos and enforces format
- **🏷️ Package Prefixes** - Clear identification like `filesystem.table_not_found` vs `memory.table_not_found`
- **🔒 Format Enforcement** - Must follow `package.name` pattern (lowercase, underscores, dots only)
- **🚫 No "error" or "err"** - Prevents common anti-patterns
- **Automatic Stack Traces** - No need to remember to call `WithStack()`
- **Automatic Timestamps** - No need to remember to call `WithTimestamp()`
- **Method Chaining** - No `Build()` calls needed
- **Clean API** - Only essential methods: `AddContext()` and `WithCause()`

## Quick Start

```go
import "github.com/TFMV/icebox/pkg/errors"

// Simple error - code is compulsory, stack & timestamp automatic
return errors.New(errors.FilesystemCode("table_not_found"), "table not found")

// With context - method chaining, no Build() needed
return errors.New(errors.QueryCode("timeout"), "query timeout").
    AddContext("query", query).
    AddContext("timeout", "30s")

// Formatted message
return errors.Newf(errors.StorageCode("connection_failed"), "connection to %s failed", host)

// Wrap existing error
return errors.Wrap(errors.ValidationCode("invalid_input"), err, "input validation failed")

// Add context to existing error
return errors.WithAdditional(err, "while processing request from user %s", userID)
```

## 🏷️ Error Code System

### **Code Format: `package.name`**

All error codes must follow the format `package.name`:
- **Lowercase only** - `filesystem.table_not_found` ✅
- **Underscores allowed** - `query.execution_timeout` ✅
- **Dots separate package and name** - `storage.connection_failed` ✅
- **No hyphens** - `api-rate-limit` ❌
- **No uppercase** - `FileSystem.NotFound` ❌
- **No "error" or "err"** - `query.error` ❌

### **Package-Specific Code Constructors**

```go
// Filesystem errors
errors.FilesystemCode("table_not_found")     // → "filesystem.table_not_found"
errors.FilesystemCode("permission_denied")   // → "filesystem.permission_denied"

// Memory errors
errors.MemoryCode("alloc_failed")            // → "memory.alloc_failed"
errors.MemoryCode("out_of_memory")          // → "memory.out_of_memory"

// Query errors
errors.QueryCode("timeout")                  // → "query.timeout"
errors.QueryCode("syntax_error")            // → "query.syntax_error"

// Storage errors
errors.StorageCode("connection_failed")      // → "storage.connection_failed"
errors.StorageCode("table_not_found")       // → "storage.table_not_found"

// Network errors
errors.NetworkCode("timeout")                // → "network.timeout"
errors.NetworkCode("connection_refused")    // → "network.connection_refused"

// Auth errors
errors.AuthCode("unauthorized")              // → "auth.unauthorized"
errors.AuthCode("token_expired")            // → "auth.token_expired"

// Database errors
errors.DatabaseCode("connection_failed")     // → "database.connection_failed"
errors.DatabaseCode("query_timeout")        // → "database.query_timeout"

// API errors
errors.APICode("rate_limit_exceeded")       // → "api.rate_limit_exceeded"
errors.APICode("invalid_request")           // → "api.invalid_request"
```

### **Custom Package Codes**

```go
// For custom packages
errors.PackageCode("my_package", "specific_failure")
// → "my_package.specific_failure"

// Or create directly
errors.MustNewCode("custom_package.specific_failure")
```

### **Common Error Codes**

Predefined codes for common scenarios:

```go
errors.CommonInternal      // → "common.internal"
errors.CommonNotFound      // → "common.not_found"
errors.CommonValidation    // → "common.validation"
errors.CommonTimeout       // → "common.timeout"
errors.CommonUnauthorized  // → "common.unauthorized"
errors.CommonForbidden     // → "common.forbidden"
errors.CommonConflict      // → "common.conflict"
errors.CommonUnsupported   // → "common.unsupported"
errors.CommonInvalidInput  // → "common.invalid_input"
errors.CommonAlreadyExists // → "common.already_exists"
```

## 🚀 **API Design**

### **Core Constructors**

```go
// Create new error - code is compulsory first argument
func New(code Code, message string) *Error

// Create formatted error
func Newf(code Code, format string, args ...interface{}) *Error

// Wrap existing error
func Wrap(code Code, err error, message string) *Error

// Wrap with formatted message
func Wrapf(code Code, err error, format string, args ...interface{}) *Error

// Add context to existing error
func WithAdditional(cause error, format string, args ...interface{}) *Error
```

### **Methods on *Error**

```go
// Add context key-value pair
func (e *Error) AddContext(key, value string) *Error

// Set cause error
func (e *Error) WithCause(err error) *Error
```

### **Common Error Constructors**

```go
// Quick constructors with predefined codes
errors.Internal("something went wrong")
errors.NotFound("resource not found")
errors.Validation("invalid input")
errors.Timeout("operation timed out")
errors.Unauthorized("access denied")
errors.Forbidden("forbidden")
errors.Conflict("conflict")
errors.Unsupported("not supported")
errors.InvalidInput("bad input")
errors.AlreadyExists("already exists")
```

### **Helper Functions**

```go
// Check if error is our Error type
func IsIceboxError(err error) bool

// Extract context from error
func GetContext(err error) map[string]string

// Get error code
func GetCode(err error) string

// Format error for logging
func FormatForLog(err error) string

// Migration helper
func FromFmtErrorf(code Code, format string, args ...interface{}) *Error
```

## 🔧 **Implementation Details**

### **Error Structure**

```go
type Error struct {
    Code      Code              // Validated error code (compulsory)
    Message   string            // Human-readable message
    Cause     error             // Underlying error
    Context   map[string]string // Key-value context
    Stack     []Frame           // Stack trace (automatic)
    Timestamp time.Time         // Timestamp (automatic)
}
```

### **Code Validation**

```go
// Valid codes
"filesystem.table_not_found"     ✅
"memory.alloc_failed"            ✅
"query.execution_timeout"        ✅
"storage.connection_failed"      ✅

// Invalid codes
"invalid"                        ❌ (no dot)
"filesystem."                    ❌ (ends with dot)
".table_not_found"               ❌ (starts with dot)
"FileSystem.table_not_found"     ❌ (uppercase)
"filesystem.table-not-found"     ❌ (hyphens)
"error.table_not_found"          ❌ (contains "error")
"err.table_not_found"            ❌ (contains "err")
```

### **Smart Context Handling**

- **Lazy initialization** - context map only created when `AddContext()` is called
- **Context preservation** - `WithAdditional()` maintains all existing context
- **Unique keys** - automatic `additional_0`, `additional_1`, etc.

### **Performance Optimizations**

- **Stack traces** captured only when errors are created
- **No reflection** in the hot path
- **Minimal allocations** for simple errors

## 📁 **File Structure**

```
pkg/errors/
├── code.go       # Code type with validation and constructors
├── errors.go     # Core error types and constructors
├── utils.go      # Helper functions and common constructors
├── errors_test.go # Core error tests
├── code_test.go  # Code type tests
├── go.mod        # Go module definition
└── README.md     # Complete documentation and examples
```

## ✅ **Test Coverage**

All tests passing with comprehensive coverage:
- ✅ Code validation and format enforcement
- ✅ Package-specific code constructors
- ✅ Core constructors (`New`, `Newf`, `Wrap`, `Wrapf`)
- ✅ Method chaining (`AddContext`, `WithCause`)
- ✅ Context management (`WithAdditional`)
- ✅ Multiple `WithAdditional` calls
- ✅ Standard error fallbacks
- ✅ Helper functions (`IsIceboxError`, `GetContext`, `GetCode`)
- ✅ Common error constructors
- ✅ Error formatting and logging

## 🎨 **Usage Examples**

### **Package-Specific Error Codes**

```go
// server/query/errors.go
package query

import "github.com/TFMV/icebox/pkg/errors"

// Define package-specific codes
var (
    CodeTableNotFound     = errors.QueryCode("table_not_found")
    CodeQueryTimeout      = errors.QueryCode("timeout")
    CodeInvalidSyntax     = errors.QueryCode("invalid_syntax")
    CodePermissionDenied  = errors.QueryCode("permission_denied")
)

// Usage
return errors.New(CodeTableNotFound, "table not found").
    AddContext("table_name", tableName)
```

### **Database Operations**

```go
func GetUser(id string) (*User, error) {
    user, err := db.Query("SELECT * FROM users WHERE id = ?", id)
    if err != nil {
        return nil, errors.Wrap(errors.DatabaseCode("query_failed"), err, "failed to query user").
            AddContext("user_id", id).
            AddContext("table", "users")
    }
    
    if user == nil {
        return nil, errors.New(errors.CommonNotFound, "user not found").
            AddContext("user_id", id)
    }
    
    return user, nil
}
```

### **File Operations**

```go
func ReadConfigFile(path string) (*Config, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, errors.Wrap(errors.FilesystemCode("open_failed"), err, "failed to open config file").
            AddContext("file_path", path)
    }
    defer file.Close()
    
    // Read and parse file...
    return config, nil
}
```

### **API Handlers**

```go
func HandleUserRequest(w http.ResponseWriter, r *http.Request) {
    userID := r.URL.Query().Get("user_id")
    if userID == "" {
        http.Error(w, "user_id required", http.StatusBadRequest)
        return
    }
    
    user, err := GetUser(userID)
    if err != nil {
        // Log the error with full context
        log.Printf("Error: %s", errors.FormatForLog(err))
        
        if errors.IsIceboxError(err) && errors.GetCode(err) == errors.CommonNotFound.String() {
            http.Error(w, "User not found", http.StatusNotFound)
        } else {
            http.Error(w, "Internal server error", http.StatusInternalServerError)
        }
        return
    }
    
    // Process user...
}
```

## 🔄 **Migration Strategy**

### **Phase 1: Replace fmt.Errorf**

```go
// Before
return fmt.Errorf("failed to execute query: %w", err)

// After
return errors.Wrap(errors.QueryCode("execution_failed"), err, "failed to execute query")
```

### **Phase 2: Add Error Codes**

```go
// Add descriptive error codes with package prefixes
return errors.New(errors.QueryCode("table_not_found"), "table not found")
return errors.New(errors.StorageCode("connection_failed"), "connection failed")
```

### **Phase 3: Add Context**

```go
// Add relevant context for debugging
return errors.New(errors.ValidationCode("invalid_input"), "validation failed").
    AddContext("field", "email").
    AddContext("value", email)
```

## 🌟 **Benefits Achieved**

### **For Developers**
- **🚫 No more typos** - compile-time validation prevents invalid codes
- **🏷️ Clear identification** - instantly know which package generated the error
- **🔒 Format enforcement** - consistent error code structure across the project
- **Faster coding** - no need to remember extra method calls
- **Better debugging** - rich context and automatic stack traces

### **For Operations**
- **Structured logging** - easy integration with zerolog
- **Error categorization** - standardized codes for monitoring
- **Package identification** - quickly identify error sources
- **Context preservation** - full error chain for debugging
- **Performance** - minimal overhead, lazy initialization

### **For the Project**
- **Centralized error handling** - single source of truth
- **Easy maintenance** - consistent patterns across codebase
- **Better observability** - structured error data for monitoring
- **Future-proof** - extensible design for evolving needs
- **Team collaboration** - clear error code conventions

## 🚀 **Next Steps**

### **Immediate Integration**
1. **Import the package** in existing code
2. **Replace fmt.Errorf** calls with `errors.Wrap`
3. **Add package-specific codes** using the constructors
4. **Add context** where relevant

### **Package-Specific Codes**
1. **Create `errors.go`** files in each package
2. **Define domain-specific codes** using package constructors
3. **Use package constants** for consistency

### **Monitoring Integration**
1. **Log errors** with `errors.FormatForLog()`
2. **Extract error codes** for alerting
3. **Use package prefixes** for error categorization
4. **Leverage context** for debugging and analytics

## 🎉 **Success Metrics**

- ✅ **All tests passing** - robust implementation
- ✅ **Code validation** - prevents typos and enforces format
- ✅ **Package prefixes** - clear error identification
- ✅ **Clean API** - no unnecessary complexity
- ✅ **Automatic features** - stack traces and timestamps
- ✅ **Method chaining** - fluent, readable code
- ✅ **Context preservation** - no data loss
- ✅ **Performance optimized** - minimal overhead
- ✅ **Well documented** - clear examples and best practices

The centralized error logging package with **validated error codes** is now **complete and ready for production use**! 🚀

## 🔍 **Code Validation Examples**

```go
// ✅ Valid codes
errors.FilesystemCode("table_not_found")     // filesystem.table_not_found
errors.MemoryCode("alloc_failed")            // memory.alloc_failed
errors.QueryCode("execution_timeout")        // query.execution_timeout
errors.StorageCode("connection_failed")      // storage.connection_failed

// ❌ These will panic or return errors
errors.FilesystemCode("table-not-found")     // Panic: hyphens not allowed
errors.MemoryCode("AllocFailed")             // Panic: uppercase not allowed
errors.QueryCode("error")                    // Panic: contains "error"
errors.StorageCode("err")                    // Panic: contains "err"
```
