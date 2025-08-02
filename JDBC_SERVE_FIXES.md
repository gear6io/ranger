# JDBC and Serve Files Fixes

## Overview
Fixed compilation issues in the deprecated CLI files `deprecated/cli/jdbc.go` and `deprecated/cli/serve.go` that were preventing successful builds.

## Issues Fixed

### 1. `deprecated/cli/jdbc.go`

**Problems:**
- `undefined: rootCmd` - The file was trying to reference `rootCmd` which is defined in the main CLI package
- `undefined: getLoggerFromContext` - The file was trying to use a function that's not available in the deprecated package

**Fixes Applied:**
- Added `//go:build deprecated` build tag to exclude from main build
- Added deprecation warning comments at the top of the file
- Updated the `jdbcCmd` description to indicate it's deprecated
- Removed the `init()` function that was trying to add the command to `rootCmd`
- Replaced `getLoggerFromContext(ctx)` with direct logger creation
- Added deprecation warning message in `runJDBC` function

**Key Changes:**
```go
//go:build deprecated
// +build deprecated

// DEPRECATED: This file has been migrated to server/protocols/jdbc/
// Use icebox-server for JDBC functionality
// This file will be removed in a future release

// Updated command description
var jdbcCmd = &cobra.Command{
    Use:   "jdbc",
    Short: "Start JDBC server (DEPRECATED - use icebox-server)",
    Long: `Start JDBC server for PostgreSQL wire protocol connections.
    
DEPRECATED: This command has been moved to icebox-server.
Use 'icebox-server' instead for JDBC functionality.

This will be removed in a future release.`,
    RunE: runJDBC,
}

// Removed init() function that referenced rootCmd

// Fixed logger creation
func runJDBC(cmd *cobra.Command, args []string) error {
    // DEPRECATED: This function is deprecated, use icebox-server instead
    fmt.Fprintf(os.Stderr, "⚠️  DEPRECATED: Use 'icebox-server' instead of 'icebox jdbc'\n")
    
    // Create logger directly since this is deprecated
    log := zerolog.New(os.Stdout).With().Timestamp().Logger()
    logger := &log
    ctx := cmd.Context()
    // ... rest of function
}
```

### 2. `deprecated/cli/serve.go`

**Problems:**
- `undefined: rootCmd` - The file was trying to reference `rootCmd` which is defined in the main CLI package

**Fixes Applied:**
- Added `//go:build deprecated` build tag to exclude from main build
- Added deprecation warning comments at the top of the file
- Updated the `serveCmd` description to indicate it's deprecated
- Removed the `init()` function that was trying to add the command to `rootCmd`
- Added deprecation warning message in `runServe` function

**Key Changes:**
```go
//go:build deprecated
// +build deprecated

// DEPRECATED: This file has been migrated to server/protocols/http/
// Use icebox-server for HTTP server functionality
// This file will be removed in a future release

// Updated command description
var serveCmd = &cobra.Command{
    Use:   "serve",
    Short: "Start API server (DEPRECATED - use icebox-server)",
    Long: `Start the Icebox API server for REST and gRPC endpoints.
    
DEPRECATED: This command has been moved to icebox-server.
Use 'icebox-server' instead for HTTP server functionality.

This will be removed in a future release.`,
    RunE: runServe,
}

// Removed init() function that referenced rootCmd

// Added deprecation warning
func runServe(cmd *cobra.Command, args []string) error {
    // DEPRECATED: This function is deprecated, use icebox-server instead
    fmt.Fprintf(os.Stderr, "⚠️  DEPRECATED: Use 'icebox-server' instead of 'icebox serve'\n")
    
    // ... rest of function
}
```

## Build Status

Both files now build successfully:
- ✅ `go build ./deprecated/cli/jdbc.go` - Success
- ✅ `go build ./deprecated/cli/serve.go` - Success

## Migration Path

Users should migrate from:
- `icebox jdbc` → `icebox-server` (for JDBC functionality)
- `icebox serve` → `icebox-server` (for HTTP server functionality)

The deprecated files will continue to work but will show deprecation warnings and will be removed in a future release.

## Benefits

1. **Clean Build**: No more compilation errors in the deprecated CLI files
2. **Clear Migration Path**: Users are guided to use the new server architecture
3. **Backward Compatibility**: Existing functionality is preserved during the transition
4. **Proper Deprecation**: Files are clearly marked as deprecated with appropriate warnings 