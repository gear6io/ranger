# ErrorCode Checker Tool

A custom Go tool that checks for unused ErrorCode variables and forbidden error patterns in the Icebox project.

## Features

- **🔍 ErrorCode Usage Tracking**: Finds all declared ErrorCode variables and checks if they're actually used
- **🚫 Forbidden Pattern Detection**: Identifies usage of `fmt.Errorf`, `errors.New`, `errors.Wrap`, etc.
- **📁 Path Exclusion**: Automatically excludes SDK packages and other non-server paths
- **📊 Detailed Reporting**: Shows where each ErrorCode is declared and used
- **🔧 Integration Ready**: Works with Makefile and pre-commit hooks

## What It Checks

### 1. ErrorCode Usage
- Finds variables starting with `Err` that use `errors.MustNewCode()`
- Tracks where each ErrorCode is declared and used
- Reports unused ErrorCode variables

### 2. Forbidden Error Patterns
- `fmt.Errorf` - Should use internal error package
- `errors.New` - Should use internal error package  
- `errors.Wrap` - Should use internal error package
- `errors.Wrapf` - Should use internal error package

### 3. Path Exclusions
Automatically excludes these paths from checking:
- `pkg/sdk/` - SDK packages can use external error libraries
- `integration_tests/` - Integration tests may have different requirements
- `testdata/` - Test data files
- `milestones/` - Documentation files
- `scripts/` - Script files
- `demo/` - Demo files
- `data/` - Data files
- `logs/` - Log files
- `workspace/` - Workspace files
- `my-lakehouse/` - Lakehouse files

## Usage

### From the Project Root

```bash
# Check all directories
make check-errorcodes

# Check specific directories
make check-errorcodes-server
make check-errorcodes-client
make check-errorcodes-pkg
```

### From the Tool Directory

```bash
cd scripts/linters/errorcode-checker

# Build the tool
make build

# Run checks
make check-all          # Check all directories
make check-server       # Check server directory only
make check-client       # Check client directory only
make check-pkg          # Check pkg directory only

# Run tests
make test

# Clean build artifacts
make clean
```

### Direct Usage

```bash
# Build the tool
go build -o errorcode-checker .

# Run with default settings
./errorcode-checker

# Check specific directory
./errorcode-checker -dir ./server

# Custom exclude paths
./errorcode-checker -exclude "custom/path,another/path"

# Disable forbidden pattern checking
./errorcode-checker -check-forbidden=false
```

## Command Line Options

- `-dir`: Directory to check (default: current directory)
- `-exclude`: Comma-separated paths to exclude
- `-check-forbidden`: Enable/disable forbidden pattern checking (default: true)

## Example Output

```
🔍 Checking ErrorCode usage in directory: .
🚫 Excluding paths: pkg/sdk/, integration_tests/, testdata/, milestones/, scripts/, demo/, data/, logs/, workspace/, my-lakehouse/

✅ USED: ErrTableNotFound declared in server/storage/memory/errors.go:10, used in: server/storage/memory/parquet_manager.go:49, server/storage/memory/parquet_manager.go:99
✅ USED: ErrDataValidationFailed declared in server/storage/memory/errors.go:31, used in: server/storage/memory/parquet_manager.go:61
❌ UNUSED: ErrUnusedCode declared in server/storage/memory/errors.go:50

🔍 Checking for forbidden error patterns...
❌ FORBIDDEN: fmt\.Errorf in server/bad_file.go:25
❌ FORBIDDEN: errors\.New\( in server/bad_file.go:30

❌ Found unused ErrorCodes!
```

## Integration

### Pre-commit Hooks
The tool is automatically run as part of pre-commit checks:
```bash
make pre-commit  # Includes ErrorCode checking
```

### CI/CD Pipeline
Add to your CI pipeline:
```yaml
- name: Check ErrorCodes
  run: make check-errorcodes
```

### GitHub Actions
```yaml
- name: Check ErrorCodes
  run: |
    cd scripts/linters/errorcode-checker
    make check-all
```

## Configuration

### Custom Exclude Paths
Modify the default exclude paths in `main.go`:
```go
excludePaths := []string{
    "pkg/sdk/",
    "custom/exclude/path",
    "another/exclude/path",
}
```

### Custom Forbidden Patterns
Add or modify forbidden patterns in `CheckForbiddenPatterns`:
```go
forbiddenPatterns := []string{
    `fmt\.Errorf`,
    `errors\.New\(`,
    `custom\.bad\.pattern`,
}
```

## Troubleshooting

### Common Issues

1. **Tool not found**: Make sure you're in the correct directory or use the full path
2. **Permission denied**: Ensure the tool has execute permissions
3. **Build errors**: Check that Go is installed and the project dependencies are available

### Debug Mode
Run with verbose output:
```bash
go run . -dir . -check-forbidden=true
```

### Test the Tool
```bash
cd scripts/linters/errorcode-checker
make test
```

## Contributing

To modify the ErrorCode checker:

1. Edit `main.go` to change the logic
2. Update tests in `errorcode_checker_test.go`
3. Run tests: `make test`
4. Test with real code: `make check-all`

## Architecture

The tool uses Go's AST (Abstract Syntax Tree) parser to:
1. Parse Go source files
2. Find ErrorCode declarations (`errors.MustNewCode`)
3. Track ErrorCode usage throughout the codebase
4. Report unused ErrorCodes and forbidden patterns

This approach is more reliable than regex-based solutions and handles Go syntax correctly.
