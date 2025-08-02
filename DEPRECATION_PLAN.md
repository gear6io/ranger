# Icebox Deprecation Plan

## Overview

This document outlines the deprecation plan for older code files that have been migrated to the new client-server architecture.

## Files to Deprecate

### 1. CLI Package (`cli/`)

#### ✅ **Already Migrated (Safe to Deprecate)**
- `cli/jdbc.go` → Migrated to `server/protocols/jdbc/`
- `cli/serve.go` → Migrated to `server/protocols/http/`
- `cli/shell.go` → Migrated to `client/shell/`
- `cli/sql.go` → Migrated to `client/commands/`
- `cli/import.go` → Migrated to `client/commands/`
- `cli/table.go` → Migrated to `client/commands/`
- `cli/catalog_mgmt.go` → Migrated to `client/commands/`

#### ⚠️ **Partially Migrated (Needs Review)**
- `cli/init.go` → Needs migration to client commands
- `cli/demo.go` → Needs migration to client commands
- `cli/pack.go` → Needs migration to client commands
- `cli/timetravel.go` → Needs migration to client commands
- `cli/ui.go` → Needs migration to client display

#### 📝 **Test Files (Keep for Reference)**
- `cli/*_test.go` → Keep for reference, migrate tests to new structure

### 2. Engine Package (`engine/`)

#### ✅ **Already Migrated**
- `engine/jdbc/` → Migrated to `server/protocols/jdbc/`
- `engine/duckdb/` → Migrated to `server/query/`

### 3. Catalog Package (`catalog/`)

#### ✅ **Already Migrated**
- `catalog/` → Migrated to `server/storage/`

### 4. Filesystem Package (`fs/`)

#### ✅ **Already Migrated**
- `fs/` → Migrated to `server/storage/`

### 5. Configuration Package (`config/`)

#### ⚠️ **Needs Consolidation**
- `config/` → Merge with `server/config/` and `client/config/`

### 6. Display Package (`display/`)

#### ⚠️ **Needs Migration**
- `display/` → Migrate to `client/display/`

## Deprecation Strategy

### Phase 1: Mark as Deprecated (Current)
- Add deprecation warnings to all migrated files
- Update imports to use new packages
- Keep functionality working for backward compatibility

### Phase 2: Soft Deprecation (Next Release)
- Remove deprecated files from main builds
- Keep in separate branch for reference
- Update documentation to use new commands

### Phase 3: Hard Deprecation (Future Release)
- Remove deprecated files entirely
- Update all references
- Clean up imports

## Migration Checklist

### Server Components
- [x] JDBC protocol implementation
- [x] HTTP server implementation
- [x] Storage management
- [x] Query engine
- [x] Configuration management

### Client Components
- [x] Basic client structure
- [x] Configuration management
- [ ] Shell implementation
- [ ] Command implementations
- [ ] Display system

### Commands to Migrate
- [x] `serve` → `icebox-server`
- [x] `jdbc` → `icebox-server` (JDBC protocol)
- [ ] `sql` → `icebox-client query`
- [ ] `shell` → `icebox-client shell`
- [ ] `import` → `icebox-client import`
- [ ] `table` → `icebox-client table`
- [ ] `catalog` → `icebox-client catalog`
- [ ] `init` → `icebox-client init`
- [ ] `demo` → `icebox-client demo`
- [ ] `pack` → `icebox-client pack`
- [ ] `timetravel` → `icebox-client timetravel`

## Implementation Plan

### Step 1: Add Deprecation Warnings
Add deprecation warnings to all migrated files:

```go
// DEPRECATED: This file has been migrated to server/protocols/jdbc/
// Use icebox-server for JDBC functionality
// This file will be removed in a future release
```

### Step 2: Update Main Entry Points
- Update `cmd/icebox/main.go` to show deprecation notice
- Redirect users to new commands
- Keep basic functionality working

### Step 3: Migrate Remaining Commands
- Move remaining CLI commands to client structure
- Update command interfaces
- Maintain backward compatibility

### Step 4: Update Documentation
- Update README.md to reflect new architecture
- Add migration guides
- Update examples

### Step 5: Clean Up
- Remove deprecated files
- Update imports
- Clean up dependencies

## Backward Compatibility

### During Transition Period
- Old CLI (`./icebox`) continues to work
- New CLI (`./icebox-client`, `./icebox-server`) available
- Both can be used simultaneously
- Clear migration path provided

### After Deprecation
- Old CLI removed
- Only new client-server architecture available
- Migration guide provided for users

## Testing Strategy

### Before Deprecation
- [ ] All functionality works in new architecture
- [ ] Backward compatibility maintained
- [ ] Migration path tested
- [ ] Documentation updated

### After Deprecation
- [ ] New architecture fully functional
- [ ] All tests passing
- [ ] Performance acceptable
- [ ] User experience improved

## Timeline

### Immediate (Current)
- Mark files as deprecated
- Start migration of remaining commands
- Update documentation

### Short Term (Next Release)
- Complete command migration
- Remove deprecated files from main builds
- Update all examples

### Long Term (Future Release)
- Remove deprecated files entirely
- Clean up codebase
- Optimize new architecture 