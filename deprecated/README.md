# Deprecated Files

This directory contains files that have been deprecated and migrated to the new client-server architecture.

## Migration Status

### ✅ Fully Migrated (Safe to Remove)
- `cli/jdbc.go` → `server/protocols/jdbc/`
- `cli/serve.go` → `server/protocols/http/`
- `engine/jdbc/` → `server/protocols/jdbc/`
- `engine/duckdb/` → `server/query/`
- `catalog/` → `server/storage/`
- `fs/` → `server/storage/`
- `config/` → `server/config/` and `client/config/`
- `display/` → `client/display/`

### ⚠️ Partially Migrated (Needs Review)
- `cli/shell.go` → `client/shell/`
- `cli/sql.go` → `client/commands/`
- `cli/import.go` → `client/commands/`
- `cli/table.go` → `client/commands/`
- `cli/catalog_mgmt.go` → `client/commands/`

### 📝 Test Files (Keep for Reference)
- `cli/*_test.go` → Migrate tests to new structure

## Removal Timeline

### Phase 1: Mark as Deprecated (Current)
- Files marked with deprecation warnings
- Functionality preserved for backward compatibility

### Phase 2: Soft Deprecation (Next Release)
- Remove from main builds
- Keep in separate branch for reference

### Phase 3: Hard Deprecation (Future Release)
- Remove files entirely
- Update all references

## Migration Guide

See `MIGRATION.md` in the root directory for detailed migration instructions.

## New Architecture

- **Server**: `./icebox-server` (or `go run cmd/icebox-server/main.go`)
- **Client**: `./icebox-client` (or `go run cmd/icebox-client/main.go`)

## Commands Migration

| Old Command | New Command |
|-------------|-------------|
| `./icebox serve` | `./icebox-server` |
| `./icebox jdbc` | `./icebox-server` (JDBC protocol) |
| `./icebox sql` | `./icebox-client query` |
| `./icebox shell` | `./icebox-client shell` |
| `./icebox import` | `./icebox-client import` |
| `./icebox table` | `./icebox-client table` |
| `./icebox catalog` | `./icebox-client catalog` |
