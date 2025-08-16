package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/TFMV/icebox/server/metadata/types"
	_ "github.com/mattn/go-sqlite3"
)

// Store implements metadata storage using SQLite with bun migrations
type Store struct {
	db          *sql.DB
	dbPath      string
	basePath    string
	bunMigrator *BunMigrationManager
}

// NewStore creates a new SQLite-based metadata store with bun migrations
func NewStore(dbPath, basePath string) (*Store, error) {
	return NewStoreWithOptions(dbPath, basePath, true)
}

// NewStoreWithOptions creates a new store with migration options
func NewStoreWithOptions(dbPath, basePath string, useBun bool) (*Store, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create SQLite directory: %w", err)
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	store := &Store{
		db:       db,
		dbPath:   dbPath,
		basePath: basePath,
	}

	// Always use bun migrations now
	bunMigrator, err := NewBunMigrationManager(dbPath)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bun migrator: %w", err)
	}
	store.bunMigrator = bunMigrator

	// Run bun migrations
	ctx := context.Background()
	if err := bunMigrator.MigrateToLatest(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run bun migrations: %w", err)
	}

	// Verify bun schema
	if err := bunMigrator.VerifySchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("bun schema verification failed: %w", err)
	}

	return store, nil
}

// Close closes the database connection
func (sm *Store) Close() error {
	if sm.bunMigrator != nil {
		sm.bunMigrator.Close()
	}

	if sm.db != nil {
		return sm.db.Close()
	}
	return nil
}

// GetBunMigrationManager returns the bun migration manager
func (sm *Store) GetBunMigrationManager() *BunMigrationManager {
	return sm.bunMigrator
}

// IsUsingBun returns true if the store is using bun migrations
func (sm *Store) IsUsingBun() bool {
	return true // Always true now
}

// CreateDatabase creates a new database
func (sm *Store) CreateDatabase(ctx context.Context, dbName string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// Insert database record
	insertSQL := `INSERT INTO databases (name, created, modified, table_count) VALUES (?, ?, ?, 0)`
	_, err := sm.db.ExecContext(ctx, insertSQL, dbName, now, now)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %w", dbName, err)
	}

	// Create database directory
	dbPath := filepath.Join(sm.basePath, "databases", dbName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	return nil
}

// DropDatabase drops a database and all its tables
func (sm *Store) DropDatabase(ctx context.Context, dbName string) error {
	// Delete database record (tables will be deleted via CASCADE)
	deleteSQL := `DELETE FROM databases WHERE name = ?`
	result, err := sm.db.ExecContext(ctx, deleteSQL, dbName)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// Remove database directory
	dbPath := filepath.Join(sm.basePath, "databases", dbName)
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}

	return nil
}

// ListDatabases returns a list of all databases
func (sm *Store) ListDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT name FROM databases ORDER BY name`
	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, dbName)
	}

	return databases, nil
}

// DatabaseExists checks if a database exists
func (sm *Store) DatabaseExists(ctx context.Context, dbName string) bool {
	query := `SELECT 1 FROM databases WHERE name = ? LIMIT 1`
	var exists int
	err := sm.db.QueryRowContext(ctx, query, dbName).Scan(&exists)
	return err == nil
}

// CreateTable creates a new table in the specified database
func (sm *Store) CreateTable(ctx context.Context, dbName, tableName string) error {
	// Check if database exists
	if !sm.DatabaseExists(ctx, dbName) {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Begin transaction
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert table record
	insertTableSQL := `INSERT INTO tables (database_name, table_name, created, modified) VALUES (?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertTableSQL, dbName, tableName, now, now)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Update database table count
	updateDBSQL := `UPDATE databases SET table_count = table_count + 1, modified = ? WHERE name = ?`
	_, err = tx.ExecContext(ctx, updateDBSQL, now, dbName)
	if err != nil {
		return fmt.Errorf("failed to update database table count: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Create table directory
	tablePath := filepath.Join(sm.basePath, "databases", dbName, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	return nil
}

// DropTable drops a table from the specified database
func (sm *Store) DropTable(ctx context.Context, dbName, tableName string) error {
	// Begin transaction
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete table record
	deleteTableSQL := `DELETE FROM tables WHERE database_name = ? AND table_name = ?`
	result, err := tx.ExecContext(ctx, deleteTableSQL, dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("table %s does not exist in database %s", tableName, dbName)
	}

	// Update database table count
	now := time.Now().UTC().Format(time.RFC3339)
	updateDBSQL := `UPDATE databases SET table_count = table_count - 1, modified = ? WHERE name = ?`
	_, err = tx.ExecContext(ctx, updateDBSQL, now, dbName)
	if err != nil {
		return fmt.Errorf("failed to update database table count: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Remove table directory
	tablePath := filepath.Join(sm.basePath, "databases", dbName, tableName)
	if err := os.RemoveAll(tablePath); err != nil {
		return fmt.Errorf("failed to remove table directory: %w", err)
	}

	return nil
}

// ListTables returns a list of tables in the specified database
func (sm *Store) ListTables(ctx context.Context, dbName string) ([]string, error) {
	query := `SELECT table_name FROM tables WHERE database_name = ? ORDER BY table_name`
	rows, err := sm.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// TableExists checks if a table exists in the specified database
func (sm *Store) TableExists(ctx context.Context, dbName, tableName string) bool {
	query := `SELECT 1 FROM tables WHERE database_name = ? AND table_name = ? LIMIT 1`
	var exists int
	err := sm.db.QueryRowContext(ctx, query, dbName, tableName).Scan(&exists)
	return err == nil
}

// CreateTableMetadata creates detailed metadata for a table (for storage operations)
func (sm *Store) CreateTableMetadata(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) (*types.TableMetadata, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Serialize engine config to JSON
	engineConfigJSON := "{}"
	if engineConfig != nil {
		configBytes, err := json.Marshal(engineConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine config: %w", err)
		}
		engineConfigJSON = string(configBytes)
	}

	// Insert table metadata record
	insertSQL := `INSERT INTO table_metadata (database_name, table_name, schema, storage_engine, engine_config, file_count, total_size, last_modified, created) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)`
	_, err := sm.db.ExecContext(ctx, insertSQL, database, tableName, schema, storageEngine, engineConfigJSON, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create table metadata for %s.%s: %w", database, tableName, err)
	}

	// Create metadata object
	tableMetadata := &types.TableMetadata{
		Database:      database,
		Name:          tableName,
		Schema:        schema,
		StorageEngine: storageEngine,
		EngineConfig:  engineConfig,
		FileCount:     0,
		TotalSize:     0,
		LastModified:  now,
		Created:       now,
		Files:         []types.FileInfo{},
	}

	return tableMetadata, nil
}

// LoadTableMetadata loads detailed metadata for a table
func (sm *Store) LoadTableMetadata(ctx context.Context, database, tableName string) (*types.TableMetadata, error) {
	query := `SELECT table_name, schema, storage_engine, engine_config, file_count, total_size, last_modified, created FROM table_metadata WHERE database_name = ? AND table_name = ?`

	var tableNameStr, storageEngine, engineConfigJSON, lastModified, created string
	var schema []byte
	var fileCount int
	var totalSize int64

	err := sm.db.QueryRowContext(ctx, query, database, tableName).Scan(
		&tableNameStr, &schema, &storageEngine, &engineConfigJSON,
		&fileCount, &totalSize, &lastModified, &created)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table metadata not found: %s.%s", database, tableName)
		}
		return nil, fmt.Errorf("failed to load table metadata: %w", err)
	}

	// Parse engine config JSON
	var engineConfig map[string]interface{}
	if engineConfigJSON != "{}" {
		if err := json.Unmarshal([]byte(engineConfigJSON), &engineConfig); err != nil {
			return nil, fmt.Errorf("failed to parse engine config: %w", err)
		}
	}

	// Load files for this table
	files, err := sm.loadTableFiles(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to load table files: %w", err)
	}

	tableMetadata := &types.TableMetadata{
		Database:      database,
		Name:          tableNameStr,
		Schema:        schema,
		StorageEngine: storageEngine,
		EngineConfig:  engineConfig,
		FileCount:     fileCount,
		TotalSize:     totalSize,
		LastModified:  lastModified,
		Created:       created,
		Files:         files,
	}

	return tableMetadata, nil
}

// ListAllTables returns a list of all tables across all databases (for storage manager)
func (sm *Store) ListAllTables(ctx context.Context) ([]string, error) {
	query := `SELECT database_name, table_name FROM tables ORDER BY database_name, table_name`
	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var databaseName, tableName string
		if err := rows.Scan(&databaseName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table info: %w", err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", databaseName, tableName))
	}

	return tables, nil
}

// loadTableFiles loads file information for a table
func (sm *Store) loadTableFiles(ctx context.Context, database, tableName string) ([]types.FileInfo, error) {
	query := `SELECT file_name, file_size, created, modified, date FROM table_files WHERE database_name = ? AND table_name = ? ORDER BY file_name`
	rows, err := sm.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table files: %w", err)
	}
	defer rows.Close()

	var files []types.FileInfo
	for rows.Next() {
		var fileName, created, modified, date string
		var fileSize int64

		if err := rows.Scan(&fileName, &fileSize, &created, &modified, &date); err != nil {
			return nil, fmt.Errorf("failed to scan file info: %w", err)
		}

		fileInfo := types.FileInfo{
			Name:     fileName,
			Size:     fileSize,
			Created:  created,
			Modified: modified,
			Date:     date,
		}
		files = append(files, fileInfo)
	}

	return files, nil
}
