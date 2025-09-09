package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/metadata/registry/system"
	_ "github.com/mattn/go-sqlite3"
	"github.com/uptrace/bun"
)

// Package-specific error codes for registry operations
var (
	RegistryDatabaseNotFound    = errors.MustNewCode("registry.database_not_found")
	RegistryDatabaseExists      = errors.MustNewCode("registry.database_exists")
	RegistryTableNotFound       = errors.MustNewCode("registry.table_not_found")
	RegistryTableExists         = errors.MustNewCode("registry.table_exists")
	RegistryMigrationFailed     = errors.MustNewCode("registry.migration_failed")
	RegistrySchemaVerification  = errors.MustNewCode("registry.schema_verification_failed")
	RegistryTransactionFailed   = errors.MustNewCode("registry.transaction_failed")
	RegistryFileOperationFailed = errors.MustNewCode("registry.file_operation_failed")
)

// Store implements metadata storage using SQLite with bun migrations
type Store struct {
	db          *sql.DB
	dbPath      string
	basePath    string
	bunMigrator *BunMigrationManager
	system      *system.Manager
}

// NewStore creates a new SQLite-based metadata store with bun migrations
func NewStore(dbPath, basePath string) (*Store, error) {
	return NewStoreWithOptions(dbPath, basePath, true)
}

// NewStoreWithOptions creates a new store with migration options
func NewStoreWithOptions(dbPath, basePath string, useBun bool) (*Store, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to create SQLite directory", err).AddContext("path", dbPath)
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to open SQLite database", err).AddContext("path", dbPath)
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
		return nil, errors.New(RegistryMigrationFailed, "failed to create bun migrator", err).AddContext("path", dbPath)
	}
	store.bunMigrator = bunMigrator

	// Run bun migrations
	ctx := context.Background()
	if err := bunMigrator.MigrateToLatest(ctx); err != nil {
		db.Close()
		return nil, errors.New(RegistryMigrationFailed, "failed to run bun migrations", err).AddContext("path", dbPath)
	}

	// Verify bun schema
	if err := bunMigrator.VerifySchema(ctx); err != nil {
		db.Close()
		return nil, errors.New(RegistrySchemaVerification, "bun schema verification failed", err).AddContext("path", dbPath)
	}

	// Initialize system manager (views are now created in migration)
	store.system, err = system.NewManager(ctx, db)
	if err != nil {
		db.Close()
		return nil, errors.New(RegistryMigrationFailed, "failed to initialize system manager", err).AddContext("path", dbPath)
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

// GetPendingFilesForIceberg returns files that need Iceberg metadata generation
func (sm *Store) GetPendingFilesForIceberg(ctx context.Context) ([]*regtypes.TableFile, error) {
	query := `
		SELECT 
			tf.id, tf.table_id, tf.file_name, tf.file_path, tf.file_size, tf.file_type,
			tf.partition_path, tf.row_count, tf.checksum, tf.is_compressed,
			tf.created_at, tf.updated_at, tf.iceberg_metadata_state
		FROM table_files tf
		WHERE tf.iceberg_metadata_state IN ('pending', 'failed')
		ORDER BY tf.created_at ASC
	`

	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query pending files", err)
	}
	defer rows.Close()

	var files []*regtypes.TableFile
	for rows.Next() {
		var file regtypes.TableFile
		err := rows.Scan(
			&file.ID, &file.TableID, &file.FileName, &file.FilePath, &file.FileSize, &file.FileType,
			&file.PartitionPath, &file.RowCount, &file.Checksum, &file.IsCompressed,
			&file.CreatedAt, &file.UpdatedAt, &file.IcebergMetadataState,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan table file", err)
		}
		files = append(files, &file)
	}

	return files, nil
}

// CreateDatabase creates a new database
func (sm *Store) CreateDatabase(ctx context.Context, dbName string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// Insert database record using the actual schema that exists
	insertSQL := `INSERT INTO databases (name, description, is_system, is_read_only, table_count, total_size, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := sm.db.ExecContext(ctx, insertSQL, dbName, "", false, false, 0, 0, now, now)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to create database", err).AddContext("database", dbName)
	}

	// Create database directory
	dbPath := filepath.Join(sm.basePath, "databases", dbName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return errors.New(RegistryFileOperationFailed, "failed to create database directory", err).AddContext("path", dbPath)
	}

	return nil
}

// DropDatabase drops a database and all its tables
func (sm *Store) DropDatabase(ctx context.Context, dbName string) error {
	// Delete database record (tables will be deleted via CASCADE)
	deleteSQL := `DELETE FROM databases WHERE name = ?`
	result, err := sm.db.ExecContext(ctx, deleteSQL, dbName)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to drop database", err).AddContext("database", dbName)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return errors.New(RegistryDatabaseNotFound, "database does not exist", nil).AddContext("database", dbName)
	}

	// Remove database directory
	dbPath := filepath.Join(sm.basePath, "databases", dbName)
	if err := os.RemoveAll(dbPath); err != nil {
		return errors.New(RegistryFileOperationFailed, "failed to remove database directory", err).AddContext("path", dbPath)
	}

	return nil
}

// ListDatabases returns a list of all databases
func (sm *Store) ListDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT name FROM databases ORDER BY name`
	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan database name", err)
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

// CreateTable creates a new table with complete metadata using Bun ORM
// Can optionally create columns if provided
func (sm *Store) CreateTable(ctx context.Context, database, tableName string, schema []byte, storageEngine string, settings map[string]interface{}, columns ...[]*regtypes.TableColumn) (*CompleteTableInfo, error) {
	settingsJSON := "{}"
	if settings != nil {
		settingsBytes, err := json.Marshal(settings)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to marshal engine config", err)
		}
		settingsJSON = string(settingsBytes)
	}

	tableRecord := &regtypes.Table{
		Name:          tableName,
		RowCount:      0,
		FileCount:     0,
		TotalSize:     0,
		StorageEngine: storageEngine,
		Settings:      settingsJSON,
	}

	// Use transaction
	err := sm.bunMigrator.GetDB().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var dbRecord regtypes.Database
		err := tx.NewSelect().
			Model(&dbRecord).
			Where("name = ?", database).
			Scan(ctx)
		if err != nil {
			return errors.New(errors.CommonInternal, "failed to get database ID", err).AddContext("database", database)
		}

		tableRecord.DatabaseID = dbRecord.ID
		_, err = tx.NewInsert().Model(tableRecord).Exec(ctx)
		if err != nil {
			return errors.New(errors.CommonInternal, "failed to create table", err).AddContext("table", tableName)
		}

		// Create column records if provided
		if len(columns) > 0 && len(columns[0]) > 0 {
			for _, column := range columns[0] {
				column.TableID = tableRecord.ID
				// Timestamps will be set by SQLite DEFAULT values¯
				_, err = tx.NewInsert().Model(column).Exec(ctx)
				if err != nil {
					return errors.New(errors.CommonInternal, "failed to create table column", err).
						AddContext("table", tableName).
						AddContext("column", column.ColumnName)
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Create table directory
	tablePath := filepath.Join(sm.basePath, "databases", database, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return nil, errors.New(RegistryFileOperationFailed, "failed to create table directory", err).AddContext("path", tablePath)
	}

	// Return the complete table info using the existing method
	return sm.GetCompleteTableInfoByID(ctx, tableRecord.ID)
}

// DropTable drops a table from the specified database
func (sm *Store) DropTable(ctx context.Context, dbName, tableName string) error {
	// Get database ID
	var dbID int64
	query := `SELECT id FROM databases WHERE name = ?`
	err := sm.db.QueryRowContext(ctx, query, dbName).Scan(&dbID)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to get database ID", err).AddContext("database", dbName)
	}

	// Begin transaction
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New(RegistryTransactionFailed, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	// Delete table record using new production schema
	deleteTableSQL := `DELETE FROM tables WHERE database_id = ? AND name = ?`
	result, err := tx.ExecContext(ctx, deleteTableSQL, dbID, tableName)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to drop table", err).AddContext("table", tableName)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return errors.New(RegistryTableNotFound, "table does not exist in database", nil).AddContext("table", tableName).AddContext("database", dbName)
	}

	// Update database table count
	now := time.Now().UTC().Format(time.RFC3339)
	updateDBSQL := `UPDATE databases SET table_count = table_count - 1, updated_at = ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateDBSQL, now, dbID)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to update database table count", err).AddContext("database", dbName)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return errors.New(RegistryTransactionFailed, "failed to commit transaction", err)
	}

	// Remove table directory
	tablePath := filepath.Join(sm.basePath, "databases", dbName, tableName)
	if err := os.RemoveAll(tablePath); err != nil {
		return errors.New(RegistryFileOperationFailed, "failed to remove table directory", err).AddContext("path", tablePath)
	}

	return nil
}

// ListTables returns a list of tables in the specified database
func (sm *Store) ListTables(ctx context.Context, dbName string) ([]string, error) {
	query := `SELECT t.name FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? ORDER BY t.name`
	rows, err := sm.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query tables", err).AddContext("database", dbName)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan table name", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// TableExists checks if a table exists in the specified database
func (sm *Store) TableExists(ctx context.Context, dbName, tableName string) bool {
	query := `SELECT 1 FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ? LIMIT 1`
	var exists int
	err := sm.db.QueryRowContext(ctx, query, dbName, tableName).Scan(&exists)
	return err == nil
}

// LoadTableMetadata loads detailed metadata for a table
func (sm *Store) LoadTableMetadata(ctx context.Context, database, tableName string) (*CompleteTableInfo, error) {
	// First get the table ID
	query := `SELECT t.id FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ? AND t.deleted_at IS NULL`
	var tableID int64
	err := sm.db.QueryRowContext(ctx, query, database, tableName).Scan(&tableID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New(RegistryTableNotFound, "table not found", nil).AddContext("database", database).AddContext("table", tableName)
		}
		return nil, errors.New(errors.CommonInternal, "failed to get table ID", err).AddContext("database", database).AddContext("table", tableName)
	}

	// Use the existing GetCompleteTableInfoByID method
	return sm.GetCompleteTableInfoByID(ctx, tableID)
}

// ListAllTables returns a list of all tables across all databases (for storage manager)
func (sm *Store) ListAllTables(ctx context.Context) ([]string, error) {
	query := `SELECT d.name, t.name FROM tables t JOIN databases d ON t.database_id = d.id ORDER BY d.name, t.name`
	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query all tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var databaseName, tableName string
		if err := rows.Scan(&databaseName, &tableName); err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan table info", err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", databaseName, tableName))
	}

	return tables, nil
}

// UpdateTableAfterInsertion performs all metadata updates after successful data insertion
// This method atomically updates table files, statistics, and triggers CDC events
func (sm *Store) UpdateTableAfterInsertion(ctx context.Context, database, tableName string, fileInfo FileInsertionInfo) error {
	// Start a transaction for atomic updates
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New(RegistryTransactionFailed, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	// Get table ID
	var tableID int64
	tableQuery := `SELECT t.id FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ?`
	if err := tx.QueryRowContext(ctx, tableQuery, database, tableName).Scan(&tableID); err != nil {
		return errors.New(errors.CommonInternal, "failed to get table ID", err).AddContext("database", database).AddContext("table", tableName)
	}

	// 1. Insert table file record
	now := time.Now().Format("2006-01-02 15:04:05")
	insertFileSQL := `INSERT INTO table_files (table_id, file_name, file_path, file_size, file_type, partition_path, row_count, checksum, is_compressed, created_at, updated_at, iceberg_metadata_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = tx.ExecContext(ctx, insertFileSQL,
		tableID,
		fileInfo.FileName,
		fileInfo.FilePath,
		fileInfo.FileSize,
		fileInfo.FileType,
		fileInfo.PartitionPath,
		fileInfo.RowCount,
		fileInfo.Checksum,
		fileInfo.IsCompressed,
		now,
		now,
		regtypes.IcebergMetadataGenerationStatePending)

	if err != nil {
		return errors.New(errors.CommonInternal, "failed to insert table file", err).AddContext("table", tableName)
	}

	// 2. Update table statistics
	updateStatsSQL := `UPDATE tables SET row_count = row_count + ?, file_count = file_count + 1, total_size = total_size + ?, updated_at = ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateStatsSQL, fileInfo.RowCount, fileInfo.FileSize, now, tableID)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to update table statistics", err).AddContext("table", tableName)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return errors.New(RegistryTransactionFailed, "failed to commit transaction", err)
	}

	return nil
}

// FileInsertionInfo contains all information needed for post-insertion metadata updates
type FileInsertionInfo struct {
	FileName      string
	FilePath      string
	FileSize      int64
	FileType      string
	PartitionPath string
	RowCount      int64
	Checksum      string
	IsCompressed  bool
}

// loadTableFiles loads file information for a table
func (sm *Store) loadTableFiles(ctx context.Context, database, tableName string) ([]*regtypes.TableFile, error) {
	query := `SELECT tf.file_name, tf.file_size, tf.created_at, tf.updated_at, tf.partition_path FROM table_files tf JOIN tables t ON tf.table_id = t.id JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ? ORDER BY tf.file_name`
	rows, err := sm.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to load table files", err).AddContext("database", database).AddContext("table", tableName)
	}
	defer rows.Close()

	var files []*regtypes.TableFile
	for rows.Next() {
		var fileName, created, modified, partitionPath string
		var fileSize int64

		if err := rows.Scan(&fileName, &fileSize, &created, &modified, &partitionPath); err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan table file", err)
		}

		// Parse timestamps
		createdAt, err := time.Parse("2006-01-02 15:04:05", created)
		if err != nil {
			createdAt = time.Now() // Use current time as fallback
		}

		modifiedAt, err := time.Parse("2006-01-02 15:04:05", modified)
		if err != nil {
			modifiedAt = time.Now() // Use current time as fallback
		}

		fileInfo := &regtypes.TableFile{
			FileName:      fileName,
			FileSize:      fileSize,
			PartitionPath: partitionPath,
		}

		// Set the TimeAuditable fields
		fileInfo.CreatedAt = createdAt
		fileInfo.UpdatedAt = modifiedAt

		files = append(files, fileInfo)
	}

	return files, nil
}

// GetCompleteTableInfoByID retrieves complete table information by table ID with lazy loading
func (sm *Store) GetCompleteTableInfoByID(ctx context.Context, tableID int64) (*CompleteTableInfo, error) {
	if tableID <= 0 {
		return nil, errors.New(RegistryTableNotFound, "invalid table ID", nil).AddContext("table_id", tableID)
	}

	// First, get basic table info
	query := `
		SELECT 
			t.id, t.database_id, t.name, t.row_count, t.file_count, t.total_size,
			t.storage_engine, t.settings, t.partition_by, t.order_by,
			t.created_at, t.updated_at,
			d.name as database_name
		FROM tables t
		JOIN databases d ON t.database_id = d.id
		WHERE t.id = ? AND t.deleted_at IS NULL
	`

	row := sm.db.QueryRowContext(ctx, query, tableID)

	var tableInfo CompleteTableInfo
	var dbName string

	err := row.Scan(
		&tableInfo.ID, &tableInfo.DatabaseID, &tableInfo.Name, &tableInfo.RowCount, &tableInfo.FileCount, &tableInfo.TotalSize,
		&tableInfo.StorageEngine, &tableInfo.Settings, &tableInfo.PartitionBy, &tableInfo.OrderBy,
		&tableInfo.CreatedAt, &tableInfo.UpdatedAt,
		&dbName,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New(RegistryTableNotFound, "table not found", nil).AddContext("table_id", tableID)
		}
		return nil, errors.New(errors.CommonInternal, "failed to scan table info", err).AddContext("table_id", tableID)
	}

	// Set the database name
	tableInfo.Database = dbName

	// Note: Storage metadata fields are now part of the Table struct itself
	// The tableInfo already contains StorageEngine, Settings, etc. from the table record

	// Initialize lazy loading fields
	tableInfo.initializeLazyFields(sm.db, tableID)

	return &tableInfo, nil
}

// GetTableReferenceByID retrieves basic table reference (database + table name) by table ID
func (sm *Store) GetTableReferenceByID(ctx context.Context, tableID int64) (*regtypes.Table, error) {
	if tableID <= 0 {
		return nil, errors.New(RegistryTableNotFound, "invalid table ID", nil).AddContext("table_id", tableID)
	}

	var table regtypes.Table
	err := sm.bunMigrator.GetDB().NewSelect().
		Model(&table).
		Relation("Database").
		Where("t.id = ?", tableID).
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New(RegistryTableNotFound, "table not found", nil).AddContext("table_id", tableID)
		}
		return nil, errors.New(errors.CommonInternal, "failed to get table reference", err).AddContext("table_id", tableID)
	}

	return &table, nil
}

// ValidateTableMetadata validates that a table has complete metadata for Iceberg operations
func (sm *Store) ValidateTableMetadata(ctx context.Context, tableID int64) error {
	if tableID <= 0 {
		return errors.New(RegistryTableNotFound, "invalid table ID", nil).AddContext("table_id", tableID)
	}

	query := `
		SELECT 
			t.name as table_name,
			d.name as database_name,
			t.storage_engine
		FROM tables t
		JOIN databases d ON t.database_id = d.id
		WHERE t.id = ? AND t.deleted_at IS NULL
	`

	row := sm.db.QueryRowContext(ctx, query, tableID)

	var tableName, dbName string
	var storageEngine string

	err := row.Scan(&tableName, &dbName, &storageEngine)
	if err != nil {
		if err == sql.ErrNoRows {
			return errors.New(RegistryTableNotFound, "table not found", nil).AddContext("table_id", tableID)
		}
		return errors.New(errors.CommonInternal, "failed to validate table metadata", err).AddContext("table_id", tableID)
	}

	// Check required fields
	if dbName == "" {
		return errors.New(RegistryTableNotFound, "database name is empty", nil).AddContext("table_id", tableID)
	}

	if tableName == "" {
		return errors.New(RegistryTableNotFound, "table name is empty", nil).AddContext("table_id", tableID)
	}

	// Schema validation removed - schema is now stored in TableColumn table

	if storageEngine == "" {
		return errors.New(RegistryTableNotFound, "storage engine is not specified", nil).AddContext("table_id", tableID).AddContext("table", tableName)
	}

	return nil
}

// GetTable retrieves table information by database and table name
func (sm *Store) GetTable(ctx context.Context, databaseName, tableName string) (*regtypes.Table, error) {
	var table regtypes.Table
	err := sm.bunMigrator.GetDB().NewSelect().
		Model(&table).
		Relation("Database").
		Where("d.name = ? AND t.name = ?", databaseName, tableName).
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New(RegistryTableNotFound, "table not found", nil).
				AddContext("database", databaseName).
				AddContext("table", tableName)
		}
		return nil, errors.New(errors.CommonInternal, "failed to get table", err).
			AddContext("database", databaseName).
			AddContext("table", tableName)
	}

	return &table, nil
}

// GetSystemManager returns the system database manager
func (sm *Store) GetSystemManager() *system.Manager {
	return sm.system
}
