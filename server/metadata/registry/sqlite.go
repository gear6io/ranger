package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
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

// GetPendingFilesForIceberg returns files that need Iceberg metadata generation
func (sm *Store) GetPendingFilesForIceberg(ctx context.Context) ([]*regtypes.TableFile, error) {
	query := `
		SELECT id, table_id, file_name, file_path, file_size, file_type, 
		       partition_path, row_count, checksum, is_compressed, 
		       created_at, modified_at, iceberg_metadata_state
		FROM table_files 
		WHERE iceberg_metadata_state != 'completed'
		ORDER BY created_at ASC
	`

	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending files: %w", err)
	}
	defer rows.Close()

	var pendingFiles []*regtypes.TableFile
	for rows.Next() {
		var file regtypes.TableFile
		err := rows.Scan(
			&file.ID, &file.TableID, &file.FileName, &file.FilePath, &file.FileSize,
			&file.FileType, &file.PartitionPath, &file.RowCount, &file.Checksum,
			&file.IsCompressed, &file.CreatedAt, &file.ModifiedAt, &file.IcebergMetadataState,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file info: %w", err)
		}
		pendingFiles = append(pendingFiles, &file)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return pendingFiles, nil
}

// CreateDatabase creates a new database
func (sm *Store) CreateDatabase(ctx context.Context, dbName string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// Insert database record using new production schema (no ownership tracking)
	insertSQL := `INSERT INTO databases (name, description, is_system, is_read_only, table_count, total_size, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := sm.db.ExecContext(ctx, insertSQL, dbName, "", false, false, 0, 0, now, now)
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

// CreateTable creates a new table with complete metadata in a single transaction
func (sm *Store) CreateTable(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) (*TableMetadata, error) {
	// Check if database exists
	if !sm.DatabaseExists(ctx, database) {
		return nil, fmt.Errorf("database %s does not exist", database)
	}

	// Get database ID
	var dbID int64
	query := `SELECT id FROM databases WHERE name = ?`
	err := sm.db.QueryRowContext(ctx, query, database).Scan(&dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database ID for %s: %w", database, err)
	}

	// Begin transaction
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create table record using new production schema
	now := time.Now().UTC().Format(time.RFC3339)
	insertTableSQL := `INSERT INTO tables (database_id, name, display_name, description, table_type, is_temporary, is_external, row_count, file_count, total_size, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	result, err := tx.ExecContext(ctx, insertTableSQL, dbID, tableName, tableName, "", "user", false, false, 0, 0, 0, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	tableID, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID: %w", err)
	}

	// Create table metadata record
	engineConfigJSON := "{}"
	if engineConfig != nil {
		engineConfigBytes, err := json.Marshal(engineConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine config: %w", err)
		}
		engineConfigJSON = string(engineConfigBytes)
	}

	insertMetadataSQL := `INSERT INTO table_metadata (table_id, schema_version, schema, storage_engine, engine_config, format, compression, last_modified, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertMetadataSQL, tableID, 1, schema, storageEngine, engineConfigJSON, "parquet", "snappy", now, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create table metadata: %w", err)
	}

	// Create table files record
	filePath := filepath.Join("databases", database, tableName, "initial.parquet")
	insertFileSQL := `INSERT INTO table_files (table_id, file_name, file_path, file_size, file_type, partition_path, row_count, checksum, is_compressed, created_at, modified_at, iceberg_metadata_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertFileSQL, tableID, "initial.parquet", filePath, 0, "parquet", "", 0, "", true, now, now, "pending")
	if err != nil {
		return nil, fmt.Errorf("failed to create table file record: %w", err)
	}

	// Update database table count
	updateDBSQL := `UPDATE databases SET table_count = table_count + 1, updated_at = ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateDBSQL, now, dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to update database table count: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Create table directory
	tablePath := filepath.Join(sm.basePath, "databases", database, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	// Create metadata object
	tableMetadata := &TableMetadata{
		Database:      database,
		Name:          tableName,
		Schema:        schema,
		StorageEngine: storageEngine,
		EngineConfig:  engineConfigJSON,
		FileCount:     0,
		TotalSize:     0,
		LastModified:  time.Now(),
		Created:       time.Now(), // Use current time instead of string
		Files:         []*regtypes.TableFile{},
	}

	return tableMetadata, nil
}

// DropTable drops a table from the specified database
func (sm *Store) DropTable(ctx context.Context, dbName, tableName string) error {
	// Get database ID
	var dbID int64
	query := `SELECT id FROM databases WHERE name = ?`
	err := sm.db.QueryRowContext(ctx, query, dbName).Scan(&dbID)
	if err != nil {
		return fmt.Errorf("failed to get database ID for %s: %w", dbName, err)
	}

	// Begin transaction
	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete table record using new production schema
	deleteTableSQL := `DELETE FROM tables WHERE database_id = ? AND name = ?`
	result, err := tx.ExecContext(ctx, deleteTableSQL, dbID, tableName)
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
	updateDBSQL := `UPDATE databases SET table_count = table_count - 1, updated_at = ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateDBSQL, now, dbID)
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
	query := `SELECT t.name FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? ORDER BY t.name`
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
	query := `SELECT 1 FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ? LIMIT 1`
	var exists int
	err := sm.db.QueryRowContext(ctx, query, dbName, tableName).Scan(&exists)
	return err == nil
}

// CreateTableMetadata creates detailed metadata for a table (for storage operations)
func (sm *Store) CreateTableMetadata(ctx context.Context, database, tableName string, schema []byte, storageEngine string, engineConfig map[string]interface{}) (*TableMetadata, error) {
	// Get table ID
	var tableID int64
	query := `SELECT t.id FROM tables t JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ?`
	err := sm.db.QueryRowContext(ctx, query, database, tableName).Scan(&tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID for %s.%s: %w", database, tableName, err)
	}

	// Serialize engine config to JSON
	engineConfigJSON := "{}"
	if engineConfig != nil {
		configBytes, err := json.Marshal(engineConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine config: %w", err)
		}
		engineConfigJSON = string(configBytes)
	}

	// Get current timestamp
	now := time.Now()

	// Create metadata object
	tableMetadata := &TableMetadata{
		Database:      database,
		Name:          tableName,
		Schema:        schema,
		StorageEngine: storageEngine,
		EngineConfig:  engineConfigJSON,
		FileCount:     0,
		TotalSize:     0,
		LastModified:  now,
		Created:       now,
		Files:         []*regtypes.TableFile{},
	}

	return tableMetadata, nil
}

// LoadTableMetadata loads detailed metadata for a table
func (sm *Store) LoadTableMetadata(ctx context.Context, database, tableName string) (*TableMetadata, error) {
	query := `SELECT tm.schema, tm.storage_engine, tm.engine_config, tm.last_modified, tm.created_at FROM table_metadata tm JOIN tables t ON tm.table_id = t.id JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ?`
	row := sm.db.QueryRowContext(ctx, query, database, tableName)

	var schema []byte
	var storageEngine, engineConfig string
	var lastModified, createdAt time.Time

	err := row.Scan(&schema, &storageEngine, &engineConfig, &lastModified, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to load table metadata for %s.%s: %w", database, tableName, err)
	}

	// Load table files
	files, err := sm.loadTableFiles(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to load table files: %w", err)
	}

	// Calculate file count and total size
	fileCount := len(files)
	var totalSize int64
	for _, file := range files {
		totalSize += file.FileSize
	}

	tableMetadata := &TableMetadata{
		Database:      database,
		Name:          tableName,
		Schema:        schema,
		StorageEngine: storageEngine,
		EngineConfig:  engineConfig,
		FileCount:     fileCount,
		TotalSize:     totalSize,
		LastModified:  lastModified,
		Created:       createdAt,
		Files:         files,
	}

	return tableMetadata, nil
}

// ListAllTables returns a list of all tables across all databases (for storage manager)
func (sm *Store) ListAllTables(ctx context.Context) ([]string, error) {
	query := `SELECT d.name, t.name FROM tables t JOIN databases d ON t.database_id = d.id ORDER BY d.name, t.name`
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
func (sm *Store) loadTableFiles(ctx context.Context, database, tableName string) ([]*regtypes.TableFile, error) {
	query := `SELECT tf.file_name, tf.file_size, tf.created_at, tf.modified_at, tf.partition_path FROM table_files tf JOIN tables t ON tf.table_id = t.id JOIN databases d ON t.database_id = d.id WHERE d.name = ? AND t.name = ? ORDER BY tf.file_name`
	rows, err := sm.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to load table files: %w", err)
	}
	defer rows.Close()

	var files []*regtypes.TableFile
	for rows.Next() {
		var fileName, created, modified, partitionPath string
		var fileSize int64

		if err := rows.Scan(&fileName, &fileSize, &created, &modified, &partitionPath); err != nil {
			return nil, fmt.Errorf("failed to scan table file: %w", err)
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
			FileName:             fileName,
			FileSize:             fileSize,
			CreatedAt:            createdAt,
			ModifiedAt:           modifiedAt,
			PartitionPath:        partitionPath,
			IcebergMetadataState: "pending",
		}

		files = append(files, fileInfo)
	}

	return files, nil
}
