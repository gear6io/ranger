package migrations

import (
	"context"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/uptrace/bun"
)

// Package-specific error codes for migrations
var (
	MigrationTableCreationFailed = errors.MustNewCode("migrations.table_creation_failed")
	MigrationIndexCreationFailed = errors.MustNewCode("migrations.index_creation_failed")
	MigrationDataInsertionFailed = errors.MustNewCode("migrations.data_insertion_failed")
)

// Migration001 represents the initial production schema migration
type Migration001 struct{}

// Version returns the migration version
func (m *Migration001) Version() int {
	return 1
}

// Name returns the migration name
func (m *Migration001) Name() string {
	return "initial_production_schema"
}

// Description returns the migration description
func (m *Migration001) Description() string {
	return "Initial production-ready schema with proper normalization and foreign keys"
}

// Up runs the migration
func (m *Migration001) Up(ctx context.Context, tx bun.Tx) error {
	// Create core system tables

	// Users table (for authentication and access control)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.User)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create users table", err)
	}

	// Databases table (for organizing tables)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.Database)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create databases table", err)
	}

	// Database indexes
	databaseIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_databases_name ON databases(name)`,
	}

	// Tables table (main table registry)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.Table)(nil)).
		ForeignKey(`("database_id") REFERENCES "databases" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create tables table", err)
	}

	// Table indexes
	tableIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_tables_database ON tables(database_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name)`,
		`CREATE INDEX IF NOT EXISTS idx_tables_type ON tables(table_type)`,
	}

	// Create table-related metadata tables

	// Table metadata table (schema and engine info)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableMetadata)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_metadata table", err)
	}

	// Table metadata indexes
	tableMetadataIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_table_metadata_table ON table_metadata(table_id)`,
	}

	// Table files table (file tracking)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableFile)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_files table", err)
	}

	// Table files indexes
	tableFilesIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_table_files_table ON table_files(table_id)`,
		`CREATE INDEX IF NOT EXISTS idx_table_files_type ON table_files(file_type)`,
		`CREATE INDEX IF NOT EXISTS idx_table_files_state ON table_files(iceberg_metadata_state)`,
	}

	// Table partitions table (partitioning info)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TablePartition)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_partitions table", err)
	}

	// Table partitions indexes
	tablePartitionsIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_table_partitions_table ON table_partitions(table_id)`,
	}

	// Table columns table (column definitions)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableColumn)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_columns table", err)
	}

	// Table columns indexes
	tableColumnsIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_table_columns_table ON table_columns(table_id)`,
		`CREATE INDEX IF NOT EXISTS idx_table_columns_name ON table_columns(column_name)`,
	}

	// Table optimization tables

	// Table indexes table (index definitions)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableIndex)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_indexes table", err)
	}

	// Table indexes indexes (no specific indexes needed for this table)
	tableIndexesIndexes := []string{}

	// Table constraints table (constraint definitions)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableConstraint)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_constraints table", err)
	}

	// Table constraints indexes (no specific indexes needed for this table)
	tableConstraintsIndexes := []string{}

	// Table statistics table (performance metrics)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.TableStatistic)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_statistics table", err)
	}

	// Table statistics indexes
	tableStatisticsIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_table_statistics_table ON table_statistics(table_id)`,
	}

	// Create audit and system tables

	// Access log table (audit trail)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.AccessLog)(nil)).
		ForeignKey(`("user_id") REFERENCES "users" ("id") ON DELETE SET NULL`).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create access_log table", err)
	}

	// Access log indexes
	accessLogIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_access_log_user ON access_log(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_access_log_timestamp ON access_log(created_at)`,
	}

	// Schema versions table (migration tracking)
	if _, err := tx.NewCreateTable().
		Model((*regtypes.SchemaVersion)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create schema_versions table", err)
	}

	// Schema versions indexes (no specific indexes needed for this table)
	schemaVersionsIndexes := []string{}

	// Combine all index groups
	allIndexes := [][]string{
		databaseIndexes,
		tableIndexes,
		tableMetadataIndexes,
		tableFilesIndexes,
		tablePartitionsIndexes,
		tableColumnsIndexes,
		tableIndexesIndexes,
		tableConstraintsIndexes,
		tableStatisticsIndexes,
		accessLogIndexes,
		schemaVersionsIndexes,
	}

	// Create all indexes
	for _, indexGroup := range allIndexes {
		for _, indexSQL := range indexGroup {
			if _, err := tx.ExecContext(ctx, indexSQL); err != nil {
				return errors.New(MigrationIndexCreationFailed, "failed to create index", err)
			}
		}
	}

	// Insert initial data
	now := time.Now()

	// Insert default system user
	if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO users (username, email, display_name, is_active, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, "system", "system@ranger.local", "System User", true, now, now); err != nil {
		return errors.New(MigrationDataInsertionFailed, "failed to insert default user", err)
	}

	// Insert initial schema version
	if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO schema_versions (version, name, description, applied_at, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, 1, "production_schema_v1", "Initial production-ready schema with proper normalization and foreign keys", now, now); err != nil {
		return errors.New(MigrationDataInsertionFailed, "failed to insert schema version", err)
	}

	return nil
}
