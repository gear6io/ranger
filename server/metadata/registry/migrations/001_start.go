package migrations

import (
	"context"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
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
	// Create all tables in dependency order
	if err := m.createUsersTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create users table").WithCause(err)
	}

	if err := m.createDatabasesTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create databases table").WithCause(err)
	}

	if err := m.createTablesTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create tables table").WithCause(err)
	}

	if err := m.createTableMetadataTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_metadata table").WithCause(err)
	}

	if err := m.createTableFilesTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_files table").WithCause(err)
	}

	if err := m.createTablePartitionsTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_partitions table").WithCause(err)
	}

	if err := m.createTableIndexesTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_indexes table").WithCause(err)
	}

	if err := m.createTableConstraintsTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_constraints table").WithCause(err)
	}

	if err := m.createTableColumnsTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_columns table").WithCause(err)
	}

	if err := m.createTableStatisticsTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create table_statistics table").WithCause(err)
	}

	if err := m.createAccessLogTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create access_log table").WithCause(err)
	}

	if err := m.createSchemaVersionsTable(ctx, tx); err != nil {
		return errors.New(MigrationTableCreationFailed, "failed to create schema_versions table").WithCause(err)
	}

	// Create indexes for performance
	if err := m.createPerformanceIndexes(ctx, tx); err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create performance indexes").WithCause(err)
	}

	// Insert initial data
	if err := m.insertInitialData(ctx, tx); err != nil {
		return errors.New(MigrationDataInsertionFailed, "failed to insert initial data").WithCause(err)
	}

	return nil
}

// insertInitialData inserts the initial system data
func (m *Migration001) insertInitialData(ctx context.Context, tx bun.Tx) error {
	// Insert default system user
	if err := m.insertDefaultUser(ctx, tx); err != nil {
		return errors.New(MigrationDataInsertionFailed, "failed to insert default user").WithCause(err)
	}

	// Insert initial schema version
	if err := m.insertInitialSchemaVersion(ctx, tx); err != nil {
		return errors.New(MigrationDataInsertionFailed, "failed to insert initial schema version").WithCause(err)
	}

	return nil
}

// createUsersTable creates the users table for access control
func (m *Migration001) createUsersTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.User)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

// createDatabasesTable creates the normalized databases table
func (m *Migration001) createDatabasesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.Database)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTablesTable creates the normalized tables table
func (m *Migration001) createTablesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.Table)(nil)).
		ForeignKey(`("database_id") REFERENCES "databases" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableMetadataTable creates the comprehensive table metadata table
func (m *Migration001) createTableMetadataTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableMetadata)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableFilesTable creates the table files tracking table
func (m *Migration001) createTableFilesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableFile)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTablePartitionsTable creates the table partitions tracking table
func (m *Migration001) createTablePartitionsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TablePartition)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableIndexesTable creates the table indexes tracking table
func (m *Migration001) createTableIndexesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableIndex)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableConstraintsTable creates the table constraints tracking table
func (m *Migration001) createTableConstraintsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableConstraint)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableColumnsTable creates the table columns tracking table
func (m *Migration001) createTableColumnsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableColumn)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableStatisticsTable creates the table statistics tracking table
func (m *Migration001) createTableStatisticsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.TableStatistic)(nil)).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createAccessLogTable creates the access logging table
func (m *Migration001) createAccessLogTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.AccessLog)(nil)).
		ForeignKey(`("user_id") REFERENCES "users" ("id") ON DELETE SET NULL`).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE SET NULL`).
		ForeignKey(`("database_id") REFERENCES "databases" ("id") ON DELETE SET NULL`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createSchemaVersionsTable creates the schema versioning table
func (m *Migration001) createSchemaVersionsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model((*regtypes.SchemaVersion)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

// createPerformanceIndexes creates performance optimization indexes
func (m *Migration001) createPerformanceIndexes(ctx context.Context, tx bun.Tx) error {
	var err error

	// Database indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_databases_name ON databases(name)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create databases name index").WithCause(err)
	}

	// Table indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_database ON tables(database_id)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create tables database index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create tables name index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_type ON tables(table_type)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create tables type index").WithCause(err)
	}

	// Table metadata indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_metadata_table ON table_metadata(table_id)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create table_metadata table index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_metadata_engine ON table_metadata(storage_engine)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create table_metadata engine index").WithCause(err)
	}

	// Table files indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_files_table ON table_files(table_id)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create table_files table index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_files_partition ON table_files(partition_path)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create table_files partition index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_files_iceberg_state ON table_files(iceberg_metadata_state)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create table_files iceberg_state index").WithCause(err)
	}

	// Access log indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_timestamp ON access_log(timestamp)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create access_log timestamp index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_user ON access_log(user_id)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create access_log user index").WithCause(err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_action ON access_log(action)`)
	if err != nil {
		return errors.New(MigrationIndexCreationFailed, "failed to create access_log action index").WithCause(err)
	}

	// Create unique constraints for data integrity
	uniqueConstraints := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_tables_database_name ON tables(database_id, name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_table_files_table_path ON table_files(table_id, file_path)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_table_partitions_table_path ON table_partitions(table_id, partition_path)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_table_indexes_table_name ON table_indexes(table_id, index_name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_table_constraints_table_name ON table_constraints(table_id, constraint_name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_table_columns_table_name ON table_columns(table_id, column_name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_versions_version ON schema_versions(version)`,
	}

	for _, constraint := range uniqueConstraints {
		if _, err := tx.ExecContext(ctx, constraint); err != nil {
			return errors.New(MigrationIndexCreationFailed, "failed to create unique constraint").WithCause(err)
		}
	}

	return nil
}

// insertDefaultUser inserts a default system user
func (m *Migration001) insertDefaultUser(ctx context.Context, tx bun.Tx) error {
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO users (username, email, full_name, role, is_active, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, "system", "system@icebox.local", "System User", regtypes.UserRoleAdmin, true, now, now)

	return err
}

// insertInitialSchemaVersion inserts the initial schema version
func (m *Migration001) insertInitialSchemaVersion(ctx context.Context, tx bun.Tx) error {
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO schema_versions (version, name, description, applied_at, applied_by)
		VALUES (?, ?, ?, ?, ?)
	`, 1, "production_schema_v1", "Initial production-ready schema with proper normalization and foreign keys", now, "system")

	return err
}
