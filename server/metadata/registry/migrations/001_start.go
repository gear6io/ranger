package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
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
		return fmt.Errorf("failed to create users table: %w", err)
	}

	if err := m.createDatabasesTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create databases table: %w", err)
	}

	if err := m.createTablesTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create tables table: %w", err)
	}

	if err := m.createTableMetadataTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_metadata table: %w", err)
	}

	if err := m.createTableFilesTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_files table: %w", err)
	}

	if err := m.createTablePartitionsTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_partitions table: %w", err)
	}

	if err := m.createTableIndexesTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_indexes table: %w", err)
	}

	if err := m.createTableConstraintsTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_constraints table: %w", err)
	}

	if err := m.createTableColumnsTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_columns table: %w", err)
	}

	if err := m.createTableStatisticsTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create table_statistics table: %w", err)
	}

	if err := m.createAccessLogTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create access_log table: %w", err)
	}

	if err := m.createSchemaVersionsTable(ctx, tx); err != nil {
		return fmt.Errorf("failed to create schema_versions table: %w", err)
	}

	// Create indexes for performance
	if err := m.createPerformanceIndexes(ctx, tx); err != nil {
		return fmt.Errorf("failed to create performance indexes: %w", err)
	}

	// Insert initial data
	if err := m.insertInitialData(ctx, tx); err != nil {
		return fmt.Errorf("failed to insert initial data: %w", err)
	}

	return nil
}

// insertInitialData inserts the initial system data
func (m *Migration001) insertInitialData(ctx context.Context, tx bun.Tx) error {
	// Insert default system user
	if err := m.insertDefaultUser(ctx, tx); err != nil {
		return fmt.Errorf("failed to insert default user: %w", err)
	}

	// Insert initial schema version
	if err := m.insertInitialSchemaVersion(ctx, tx); err != nil {
		return fmt.Errorf("failed to insert initial schema version: %w", err)
	}

	return nil
}

// createUsersTable creates the users table for access control
func (m *Migration001) createUsersTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:users"`
			ID            int64  `bun:"id,pk,autoincrement"`
			Username      string `bun:"username,unique,notnull,type:text"`
			Email         string `bun:"email,unique,notnull,type:text"`
			FullName      string `bun:"full_name,type:text"`
			Role          string `bun:"role,notnull,type:text,default:'user'"`
			IsActive      bool   `bun:"is_active,notnull,default:true"`
			LastLogin     string `bun:"last_login,type:text"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
		}{}).
		IfNotExists().
		Exec(ctx)
	return err
}

// createDatabasesTable creates the normalized databases table
func (m *Migration001) createDatabasesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:databases"`
			ID            int64  `bun:"id,pk,autoincrement"`
			Name          string `bun:"name,unique,notnull,type:text"`
			Description   string `bun:"description,type:text"`
			IsSystem      bool   `bun:"is_system,notnull,default:false"`
			IsReadOnly    bool   `bun:"is_read_only,notnull,default:false"`
			TableCount    int    `bun:"table_count,notnull,default:0"`
			TotalSize     int64  `bun:"total_size,notnull,default:0"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
			DeletedAt     string `bun:"deleted_at,type:text"`
		}{}).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTablesTable creates the normalized tables table
func (m *Migration001) createTablesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:tables"`
			ID            int64  `bun:"id,pk,autoincrement"`
			DatabaseID    int64  `bun:"database_id,notnull"`
			Name          string `bun:"name,notnull,type:text"`
			DisplayName   string `bun:"display_name,type:text"`
			Description   string `bun:"description,type:text"`
			TableType     string `bun:"table_type,notnull,type:text,default:'user'"`
			IsTemporary   bool   `bun:"is_temporary,notnull,default:false"`
			IsExternal    bool   `bun:"is_external,notnull,default:false"`
			RowCount      int64  `bun:"row_count,notnull,default:0"`
			FileCount     int    `bun:"file_count,notnull,default:0"`
			TotalSize     int64  `bun:"total_size,notnull,default:0"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
			DeletedAt     string `bun:"deleted_at,type:text"`
		}{}).
		ForeignKey(`("database_id") REFERENCES "databases" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableMetadataTable creates the comprehensive table metadata table
func (m *Migration001) createTableMetadataTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_metadata"`
			ID            int64  `bun:"id,pk,autoincrement"`
			TableID       int64  `bun:"table_id,notnull"`
			SchemaVersion int    `bun:"schema_version,notnull,default:1"`
			Schema        []byte `bun:"schema,type:blob"`
			StorageEngine string `bun:"storage_engine,notnull,type:text"`
			EngineConfig  string `bun:"engine_config,type:text,default:'{}'"`
			Format        string `bun:"format,type:text"`
			Compression   string `bun:"compression,type:text"`
			PartitionBy   string `bun:"partition_by,type:text"`
			SortBy        string `bun:"sort_by,type:text"`
			Properties    string `bun:"properties,type:text,default:'{}'"`
			LastModified  string `bun:"last_modified,notnull,type:text"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableFilesTable creates the table files tracking table
func (m *Migration001) createTableFilesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_files"`
			ID            int64  `bun:"id,pk,autoincrement"`
			TableID       int64  `bun:"table_id,notnull"`
			FileName      string `bun:"file_name,notnull,type:text"`
			FilePath      string `bun:"file_path,notnull,type:text"`
			FileSize      int64  `bun:"file_size,notnull"`
			FileType      string `bun:"file_type,notnull,type:text"`
			PartitionPath string `bun:"partition_path,type:text"`
			RowCount      int64  `bun:"row_count,notnull,default:0"`
			Checksum      string `bun:"checksum,type:text"`
			IsCompressed  bool   `bun:"is_compressed,notnull,default:false"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			ModifiedAt    string `bun:"modified_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTablePartitionsTable creates the table partitions tracking table
func (m *Migration001) createTablePartitionsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_partitions"`
			ID            int64  `bun:"id,pk,autoincrement"`
			TableID       int64  `bun:"table_id,notnull"`
			PartitionKey  string `bun:"partition_key,notnull,type:text"`
			PartitionPath string `bun:"partition_path,notnull,type:text"`
			RowCount      int64  `bun:"row_count,notnull,default:0"`
			FileCount     int    `bun:"file_count,notnull,default:0"`
			TotalSize     int64  `bun:"total_size,notnull,default:0"`
			MinValues     string `bun:"min_values,type:text"`
			MaxValues     string `bun:"max_values,type:text"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableIndexesTable creates the table indexes tracking table
func (m *Migration001) createTableIndexesTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_indexes"`
			ID            int64  `bun:"id,pk,autoincrement"`
			TableID       int64  `bun:"table_id,notnull"`
			IndexName     string `bun:"index_name,notnull,type:text"`
			IndexType     string `bun:"index_type,notnull,type:text"`
			Columns       string `bun:"columns,notnull,type:text"`
			IsUnique      bool   `bun:"is_unique,notnull,default:false"`
			IsPrimary     bool   `bun:"is_primary,notnull,default:false"`
			IsActive      bool   `bun:"is_active,notnull,default:true"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
			UpdatedAt     string `bun:"updated_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableConstraintsTable creates the table constraints tracking table
func (m *Migration001) createTableConstraintsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel    `bun:"table:table_constraints"`
			ID               int64  `bun:"id,pk,autoincrement"`
			TableID          int64  `bun:"table_id,notnull"`
			ConstraintName   string `bun:"constraint_name,notnull,type:text"`
			ConstraintType   string `bun:"constraint_type,notnull,type:text"`
			Columns          string `bun:"columns,notnull,type:text"`
			ReferenceTable   string `bun:"reference_table,type:text"`
			ReferenceColumns string `bun:"reference_columns,type:text"`
			OnDelete         string `bun:"on_delete,type:text"`
			OnUpdate         string `bun:"on_update,type:text"`
			IsDeferrable     bool   `bun:"is_deferrable,notnull,default:false"`
			IsDeferred       bool   `bun:"is_deferred,notnull,default:false"`
			CreatedAt        string `bun:"created_at,notnull,type:text"`
			UpdatedAt        string `bun:"updated_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableColumnsTable creates the table columns tracking table
func (m *Migration001) createTableColumnsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel   `bun:"table:table_columns"`
			ID              int64  `bun:"id,pk,autoincrement"`
			TableID         int64  `bun:"table_id,notnull"`
			ColumnName      string `bun:"column_name,notnull,type:text"`
			DisplayName     string `bun:"display_name,type:text"`
			DataType        string `bun:"data_type,notnull,type:text"`
			IsNullable      bool   `bun:"is_nullable,notnull,default:true"`
			IsPrimary       bool   `bun:"is_primary,notnull,default:false"`
			IsUnique        bool   `bun:"is_unique,notnull,default:false"`
			DefaultValue    string `bun:"default_value,type:text"`
			Description     string `bun:"description,type:text"`
			OrdinalPosition int    `bun:"ordinal_position,notnull"`
			MaxLength       int    `bun:"max_length,type:integer"`
			Precision       int    `bun:"precision,type:integer"`
			Scale           int    `bun:"scale,type:integer"`
			CreatedAt       string `bun:"created_at,notnull,type:text"`
			UpdatedAt       string `bun:"updated_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createTableStatisticsTable creates the table statistics tracking table
func (m *Migration001) createTableStatisticsTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_statistics"`
			ID            int64  `bun:"id,pk,autoincrement"`
			TableID       int64  `bun:"table_id,notnull"`
			ColumnName    string `bun:"column_name,type:text"`
			StatType      string `bun:"stat_type,notnull,type:text"`
			StatValue     string `bun:"stat_value,type:text"`
			StatData      string `bun:"stat_data,type:text"`
			LastUpdated   string `bun:"last_updated,notnull,type:text"`
			CreatedAt     string `bun:"created_at,notnull,type:text"`
		}{}).
		ForeignKey(`("table_id") REFERENCES "tables" ("id") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	return err
}

// createAccessLogTable creates the access logging table
func (m *Migration001) createAccessLogTable(ctx context.Context, tx bun.Tx) error {
	_, err := tx.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:access_log"`
			ID            int64  `bun:"id,pk,autoincrement"`
			UserID        int64  `bun:"user_id,type:integer"`
			TableID       int64  `bun:"table_id,type:integer"`
			DatabaseID    int64  `bun:"database_id,type:integer"`
			Action        string `bun:"action,notnull,type:text"`
			Details       string `bun:"details,type:text"`
			IPAddress     string `bun:"ip_address,type:text"`
			UserAgent     string `bun:"user_agent,type:text"`
			Timestamp     string `bun:"timestamp,notnull,type:text"`
			Duration      int64  `bun:"duration,type:integer"`
			Success       bool   `bun:"success,notnull,default:true"`
		}{}).
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
		Model(&struct {
			bun.BaseModel `bun:"table:schema_versions"`
			Version       int    `bun:"version,pk,type:integer"`
			Name          string `bun:"name,notnull,type:text"`
			Description   string `bun:"description,type:text"`
			AppliedAt     string `bun:"applied_at,notnull,type:text"`
			Checksum      string `bun:"checksum,type:text"`
			AppliedBy     string `bun:"applied_by,type:text"`
		}{}).
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
		return fmt.Errorf("failed to create databases name index: %w", err)
	}

	// Table indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_database ON tables(database_id)`)
	if err != nil {
		return fmt.Errorf("failed to create tables database index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name)`)
	if err != nil {
		return fmt.Errorf("failed to create tables name index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tables_type ON tables(table_type)`)
	if err != nil {
		return fmt.Errorf("failed to create tables type index: %w", err)
	}

	// Table metadata indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_metadata_table ON table_metadata(table_id)`)
	if err != nil {
		return fmt.Errorf("failed to create table_metadata table index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_metadata_engine ON table_metadata(storage_engine)`)
	if err != nil {
		return fmt.Errorf("failed to create table_metadata engine index: %w", err)
	}

	// Table files indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_files_table ON table_files(table_id)`)
	if err != nil {
		return fmt.Errorf("failed to create table_files table index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_table_files_partition ON table_files(partition_path)`)
	if err != nil {
		return fmt.Errorf("failed to create table_files partition index: %w", err)
	}

	// Access log indexes
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_timestamp ON access_log(timestamp)`)
	if err != nil {
		return fmt.Errorf("failed to create access_log timestamp index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_user ON access_log(user_id)`)
	if err != nil {
		return fmt.Errorf("failed to create access_log user index: %w", err)
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_access_log_action ON access_log(action)`)
	if err != nil {
		return fmt.Errorf("failed to create access_log action index: %w", err)
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
			return fmt.Errorf("failed to create unique constraint: %w", err)
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
		`, "system", "system@icebox.local", "System User", "admin", true, now, now)

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
