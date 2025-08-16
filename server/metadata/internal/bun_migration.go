package internal

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

// MigrationStatus represents the status of a migration
type MigrationStatus struct {
	Version     int    `json:"version"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
	AppliedAt   string `json:"applied_at"`
}

// BunMigrationManager handles migrations using bun
type BunMigrationManager struct {
	db     *bun.DB
	dbPath string
}

// NewBunMigrationManager creates a new bun-based migration manager
func NewBunMigrationManager(dbPath string) (*BunMigrationManager, error) {
	// Create SQLite connection
	sqldb, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Create bun DB
	db := bun.NewDB(sqldb, sqlitedialect.New())

	manager := &BunMigrationManager{
		db:     db,
		dbPath: dbPath,
	}

	// Run initial schema creation
	ctx := context.Background()
	if err := manager.createInitialSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create initial schema: %w", err)
	}

	return manager, nil
}

// createInitialSchema creates the initial database schema
func (bmm *BunMigrationManager) createInitialSchema(ctx context.Context) error {
	// Create databases table
	_, err := bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:databases"`
			Name          string `bun:"name,pk,type:text"`
			Created       string `bun:"created,type:text,notnull"`
			Modified      string `bun:"modified,type:text,notnull"`
			TableCount    int    `bun:"table_count,type:integer,default:0"`
		}{}).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create databases table: %w", err)
	}

	// Create tables table
	_, err = bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:tables"`
			DatabaseName  string `bun:"database_name,type:text,notnull"`
			TableName     string `bun:"table_name,type:text,notnull"`
			Created       string `bun:"created,type:text,notnull"`
			Modified      string `bun:"modified,type:text,notnull"`
		}{}).
		ForeignKey(`("database_name") REFERENCES "databases" ("name") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create tables table: %w", err)
	}

	// Create table_metadata table for detailed table information
	_, err = bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_metadata"`
			TableName     string `bun:"table_name,pk,type:text"`
			Schema        []byte `bun:"schema,type:blob"`
			StorageEngine string `bun:"storage_engine,type:text,notnull"`
			EngineConfig  string `bun:"engine_config,type:text,default:'{}'"`
			FileCount     int    `bun:"file_count,type:integer,default:0"`
			TotalSize     int64  `bun:"total_size,type:integer,default:0"`
			LastModified  string `bun:"last_modified,type:text,notnull"`
			Created       string `bun:"created,type:text,notnull"`
		}{}).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create table_metadata table: %w", err)
	}

	// Create table_files table for file tracking
	_, err = bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:table_files"`
			TableName     string `bun:"table_name,type:text,notnull"`
			FileName      string `bun:"file_name,type:text,notnull"`
			FileSize      int64  `bun:"file_size,type:integer,notnull"`
			Created       string `bun:"created,type:text,notnull"`
			Modified      string `bun:"modified,type:text,notnull"`
			Date          string `bun:"date,type:text,notnull"`
		}{}).
		ForeignKey(`("table_name") REFERENCES "table_metadata" ("table_name") ON DELETE CASCADE`).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create table_files table: %w", err)
	}

	// Create migrations table for tracking
	_, err = bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:bun_migrations"`
			Version       int    `bun:"version,pk,type:integer"`
			Name          string `bun:"name,type:text,notnull"`
			AppliedAt     string `bun:"applied_at,type:text,notnull"`
		}{}).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Insert initial migration record
	_, err = bmm.db.ExecContext(ctx,
		"INSERT OR IGNORE INTO bun_migrations (version, name, applied_at) VALUES (?, ?, ?)",
		1, "initial_schema", "now")
	if err != nil {
		return fmt.Errorf("failed to insert initial migration record: %w", err)
	}

	return nil
}

// MigrateToLatest runs all pending migrations
func (bmm *BunMigrationManager) MigrateToLatest(ctx context.Context) error {
	log.Println("🔄 Running bun migrations...")

	// For now, just verify schema exists
	if err := bmm.VerifySchema(ctx); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	log.Println("✅ Migrations completed successfully")
	return nil
}

// GetCurrentVersion returns the current migration version
func (bmm *BunMigrationManager) GetCurrentVersion(ctx context.Context) (int, error) {
	// Query the migrations table
	var version int
	err := bmm.db.NewSelect().
		Column("version").
		Table("bun_migrations").
		Order("version DESC").
		Limit(1).
		Scan(ctx, &version)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, nil
}

// GetMigrationStatus returns migration status
func (bmm *BunMigrationManager) GetMigrationStatus(ctx context.Context) ([]MigrationStatus, error) {
	// Query the migrations table
	var migrations []struct {
		Version   int    `bun:"version"`
		Name      string `bun:"name"`
		AppliedAt string `bun:"applied_at"`
	}

	err := bmm.db.NewSelect().
		Model(&migrations).
		Table("bun_migrations").
		Order("version ASC").
		Scan(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}

	status := make([]MigrationStatus, len(migrations))
	for i, m := range migrations {
		status[i] = MigrationStatus{
			Version:     m.Version,
			Name:        m.Name,
			Description: fmt.Sprintf("Migration %d: %s", m.Version, m.Name),
			Status:      "applied",
			AppliedAt:   m.AppliedAt,
		}
	}

	return status, nil
}

// VerifySchema verifies that the current schema matches expectations
func (bmm *BunMigrationManager) VerifySchema(ctx context.Context) error {
	// Check if expected tables exist
	expectedTables := []string{"bun_migrations", "databases", "tables", "table_metadata", "table_files"}

	for _, tableName := range expectedTables {
		exists, err := bmm.tableExists(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to verify table %s: %w", tableName, err)
		}
		if !exists {
			return fmt.Errorf("expected table %s does not exist", tableName)
		}
	}

	return nil
}

// tableExists checks if a table exists
func (bmm *BunMigrationManager) tableExists(ctx context.Context, tableName string) (bool, error) {
	var exists int
	err := bmm.db.NewRaw("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(ctx, &exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Close releases resources
func (bmm *BunMigrationManager) Close() error {
	if bmm.db != nil {
		return bmm.db.Close()
	}
	return nil
}

// GetDB returns the underlying bun DB for external use
func (bmm *BunMigrationManager) GetDB() *bun.DB {
	return bmm.db
}
