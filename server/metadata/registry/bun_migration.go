package registry

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry/migrations"
	_ "github.com/mattn/go-sqlite3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

// Package-specific error codes for bun migrations
var (
	RegistryBunMigrationFailed          = errors.MustNewCode("registry.bun_migration_failed")
	RegistryBunSchemaVerificationFailed = errors.MustNewCode("registry.bun_schema_verification_failed")
)

// Migration interface that all migration files must implement
type Migration interface {
	Version() int
	Name() string
	Description() string
	Up(ctx context.Context, tx bun.Tx) error
}

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
		return nil, errors.New(errors.CommonInternal, "failed to open SQLite database", err).AddContext("path", dbPath)
	}

	// Create bun DB
	db := bun.NewDB(sqldb, sqlitedialect.New())

	manager := &BunMigrationManager{
		db:     db,
		dbPath: dbPath,
	}

	// Run migrations (this will create the initial schema)
	ctx := context.Background()
	if err := manager.MigrateToLatest(ctx); err != nil {
		db.Close()
		// On migration failure, kill the service
		log.Fatalf("Migration failed: %v. Service will exit.", err)
	}

	return manager, nil
}

// MigrateToLatest runs all pending migrations
func (bmm *BunMigrationManager) MigrateToLatest(ctx context.Context) error {
	log.Println("🔄 Running bun migrations...")

	// Get current version
	currentVersion, err := bmm.GetCurrentVersion(ctx)
	if err != nil {
		return errors.New(RegistryBunMigrationFailed, "failed to get current version", err)
	}

	// Get all available migrations
	availableMigrations := bmm.getAvailableMigrations()

	// Find pending migrations
	var pendingMigrations []Migration
	for _, migration := range availableMigrations {
		if migration.Version() > currentVersion {
			pendingMigrations = append(pendingMigrations, migration)
		}
	}

	if len(pendingMigrations) == 0 {
		log.Println("✅ No pending migrations")
		return nil
	}

	// Begin single transaction for all migrations
	tx, err := bmm.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New(RegistryBunMigrationFailed, "failed to begin transaction for migrations", err)
	}

	// Run all pending migrations within the transaction
	for _, migration := range pendingMigrations {
		log.Printf("🔄 Running migration %d: %s", migration.Version(), migration.Name())

		// Run migration UP within the transaction
		if err := migration.Up(ctx, tx); err != nil {
			// Rollback transaction on any failure
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("⚠️ Failed to rollback transaction: %v", rbErr)
			}

			// Log migration failure and kill service
			log.Printf("❌ Migration %d (%s) failed: %v", migration.Version(), migration.Name(), err)
			log.Fatalf("Migration failed. Service will exit. Rollback transaction completed.")
		}

		log.Printf("✅ Migration %d (%s) completed successfully", migration.Version(), migration.Name())
	}

	// Record all migrations in migrations table within the transaction
	now := time.Now().UTC().Format(time.RFC3339)
	for _, migration := range pendingMigrations {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO bun_migrations (version, name, applied_at) VALUES (?, ?, ?)
		`, migration.Version(), migration.Name(), now)

		if err != nil {
			// Rollback transaction on recording failure
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("⚠️ Failed to rollback transaction: %v", rbErr)
			}

			log.Printf("❌ Failed to record migration %d: %v", migration.Version(), err)
			log.Fatalf("Migration recording failed. Service will exit. Rollback transaction completed.")
		}
	}

	// Commit transaction - all migrations succeed or none do
	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit migrations: %v", err)
		log.Fatalf("Migration commit failed. Service will exit.")
	}

	log.Println("✅ All migrations completed successfully")
	return nil
}

// getAvailableMigrations returns all available migrations (hardcoded)
func (bmm *BunMigrationManager) getAvailableMigrations() []Migration {
	return []Migration{
		&migrations.Migration001{}, // from migrations/001_start.go
		// Future migrations will be added here
	}
}

// GetCurrentVersion returns the current migration version
func (bmm *BunMigrationManager) GetCurrentVersion(ctx context.Context) (int, error) {
	// First check if migrations table exists
	exists, err := bmm.tableExists(ctx, "bun_migrations")
	if err != nil {
		return 0, errors.New(RegistryBunMigrationFailed, "failed to check migrations table", err)
	}

	if !exists {
		// Create migrations table if it doesn't exist
		if err := bmm.createMigrationsTable(ctx); err != nil {
			return 0, errors.New(RegistryBunMigrationFailed, "failed to create migrations table", err)
		}
		return 0, nil
	}

	// Query the migrations table
	var version int
	err = bmm.db.NewSelect().
		Column("version").
		Table("bun_migrations").
		Order("version DESC").
		Limit(1).
		Scan(ctx, &version)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, errors.New(RegistryBunMigrationFailed, "failed to get current version", err)
	}

	return version, nil
}

// createMigrationsTable creates the migrations tracking table
func (bmm *BunMigrationManager) createMigrationsTable(ctx context.Context) error {
	_, err := bmm.db.NewCreateTable().
		Model(&struct {
			bun.BaseModel `bun:"table:bun_migrations"`
			Version       int    `bun:"version,pk,type:integer"`
			Name          string `bun:"name,type:text,notnull"`
			AppliedAt     string `bun:"applied_at,type:text,notnull"`
		}{}).
		IfNotExists().
		Exec(ctx)
	return err
}

// GetMigrationStatus returns migration status
func (bmm *BunMigrationManager) GetMigrationStatus(ctx context.Context) ([]MigrationStatus, error) {
	// Check if migrations table exists
	exists, err := bmm.tableExists(ctx, "bun_migrations")
	if err != nil {
		return nil, errors.New(RegistryBunMigrationFailed, "failed to check migrations table", err)
	}

	if !exists {
		return []MigrationStatus{}, nil
	}

	// Query the migrations table
	var migrations []struct {
		Version   int    `bun:"version"`
		Name      string `bun:"name"`
		AppliedAt string `bun:"applied_at"`
	}

	err = bmm.db.NewSelect().
		Model(&migrations).
		Table("bun_migrations").
		Order("version ASC").
		Scan(ctx)

	if err != nil {
		return nil, errors.New(RegistryBunMigrationFailed, "failed to query migrations", err)
	}

	status := make([]MigrationStatus, len(migrations))
	for i, m := range migrations {
		status[i] = MigrationStatus{
			Version:     m.Version,
			Name:        m.Name,
			Description: "Migration " + strconv.Itoa(m.Version) + ": " + m.Name,
			Status:      "applied",
			AppliedAt:   m.AppliedAt,
		}
	}

	return status, nil
}

// VerifySchema verifies that the current schema matches expectations
func (bmm *BunMigrationManager) VerifySchema(ctx context.Context) error {
	// Check if expected tables exist (new production schema tables)
	expectedTables := []string{
		"bun_migrations", "users", "databases", "tables", "table_metadata",
		"table_files", "table_partitions", "table_indexes", "table_constraints",
		"table_columns", "table_statistics", "access_log", "schema_versions",
	}

	for _, tableName := range expectedTables {
		exists, err := bmm.tableExists(ctx, tableName)
		if err != nil {
			return errors.New(RegistryBunSchemaVerificationFailed, "failed to verify table").AddContext("table", tableName).WithCause(err)
		}
		if !exists {
			return errors.New(RegistryBunSchemaVerificationFailed, "expected table does not exist").AddContext("table", tableName)
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
