package registry

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreWithProductionSchema(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "sqlite_production_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	basePath := filepath.Join(tempDir, "data")

	// Create store (this will run migrations automatically)
	store, err := NewStore(dbPath, basePath)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	t.Run("StoreCreation", func(t *testing.T) {
		// Verify store was created successfully
		assert.NotNil(t, store)
		assert.True(t, store.IsUsingBun())

		// Verify migration manager exists
		migrationManager := store.GetBunMigrationManager()
		assert.NotNil(t, migrationManager)
	})

	t.Run("SchemaVerification", func(t *testing.T) {
		// Verify that the production schema was created
		migrationManager := store.GetBunMigrationManager()
		err := migrationManager.VerifySchema(ctx)
		require.NoError(t, err)
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		// Create a test database
		err := store.CreateDatabase(ctx, "testdb")
		require.NoError(t, err)

		// Verify database exists
		exists := store.DatabaseExists(ctx, "testdb")
		assert.True(t, exists)

		// List databases
		databases, err := store.ListDatabases(ctx)
		require.NoError(t, err)
		assert.Contains(t, databases, "testdb")
	})

	t.Run("CreateTable", func(t *testing.T) {
		// Create a test table
		tableMeta, err := store.CreateTable(ctx, "testdb", "testtable", []byte("{}"), "memory", nil)
		require.NoError(t, err)
		require.NotNil(t, tableMeta)

		// Verify table exists
		exists := store.TableExists(ctx, "testdb", "testtable")
		assert.True(t, exists)

		// List tables
		tables, err := store.ListTables(ctx, "testdb")
		require.NoError(t, err)
		assert.Contains(t, tables, "testtable")
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		// Create another database
		err := store.CreateDatabase(ctx, "analytics")
		require.NoError(t, err)

		// Create multiple tables
		tableNames := []string{"users", "events", "metrics"}
		for _, tableName := range tableNames {
			tableMeta, err := store.CreateTable(ctx, "analytics", tableName, []byte("{}"), "memory", nil)
			require.NoError(t, err)
			require.NotNil(t, tableMeta)
		}

		// Verify all tables exist
		tableList, err := store.ListTables(ctx, "analytics")
		require.NoError(t, err)
		assert.Len(t, tableList, 3)
		assert.Contains(t, tableList, "users")
		assert.Contains(t, tableList, "events")
		assert.Contains(t, tableList, "metrics")

		// Verify database count
		databases, err := store.ListDatabases(ctx)
		require.NoError(t, err)
		assert.Len(t, databases, 2)
		assert.Contains(t, databases, "testdb")
		assert.Contains(t, databases, "analytics")
	})

	t.Run("SchemaIntegrity", func(t *testing.T) {
		// Test that foreign key constraints work
		// Try to create a table in non-existent database
		_, err := store.CreateTable(ctx, "nonexistent", "testtable", []byte("{}"), "memory", nil)
		assert.Error(t, err) // Should fail due to foreign key constraint
	})

	t.Run("PerformanceIndexes", func(t *testing.T) {
		// The schema should have performance indexes created
		// Verify that we can query efficiently
		start := time.Now()
		_, err := store.ListDatabases(ctx)
		duration := time.Since(start)
		require.NoError(t, err)

		// Query should be fast (less than 100ms for simple operation)
		assert.Less(t, duration, 100*time.Millisecond)
	})

	t.Run("DropTable", func(t *testing.T) {
		// Create a table to drop
		tableMeta, err := store.CreateTable(ctx, "testdb", "droptable", []byte("{}"), "memory", nil)
		require.NoError(t, err)
		require.NotNil(t, tableMeta)

		// Verify it exists
		assert.True(t, store.TableExists(ctx, "testdb", "droptable"))

		// Drop the table
		err = store.DropTable(ctx, "testdb", "droptable")
		require.NoError(t, err)

		// Verify it no longer exists
		assert.False(t, store.TableExists(ctx, "testdb", "droptable"))
	})

	t.Run("DropDatabase", func(t *testing.T) {
		// Create a database to drop
		err := store.CreateDatabase(ctx, "dropdb")
		require.NoError(t, err)

		// Verify it exists
		assert.True(t, store.DatabaseExists(ctx, "dropdb"))

		// Drop the database
		err = store.DropDatabase(ctx, "dropdb")
		require.NoError(t, err)

		// Verify it no longer exists
		assert.False(t, store.DatabaseExists(ctx, "dropdb"))
	})
}

func TestMigrationSystem(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "migration_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	t.Run("MigrationManagerCreation", func(t *testing.T) {
		// Create migration manager
		migrationManager, err := NewBunMigrationManager(dbPath)
		require.NoError(t, err)
		defer migrationManager.Close()

		// Verify migration manager was created
		assert.NotNil(t, migrationManager)
	})

	t.Run("MigrationStatus", func(t *testing.T) {
		// Create migration manager
		migrationManager, err := NewBunMigrationManager(dbPath)
		require.NoError(t, err)
		defer migrationManager.Close()

		ctx := context.Background()

		// Get migration status
		status, err := migrationManager.GetMigrationStatus(ctx)
		require.NoError(t, err)
		assert.NotNil(t, status)

		// Should have at least one migration applied
		assert.GreaterOrEqual(t, len(status), 1)

		// Get current version
		version, err := migrationManager.GetCurrentVersion(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, version, 1)
	})
}

func TestProductionSchemaConstraintsInStore(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "constraints_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	// Create store and schema
	store, err := NewStore(dbPath, tempDir)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	t.Run("ForeignKeyConstraints", func(t *testing.T) {
		// Test that foreign key constraints are enforced
		// Try to insert a table with non-existent database_id
		_, err := store.GetBunMigrationManager().GetDB().ExecContext(ctx, `
			INSERT INTO tables (database_id, name, display_name, description, table_type, is_temporary, is_external, row_count, file_count, total_size, created_at, updated_at)
			VALUES (999, 'test', 'test', 'test', 'user', false, false, 0, 0, 0, '2023-01-01T00:00:00Z', '2023-01-01T00:00:00Z')
		`)

		// This should fail due to foreign key constraint
		assert.Error(t, err)
	})

	t.Run("UniqueConstraints", func(t *testing.T) {
		// Test that unique constraints are enforced
		// Insert first database
		_, err = store.GetBunMigrationManager().GetDB().ExecContext(ctx, `
			INSERT INTO databases (name, description, is_system, is_read_only, table_count, total_size, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, "uniquetest", "test", false, false, 0, 0, "2023-01-01T00:00:00Z", "2023-01-01T00:00:00Z")
		require.NoError(t, err)

		// Try to insert duplicate database name
		_, err = store.GetBunMigrationManager().GetDB().ExecContext(ctx, `
			INSERT INTO databases (name, description, is_system, is_read_only, table_count, total_size, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, "uniquetest", "test2", false, false, 0, 0, "2023-01-01T00:00:00Z", "2023-01-01T00:00:00Z")

		// This should fail due to unique constraint
		assert.Error(t, err)
	})
}
