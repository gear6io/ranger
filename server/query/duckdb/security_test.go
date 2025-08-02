package duckdb

import (
	"context"
	"strings"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLInjectionProtection(t *testing.T) {
	// Create test catalog and engine
	cfg := &config.Config{
		Name: "test-security",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Create engine with security enabled
	engineConfig := DefaultEngineConfig()
	engineConfig.EnableQueryValidation = true

	engine, err := NewEngineWithConfig(catalog, engineConfig)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	t.Run("AllowedQueries", func(t *testing.T) {
		allowedQueries := []string{
			"SELECT 1",
			"SELECT COUNT(*) FROM test_table",
			"SHOW TABLES",
			"DESCRIBE test_table",
			"EXPLAIN SELECT * FROM test_table",
			"WITH cte AS (SELECT 1) SELECT * FROM cte",
		}

		for _, query := range allowedQueries {
			t.Run(query, func(t *testing.T) {
				// These should not be blocked by security validation
				// (they may fail for other reasons like missing tables)
				_, err := engine.ExecuteQuery(ctx, query)
				// Check that it's not a security error
				if err != nil {
					assert.False(t, strings.Contains(err.Error(), "security violation"),
						"Query should not be blocked by security: %s", query)
				}
			})
		}
	})

	t.Run("BlockedStatements", func(t *testing.T) {
		blockedQueries := []string{
			"DROP TABLE test",
			"CREATE TABLE malicious (id INT)",
			"INSERT INTO test VALUES (1)",
			"UPDATE test SET id = 1",
			"DELETE FROM test",
			"ALTER TABLE test ADD COLUMN evil INT",
			"TRUNCATE TABLE test",
		}

		for _, query := range blockedQueries {
			t.Run(query, func(t *testing.T) {
				_, err := engine.ExecuteQuery(ctx, query)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "security violation")
				assert.Contains(t, err.Error(), "statement type not allowed")
			})
		}
	})

	t.Run("BlockedKeywords", func(t *testing.T) {
		blockedQueries := []string{
			"SELECT * FROM test; COPY test TO '/tmp/evil.csv'",
			"SELECT LOAD('malicious.so')",
			"SELECT * FROM test; ATTACH DATABASE '/tmp/evil.db' AS evil",
			"SELECT * FROM test; DETACH DATABASE evil",
			"SELECT PRAGMA table_info(test)",
			"SELECT * FROM test; SET memory_limit = '1GB'",
			"SELECT CALL evil_function()",
			"SELECT * FROM test; EXPORT DATABASE '/tmp/export'",
			"SELECT * FROM test; IMPORT DATABASE '/tmp/import'",
			"SELECT * FROM test; FORCE CHECKPOINT",
			"SELECT * FROM test; VACUUM",
		}

		for _, query := range blockedQueries {
			t.Run(query, func(t *testing.T) {
				_, err := engine.ExecuteQuery(ctx, query)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "security violation")
				assert.Contains(t, err.Error(), "blocked keyword")
			})
		}
	})

	t.Run("SQLInjectionPatterns", func(t *testing.T) {
		injectionQueries := []string{
			"SELECT * FROM test WHERE id = 1; DROP TABLE test; --",
			"SELECT * FROM test WHERE name = 'test' OR 1=1",
			"SELECT * FROM test WHERE id = 1 AND 1=1",
			"SELECT * FROM test WHERE name = 'test' OR 'a'='a'",
			"SELECT * FROM test WHERE id = 1 UNION SELECT * FROM sensitive_table",
			"SELECT * FROM test WHERE id = 1; DELETE FROM test; --",
			"SELECT * FROM test WHERE name = 'test' /* comment */ OR 1=1",
			"SELECT * FROM test WHERE id = EXEC('evil code')",
			"SELECT * FROM test WHERE name = CHAR(65)",
			"SELECT CONCAT('SELECT * FROM ', (SELECT table_name FROM information_schema.tables))",
		}

		for _, query := range injectionQueries {
			t.Run(query, func(t *testing.T) {
				_, err := engine.ExecuteQuery(ctx, query)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "security violation")
				assert.Contains(t, err.Error(), "potential SQL injection detected")
			})
		}
	})

	t.Run("EmptyQuery", func(t *testing.T) {
		_, err := engine.ExecuteQuery(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "security violation")
		assert.Contains(t, err.Error(), "empty query not allowed")
	})

	t.Run("WhitespaceOnlyQuery", func(t *testing.T) {
		_, err := engine.ExecuteQuery(ctx, "   \n\t  ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "security violation")
		assert.Contains(t, err.Error(), "empty query not allowed")
	})

	t.Run("MetricsTracking", func(t *testing.T) {
		initialMetrics := engine.GetMetrics()
		initialBlocked := initialMetrics.BlockedQueries

		// Execute a blocked query
		_, err := engine.ExecuteQuery(ctx, "DROP TABLE test")
		require.Error(t, err)

		// Check metrics updated
		finalMetrics := engine.GetMetrics()
		assert.Equal(t, initialBlocked+1, finalMetrics.BlockedQueries)
	})
}

func TestDescribeTableSecurity(t *testing.T) {
	// Create test catalog and engine
	cfg := &config.Config{
		Name: "test-security",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	t.Run("ValidTableName", func(t *testing.T) {
		// This should not be blocked (may fail for other reasons)
		_, err := engine.DescribeTable(ctx, "valid_table_name")
		// Should not be a security error
		if err != nil {
			assert.False(t, strings.Contains(err.Error(), "invalid table name"))
		}
	})

	t.Run("MaliciousTableNames", func(t *testing.T) {
		maliciousNames := []string{
			"test'; DROP TABLE users; --",
			"test\" OR 1=1 --",
			"test\\'; DELETE FROM users; --",
			"test-table; EXEC('evil')",
			"test/*comment*/table",
		}

		for _, name := range maliciousNames {
			t.Run(name, func(t *testing.T) {
				_, err := engine.DescribeTable(ctx, name)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid table name")
				assert.Contains(t, err.Error(), "potentially dangerous characters")
			})
		}
	})
}

func TestSecurityConfiguration(t *testing.T) {
	cfg := &config.Config{
		Name: "test-security",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
	}

	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	t.Run("SecurityDisabled", func(t *testing.T) {
		// Create engine with security disabled
		engineConfig := DefaultEngineConfig()
		engineConfig.EnableQueryValidation = false

		engine, err := NewEngineWithConfig(catalog, engineConfig)
		require.NoError(t, err)
		defer engine.Close()

		ctx := context.Background()

		// This should not be blocked when security is disabled
		_, err = engine.ExecuteQuery(ctx, "DROP TABLE test")
		// Should not be a security error (may fail for other reasons)
		if err != nil {
			assert.False(t, strings.Contains(err.Error(), "security violation"))
		}
	})

	t.Run("CustomAllowedStatements", func(t *testing.T) {
		// Create engine with custom allowed statements
		engineConfig := DefaultEngineConfig()
		engineConfig.AllowedStatements = []string{"SELECT", "INSERT"}

		engine, err := NewEngineWithConfig(catalog, engineConfig)
		require.NoError(t, err)
		defer engine.Close()

		ctx := context.Background()

		// SELECT should be allowed
		_, err = engine.ExecuteQuery(ctx, "SELECT 1")
		if err != nil {
			assert.False(t, strings.Contains(err.Error(), "statement type not allowed"))
		}

		// SHOW should be blocked (not in allowed list)
		_, err = engine.ExecuteQuery(ctx, "SHOW TABLES")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "statement type not allowed")
	})

	t.Run("CustomBlockedKeywords", func(t *testing.T) {
		// Create engine with custom blocked keywords
		engineConfig := DefaultEngineConfig()
		engineConfig.BlockedKeywords = []string{"EVIL", "MALICIOUS"}

		engine, err := NewEngineWithConfig(catalog, engineConfig)
		require.NoError(t, err)
		defer engine.Close()

		ctx := context.Background()

		// Query with custom blocked keyword should be blocked
		_, err = engine.ExecuteQuery(ctx, "SELECT EVIL FROM test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "blocked keyword 'EVIL' detected")
	})
}
