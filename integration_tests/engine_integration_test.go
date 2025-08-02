package integration_tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/json"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEngineIntegrationAllCatalogs tests the DuckDB engine with all catalog types
func TestEngineIntegrationAllCatalogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test with SQLite catalog
	t.Run("SQLiteCatalog", func(t *testing.T) {
		testEngineWithCatalogType(t, "sqlite")
	})

	// Test with JSON catalog
	t.Run("JSONCatalog", func(t *testing.T) {
		testEngineWithCatalogType(t, "json")
	})
}

// testEngineWithCatalogType tests the engine with a specific catalog type
func testEngineWithCatalogType(t *testing.T, catalogType string) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-integration-"+catalogType)
	defer os.RemoveAll(tempDir)

	// Ensure the directory exists
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	// Create catalog based on type
	var cat catalog.CatalogInterface

	switch catalogType {
	case "sqlite":
		cfg := &config.Config{
			Name: "test-catalog",
			Catalog: config.CatalogConfig{
				SQLite: &config.SQLiteConfig{
					Path: filepath.Join(tempDir, "catalog.db"),
				},
			},
			Storage: config.StorageConfig{
				FileSystem: &config.FileSystemConfig{
					RootPath: tempDir,
				},
			},
		}
		cat, err = sqlite.NewCatalog(cfg)
		require.NoError(t, err)

	case "json":
		cfg := &config.Config{
			Name: "test-catalog",
			Catalog: config.CatalogConfig{
				JSON: &config.JSONConfig{
					URI:       filepath.Join(tempDir, "catalog.json"),
					Warehouse: tempDir,
				},
			},
			Storage: config.StorageConfig{
				FileSystem: &config.FileSystemConfig{
					RootPath: tempDir,
				},
			},
		}
		cat, err = json.NewCatalog(cfg)
		require.NoError(t, err)

	default:
		t.Fatalf("Unknown catalog type: %s", catalogType)
	}

	defer cat.Close()

	// Create DuckDB engine
	engine, err := duckdb.NewEngine(cat)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Test 1: Basic engine functionality
	t.Run("BasicFunctionality", func(t *testing.T) {
		// Test simple query
		result, err := engine.ExecuteQuery(ctx, "SELECT 42 as answer, 'DuckDB' as engine")
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.RowCount)
		assert.Equal(t, []string{"answer", "engine"}, result.Columns)
		assert.Equal(t, []interface{}{int32(42), "DuckDB"}, result.Rows[0])

		// Test table listing (should be empty initially)
		tables, err := engine.ListTables(ctx)
		require.NoError(t, err)
		assert.Empty(t, tables)
	})

	// Test 2: Iceberg table creation and registration
	t.Run("IcebergTableOperations", func(t *testing.T) {
		// Create namespace
		namespace := table.Identifier{"analytics"}
		err := cat.CreateNamespace(ctx, namespace, iceberg.Properties{
			"description": "Analytics namespace for " + catalogType + " catalog",
			"owner":       "integration-test",
		})
		require.NoError(t, err)

		// Create table schema
		schema := createTestSchema()
		tableIdent := table.Identifier{"analytics", "user_events"}

		// Create Iceberg table
		icebergTable, err := cat.CreateTable(ctx, tableIdent, schema)
		require.NoError(t, err)
		assert.NotNil(t, icebergTable)

		// Register table with engine (now works with both SQLite and JSON catalogs in DuckDB v1.3.0)
		err = engine.RegisterTable(ctx, tableIdent, icebergTable)
		require.NoError(t, err)

		// Verify table is registered
		tables, err := engine.ListTables(ctx)
		require.NoError(t, err)
		assert.Contains(t, tables, "analytics_user_events")
		assert.Contains(t, tables, "user_events") // Should have alias

		// Test table description
		result, err := engine.DescribeTable(ctx, "analytics_user_events")
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.RowCount > 0)

		// Verify schema columns are present
		columnNames := make(map[string]bool)
		for _, row := range result.Rows {
			if len(row) > 0 {
				if colName, ok := row[0].(string); ok {
					columnNames[colName] = true
				}
			}
		}
		assert.True(t, columnNames["id"], "Should have id column")
		assert.True(t, columnNames["name"], "Should have name column")
		assert.True(t, columnNames["amount"], "Should have amount column")
		assert.True(t, columnNames["created_at"], "Should have created_at column")
	})

	// Test 3: Query performance and metrics
	t.Run("PerformanceAndMetrics", func(t *testing.T) {
		initialMetrics := engine.GetMetrics()
		initialQueries := initialMetrics.QueriesExecuted

		// Execute multiple queries to test performance
		queries := []string{
			"SELECT 1",
			"SELECT COUNT(*) FROM (VALUES (1), (2), (3)) AS t(x)",
			"SELECT x * 2 FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)",
			"SELECT 'test' || '_' || CAST(x AS VARCHAR) FROM (VALUES (1), (2)) AS t(x)",
		}

		start := time.Now()
		for _, query := range queries {
			result, err := engine.ExecuteQuery(ctx, query)
			require.NoError(t, err)
			assert.NotNil(t, result)
			assert.True(t, result.RowCount > 0)
		}
		duration := time.Since(start)

		// Verify metrics updated
		finalMetrics := engine.GetMetrics()
		assert.Equal(t, initialQueries+int64(len(queries)), finalMetrics.QueriesExecuted)
		assert.True(t, finalMetrics.TotalQueryTime > 0)

		// Performance should be reasonable (all queries under 1 second)
		assert.True(t, duration < time.Second, "Queries should complete quickly")

		t.Logf("Executed %d queries in %v with %s catalog", len(queries), duration, catalogType)
	})

	// Test 4: Error handling and edge cases
	t.Run("ErrorHandling", func(t *testing.T) {
		// Test invalid SQL
		_, err := engine.ExecuteQuery(ctx, "INVALID SQL SYNTAX")
		assert.Error(t, err)

		// Test non-existent table
		_, err = engine.ExecuteQuery(ctx, "SELECT * FROM non_existent_table")
		assert.Error(t, err)

		// Test nil table registration
		err = engine.RegisterTable(ctx, table.Identifier{"test"}, nil)
		assert.Error(t, err)

		// Verify error metrics increased
		metrics := engine.GetMetrics()
		assert.True(t, metrics.ErrorCount > 0)
	})

	// Test 5: Complex queries and data types
	t.Run("ComplexQueries", func(t *testing.T) {
		// Test various data types
		result, err := engine.ExecuteQuery(ctx, `
			SELECT 
				42::BIGINT as big_int,
				3.14::DOUBLE as float_val,
				'hello world'::VARCHAR as text_val,
				TRUE::BOOLEAN as bool_val,
				CURRENT_DATE as date_val,
				CURRENT_TIMESTAMP as timestamp_val
		`)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.RowCount)
		assert.Len(t, result.Columns, 6)

		// Test aggregation
		result, err = engine.ExecuteQuery(ctx, `
			SELECT 
				COUNT(*) as count,
				SUM(x) as sum,
				AVG(x) as avg,
				MIN(x) as min_val,
				MAX(x) as max_val
			FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
		`)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.RowCount)
		assert.Equal(t, []string{"count", "sum", "avg", "min_val", "max_val"}, result.Columns)

		// Test joins
		result, err = engine.ExecuteQuery(ctx, `
			SELECT a.x, b.y
			FROM (VALUES (1, 'a'), (2, 'b')) AS a(x, name)
			JOIN (VALUES (1, 100), (2, 200)) AS b(x, y) ON a.x = b.x
			ORDER BY a.x
		`)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.RowCount)
	})

	// Test 6: Memory management and large result sets
	t.Run("MemoryManagement", func(t *testing.T) {
		// Test with a moderately large result set
		result, err := engine.ExecuteQuery(ctx, `
			SELECT 
				x as id,
				'user_' || CAST(x AS VARCHAR) as username,
				x * 1.5 as score
			FROM generate_series(1, 1000) AS t(x)
			WHERE x % 10 = 0
		`)
		require.NoError(t, err)
		assert.Equal(t, int64(100), result.RowCount) // 1000/10 = 100 rows
		assert.Len(t, result.Columns, 3)

		// Verify memory usage is reasonable
		assert.True(t, len(result.Rows) == 100)
		assert.True(t, len(result.Rows[0]) == 3)
	})

	t.Logf("Successfully completed all integration tests with %s catalog", catalogType)
}

// createTestSchema creates a comprehensive test schema for integration testing
func createTestSchema() *iceberg.Schema {
	fields := []iceberg.NestedField{
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 4, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
		iceberg.NestedField{ID: 5, Name: "is_active", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 6, Name: "category_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	}
	return iceberg.NewSchema(0, fields...)
}

// TestEngineConfigurationOptions tests various engine configuration options
func TestEngineConfigurationOptions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping configuration test in short mode")
	}

	tempDir := filepath.Join(os.TempDir(), "icebox-engine-config-test")
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: tempDir,
			},
		},
	}

	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Test different engine configurations
	configs := []*duckdb.EngineConfig{
		{
			MaxMemoryMB:        128,
			QueryTimeoutSec:    30,
			EnableQueryLog:     false,
			EnableOptimization: true,
			CacheSize:          50,
			IcebergCatalogName: "test_catalog_1",
		},
		{
			MaxMemoryMB:        256,
			QueryTimeoutSec:    60,
			EnableQueryLog:     true,
			EnableOptimization: true,
			CacheSize:          100,
			IcebergCatalogName: "test_catalog_2",
		},
		{
			MaxMemoryMB:        64,
			QueryTimeoutSec:    10,
			EnableQueryLog:     false,
			EnableOptimization: false,
			CacheSize:          25,
			IcebergCatalogName: "test_catalog_3",
		},
	}

	for i, config := range configs {
		t.Run(fmt.Sprintf("Config%d", i+1), func(t *testing.T) {
			engine, err := duckdb.NewEngineWithConfig(catalog, config)
			require.NoError(t, err)
			defer engine.Close()

			// Verify configuration
			assert.Equal(t, config.MaxMemoryMB, engine.GetConfig().MaxMemoryMB)
			assert.Equal(t, config.QueryTimeoutSec, engine.GetConfig().QueryTimeoutSec)
			assert.Equal(t, config.EnableQueryLog, engine.GetConfig().EnableQueryLog)
			assert.Equal(t, config.IcebergCatalogName, engine.GetConfig().IcebergCatalogName)

			// Test basic functionality
			ctx := context.Background()
			result, err := engine.ExecuteQuery(ctx, "SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, int64(1), result.RowCount)
		})
	}
}

// TestEngineConcurrency tests concurrent access to the engine
func TestEngineConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	tempDir := filepath.Join(os.TempDir(), "icebox-engine-concurrency-test")
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: tempDir,
			},
		},
	}

	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := duckdb.NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()
	const numGoroutines = 10
	const queriesPerGoroutine = 20

	// Channel to collect results
	results := make(chan error, numGoroutines)

	// Launch concurrent queries
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < queriesPerGoroutine; j++ {
				query := fmt.Sprintf("SELECT %d as goroutine_id, %d as query_num", goroutineID, j)
				result, err := engine.ExecuteQuery(ctx, query)
				if err != nil {
					results <- fmt.Errorf("goroutine %d, query %d failed: %w", goroutineID, j, err)
					return
				}
				if result.RowCount != 1 {
					results <- fmt.Errorf("goroutine %d, query %d returned %d rows, expected 1", goroutineID, j, result.RowCount)
					return
				}
			}
			results <- nil
		}(i)
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err)
	}

	// Verify metrics
	metrics := engine.GetMetrics()
	expectedQueries := int64(numGoroutines * queriesPerGoroutine)
	assert.True(t, metrics.QueriesExecuted >= expectedQueries,
		"Expected at least %d queries, got %d", expectedQueries, metrics.QueriesExecuted)

	t.Logf("Successfully executed %d concurrent queries across %d goroutines",
		numGoroutines*queriesPerGoroutine, numGoroutines)
}
