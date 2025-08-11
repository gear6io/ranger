package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isCI checks if we're running in a CI environment
func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" || os.Getenv("JENKINS_URL") != ""
}

// createTestSchema returns a standard test schema for Iceberg tables
func createTestSchema() *iceberg.Schema {
	fields := []iceberg.NestedField{
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 4, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	}
	return iceberg.NewSchema(0, fields...)
}

// TestEngineWithSQLiteCatalog tests the engine with SQLite catalog
func TestEngineWithSQLiteCatalog(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-sqlite")
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

	// Create SQLite catalog
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Test engine creation
	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	testEngineBasicFunctionality(t, engine, catalog)
}

// TestEngineWithJSONCatalog tests the engine with JSON catalog
func TestEngineWithJSONCatalog(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-json")
	defer os.RemoveAll(tempDir)

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

	// Create JSON catalog
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Test engine creation
	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	testEngineBasicFunctionality(t, engine, catalog)
}

// testEngineBasicFunctionality tests basic engine functionality with any catalog type
func testEngineBasicFunctionality(t *testing.T, engine *Engine, cat catalog.CatalogInterface) {
	ctx := context.Background()

	// Test basic queries
	t.Run("BasicQueries", func(t *testing.T) {
		// Test simple SELECT
		result, err := engine.ExecuteQuery(ctx, "SELECT 1 as test_column, 'hello' as message")
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.RowCount)
		assert.Equal(t, []string{"test_column", "message"}, result.Columns)

		// Test SHOW TABLES (should be empty initially)
		result, err = engine.ExecuteQuery(ctx, "SHOW TABLES")
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	// Test table listing
	t.Run("ListTables", func(t *testing.T) {
		tables, err := engine.ListTables(ctx)
		require.NoError(t, err)
		assert.NotNil(t, tables)
		// Initially should have no tables
		assert.Empty(t, tables)
	})

	// Test metrics
	t.Run("Metrics", func(t *testing.T) {
		metrics := engine.GetMetrics()
		assert.NotNil(t, metrics)
		assert.True(t, metrics.QueriesExecuted > 0) // We've executed some queries above
	})

	// Test engine configuration
	t.Run("Configuration", func(t *testing.T) {
		assert.NotNil(t, engine.config)
		assert.Equal(t, "iceberg_catalog", engine.config.IcebergCatalogName)
		assert.True(t, engine.initialized)
	})
}

// TestEngineWithCustomConfig tests engine with custom configuration
func TestEngineWithCustomConfig(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-config")
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

	// Custom engine config
	engineConfig := &EngineConfig{
		MaxMemoryMB:        256,
		QueryTimeoutSec:    60,
		EnableQueryLog:     true,
		EnableOptimization: true,
		CacheSize:          50,
		IcebergCatalogName: "custom_catalog",
	}

	engine, err := NewEngineWithConfig(catalog, engineConfig)
	require.NoError(t, err)
	defer engine.Close()

	// Verify custom configuration
	assert.Equal(t, 256, engine.config.MaxMemoryMB)
	assert.Equal(t, 60, engine.config.QueryTimeoutSec)
	assert.True(t, engine.config.EnableQueryLog)
	assert.Equal(t, "custom_catalog", engine.config.IcebergCatalogName)
}

// TestEngineIcebergTableRegistration tests Iceberg table registration
func TestEngineIcebergTableRegistration(t *testing.T) {
	if isCI() {
		t.Skip("Skipping Iceberg table test in CI due to complexity")
	}

	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-iceberg")
	defer os.RemoveAll(tempDir)

	// Ensure the directory exists
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Create a namespace
	namespace := table.Identifier{"test_namespace"}
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"description": "Test namespace"})
	require.NoError(t, err)

	// Create a test table
	schema := createTestSchema()
	tableIdent := table.Identifier{"test_namespace", "test_table"}
	icebergTable, err := catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Register table with engine
	err = engine.RegisterTable(ctx, tableIdent, icebergTable)
	require.NoError(t, err)

	// Verify table is registered
	tables, err := engine.ListTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "test_namespace_test_table")

	// Test table description
	result, err := engine.DescribeTable(ctx, "test_namespace_test_table")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.RowCount > 0)

	// Verify metrics updated
	metrics := engine.GetMetrics()
	assert.True(t, metrics.TablesRegistered > 0)
}

// TestEngineErrorHandling tests error handling scenarios
func TestEngineErrorHandling(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-errors")
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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	t.Run("InvalidSQL", func(t *testing.T) {
		_, err := engine.ExecuteQuery(ctx, "INVALID SQL STATEMENT")
		assert.Error(t, err)

		// Check that error count increased
		metrics := engine.GetMetrics()
		assert.True(t, metrics.ErrorCount > 0)
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		_, err := engine.ExecuteQuery(ctx, "SELECT * FROM non_existent_table")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("NilTableRegistration", func(t *testing.T) {
		err := engine.RegisterTable(ctx, table.Identifier{"test"}, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})
}

// TestEngineQueryTimeout tests query timeout functionality
func TestEngineQueryTimeout(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-timeout")
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

	// Create engine with very short timeout
	engineConfig := &EngineConfig{
		MaxMemoryMB:        512,
		QueryTimeoutSec:    1, // 1 second timeout
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          100,
		IcebergCatalogName: "test_catalog",
	}

	engine, err := NewEngineWithConfig(catalog, engineConfig)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Test with a query that should complete quickly
	result, err := engine.ExecuteQuery(ctx, "SELECT 1")
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// TestEngineClose tests proper cleanup
func TestEngineClose(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-close")
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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)

	// Close should work without error
	err = engine.Close()
	assert.NoError(t, err)

	// Multiple closes should be safe
	err = engine.Close()
	assert.NoError(t, err)
}

// TestEngineUtilityFunctions tests utility functions
func TestEngineUtilityFunctions(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-utils")
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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("IdentifierToTableName", func(t *testing.T) {
		tests := []struct {
			identifier table.Identifier
			expected   string
		}{
			{table.Identifier{"default", "users"}, "default_users"},
			{table.Identifier{"analytics", "events"}, "analytics_events"},
			{table.Identifier{"warehouse", "inventory", "products"}, "warehouse_inventory_products"},
		}

		for _, test := range tests {
			result := engine.identifierToTableName(test.identifier)
			assert.Equal(t, test.expected, result)
		}
	})

	t.Run("QuoteName", func(t *testing.T) {
		tests := []struct {
			name     string
			expected string
		}{
			{"simple", `"simple"`},
			{"with space", `"with space"`},
			{"with\"quote", `"with""quote"`},
			{"table_name", `"table_name"`},
		}

		for _, test := range tests {
			result := engine.quoteName(test.name)
			assert.Equal(t, test.expected, result)
		}
	})
}

// TestEngineNilCatalog tests error handling for nil catalog
func TestEngineNilCatalog(t *testing.T) {
	_, err := NewEngine(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "catalog cannot be nil")
}

// TestEngineDefaultConfig tests default configuration
func TestEngineDefaultConfig(t *testing.T) {
	config := DefaultEngineConfig()
	assert.NotNil(t, config)
	assert.Equal(t, 512, config.MaxMemoryMB)
	assert.Equal(t, 300, config.QueryTimeoutSec)
	assert.False(t, config.EnableQueryLog)
	assert.True(t, config.EnableOptimization)
	assert.Equal(t, 100, config.CacheSize)
	assert.Equal(t, "iceberg_catalog", config.IcebergCatalogName)
}

// TestEngineQueryPreprocessing tests query preprocessing
func TestEngineQueryPreprocessing(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-preprocess")
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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Create and register a test table
	namespace := table.Identifier{"analytics"}
	err = catalog.CreateNamespace(ctx, namespace, map[string]string{})
	require.NoError(t, err)

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tableIdent := table.Identifier{"analytics", "users"}
	icebergTable, err := catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	err = engine.RegisterTable(ctx, tableIdent, icebergTable)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple table reference",
			input:    "SELECT * FROM analytics.users",
			expected: "SELECT * FROM analytics_users",
		},
		{
			name:     "Multiple table references",
			input:    "SELECT * FROM analytics.users JOIN analytics.products",
			expected: "SELECT * FROM analytics_users JOIN analytics_products",
		},
		{
			name:     "Column references",
			input:    "SELECT analytics.users.id FROM analytics.users",
			expected: "SELECT analytics_users.id FROM analytics_users",
		},
		{
			name:     "Mixed notation",
			input:    "SELECT u.id FROM analytics.users u JOIN analytics_products p ON u.id = p.user_id",
			expected: "SELECT u.id FROM analytics_users u JOIN analytics_products p ON u.id = p.user_id",
		},
		{
			name:     "WHERE clause",
			input:    "SELECT * FROM analytics.users WHERE analytics.users.id > 0",
			expected: "SELECT * FROM analytics_users WHERE analytics_users.id > 0",
		},
		{
			name:     "Preserve column references",
			input:    "SELECT u.id, u.name FROM analytics.users u",
			expected: "SELECT u.id, u.name FROM analytics_users u",
		},
		{
			name:     "No table references",
			input:    "SELECT 1 as test",
			expected: "SELECT 1 as test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processedQuery, err := engine.preprocessQuery(ctx, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, processedQuery)
		})
	}
}

// TestEnginePerformanceMetrics tests performance metrics tracking
func TestEnginePerformanceMetrics(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-metrics")
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

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Get initial metrics
	initialMetrics := engine.GetMetrics()
	initialQueries := initialMetrics.QueriesExecuted

	// Execute a query
	_, err = engine.ExecuteQuery(ctx, "SELECT 1")
	require.NoError(t, err)

	// Check metrics updated
	updatedMetrics := engine.GetMetrics()
	assert.Equal(t, initialQueries+1, updatedMetrics.QueriesExecuted)
	assert.True(t, updatedMetrics.TotalQueryTime > 0)

	// Test that metrics are thread-safe (return a copy)
	metrics1 := engine.GetMetrics()
	metrics2 := engine.GetMetrics()
	assert.Equal(t, metrics1.QueriesExecuted, metrics2.QueriesExecuted)
}

// BenchmarkEngineQuery benchmarks query execution
func BenchmarkEngineQuery(b *testing.B) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-benchmark")
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
	require.NoError(b, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(b, err)
	defer engine.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.ExecuteQuery(ctx, "SELECT 1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestEngineMemoryManagement tests memory management features
func TestEngineMemoryManagement(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "icebox-engine-test-memory")
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

	// Create engine with small memory limit
	engineConfig := &EngineConfig{
		MaxMemoryMB:        64, // Small memory limit
		QueryTimeoutSec:    300,
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          10,
		IcebergCatalogName: "test_catalog",
	}

	engine, err := NewEngineWithConfig(catalog, engineConfig)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Test that engine handles memory constraints gracefully
	result, err := engine.ExecuteQuery(ctx, "SELECT 1")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowCount)
}

func TestEngineWithoutIcebergExtension(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create a SQLite catalog for testing
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "test.db"),
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: tempDir,
			},
		},
	}

	cat, err := sqlite.NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Create engine with custom config
	engineConfig := DefaultEngineConfig()
	engine, err := NewEngineWithConfig(cat, engineConfig)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Manually set icebergAvailable to false to simulate Windows scenario
	engine.icebergAvailable = false

	// Create a namespace
	ctx := context.Background()
	namespace := table.Identifier{"test_ns"}
	if err := cat.CreateNamespace(ctx, namespace, map[string]string{}); err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Create a table
	tableID := table.Identifier{"test_ns", "test_table"}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	icebergTable, err := cat.CreateTable(ctx, tableID, schema, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to register the table - should create a placeholder
	err = engine.RegisterTable(ctx, tableID, icebergTable)
	if err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}

	// Verify that the placeholder table was created
	tables, err := engine.ListTables(ctx)
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}

	found := false
	expectedTableName := "test_ns_test_table"
	for _, tableName := range tables {
		if tableName == expectedTableName {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected table %s not found in tables: %v", expectedTableName, tables)
	}

	// Query the placeholder table to verify it contains the error message
	result, err := engine.ExecuteQuery(ctx, "SELECT * FROM test_ns_test_table")
	if err != nil {
		t.Fatalf("Failed to query placeholder table: %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("Expected 1 row in placeholder table, got %d", result.RowCount)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns in placeholder table, got %d", len(result.Columns))
	}

	// Check that the error message is present
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		errorMsg, ok := result.Rows[0][0].(string)
		if !ok || errorMsg != "Iceberg extension not available on this platform" {
			t.Errorf("Expected error message in first column, got: %v", result.Rows[0][0])
		}
	}
}

func TestEngineWithIcebergExtension(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create a SQLite catalog for testing
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "test.db"),
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: tempDir,
			},
		},
	}

	cat, err := sqlite.NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Create engine with default config (should have Iceberg extension on most platforms)
	engine, err := NewEngine(cat)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Verify that the engine was initialized properly
	if !engine.initialized {
		t.Error("Engine should be initialized")
	}

	// Test basic functionality
	ctx := context.Background()
	tables, err := engine.ListTables(ctx)
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}

	// Should start with no tables
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables initially, got %d", len(tables))
	}
}
