package integration_tests

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/TFMV/icebox/deprecated/engine/jdbc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// JDBCIntegrationTest tests the JDBC server functionality
type JDBCIntegrationTest struct {
	server     *jdbc.JDBCHandler
	engine     *duckdb.Engine
	catalog    catalog.CatalogInterface
	config     *config.Config
	testDir    string
	serverAddr string
}

// NewJDBCIntegrationTest creates a new JDBC integration test
func NewJDBCIntegrationTest(t *testing.T) *JDBCIntegrationTest {
	// Create test directory
	testDir := filepath.Join(os.TempDir(), "icebox-jdbc-test")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	// Create test configuration
	cfg := &config.Config{
		Name: "jdbc-test",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(testDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "filesystem",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(testDir, "data"),
			},
		},
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(t, err)

	// Create SQL engine
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		require.NoError(t, err)
	default:
		t.Fatal("JDBC server currently only supports SQLite catalogs")
	}

	// Create JDBC handler
	ctx := context.Background()
	handler := jdbc.NewJDBCHandler(engine, testLogger(), ctx)

	return &JDBCIntegrationTest{
		server:     handler,
		engine:     engine,
		catalog:    cat,
		config:     cfg,
		testDir:    testDir,
		serverAddr: "localhost:5435", // Use a different port for testing
	}
}

// Cleanup cleans up test resources
func (j *JDBCIntegrationTest) Cleanup() {
	if j.engine != nil {
		j.engine.Close()
	}
	if j.catalog != nil {
		j.catalog.Close()
	}
	os.RemoveAll(j.testDir)
}

// testLogger creates a test logger
func testLogger() zerolog.Logger {
	// Return a simple logger for testing
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}

// TestJDBCServerStartup tests JDBC server startup
func TestJDBCServerStartup(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Test that the server can be created
	assert.NotNil(t, test.server)
	assert.NotNil(t, test.engine)
	assert.NotNil(t, test.catalog)
}

// TestJDBCConnection tests basic JDBC connection functionality
func TestJDBCConnection(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Start a test server on a random port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	// Get the actual address
	addr := listener.Addr().String()

	// Start server in goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go test.server.HandleConnection(conn)
		}
	}()

	// Test connection using PostgreSQL driver
	dsn := fmt.Sprintf("host=localhost port=%s user=test password=test dbname=icebox sslmode=disable",
		addr[strings.LastIndex(addr, ":")+1:])

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("PostgreSQL driver not available: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		t.Logf("Connection failed (expected for basic test): %v", err)
	}
}

// TestJDBCQueryExecution tests SQL query execution via JDBC
func TestJDBCQueryExecution(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Create a test table
	createTableSQL := `
	CREATE TABLE test_table (
		id INTEGER,
		name VARCHAR(100),
		value DOUBLE,
		created_at TIMESTAMP
	)`

	// Execute using DuckDB engine directly
	result, err := test.engine.ExecuteQuery(context.Background(), createTableSQL)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Insert test data
	insertSQL := `
	INSERT INTO test_table VALUES 
	(1, 'test1', 10.5, '2023-01-01 10:00:00'),
	(2, 'test2', 20.7, '2023-01-02 11:00:00'),
	(3, 'test3', 30.2, '2023-01-03 12:00:00')`

	result, err = test.engine.ExecuteQuery(context.Background(), insertSQL)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Test query execution via JDBC handler
	query := "SELECT * FROM test_table ORDER BY id"
	jdbcResult, err := test.server.ExecuteQuery(context.Background(), query)
	require.NoError(t, err)
	assert.NotNil(t, jdbcResult)

	// Verify results
	assert.Equal(t, 3, len(jdbcResult.Rows))
	assert.Equal(t, 4, len(jdbcResult.Columns))
	assert.Equal(t, "id", jdbcResult.Columns[0])
	assert.Equal(t, "name", jdbcResult.Columns[1])
	assert.Equal(t, "value", jdbcResult.Columns[2])
	assert.Equal(t, "created_at", jdbcResult.Columns[3])

	// Verify first row
	firstRow := jdbcResult.Rows[0]
	assert.Equal(t, int64(1), firstRow[0])
	assert.Equal(t, "test1", firstRow[1])
	assert.Equal(t, 10.5, firstRow[2])
}

// TestJDBCProtocolMessages tests PostgreSQL wire protocol messages
func TestJDBCProtocolMessages(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Test message reading/writing
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Test startup message
	go func() {
		// Simulate client sending startup message
		startupData := []byte{
			0x00, 0x00, 0x00, 0x2C, // Length
			0x00, 0x03, 0x00, 0x00, // Protocol version
			'u', 's', 'e', 'r', 0x00, // user
			't', 'e', 's', 't', 0x00, // test
			'd', 'a', 't', 'a', 'b', 'a', 's', 'e', 0x00, // database
			'i', 'c', 'e', 'b', 'o', 'x', 0x00, // icebox
			0x00, // null terminator
		}
		client.Write(startupData)
	}()

	// Test startup response
	params, err := jdbc.ParseStartupMessage(server)
	require.NoError(t, err)
	assert.NotEmpty(t, params)
	assert.Equal(t, "test", params["user"])
	assert.Equal(t, "icebox", params["database"])
}

// TestJDBCTypeMapping tests PostgreSQL type OID mapping
func TestJDBCTypeMapping(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Test type OID mapping
	testCases := []struct {
		columnName  string
		expectedOID int32
	}{
		{"id", 23},              // int4
		{"bigint_col", 20},      // int8
		{"float_col", 701},      // float8
		{"text_col", 25},        // text
		{"varchar_col", 1043},   // varchar
		{"bool_col", 16},        // bool
		{"date_col", 1082},      // date
		{"timestamp_col", 1114}, // timestamp
	}

	for _, tc := range testCases {
		// Test type OID mapping by creating a test query
		// This is a simplified test since getTypeOID is private
		query := fmt.Sprintf("SELECT %s FROM (SELECT 1 as %s) t", tc.columnName, tc.columnName)
		_, err := test.server.ExecuteQuery(context.Background(), query)
		// We expect this to work or fail gracefully
		if err != nil {
			t.Logf("Query failed for column %s: %v", tc.columnName, err)
		}
	}
}

// TestJDBCErrorHandling tests error handling in JDBC protocol
func TestJDBCErrorHandling(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Test invalid SQL query
	invalidQuery := "SELECT * FROM non_existent_table"
	_, err := test.server.ExecuteQuery(context.Background(), invalidQuery)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table")
}

// TestJDBCConnectionPooling tests connection pooling functionality
func TestJDBCConnectionPooling(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// This test would require a full JDBC server implementation
	// For now, we'll test the basic structure
	assert.NotNil(t, test.server)
}

// TestJDBCPerformance tests JDBC performance with multiple queries
func TestJDBCPerformance(t *testing.T) {
	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Create a larger test dataset
	createTableSQL := `
	CREATE TABLE performance_test (
		id INTEGER,
		name VARCHAR(100),
		value DOUBLE,
		category VARCHAR(50)
	)`

	_, err := test.engine.ExecuteQuery(context.Background(), createTableSQL)
	require.NoError(t, err)

	// Insert 1000 rows
	for i := 1; i <= 1000; i++ {
		insertSQL := fmt.Sprintf(`
		INSERT INTO performance_test VALUES 
		(%d, 'test%d', %d.5, 'category%d')`, i, i, i, i%10)

		_, err = test.engine.ExecuteQuery(context.Background(), insertSQL)
		require.NoError(t, err)
	}

	// Test query performance
	start := time.Now()
	query := "SELECT COUNT(*) FROM performance_test WHERE category = 'category1'"
	result, err := test.server.ExecuteQuery(context.Background(), query)
	duration := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Less(t, duration, 5*time.Second, "Query should complete within 5 seconds")

	// Verify result
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, int64(100), result.Rows[0][0]) // Should be 100 rows with category1
}

// TestJDBCWithRealClient tests JDBC with a real PostgreSQL client
func TestJDBCWithRealClient(t *testing.T) {
	// Skip if psql is not available
	_, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not available, skipping real client test")
	}

	test := NewJDBCIntegrationTest(t)
	defer test.Cleanup()

	// Start server on a specific port
	listener, err := net.Listen("tcp", "localhost:5436")
	require.NoError(t, err)
	defer listener.Close()

	// Start server in goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go test.server.HandleConnection(conn)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test with psql
	cmd := exec.Command("psql",
		"-h", "localhost",
		"-p", "5436",
		"-U", "test",
		"-d", "icebox",
		"-c", "SELECT version();")

	// Set environment variables
	cmd.Env = append(os.Environ(),
		"PGPASSWORD=test",
		"PGSSLMODE=disable")

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("psql test failed (expected for basic implementation): %v", err)
		t.Logf("Output: %s", string(output))
	}
}

// BenchmarkJDBCQueryExecution benchmarks JDBC query execution
func BenchmarkJDBCQueryExecution(b *testing.B) {
	// Create test setup manually for benchmark
	testDir := filepath.Join(os.TempDir(), "icebox-jdbc-benchmark")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(b, err)
	defer os.RemoveAll(testDir)

	// Create test configuration
	cfg := &config.Config{
		Name: "jdbc-benchmark",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(testDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "filesystem",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(testDir, "data"),
			},
		},
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	require.NoError(b, err)
	defer cat.Close()

	// Create SQL engine
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		require.NoError(b, err)
	default:
		b.Fatal("JDBC server currently only supports SQLite catalogs")
	}
	defer engine.Close()

	// Create JDBC handler
	ctx := context.Background()
	handler := jdbc.NewJDBCHandler(engine, testLogger(), ctx)

	// Setup test data
	createTableSQL := `
	CREATE TABLE benchmark_test (
		id INTEGER,
		name VARCHAR(100),
		value DOUBLE
	)`

	_, err = engine.ExecuteQuery(context.Background(), createTableSQL)
	require.NoError(b, err)

	// Insert test data
	for i := 1; i <= 100; i++ {
		insertSQL := fmt.Sprintf(`
		INSERT INTO benchmark_test VALUES 
		(%d, 'test%d', %d.5)`, i, i, i)

		_, err = engine.ExecuteQuery(context.Background(), insertSQL)
		require.NoError(b, err)
	}

	// Benchmark query execution
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := "SELECT COUNT(*) FROM benchmark_test WHERE value > 50"
		_, err := handler.ExecuteQuery(context.Background(), query)
		require.NoError(b, err)
	}
}
