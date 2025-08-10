package sdk

import (
	"context"
	"database/sql"
	"fmt"
	stdio "io"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/TFMV/icebox/deprecated/fs/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	_ "github.com/mattn/go-sqlite3"
)

// TestBox represents an ephemeral Icebox instance for testing
type TestBox struct {
	t        *testing.T
	config   *config.Config
	catalog  *sqlite.Catalog
	engine   *duckdb.Engine
	memoryFS *memory.MemoryFileSystem
	tempDir  string
	cleanup  []func()
}

// TestBoxOption configures a TestBox instance
type TestBoxOption func(*TestBoxConfig)

// TestBoxConfig holds configuration for test instances
type TestBoxConfig struct {
	Name         string
	TempDir      string
	UseMemoryFS  bool
	MemoryLimit  string
	QueryTimeout string
	Properties   map[string]string
}

// NewTestBox creates a new ephemeral Icebox instance for testing
// This provides the "sdk.NewTestBox(t)" functionality mentioned in the design
func NewTestBox(t *testing.T, opts ...TestBoxOption) *TestBox {
	t.Helper()

	// Apply default configuration
	testConfig := &TestBoxConfig{
		Name:         fmt.Sprintf("test-catalog-%d", os.Getpid()),
		UseMemoryFS:  true,
		MemoryLimit:  "1GB",
		QueryTimeout: "30s",
		Properties:   make(map[string]string),
	}

	// Apply options
	for _, opt := range opts {
		opt(testConfig)
	}

	// Create temporary directory if not using memory FS
	var tempDir string
	var err error
	if !testConfig.UseMemoryFS {
		if testConfig.TempDir != "" {
			tempDir = testConfig.TempDir
			err = os.MkdirAll(tempDir, 0755)
		} else {
			tempDir, err = os.MkdirTemp("", "icebox-test-*")
		}
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
	}

	testBox := &TestBox{
		t:       t,
		tempDir: tempDir,
		cleanup: make([]func(), 0),
	}

	// Create Icebox configuration
	cfg, err := testBox.createConfig(testConfig)
	if err != nil {
		t.Fatalf("Failed to create test configuration: %v", err)
	}
	testBox.config = cfg

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		testBox.cleanupAll()
		t.Fatalf("Failed to create test catalog: %v", err)
	}
	testBox.catalog = catalog
	testBox.cleanup = append(testBox.cleanup, func() { catalog.Close() })

	// Override with memory filesystem if enabled
	if testConfig.UseMemoryFS && testBox.memoryFS != nil {
		catalog.Close() // Close the standard catalog first

		// Create in-memory database
		db, err := sql.Open("sqlite3", ":memory:?_foreign_keys=on")
		if err != nil {
			testBox.cleanupAll()
			t.Fatalf("Failed to create in-memory database: %v", err)
		}

		// Create adapter for memory filesystem
		memoryAdapter := &memoryFileSystemAdapter{memFS: testBox.memoryFS}

		// Create memory IO for iceberg-go
		memoryIOAdapter := &memoryIO{memFS: testBox.memoryFS}

		// Create catalog with memory filesystem
		catalog, err = sqlite.NewCatalogWithIO(cfg.Name, ":memory:", db, memoryAdapter, memoryIOAdapter, "/test")
		if err != nil {
			testBox.cleanupAll()
			t.Fatalf("Failed to create memory catalog: %v", err)
		}
		testBox.catalog = catalog
		testBox.cleanup = append(testBox.cleanup, func() { catalog.Close() })
	}

	// Create SQL engine
	engineConfig := duckdb.DefaultEngineConfig()
	if testConfig.MemoryLimit != "" {
		// Parse memory limit if needed
		engineConfig.MaxMemoryMB = 1024 // Default to 1GB
	}

	engine, err := duckdb.NewEngineWithConfig(catalog, engineConfig)
	if err != nil {
		testBox.cleanupAll()
		t.Fatalf("Failed to create test engine: %v", err)
	}
	testBox.engine = engine
	testBox.cleanup = append(testBox.cleanup, func() { engine.Close() })

	// Set up automatic cleanup
	t.Cleanup(testBox.cleanupAll)

	return testBox
}

// createConfig creates an Icebox configuration for testing
func (tb *TestBox) createConfig(testConfig *TestBoxConfig) (*config.Config, error) {
	var storageConfig config.StorageConfig

	if testConfig.UseMemoryFS {
		// Use memory filesystem - set up as filesystem storage with memory backing
		tb.memoryFS = memory.NewMemoryFileSystem()

		// For memory filesystem, we still use "fs" type but with a memory-backed implementation
		// The catalog will need to be configured to use the memory filesystem
		storageConfig = config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: "/test", // Virtual root path for memory filesystem
			},
		}
	} else {
		// Use local filesystem
		dataDir := filepath.Join(tb.tempDir, "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}

		storageConfig = config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: dataDir,
			},
		}
	}

	var catalogPath string
	if testConfig.UseMemoryFS {
		catalogPath = ":memory:"
	} else {
		catalogPath = filepath.Join(tb.tempDir, "catalog.db")
	}

	cfg := &config.Config{
		Name: testConfig.Name,
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: catalogPath,
			},
		},
		Storage: storageConfig,
	}

	return cfg, nil
}

// GetConfig returns the test configuration
func (tb *TestBox) GetConfig() *config.Config {
	return tb.config
}

// GetCatalog returns the test catalog
func (tb *TestBox) GetCatalog() *sqlite.Catalog {
	return tb.catalog
}

// GetEngine returns the test SQL engine
func (tb *TestBox) GetEngine() *duckdb.Engine {
	return tb.engine
}

// GetMemoryFS returns the memory filesystem if enabled
func (tb *TestBox) GetMemoryFS() *memory.MemoryFileSystem {
	return tb.memoryFS
}

// CreateNamespace creates a test namespace
func (tb *TestBox) CreateNamespace(name string, properties ...map[string]string) table.Identifier {
	tb.t.Helper()

	namespace := table.Identifier{name}
	props := iceberg.Properties{}

	// Merge properties
	for _, p := range properties {
		for k, v := range p {
			props[k] = v
		}
	}

	err := tb.catalog.CreateNamespace(context.Background(), namespace, props)
	if err != nil {
		tb.t.Fatalf("Failed to create test namespace %s: %v", name, err)
	}

	return namespace
}

// CreateTable creates a test table with a simple schema
func (tb *TestBox) CreateTable(namespace, tableName string, schema ...*iceberg.Schema) *table.Table {
	tb.t.Helper()

	tableIdent := table.Identifier{namespace, tableName}

	// Use provided schema or create a default one
	var tableSchema *iceberg.Schema
	if len(schema) > 0 && schema[0] != nil {
		tableSchema = schema[0]
	} else {
		// Default test schema
		tableSchema = iceberg.NewSchema(1,
			iceberg.NestedField{
				ID:       1,
				Name:     "id",
				Type:     iceberg.PrimitiveTypes.Int64,
				Required: true,
			},
			iceberg.NestedField{
				ID:       2,
				Name:     "name",
				Type:     iceberg.PrimitiveTypes.String,
				Required: false,
			},
			iceberg.NestedField{
				ID:       3,
				Name:     "timestamp",
				Type:     iceberg.PrimitiveTypes.Timestamp,
				Required: false,
			},
		)
	}

	icebergTable, err := tb.catalog.CreateTable(context.Background(), tableIdent, tableSchema)
	if err != nil {
		tb.t.Fatalf("Failed to create test table %s.%s: %v", namespace, tableName, err)
	}

	return icebergTable
}

// ExecuteSQL executes a SQL query and returns results
func (tb *TestBox) ExecuteSQL(query string) (*duckdb.QueryResult, error) {
	return tb.engine.ExecuteQuery(context.Background(), query)
}

// MustExecuteSQL executes a SQL query and fails the test on error
func (tb *TestBox) MustExecuteSQL(query string) *duckdb.QueryResult {
	tb.t.Helper()

	result, err := tb.ExecuteSQL(query)
	if err != nil {
		tb.t.Fatalf("Failed to execute SQL query '%s': %v", query, err)
	}

	return result
}

// RegisterTable registers a table with the SQL engine for querying
func (tb *TestBox) RegisterTable(icebergTable *table.Table) {
	tb.t.Helper()

	err := tb.engine.RegisterTable(context.Background(), icebergTable.Identifier(), icebergTable)
	if err != nil {
		tb.t.Fatalf("Failed to register table %v: %v", icebergTable.Identifier(), err)
	}
}

// cleanupAll cleans up all resources
func (tb *TestBox) cleanupAll() {
	// Run cleanup functions in reverse order
	for i := len(tb.cleanup) - 1; i >= 0; i-- {
		tb.cleanup[i]()
	}

	// Clean up temporary directory
	if tb.tempDir != "" {
		os.RemoveAll(tb.tempDir)
	}

	// Clear memory filesystem
	if tb.memoryFS != nil {
		tb.memoryFS.Clear()
	}
}

// Option functions for configuring TestBox

// WithName sets the catalog name for the test instance
func WithName(name string) TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		testConfig.Name = name
	}
}

// WithTempDir sets a custom temporary directory (only used with filesystem storage)
func WithTempDir(dir string) TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		testConfig.TempDir = dir
	}
}

// WithFileSystem forces the use of filesystem storage instead of memory
func WithFileSystem() TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		testConfig.UseMemoryFS = false
	}
}

// WithMemoryLimit sets the DuckDB memory limit
func WithMemoryLimit(limit string) TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		testConfig.MemoryLimit = limit
	}
}

// WithProperty sets a configuration property
func WithProperty(key, value string) TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		testConfig.Properties[key] = value
	}
}

// WithDefaults provides default configuration optimized for CI environments
func WithDefaults(defaults DefaultConfig) TestBoxOption {
	return func(testConfig *TestBoxConfig) {
		switch defaults {
		case CIDefaults:
			testConfig.UseMemoryFS = true
			testConfig.MemoryLimit = "512MB"
			testConfig.QueryTimeout = "10s"
		case LocalDefaults:
			testConfig.UseMemoryFS = false
			testConfig.MemoryLimit = "2GB"
			testConfig.QueryTimeout = "60s"
		case FastDefaults:
			testConfig.UseMemoryFS = true
			testConfig.MemoryLimit = "256MB"
			testConfig.QueryTimeout = "5s"
		}
	}
}

// DefaultConfig represents pre-configured defaults
type DefaultConfig int

const (
	CIDefaults DefaultConfig = iota
	LocalDefaults
	FastDefaults
)

// memoryFileSystemAdapter adapts memory filesystem to match FileSystemInterface
type memoryFileSystemAdapter struct {
	memFS *memory.MemoryFileSystem
}

func (m *memoryFileSystemAdapter) Create(path string) (stdio.WriteCloser, error) {
	// For now, just use the memory filesystem's WriteFile method
	// This is a simplified adapter that creates a temporary writer
	return &memoryFileWriter{
		adapter: m,
		path:    path,
		buffer:  make([]byte, 0),
	}, nil
}

// memoryFileWriter implements io.WriteCloser for the memory filesystem
type memoryFileWriter struct {
	adapter *memoryFileSystemAdapter
	path    string
	buffer  []byte
}

func (w *memoryFileWriter) Write(p []byte) (n int, err error) {
	w.buffer = append(w.buffer, p...)
	return len(p), nil
}

func (w *memoryFileWriter) Close() error {
	return w.adapter.memFS.WriteFile(w.path, w.buffer)
}

// memoryIO implements iceberg-go io.IO interface for memory filesystem
type memoryIO struct {
	memFS *memory.MemoryFileSystem
}

func (m *memoryIO) Open(path string) (icebergio.File, error) {
	return m.memFS.Open(path)
}

func (m *memoryIO) Create(path string) (icebergio.File, error) {
	return m.memFS.Create(path)
}

func (m *memoryIO) Remove(path string) error {
	return m.memFS.Remove(path)
}
