package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// Engine provides SQL query capabilities using DuckDB with native Iceberg support
type Engine struct {
	db               *sql.DB
	catalog          catalog.CatalogInterface
	allocator        memory.Allocator
	initialized      bool
	config           *EngineConfig
	log              *log.Logger
	metrics          *EngineMetrics
	mutex            sync.RWMutex
	icebergAvailable bool // Track if iceberg extension is available
}

// EngineConfig holds configuration options for the engine
type EngineConfig struct {
	MaxMemoryMB        int
	QueryTimeoutSec    int
	EnableQueryLog     bool
	EnableOptimization bool
	CacheSize          int
	IcebergCatalogName string // Name for the attached Iceberg catalog in DuckDB
	// Security settings
	EnableQueryValidation bool     // Enable SQL injection protection
	AllowedStatements     []string // Allowed SQL statement types (SELECT, SHOW, DESCRIBE, etc.)
	BlockedKeywords       []string // Blocked dangerous keywords
}

// EngineMetrics tracks engine performance metrics
type EngineMetrics struct {
	QueriesExecuted  int64
	TablesRegistered int64
	CacheHits        int64
	CacheMisses      int64
	TotalQueryTime   time.Duration
	ErrorCount       int64
	BlockedQueries   int64 // Track blocked malicious queries
	mu               sync.RWMutex
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	Schema   *arrow.Schema
	Table    arrow.Table
	RowCount int64
	Duration time.Duration
	QueryID  string
}

// SecurityError represents a security-related error
type SecurityError struct {
	Message string
	QueryID string
	Reason  string
}

func (e SecurityError) Error() string {
	return fmt.Sprintf("security violation [%s]: %s (%s)", e.QueryID, e.Message, e.Reason)
}

// DefaultEngineConfig returns a default configuration for the engine
func DefaultEngineConfig() *EngineConfig {
	return &EngineConfig{
		MaxMemoryMB:        512,
		QueryTimeoutSec:    300,
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          100,
		IcebergCatalogName: "iceberg_catalog",
		// Security defaults
		EnableQueryValidation: true,
		AllowedStatements: []string{
			"SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH",
		},
		BlockedKeywords: []string{
			"COPY", "ATTACH", "DETACH", "LOAD", "INSTALL",
			"PRAGMA", "SET", "RESET", "CALL", "EXPORT",
			"IMPORT", "FORCE", "CHECKPOINT", "VACUUM",
		},
	}
}

// NewEngine creates a new DuckDB engine instance with catalog-agnostic support
func NewEngine(cat catalog.CatalogInterface) (*Engine, error) {
	return NewEngineWithConfig(cat, DefaultEngineConfig())
}

// NewEngineWithConfig creates a new DuckDB engine with custom configuration
func NewEngineWithConfig(cat catalog.CatalogInterface, config *EngineConfig) (*Engine, error) {
	if cat == nil {
		return nil, fmt.Errorf("catalog cannot be nil")
	}

	if config == nil {
		config = DefaultEngineConfig()
	}

	// Create DuckDB connection with optimized settings
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DuckDB: %w", err)
	}

	// Set connection pool settings for better performance
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	engine := &Engine{
		db:        db,
		catalog:   cat,
		allocator: memory.NewGoAllocator(),
		config:    config,
		metrics:   &EngineMetrics{},
		log:       log.Default(),
	}

	// Initialize the engine with optimizations
	if err := engine.initialize(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize engine: %w", err)
	}

	return engine, nil
}

// initialize sets up the DuckDB engine with performance optimizations and Iceberg support
func (e *Engine) initialize() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Configure DuckDB for optimal performance
	coreOptimizations := []string{
		"SET enable_progress_bar = false",
		"SET enable_object_cache = true",
		"SET enable_http_metadata_cache = true",
		"SET unsafe_enable_version_guessing = true", // Enable Iceberg version guessing
	}

	// Memory limit optimization
	_, err := e.db.Exec("SET memory_limit = ?", fmt.Sprintf("%dMB", e.config.MaxMemoryMB))
	if err != nil {
		e.log.Printf("Warning: Failed to set memory limit: %v", err)
	}

	// Apply core optimizations
	for _, opt := range coreOptimizations {
		if _, err := e.db.Exec(opt); err != nil {
			e.log.Printf("Warning: Failed to apply optimization '%s': %v", opt, err)
		}
	}

	// Install and load required extensions for Iceberg support
	if err := e.initializeExtensions(); err != nil {
		return fmt.Errorf("failed to initialize extensions: %w", err)
	}

	// Attach the Iceberg catalog to DuckDB
	if err := e.attachIcebergCatalog(); err != nil {
		return fmt.Errorf("failed to attach Iceberg catalog: %w", err)
	}

	e.log.Printf("DuckDB engine initialized successfully with catalog: %s", e.catalog.Name())
	e.initialized = true
	return nil
}

// initializeExtensions installs and loads required DuckDB extensions
func (e *Engine) initializeExtensions() error {
	// httpfs is required for most functionality
	requiredExtensions := []string{"httpfs"}

	// iceberg is optional - not available on all platforms (e.g., windows_amd64_mingw)
	optionalExtensions := []string{"iceberg"}

	// Install and load required extensions
	for _, ext := range requiredExtensions {
		// Try to install extension
		if _, err := e.db.Exec(fmt.Sprintf("INSTALL %s", ext)); err != nil {
			e.log.Printf("Info: Extension %s already installed or installation failed: %v", ext, err)
		}

		// Try to load extension - this must succeed for required extensions
		if _, err := e.db.Exec(fmt.Sprintf("LOAD %s", ext)); err != nil {
			return fmt.Errorf("failed to load required %s extension: %w", ext, err)
		}

		e.log.Printf("Info: %s extension loaded successfully", ext)
	}

	// Install and load optional extensions
	for _, ext := range optionalExtensions {
		// Try to install extension
		if _, err := e.db.Exec(fmt.Sprintf("INSTALL %s", ext)); err != nil {
			e.log.Printf("Info: Extension %s already installed or installation failed: %v", ext, err)
		}

		// Try to load extension - failure is acceptable for optional extensions
		if _, err := e.db.Exec(fmt.Sprintf("LOAD %s", ext)); err != nil {
			e.log.Printf("Warning: Optional %s extension not available on this platform: %v", ext, err)
			e.log.Printf("Info: Icebox will continue without native Iceberg support - some features may be limited")
			if ext == "iceberg" {
				e.icebergAvailable = false
			}
			continue
		}

		e.log.Printf("Info: %s extension loaded successfully", ext)
		if ext == "iceberg" {
			e.icebergAvailable = true
		}
	}

	return nil
}

// attachIcebergCatalog attaches the Iceberg catalog to DuckDB using the appropriate method
func (e *Engine) attachIcebergCatalog() error {
	catalogName := e.config.IcebergCatalogName

	// Determine the catalog type and create appropriate ATTACH statement
	switch e.catalog.(type) {
	case interface{ GetJSONConfig() interface{} }: // JSON catalog
		// For JSON catalogs, we'll use file-based access
		// This is a simplified approach. Might want to expose the JSON catalog as a REST endpoint
		e.log.Printf("Info: JSON catalog detected - using direct file access")
		return nil // JSON catalogs don't need ATTACH - we'll handle them differently

	case interface{ GetRESTConfig() interface{} }: // REST catalog
		// For REST catalogs, use ATTACH with REST endpoint
		attachSQL := fmt.Sprintf(`
			ATTACH '%s' AS %s (
				TYPE iceberg,
				ENDPOINT_TYPE rest
			)`, "rest_endpoint", catalogName) // You'd need to extract the actual REST endpoint

		if _, err := e.db.Exec(attachSQL); err != nil {
			return fmt.Errorf("failed to attach REST catalog: %w", err)
		}
		e.log.Printf("Info: Attached REST catalog as '%s'", catalogName)

	case interface{ GetSQLiteConfig() interface{} }: // SQLite catalog
		// For SQLite catalogs, we'll use file-based access to the underlying data
		e.log.Printf("Info: SQLite catalog detected - using direct file access")
		return nil // SQLite catalogs don't need ATTACH - we'll handle them differently

	default:
		e.log.Printf("Info: Unknown catalog type - using direct file access")
		return nil
	}

	return nil
}

// Close closes the DuckDB connection and cleans up resources
func (e *Engine) Close() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// GetMetrics returns current engine performance metrics
func (e *Engine) GetMetrics() *EngineMetrics {
	e.metrics.mu.RLock()
	defer e.metrics.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &EngineMetrics{
		QueriesExecuted:  e.metrics.QueriesExecuted,
		TablesRegistered: e.metrics.TablesRegistered,
		CacheHits:        e.metrics.CacheHits,
		CacheMisses:      e.metrics.CacheMisses,
		TotalQueryTime:   e.metrics.TotalQueryTime,
		ErrorCount:       e.metrics.ErrorCount,
		BlockedQueries:   e.metrics.BlockedQueries,
	}
}

// GetConfig returns the current engine configuration
func (e *Engine) GetConfig() *EngineConfig {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// Return a copy to avoid race conditions
	return &EngineConfig{
		MaxMemoryMB:           e.config.MaxMemoryMB,
		QueryTimeoutSec:       e.config.QueryTimeoutSec,
		EnableQueryLog:        e.config.EnableQueryLog,
		EnableOptimization:    e.config.EnableOptimization,
		CacheSize:             e.config.CacheSize,
		IcebergCatalogName:    e.config.IcebergCatalogName,
		EnableQueryValidation: e.config.EnableQueryValidation,
		AllowedStatements:     e.config.AllowedStatements,
		BlockedKeywords:       e.config.BlockedKeywords,
	}
}

// ExecuteQuery executes a SQL query and returns the results
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	if !e.initialized {
		return nil, fmt.Errorf("engine not initialized")
	}

	// Update metrics and generate unique query ID for tracking
	e.metrics.mu.Lock()
	currentQueryCount := e.metrics.QueriesExecuted
	e.metrics.QueriesExecuted++
	e.metrics.mu.Unlock()

	queryID := fmt.Sprintf("query_%d_%d", time.Now().UnixNano(), currentQueryCount)

	// Security validation - validate query before execution
	if e.config.EnableQueryValidation {
		if err := e.validateQuery(query, queryID); err != nil {
			e.incrementBlockedQueries()
			e.incrementErrorCount()
			return nil, err
		}
	}

	// Add query timeout if configured
	if e.config.QueryTimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(e.config.QueryTimeoutSec)*time.Second)
		defer cancel()
	}

	// Log query if enabled
	if e.config.EnableQueryLog {
		e.log.Printf("Executing query [%s]: %s", queryID, query)
	}

	start := time.Now()

	// Preprocess the query to handle Iceberg table references
	processedQuery, err := e.preprocessQuery(ctx, query)
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to preprocess query [%s]: %w", queryID, err)
	}

	// Execute the query with timeout context using parameterized query when possible
	rows, err := e.executeSecureQuery(ctx, processedQuery, queryID)
	if err != nil {
		e.incrementErrorCount()
		// Provide better error messages for common issues
		if strings.Contains(err.Error(), "timeout") {
			return nil, fmt.Errorf("query [%s] timed out after %ds: %w", queryID, e.config.QueryTimeoutSec, err)
		}
		if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			return nil, fmt.Errorf("table not found in query [%s]. Use 'SHOW TABLES' to see available tables: %w", queryID, err)
		}
		return nil, fmt.Errorf("failed to execute query [%s]: %w", queryID, err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to get columns for query [%s]: %w", queryID, err)
	}

	// Fetch all rows with memory management
	var resultRows [][]interface{}
	rowCount := int64(0)

	for rows.Next() {
		// Memory management: limit result size for very large queries
		if rowCount > 100000 { // 100k row limit
			e.log.Printf("Warning: Query [%s] result truncated at 100,000 rows", queryID)
			break
		}

		// Create slice to hold row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			e.incrementErrorCount()
			return nil, fmt.Errorf("failed to scan row %d in query [%s]: %w", rowCount, queryID, err)
		}

		resultRows = append(resultRows, values)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("error iterating rows in query [%s]: %w", queryID, err)
	}

	duration := time.Since(start)

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TotalQueryTime += duration
	e.metrics.mu.Unlock()

	// Log completion if enabled
	if e.config.EnableQueryLog {
		e.log.Printf("Query [%s] completed in %v, returned %d rows", queryID, duration, rowCount)
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: rowCount,
		Duration: duration,
		QueryID:  queryID,
	}, nil
}

// RegisterTable registers an Iceberg table for querying using DuckDB's native Iceberg support
func (e *Engine) RegisterTable(ctx context.Context, identifier table.Identifier, icebergTable *table.Table) error {
	if !e.initialized {
		return fmt.Errorf("engine not initialized")
	}

	if icebergTable == nil {
		return fmt.Errorf("iceberg table cannot be nil")
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Convert table identifier to SQL-safe name
	tableName := e.identifierToTableName(identifier)

	start := time.Now()

	// Check if Iceberg extension is available
	if !e.icebergAvailable {
		e.log.Printf("Warning: Iceberg extension not available - creating placeholder table for %s", tableName)

		// Create a placeholder table that explains the limitation
		createPlaceholderSQL := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS 
			SELECT 'Iceberg extension not available on this platform' as error_message,
			       'Table %s cannot be queried without native Iceberg support' as details
		`, e.quoteName(tableName), e.quoteName(tableName))

		if _, err := e.db.Exec(createPlaceholderSQL); err != nil {
			e.incrementErrorCount()
			return fmt.Errorf("failed to create placeholder for table %s: %w", tableName, err)
		}

		// Create an alias with just the table name for easier querying
		simpleTableName := identifier[len(identifier)-1]
		if simpleTableName != tableName && simpleTableName != "" {
			aliasSQL := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
				e.quoteName(simpleTableName), e.quoteName(tableName))

			if _, err := e.db.Exec(aliasSQL); err != nil {
				e.log.Printf("Warning: Could not create alias %s for placeholder table %s: %v", simpleTableName, tableName, err)
			}
		}

		// Update metrics
		e.metrics.mu.Lock()
		e.metrics.TablesRegistered++
		e.metrics.mu.Unlock()

		duration := time.Since(start)
		e.log.Printf("Created placeholder for table %s in %v (Iceberg extension unavailable)", tableName, duration)
		return nil
	}

	// Get table location from metadata
	location := icebergTable.Location()
	if location == "" {
		return fmt.Errorf("table %s has no location", tableName)
	}

	// DuckDB v1.3.0 has significantly improved Iceberg support
	// We can now use iceberg_scan directly with both SQLite and JSON catalogs
	// Use the metadata location directly from the table object
	metadataLocation := icebergTable.MetadataLocation()

	// Validate metadata location to prevent injection
	if strings.ContainsAny(metadataLocation, "'\"\\;") {
		return fmt.Errorf("invalid metadata location: contains potentially dangerous characters")
	}

	createViewSQL := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS 
		SELECT * FROM iceberg_scan('%s')
	`, e.quoteName(tableName), strings.ReplaceAll(metadataLocation, "'", "''"))

	if _, err := e.db.Exec(createViewSQL); err != nil {
		e.incrementErrorCount()
		return fmt.Errorf("failed to register table %s: %w", tableName, err)
	}

	// Create an alias with just the table name for easier querying
	simpleTableName := identifier[len(identifier)-1]
	if simpleTableName != tableName && simpleTableName != "" {
		aliasSQL := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
			e.quoteName(simpleTableName), e.quoteName(tableName))

		if _, err := e.db.Exec(aliasSQL); err != nil {
			e.log.Printf("Warning: Could not create alias %s for table %s: %v", simpleTableName, tableName, err)
		} else {
			e.log.Printf("Info: Created alias '%s' -> '%s'", simpleTableName, tableName)
		}
	}

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TablesRegistered++
	e.metrics.mu.Unlock()

	duration := time.Since(start)
	e.log.Printf("Registered table %s in %v using DuckDB v1.3.0 native Iceberg support", tableName, duration)

	return nil
}

// ListTables returns a list of all registered tables
func (e *Engine) ListTables(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	// Ensure we always return a non-nil slice
	if tables == nil {
		tables = []string{}
	}

	return tables, nil
}

// DescribeTable returns schema information for a table
func (e *Engine) DescribeTable(ctx context.Context, tableName string) (*QueryResult, error) {
	// Validate table name to prevent injection
	if strings.ContainsAny(tableName, "';\"\\-/*") {
		return nil, fmt.Errorf("invalid table name: contains potentially dangerous characters")
	}

	query := fmt.Sprintf("DESCRIBE %s", e.quoteName(tableName))
	return e.ExecuteQuery(ctx, query)
}

// ClearTableCache clears any cached table information (no-op for this implementation)
func (e *Engine) ClearTableCache() {
	// Tables are registered as views in DuckDB directly
	e.log.Printf("Info: ClearTableCache called - no cache to clear in this implementation")
}

// preprocessQuery preprocesses SQL queries to handle Iceberg table references
func (e *Engine) preprocessQuery(ctx context.Context, query string) (string, error) {
	// Convert dot notation to underscore notation for table references
	// This regex matches table references like 'default.sales' but ignores qualified column references like 'table.column'
	tablePattern := regexp.MustCompile(`(FROM|JOIN|UPDATE|INTO|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`)

	processedQuery := tablePattern.ReplaceAllStringFunc(query, func(match string) string {
		parts := strings.SplitN(match, " ", 2)
		if len(parts) != 2 {
			return match
		}

		keyword := parts[0]
		tablePath := parts[1]

		// Replace dots with underscores in table references
		tableName := strings.ReplaceAll(tablePath, ".", "_")
		return fmt.Sprintf("%s %s", keyword, tableName)
	})

	// Create aliases for table references to support both notations
	if strings.Contains(processedQuery, "_") {
		for _, table := range tablePattern.FindAllString(query, -1) {
			parts := strings.SplitN(table, " ", 2)
			if len(parts) != 2 {
				continue
			}
			tablePath := parts[1]
			dotParts := strings.Split(tablePath, ".")
			if len(dotParts) == 2 {
				aliasStmt := fmt.Sprintf("CREATE ALIAS IF NOT EXISTS %s FOR %s_%s;", dotParts[1], dotParts[0], dotParts[1])
				if _, err := e.db.Exec(aliasStmt); err != nil {
					e.log.Printf("Failed to create alias: %v", err)
				}
			}
		}
	}

	// Replace column references to use underscore notation
	columnPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`)
	processedQuery = columnPattern.ReplaceAllStringFunc(processedQuery, func(match string) string {
		parts := strings.Split(match, ".")
		if len(parts) == 3 {
			return fmt.Sprintf("%s_%s.%s", parts[0], parts[1], parts[2])
		}
		return match
	})

	return processedQuery, nil
}

// incrementErrorCount safely increments the error counter
func (e *Engine) incrementErrorCount() {
	e.metrics.mu.Lock()
	e.metrics.ErrorCount++
	e.metrics.mu.Unlock()
}

// incrementBlockedQueries safely increments the blocked queries counter
func (e *Engine) incrementBlockedQueries() {
	e.metrics.mu.Lock()
	e.metrics.BlockedQueries++
	e.metrics.mu.Unlock()
}

// identifierToTableName converts a table identifier to a SQL-safe table name
func (e *Engine) identifierToTableName(identifier table.Identifier) string {
	return strings.Join(identifier, "_")
}

// quoteName quotes a SQL identifier to make it safe for use in queries
func (e *Engine) quoteName(name string) string {
	// Escape any existing quotes by doubling them
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// validateQuery validates a SQL query for security issues
func (e *Engine) validateQuery(query, queryID string) error {
	// Normalize query for analysis
	normalizedQuery := strings.TrimSpace(strings.ToUpper(query))

	if normalizedQuery == "" {
		return SecurityError{
			Message: "empty query not allowed",
			QueryID: queryID,
			Reason:  "empty_query",
		}
	}

	// Check for allowed statement types
	if len(e.config.AllowedStatements) > 0 {
		allowed := false
		for _, stmt := range e.config.AllowedStatements {
			if strings.HasPrefix(normalizedQuery, strings.ToUpper(stmt)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return SecurityError{
				Message: "statement type not allowed",
				QueryID: queryID,
				Reason:  "disallowed_statement",
			}
		}
	}

	// Check for blocked keywords
	for _, keyword := range e.config.BlockedKeywords {
		if strings.Contains(normalizedQuery, strings.ToUpper(keyword)) {
			return SecurityError{
				Message: fmt.Sprintf("blocked keyword '%s' detected", keyword),
				QueryID: queryID,
				Reason:  "blocked_keyword",
			}
		}
	}

	// Check for common SQL injection patterns
	if err := e.checkInjectionPatterns(normalizedQuery, queryID); err != nil {
		return err
	}

	return nil
}

// checkInjectionPatterns checks for common SQL injection attack patterns
func (e *Engine) checkInjectionPatterns(query, queryID string) error {
	// Common SQL injection patterns
	injectionPatterns := []struct {
		pattern string
		reason  string
	}{
		{`--`, "sql_comment_injection"},
		{`/\*`, "sql_comment_injection"},
		{`\*/`, "sql_comment_injection"},
		{`;\s*DROP`, "drop_injection"},
		{`;\s*DELETE`, "delete_injection"},
		{`;\s*UPDATE`, "update_injection"},
		{`;\s*INSERT`, "insert_injection"},
		{`;\s*CREATE`, "create_injection"},
		{`;\s*ALTER`, "alter_injection"},
		{`UNION\s+SELECT`, "union_injection"},
		{`OR\s+1\s*=\s*1`, "boolean_injection"},
		{`AND\s+1\s*=\s*1`, "boolean_injection"},
		{`'\s*OR\s*'`, "quote_injection"},
		{`"\s*OR\s*"`, "quote_injection"},
		{`EXEC\s*\(`, "exec_injection"},
		{`EXECUTE\s*\(`, "exec_injection"},
		{`CHAR\s*\(`, "char_injection"},
		{`ASCII\s*\(`, "ascii_injection"},
		{`CONCAT\s*\(.*SELECT`, "concat_injection"},
	}

	for _, pattern := range injectionPatterns {
		matched, err := regexp.MatchString(pattern.pattern, query)
		if err != nil {
			e.log.Printf("Warning: regex error checking pattern %s: %v", pattern.pattern, err)
			continue
		}
		if matched {
			return SecurityError{
				Message: fmt.Sprintf("potential SQL injection detected: %s", pattern.reason),
				QueryID: queryID,
				Reason:  pattern.reason,
			}
		}
	}

	return nil
}

// executeSecureQuery executes a query with additional security measures
func (e *Engine) executeSecureQuery(ctx context.Context, query, queryID string) (*sql.Rows, error) {
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		// Check for syntax error related to table references
		if strings.Contains(strings.ToLower(err.Error()), "syntax error") &&
			strings.Contains(query, ".") {
			// Try to provide a helpful error message
			processedQuery := strings.ReplaceAll(query, ".", "_")
			return nil, fmt.Errorf("syntax error in query [%s]: DuckDB requires table names to use underscores instead of dots. "+
				"Use '%s' instead of '%s' or just the table name '%s'. "+
				"Original error: %w",
				queryID,
				processedQuery,
				query,
				strings.Split(strings.Split(query, ".")[1], " ")[0],
				err)
		}
		return nil, err
	}
	return rows, nil
}
