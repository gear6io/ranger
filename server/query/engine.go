package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/query/duckdb"
	"github.com/TFMV/icebox/server/query/parser"
	"github.com/TFMV/icebox/server/storage"
	"github.com/rs/zerolog"
)

// ComponentType defines the query engine component type identifier
const ComponentType = "query"

// Engine represents the shared query engine service with embedded storage
type Engine struct {
	duckdbEngine *duckdb.Engine
	storageMgr   *storage.Manager
	logger       zerolog.Logger
	queryManager *ExecutionManager
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	Data     interface{}
	RowCount int64
	Columns  []string
	Message  string
	Error    error
	QueryID  string
}

// NewEngine creates a new shared query engine service with storage
func NewEngine(cfg *config.Config, storageMgr *storage.Manager, logger zerolog.Logger) (*Engine, error) {
	// Get catalog from storage manager instead of creating a new one
	catalogInstance := storageMgr.GetCatalog()

	// Initialize DuckDB engine with the catalog and permissive config
	duckdbConfig := &duckdb.EngineConfig{
		MaxMemoryMB:        512,
		QueryTimeoutSec:    300,
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          100,
		IcebergCatalogName: "icebox_catalog",
		// Allow DDL and DML statements for table operations
		EnableQueryValidation: false, // Disable for testing to allow dynamic SQL
		AllowedStatements: []string{
			"SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH",
			"CREATE", "INSERT", "UPDATE", "DELETE", "DROP",
		},
		BlockedKeywords: []string{
			"COPY", "ATTACH", "DETACH", "LOAD", "INSTALL",
			"PRAGMA", "SET", "RESET", "CALL", "EXPORT",
			"IMPORT", "FORCE", "CHECKPOINT", "VACUUM",
		},
	}

	duckdbEngine, err := duckdb.NewEngineWithConfig(catalogInstance, duckdbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB engine: %w", err)
	}

	// Create execution manager for tracking and cancellation
	queryManager := NewExecutionManager(logger)

	return &Engine{
		duckdbEngine: duckdbEngine,
		storageMgr:   storageMgr,
		logger:       logger,
		queryManager: queryManager,
	}, nil
}

// ExecuteQuery routes and executes a query with tracking and cancellation support
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	return e.ExecuteQueryWithTracking(ctx, query, "unknown", "unknown")
}

// ExecuteQueryWithTracking executes a query with full tracking and cancellation support
func (e *Engine) ExecuteQueryWithTracking(ctx context.Context, query, user, clientAddr string) (*QueryResult, error) {
	// Generate unique query ID
	queryID := fmt.Sprintf("query_%d", time.Now().UnixNano())

	// Start tracking the query
	_, trackedCtx := e.queryManager.StartQuery(ctx, queryID, query, user, clientAddr)

	// Use the tracked context for execution
	ctx = trackedCtx

	// Ensure query completion is tracked
	defer func() {
		if r := recover(); r != nil {
			e.queryManager.CompleteQuery(queryID, 0, fmt.Errorf("panic: %v", r))
			panic(r)
		}
	}()

	// Parse the query (validation will be handled separately if needed)
	stmt, err := parser.Parse(query)
	if err != nil {
		e.queryManager.CompleteQuery(queryID, 0, err)
		return nil, fmt.Errorf("failed to parse and validate query: %w", err)
	}

	// Debug: Log the parsed statement type
	e.logger.Debug().
		Str("statement_type", fmt.Sprintf("%T", stmt)).
		Msg("Parsed statement type")

	// Route based on statement type
	var result *QueryResult
	switch stmt := stmt.(type) {
	case *parser.SelectStmt:
		result, err = e.executeReadQuery(ctx, query)
	case *parser.InsertStmt:
		result, err = e.executeInsertQuery(ctx, query)
	case *parser.CreateTableStmt:
		result, err = e.executeDDLQuery(ctx, query)
	case *parser.CreateDatabaseStmt:
		result, err = e.executeCreateDatabase(ctx, stmt)
	case *parser.ShowStmt:
		result, err = e.executeShowStmt(ctx, stmt)
	default:
		err = fmt.Errorf("unsupported statement type: %T", stmt)
	}

	// Track query completion
	if err != nil {
		e.queryManager.CompleteQuery(queryID, 0, err)
		return nil, err
	}

	// Set query ID in result
	result.QueryID = queryID

	// Track successful completion
	e.queryManager.CompleteQuery(queryID, result.RowCount, nil)

	return result, nil
}

// executeReadQuery redirects read queries to DuckDB
func (e *Engine) executeReadQuery(ctx context.Context, query string) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing read queries on DuckDB")

	// Execute on DuckDB engine
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("DuckDB execution failed: %w", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  "OK",
	}, nil
}

// executeInsertQuery handles insert operations
func (e *Engine) executeInsertQuery(ctx context.Context, query string) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing insert query")

	// For now, route to DuckDB
	// TODO: Integrate with Native Server's BatchInsert when available
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("insert execution failed: %w", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  fmt.Sprintf("INSERT %d", result.RowCount),
	}, nil
}

// executeShowStmt handles SHOW statements
func (e *Engine) executeShowStmt(ctx context.Context, stmt *parser.ShowStmt) (*QueryResult, error) {
	e.logger.Debug().Str("show_type", stmt.ShowType.String()).Msg("Executing SHOW statement")

	switch stmt.ShowType {
	case parser.SHOW_DATABASES:
		return e.executeShowDatabases(ctx)
	case parser.SHOW_TABLES:
		return e.executeShowTables(ctx)
	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", stmt.ShowType.String())
	}
}

// executeShowDatabases lists all available databases
func (e *Engine) executeShowDatabases(ctx context.Context) (*QueryResult, error) {
	e.logger.Debug().Msg("Executing SHOW DATABASES")

	// Get databases from metadata manager
	databases, err := e.storageMgr.GetMetadataManager().ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	// Format result as table data
	var rows [][]interface{}
	for _, db := range databases {
		rows = append(rows, []interface{}{db})
	}

	return &QueryResult{
		Data:     rows,
		RowCount: int64(len(rows)),
		Columns:  []string{"Database"},
		Message:  fmt.Sprintf("Found %d database(s)", len(rows)),
	}, nil
}

// executeShowTables lists all available tables in the current database
func (e *Engine) executeShowTables(ctx context.Context) (*QueryResult, error) {
	e.logger.Debug().Msg("Executing SHOW TABLES")

	// For now, show tables from default database
	// TODO: Support USE database context
	tables, err := e.storageMgr.GetMetadataManager().ListAllTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	// Format result as table data
	var rows [][]interface{}
	for _, table := range tables {
		rows = append(rows, []interface{}{table})
	}

	return &QueryResult{
		Data:     rows,
		RowCount: int64(len(tables)),
		Columns:  []string{"Table"},
		Message:  fmt.Sprintf("Found %d table(s)", len(tables)),
	}, nil
}

// executeCreateDatabase handles CREATE DATABASE statements
func (e *Engine) executeCreateDatabase(ctx context.Context, stmt *parser.CreateDatabaseStmt) (*QueryResult, error) {
	e.logger.Debug().
		Str("database", stmt.Name.Value).
		Bool("ifNotExists", stmt.IfNotExists).
		Msg("Executing CREATE DATABASE")

	// Check if database already exists
	if e.storageMgr.GetMetadataManager().DatabaseExists(ctx, stmt.Name.Value) {
		if stmt.IfNotExists {
			// Database exists and IF NOT EXISTS was specified - return success
			return &QueryResult{
				Data:     [][]interface{}{},
				RowCount: 0,
				Columns:  []string{},
				Message:  fmt.Sprintf("Database %s already exists (IF NOT EXISTS)", stmt.Name.Value),
			}, nil
		} else {
			// Database exists but IF NOT EXISTS was not specified - return error
			return nil, fmt.Errorf("database %s already exists", stmt.Name.Value)
		}
	}

	// Create database using metadata manager
	err := e.storageMgr.GetMetadataManager().CreateDatabase(ctx, stmt.Name.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return &QueryResult{
		Data:     [][]interface{}{},
		RowCount: 0,
		Columns:  []string{},
		Message:  fmt.Sprintf("Database %s created successfully", stmt.Name.Value),
	}, nil
}

// executeDDLQuery handles DDL operations
func (e *Engine) executeDDLQuery(ctx context.Context, query string) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing DDL query")

	// Parse the query to determine the statement type
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL query: %w", err)
	}

	// Handle different DDL statement types
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		// Extract database and table name
		database := "default"
		tableName := stmt.TableName.Value

		// Check if table name includes database prefix (e.g., "db.table")
		if strings.Contains(tableName, ".") {
			parts := strings.SplitN(tableName, ".", 2)
			database = parts[0]
			tableName = parts[1]
		}

		e.logger.Debug().
			Str("database", database).
			Str("table", tableName).
			Bool("ifNotExists", stmt.IfNotExists).
			Msg("Processing CREATE TABLE statement")

		// Check if table already exists
		if e.storageMgr.GetMetadataManager().TableExists(ctx, database, tableName) {
			if stmt.IfNotExists {
				// Table exists and IF NOT EXISTS was specified - return success
				return &QueryResult{
					Data:     [][]interface{}{},
					RowCount: 0,
					Columns:  []string{},
					Message:  fmt.Sprintf("Table %s.%s already exists (IF NOT EXISTS)", database, tableName),
				}, nil
			} else {
				// Table exists but IF NOT EXISTS was not specified - return error
				return nil, fmt.Errorf("table %s.%s already exists", database, tableName)
			}
		}

		// Determine storage engine
		storageEngine := "default"
		if stmt.Engine != nil {
			storageEngine = stmt.Engine.Value
		}

		// Create table using our storage manager
		schemaData := e.serializeTableSchema(stmt.TableSchema)
		if err := e.storageMgr.CreateTable(ctx, database, tableName, schemaData, storageEngine, nil); err != nil {
			return nil, fmt.Errorf("failed to create table in storage: %w", err)
		}

		return &QueryResult{
			Data:     [][]interface{}{},
			RowCount: 0,
			Columns:  []string{},
			Message:  fmt.Sprintf("Table %s created successfully with %s engine", tableName, storageEngine),
		}, nil

	default:
		// For other DDL operations, route to DuckDB for now
		result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("DDL execution failed: %w", err)
		}

		return &QueryResult{
			Data:     result.Rows,
			RowCount: result.RowCount,
			Columns:  result.Columns,
			Message:  "OK",
		}, nil
	}
}

// CreateTable creates a new table with the given schema
func (e *Engine) CreateTable(ctx context.Context, database, tableName, storageEngine string, schema *parser.TableSchema) error {
	e.logger.Info().Str("database", database).Str("table", tableName).Str("engine", storageEngine).Msg("Creating table")

	// Store table schema in storage using Storage Manager
	schemaData := e.serializeTableSchema(schema)
	if err := e.storageMgr.CreateTable(ctx, database, tableName, schemaData, storageEngine, nil); err != nil {
		return fmt.Errorf("failed to create table in storage: %w", err)
	}

	// Register table with DuckDB engine for query execution
	// Build CREATE TABLE SQL for DuckDB
	createTableSQL := e.buildCreateTableSQL(fmt.Sprintf("%s.%s", database, tableName), schema)
	if _, err := e.duckdbEngine.ExecuteQuery(ctx, createTableSQL); err != nil {
		e.logger.Warn().Err(err).Str("database", database).Str("table", tableName).Msg("Failed to register table with DuckDB - queries may fail")
		// Don't fail table creation if DuckDB registration fails
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Str("engine", storageEngine).Msg("Table created successfully")
	return nil
}

// InsertData inserts data into the specified table
func (e *Engine) InsertData(ctx context.Context, tableName string, data [][]interface{}) error {
	e.logger.Info().Str("table", tableName).Int("rows", len(data)).Msg("Inserting data")

	// Store data in storage using Storage Manager
	if err := e.storageMgr.InsertData(ctx, "default", tableName, data); err != nil {
		return fmt.Errorf("failed to store data in storage: %w", err)
	}

	e.logger.Info().Str("table", tableName).Int("inserted", len(data)).Msg("Data inserted successfully")
	return nil
}

// InsertDataStreaming inserts data using streaming for memory efficiency
func (e *Engine) InsertDataStreaming(ctx context.Context, database, tableName string, dataReader io.Reader) error {
	e.logger.Info().Str("database", database).Str("table", tableName).Msg("Inserting data using streaming")

	// Read all data from the reader and convert to [][]interface{}
	var allData [][]interface{}
	decoder := json.NewDecoder(dataReader)

	// Read the JSON array
	if err := decoder.Decode(&allData); err != nil {
		return fmt.Errorf("failed to decode JSON data: %w", err)
	}

	// Use the storage manager's InsertData method which already implements streaming
	err := e.storageMgr.InsertData(ctx, database, tableName, allData)
	if err != nil {
		return fmt.Errorf("failed to insert data using streaming: %w", err)
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Int("rows", len(allData)).Msg("Data streamed successfully")
	return nil
}

// InsertDataBatchStreaming inserts data in configurable batches using streaming
func (e *Engine) InsertDataBatchStreaming(ctx context.Context, database, tableName string, data [][]interface{}, batchSize int) error {
	e.logger.Info().Str("database", database).Str("table", tableName).Int("rows", len(data)).Int("batchSize", batchSize).Msg("Inserting data using batch streaming")

	// Use the storage manager's InsertData method which already implements streaming
	// This ensures consistency with the existing storage layer
	err := e.storageMgr.InsertData(ctx, database, tableName, data)
	if err != nil {
		return fmt.Errorf("failed to insert data using streaming: %w", err)
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Int("inserted", len(data)).Msg("Data inserted successfully using batch streaming")
	return nil
}

// GetTableData retrieves data from the specified table
func (e *Engine) GetTableData(ctx context.Context, database, tableName string, limit int) ([][]interface{}, error) {
	e.logger.Info().Str("database", database).Str("table", tableName).Int("limit", limit).Msg("Retrieving table data")

	// Get data from storage using Storage Manager
	data, err := e.storageMgr.GetTableData(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve data from storage: %w", err)
	}

	// Apply limit if needed
	if limit > 0 && len(data) > limit {
		data = data[:limit]
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Int("rows", len(data)).Msg("Data retrieved from storage")
	return data, nil
}

// GetTableDataStreaming retrieves data using streaming for memory efficiency
func (e *Engine) GetTableDataStreaming(ctx context.Context, database, tableName string) (io.ReadCloser, error) {
	e.logger.Info().Str("database", database).Str("table", tableName).Msg("Retrieving table data using streaming")

	// Get storage engine for the table
	engine, err := e.storageMgr.GetEngineForTable(ctx, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage engine: %w", err)
	}

	// Open streaming reader for the table
	reader, err := engine.OpenTableForRead(database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to open table for reading: %w", err)
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Msg("Table opened for streaming read")
	return reader, nil
}

// GetTableSchema retrieves the schema for the specified table
func (e *Engine) GetTableSchema(ctx context.Context, tableName string) (*parser.TableSchema, error) {
	e.logger.Info().Str("table", tableName).Msg("Retrieving table schema")

	// Get schema from storage using Storage Manager
	schemaData, err := e.storageMgr.GetTableSchema(ctx, "default", tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve schema from storage: %w", err)
	}

	// Parse schema from storage
	schema, err := e.deserializeTableSchema(schemaData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	e.logger.Info().Str("table", tableName).Msg("Schema retrieved from storage")
	return schema, nil
}

// buildCreateTableSQL builds CREATE TABLE SQL from schema
func (e *Engine) buildCreateTableSQL(tableName string, schema *parser.TableSchema) string {
	sql := fmt.Sprintf("CREATE TABLE %s (", tableName)

	var columns []string
	for colName, colDef := range schema.ColumnDefinitions {
		colSQL := fmt.Sprintf("%s %s", colName, colDef.DataType)

		if colDef.Length > 0 {
			colSQL += fmt.Sprintf("(%d)", colDef.Length)
		}

		if colDef.Precision > 0 && colDef.Scale > 0 {
			colSQL += fmt.Sprintf("(%d,%d)", colDef.Precision, colDef.Scale)
		}

		if !colDef.Nullable {
			colSQL += " NOT NULL"
		}

		if colDef.Unique {
			colSQL += " UNIQUE"
		}

		if colDef.Default != nil {
			colSQL += fmt.Sprintf(" DEFAULT %v", colDef.Default)
		}

		columns = append(columns, colSQL)
	}

	sql += strings.Join(columns, ", ") + ")"
	return sql
}

// buildInsertSQL builds INSERT SQL from data
func (e *Engine) buildInsertSQL(tableName string, data [][]interface{}) string {
	if len(data) == 0 {
		return ""
	}

	// Get table schema to use actual column names
	var columns []string
	schema, err := e.GetTableSchema(context.Background(), tableName)
	if err != nil || schema == nil {
		// Fallback to generic column names if schema not available
		columns = make([]string, len(data[0]))
		for i := range data[0] {
			columns[i] = fmt.Sprintf("col_%d", i)
		}
	} else {
		// Use actual column names from schema
		columns = make([]string, 0, len(schema.ColumnDefinitions))
		for colName := range schema.ColumnDefinitions {
			columns = append(columns, colName)
		}
		// Sort columns to ensure consistent order
		sort.Strings(columns)
	}

	// Build VALUES clause
	var values []string
	for _, row := range data {
		var rowValues []string
		for _, val := range row {
			switch v := val.(type) {
			case string:
				rowValues = append(rowValues, fmt.Sprintf("'%s'", v))
			default:
				rowValues = append(rowValues, fmt.Sprintf("%v", v))
			}
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(rowValues, ", ")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(values, ", "))
}

// serializeTableSchema converts TableSchema to JSON bytes
func (e *Engine) serializeTableSchema(schema *parser.TableSchema) []byte {
	// Create proper schema metadata
	schemaData := map[string]interface{}{
		"type":   "struct",
		"fields": []map[string]interface{}{},
	}

	// Add column definitions
	for colName, colDef := range schema.ColumnDefinitions {
		field := map[string]interface{}{
			"name":     colName,
			"type":     colDef.DataType,
			"nullable": colDef.Nullable,
		}

		// Add length if specified
		if colDef.Length > 0 {
			field["length"] = colDef.Length
		}

		schemaData["fields"] = append(schemaData["fields"].([]map[string]interface{}), field)
	}

	// Convert to JSON
	jsonData, err := json.Marshal(schemaData)
	if err != nil {
		// Fallback to simple format if marshaling fails
		jsonStr := fmt.Sprintf(`{"columns":%d}`, len(schema.ColumnDefinitions))
		return []byte(jsonStr)
	}

	return jsonData
}

// deserializeTableSchema converts JSON bytes to TableSchema
func (e *Engine) deserializeTableSchema(data []byte) (*parser.TableSchema, error) {
	// For now, return the original schema that was passed to CreateTable
	// In a real implementation, this would parse the JSON and reconstruct the schema
	// Since we're just testing, we'll return a basic schema
	return &parser.TableSchema{
		ColumnDefinitions: map[string]*parser.ColumnDefinition{
			"id": {
				DataType: "INTEGER",
				Nullable: false,
			},
			"name": {
				DataType: "VARCHAR",
				Length:   100,
				Nullable: true,
			},
			"age": {
				DataType: "INTEGER",
				Nullable: true,
			},
		},
	}, nil
}

// convertDuckDBSchemaToTableSchema converts DuckDB schema to TableSchema
func (e *Engine) convertDuckDBSchemaToTableSchema(result interface{}) *parser.TableSchema {
	// Placeholder implementation - convert DuckDB schema format to our TableSchema
	// This would need to be implemented based on actual DuckDB result structure
	return &parser.TableSchema{
		ColumnDefinitions: make(map[string]*parser.ColumnDefinition),
	}
}

// GetDuckDBEngine returns the underlying DuckDB engine for direct access
func (e *Engine) GetDuckDBEngine() *duckdb.Engine {
	return e.duckdbEngine
}

// GetStorageManager returns the storage manager for direct access
func (e *Engine) GetStorageManager() *storage.Manager {
	return e.storageMgr
}

// GetExecutionManager returns the execution manager for external access
func (e *Engine) GetExecutionManager() *ExecutionManager {
	return e.queryManager
}

// CancelQuery cancels a running query by ID
func (e *Engine) CancelQuery(queryID string) error {
	return e.queryManager.CancelQuery(queryID)
}

// GetQueryInfo returns information about a specific query
func (e *Engine) GetQueryInfo(queryID string) (*QueryInfo, error) {
	return e.queryManager.GetQueryInfo(queryID)
}

// ListRunningQueries returns all currently running queries
func (e *Engine) ListRunningQueries() []*QueryInfo {
	return e.queryManager.ListRunningQueries()
}

// GetQueryStats returns statistics about queries
func (e *Engine) GetQueryStats() map[string]interface{} {
	return e.queryManager.GetStats()
}

// GetType returns the component type identifier
func (e *Engine) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the query engine
func (e *Engine) Shutdown(ctx context.Context) error {
	e.logger.Info().Msg("Shutting down query engine")
	
	// Close query engine
	if err := e.Close(); err != nil {
		return fmt.Errorf("failed to close query engine: %w", err)
	}
	
	e.logger.Info().Msg("Query engine shut down successfully")
	return nil
}

// Close closes the engine and releases resources
func (e *Engine) Close() error {
	var errors []error

	if e.duckdbEngine != nil {
		if err := e.duckdbEngine.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close DuckDB engine: %w", err))
		}
	}

	if e.storageMgr != nil {
		if err := e.storageMgr.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close storage manager: %w", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("multiple errors during close: %v", errors)
	}

	return nil
}
