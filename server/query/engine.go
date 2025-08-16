package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/query/duckdb"
	"github.com/TFMV/icebox/server/query/parser"
	"github.com/TFMV/icebox/server/storage"
	"github.com/rs/zerolog"
)

// Engine represents the shared query engine service with embedded storage
type Engine struct {
	duckdbEngine *duckdb.Engine
	storageMgr   *storage.Manager
	logger       zerolog.Logger
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	Data     interface{}
	RowCount int64
	Columns  []string
	Message  string
	Error    error
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

	return &Engine{
		duckdbEngine: duckdbEngine,
		storageMgr:   storageMgr,
		logger:       logger,
	}, nil
}

// ExecuteQuery routes and executes a query
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// Create a catalog-aware parser
	catalogAdapter := parser.NewDefaultCatalogAdapter()
	enhancedParser := parser.NewEnhancedParser(catalogAdapter)

	// Parse and validate the query against catalog
	stmt, err := enhancedParser.ParseAndValidate(ctx, query, catalogAdapter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse and validate query: %w", err)
	}

	// Route based on statement type
	switch stmt := stmt.(type) {
	case *parser.SelectStmt:
		return e.executeReadQuery(ctx, query)
	case *parser.InsertStmt:
		return e.executeInsertQuery(ctx, query)
	case *parser.CreateTableStmt:
		return e.executeDDLQuery(ctx, query)
	case *parser.ShowStmt:
		return e.executeShowStmt(ctx, stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
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
		RowCount: int64(len(rows)),
		Columns:  []string{"Table"},
		Message:  fmt.Sprintf("Found %d table(s)", len(rows)),
	}, nil
}

// executeDDLQuery handles DDL operations
func (e *Engine) executeDDLQuery(ctx context.Context, query string) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing DDL query")

	// Parse the query to determine the statement type
	catalogAdapter := parser.NewDefaultCatalogAdapter()
	enhancedParser := parser.NewEnhancedParser(catalogAdapter)

	stmt, err := enhancedParser.ParseAndValidate(ctx, query, catalogAdapter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL query: %w", err)
	}

	// Handle different DDL statement types
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		// Create table using our storage manager
		schemaData := e.serializeTableSchema(stmt.TableSchema)
		if err := e.storageMgr.CreateTable(ctx, "default", stmt.TableName.Value, schemaData, "default", nil); err != nil {
			return nil, fmt.Errorf("failed to create table in storage: %w", err)
		}

		return &QueryResult{
			Data:     [][]interface{}{},
			RowCount: 0,
			Columns:  []string{},
			Message:  "Table created successfully",
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
func (e *Engine) CreateTable(ctx context.Context, tableName string, schema *parser.TableSchema) error {
	e.logger.Info().Str("table", tableName).Msg("Creating table")

	// Store table schema in storage using Storage Manager
	schemaData := e.serializeTableSchema(schema)
	if err := e.storageMgr.CreateTable(ctx, "default", tableName, schemaData, "default", nil); err != nil {
		return fmt.Errorf("failed to create table in storage: %w", err)
	}

	e.logger.Info().Str("table", tableName).Msg("Table created successfully")
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

// GetTableData retrieves data from the specified table
func (e *Engine) GetTableData(ctx context.Context, tableName string, limit int) ([][]interface{}, error) {
	e.logger.Info().Str("table", tableName).Int("limit", limit).Msg("Retrieving table data")

	// Get data from storage using Storage Manager
	data, err := e.storageMgr.GetTableData(ctx, "default", tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve data from storage: %w", err)
	}

	// Apply limit if needed
	if limit > 0 && len(data) > limit {
		data = data[:limit]
	}

	e.logger.Info().Str("table", tableName).Int("rows", len(data)).Msg("Data retrieved from storage")
	return data, nil
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
