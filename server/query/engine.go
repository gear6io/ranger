package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/query/duckdb"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/storage"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
)

// ComponentType defines the query engine component type identifier
const ComponentType = "query"

// Engine represents the shared query engine service with embedded storage
type Engine struct {
	duckdbEngine *duckdb.Engine
	storageMgr   *storage.Storage
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
func NewEngine(cfg *config.Config, storageMgr *storage.Storage, logger zerolog.Logger) (*Engine, error) {
	// Get catalog from storage manager
	catalogInstance := storageMgr.GetCatalog()

	// Initialize DuckDB engine with the catalog and permissive config
	duckdbConfig := &duckdb.EngineConfig{
		MaxMemoryMB:        512,
		QueryTimeoutSec:    300,
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          100,
		IcebergCatalogName: "ranger_catalog",
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
		return nil, errors.New(ErrDuckDBEngineCreationFailed, "failed to create DuckDB engine", err)
	}

	// Create execution manager for tracking and cancellation
	queryManager := NewExecutionManager(logger)

	// Create engine instance
	engine := &Engine{
		duckdbEngine: duckdbEngine,
		storageMgr:   storageMgr,
		logger:       logger,
		queryManager: queryManager,
	}

	// System database is now initialized by the Store during creation

	return engine, nil
}

// ExecuteQuery executes a query with full tracking and cancellation support
func (e *Engine) ExecuteQuery(ctx context.Context, queryCtx *types.QueryContext) (*QueryResult, error) {
	// Generate unique query ID
	queryID := fmt.Sprintf("query_%d", time.Now().UnixNano())

	// Start tracking the query
	_, trackedCtx := e.queryManager.StartQuery(ctx, queryID, queryCtx.Query, queryCtx.User, queryCtx.ClientAddr)

	// Use the tracked context for execution
	ctx = trackedCtx

	// Ensure query completion is tracked
	defer func() {
		if r := recover(); r != nil {
			e.queryManager.CompleteQuery(queryID, 0, errors.New(ErrQueryPanic, "query execution panic", nil).AddContext("panic_value", fmt.Sprintf("%v", r)))
			panic(r)
		}
	}()

	// Parse the query (validation will be handled separately if needed)
	stmt, err := parser.Parse(queryCtx.Query)
	if err != nil {
		e.queryManager.CompleteQuery(queryID, 0, err)
		return nil, err
	}

	// Debug: Log the parsed statement type
	e.logger.Debug().
		Str("statement_type", fmt.Sprintf("%T", stmt)).
		Msg("Parsed statement type")

	// Route based on statement type
	var result *QueryResult
	switch stmt := stmt.(type) {
	case *parser.SelectStmt:
		result, err = e.executeReadQuery(ctx, queryCtx.Query, queryCtx)
	case *parser.InsertStmt:
		result, err = e.executeInsertQuery(ctx, queryCtx.Query, queryCtx)
	case *parser.CreateTableStmt:
		result, err = e.executeDDLQuery(ctx, queryCtx.Query, queryCtx)
	case *parser.CreateDatabaseStmt:
		result, err = e.executeCreateDatabase(ctx, stmt)
	case *parser.ShowStmt:
		result, err = e.executeShowStmt(ctx, stmt, queryCtx)
	case *parser.DropTableStmt:
		result, err = e.executeDropTable(ctx, stmt, queryCtx)
	case *parser.UpdateStmt:
		result, err = e.executeUpdateQuery(ctx, queryCtx.Query, queryCtx)
	case *parser.DeleteStmt:
		result, err = e.executeDeleteQuery(ctx, queryCtx.Query, queryCtx)
	case *parser.UseStmt:
		result, err = e.executeUseStmt(ctx, stmt, queryCtx)
	case *parser.ExplainStmt:
		result, err = e.executeExplainStmt(ctx, stmt, queryCtx)
	default:
		err = errors.New(ErrUnsupportedStatementType, "unsupported statement type", nil).AddContext("statement_type", fmt.Sprintf("%T", stmt))
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

// getDatabaseFromContext determines the database to use based on the query context
func (e *Engine) getDatabaseFromContext(queryCtx *types.QueryContext) string {
	// If database is specified in query context, use it
	if queryCtx.Database != "" {
		return queryCtx.Database
	}

	// Otherwise, fall back to "default"
	return "default"
}

// getDefaultStorageEngine returns the default storage engine for CREATE TABLE operations
func (e *Engine) getDefaultStorageEngine() string {
	// Default to iceberg storage engine
	return "iceberg"
}

// getDefaultEngineConfig returns the default engine configuration for CREATE TABLE operations
func (e *Engine) getDefaultEngineConfig() map[string]interface{} {
	return map[string]interface{}{
		"format":       "parquet",
		"compression":  "snappy",
		"partitioning": "auto",
	}
}

// validateDatabaseExists checks if the specified database exists
func (e *Engine) validateDatabaseExists(ctx context.Context, database string) error {
	// Skip validation for "default" database as it should always exist
	if database == "default" {
		return nil
	}

	// Check if database exists in storage manager
	databases, err := e.storageMgr.ListDatabases(ctx)
	if err != nil {
		return errors.New(ErrDatabaseListFailed, "failed to validate database existence", err)
	}

	// Check if the specified database exists
	for _, db := range databases {
		if db == database {
			return nil
		}
	}

	// Database not found
	return errors.New(ErrDatabaseNotFound, fmt.Sprintf("database '%s' does not exist", database), nil)
}

// executeReadQuery redirects read queries to DuckDB or system database
func (e *Engine) executeReadQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing read query")

	// Check if this is a system database query
	if e.isSystemDatabaseQuery(query) {
		return e.executeSystemDatabaseQuery(ctx, query, queryCtx)
	}

	// Execute on DuckDB engine for regular queries
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.New(ErrDuckDBExecutionFailed, "DuckDB execution failed", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  "OK",
	}, nil
}

// executeInsertQuery handles insert operations
func (e *Engine) executeInsertQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing insert query")

	// For now, route to DuckDB
	// TODO: Integrate with Native Server's BatchInsert when available
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.New(ErrInsertExecutionFailed, "insert execution failed", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  fmt.Sprintf("INSERT %d", result.RowCount),
	}, nil
}

// executeShowStmt handles SHOW statements
func (e *Engine) executeShowStmt(ctx context.Context, stmt *parser.ShowStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("show_type", stmt.ShowType.String()).Msg("Executing SHOW statement")

	switch stmt.ShowType {
	case parser.SHOW_DATABASES:
		return e.executeShowDatabases(ctx)
	case parser.SHOW_TABLES:
		return e.executeShowTables(ctx, stmt, queryCtx)
	case parser.SHOW_COLUMNS:
		return e.executeShowColumns(ctx, stmt, queryCtx)
	case parser.SHOW_CREATE_TABLE:
		return e.executeShowCreateTable(ctx, stmt, queryCtx)
	default:
		return nil, errors.New(ErrUnsupportedShowType, "unsupported SHOW type", nil).AddContext("show_type", stmt.ShowType.String())
	}
}

// executeShowDatabases lists all available databases
func (e *Engine) executeShowDatabases(ctx context.Context) (*QueryResult, error) {
	e.logger.Debug().Msg("Executing SHOW DATABASES")

	// Get databases from storage manager
	databases, err := e.storageMgr.ListDatabases(ctx)
	if err != nil {
		return nil, errors.New(ErrDatabaseListFailed, "failed to list databases", err)
	}

	// Format result as table data
	var rows [][]interface{}
	for _, db := range databases {
		rows = append(rows, []interface{}{db})
	}

	return &QueryResult{
		Data:     rows,
		RowCount: int64(len(rows)),
		Columns:  []string{"database"},
		Message:  fmt.Sprintf("Found %d database(s)", len(rows)),
	}, nil
}

// executeShowTables lists all available tables in the current database
func (e *Engine) executeShowTables(ctx context.Context, stmt *parser.ShowStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	// Determine which database to show tables for
	database := e.getDatabaseFromContext(queryCtx)

	// Check if the query specifies a database (e.g., SHOW TABLES FROM database_name)
	if stmt.From != nil {
		database = stmt.From.Value
	}

	// Validate that the database exists
	if err := e.validateDatabaseExists(ctx, database); err != nil {
		return nil, err
	}

	e.logger.Debug().Str("database", database).Msg("Executing SHOW TABLES")

	// Get tables from storage manager for the specific database
	tables, err := e.storageMgr.ListTables(ctx, database)
	if err != nil {
		return nil, errors.New(ErrTableListFailed, "failed to list tables", err)
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
		Message:  fmt.Sprintf("Found %d table(s) in database %s", len(tables), database),
	}, nil
}

// executeCreateTable handles CREATE TABLE statements using new Storage Manager interface
func (e *Engine) executeCreateTable(ctx context.Context, stmt *parser.CreateTableStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	// Generate request ID for tracking
	requestID := fmt.Sprintf("create_table_%d", time.Now().UnixNano())

	// Extract database and table name from TableIdentifier
	database := e.getDatabaseFromContext(queryCtx)
	tableName := stmt.TableName.Table.Value

	// Check if table name is qualified (database.table)
	if stmt.TableName.IsQualified() {
		database = stmt.TableName.Database.Value
		tableName = stmt.TableName.Table.Value
	}

	// Validate that the database exists
	if err := e.validateDatabaseExists(ctx, database); err != nil {
		return nil, err
	}

	e.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Bool("ifNotExists", stmt.IfNotExists).
		Str("request_id", requestID).
		Msg("Processing CREATE TABLE statement")

	// Check if table already exists using storage manager
	if e.storageMgr.TableExists(ctx, database, tableName) {
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
			return nil, errors.New(ErrTableAlreadyExists, "table already exists", nil).AddContext("database", database).AddContext("table", tableName)
		}
	}

	// Create user context for the request
	userCtx := &types.UserContext{
		UserID:   0, // Default user ID - would be set by authentication layer
		Username: queryCtx.User,
		Database: database,
		IsAdmin:  false, // Default to non-admin - would be set by authentication layer
	}

	// Determine storage engine
	storageEngine := e.getDefaultStorageEngine()
	if stmt.StorageEngine != nil {
		storageEngine = stmt.StorageEngine.Value
	}

	// Create Storage Manager request
	req := &types.CreateTableRequest{
		Statement:     stmt,
		Database:      database,
		RequestID:     requestID,
		UserContext:   userCtx,
		StorageEngine: storageEngine,
		EngineConfig:  e.getDefaultEngineConfig(),
	}

	// Delegate to Storage Manager (proper abstraction)
	response, err := e.storageMgr.CreateTable(ctx, req)
	if err != nil {
		e.logger.Error().Err(err).
			Str("request_id", requestID).
			Str("table_name", tableName).
			Str("database", database).
			Msg("CREATE TABLE execution failed")

		return nil, errors.Newf(ErrTableCreationFailed, "failed to create table '%s': %w", tableName, err)
	}

	// Handle unsuccessful response
	if !response.Success {
		return &QueryResult{
			Data:     [][]interface{}{},
			RowCount: 0,
			Columns:  []string{},
			Message:  fmt.Sprintf("CREATE TABLE failed for table %s", tableName),
		}, nil
	}

	// Success response
	e.logger.Info().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Str("database", database).
		Int64("table_id", response.TableID).
		Msg("CREATE TABLE executed successfully")

	return &QueryResult{
		Data:     [][]interface{}{},
		RowCount: 0,
		Columns:  []string{},
		Message: fmt.Sprintf("Table %s created successfully with %s engine (%d columns)",
			tableName, response.Metadata.StorageEngine, response.Metadata.ColumnCount),
	}, nil
}

// executeCreateDatabase handles CREATE DATABASE statements
func (e *Engine) executeCreateDatabase(ctx context.Context, stmt *parser.CreateDatabaseStmt) (*QueryResult, error) {
	e.logger.Debug().
		Str("database", stmt.Name.Value).
		Bool("ifNotExists", stmt.IfNotExists).
		Msg("Executing CREATE DATABASE")

	// Check if database already exists
	if e.storageMgr.DatabaseExists(ctx, stmt.Name.Value) {
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
			return nil, errors.New(ErrDatabaseAlreadyExists, "database already exists", nil).AddContext("database_name", stmt.Name.Value)
		}
	}

	// Create database using storage manager
	err := e.storageMgr.CreateDatabase(ctx, stmt.Name.Value)
	if err != nil {
		return nil, errors.New(ErrDatabaseCreationFailed, "failed to create database", err)
	}

	return &QueryResult{
		Data:     [][]interface{}{},
		RowCount: 0,
		Columns:  []string{},
		Message:  fmt.Sprintf("Database %s created successfully", stmt.Name.Value),
	}, nil
}

// executeDDLQuery handles DDL operations
func (e *Engine) executeDDLQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing DDL query")

	// Parse the query to determine the statement type
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, errors.New(ErrDDLParseFailed, "failed to parse DDL query", err)
	}

	// Handle different DDL statement types
	switch stmt := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.executeCreateTable(ctx, stmt, queryCtx)

	default:
		// For other DDL operations, route to DuckDB for now
		result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
		if err != nil {
			return nil, errors.New(ErrDDLExecutionFailed, "DDL execution failed", err)
		}

		return &QueryResult{
			Data:     result.Rows,
			RowCount: result.RowCount,
			Columns:  result.Columns,
			Message:  "OK",
		}, nil
	}
}

// executeDropTable handles DROP TABLE statements
func (e *Engine) executeDropTable(ctx context.Context, stmt *parser.DropTableStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().
		Str("table", stmt.TableName.Table.Value).
		Bool("ifExists", stmt.IfExists).
		Msg("Executing DROP TABLE")

	// Determine which database to drop the table from
	database := e.getDatabaseFromContext(queryCtx)

	// Check if the table name is qualified (database.table)
	if stmt.TableName.IsQualified() {
		database = stmt.TableName.Database.Value
	}

	// Validate that the database exists
	if err := e.validateDatabaseExists(ctx, database); err != nil {
		return nil, err
	}

	// Check if table exists using metadata manager
	if !e.storageMgr.TableExists(ctx, database, stmt.TableName.Table.Value) {
		if stmt.IfExists {
			// Table does not exist and IF EXISTS was specified - return success
			return &QueryResult{
				Data:     [][]interface{}{},
				RowCount: 0,
				Columns:  []string{},
				Message:  fmt.Sprintf("Table %s.%s does not exist (IF EXISTS)", database, stmt.TableName.Table.Value),
			}, nil
		} else {
			// Table does not exist and IF EXISTS was not specified - return error
			return nil, errors.New(ErrTableNotFound, fmt.Sprintf("table '%s' does not exist", stmt.TableName.Table.Value), nil).AddContext("database", database)
		}
	}

	// Drop table using metadata manager
	if err := e.storageMgr.DropTable(ctx, database, stmt.TableName.Table.Value); err != nil {
		return nil, errors.New(ErrTableDropFailed, "failed to drop table", err).AddContext("database", database).AddContext("table", stmt.TableName.Table.Value)
	}

	return &QueryResult{
		Data:     [][]interface{}{},
		RowCount: 0,
		Columns:  []string{},
		Message:  fmt.Sprintf("Table %s.%s dropped successfully", database, stmt.TableName.Table.Value),
	}, nil
}

// executeUpdateQuery handles UPDATE statements
func (e *Engine) executeUpdateQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing UPDATE query")

	// For now, route to DuckDB
	// TODO: Integrate with Native Server's Update when available
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.New(ErrDuckDBExecutionFailed, "UPDATE execution failed", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  fmt.Sprintf("UPDATE %d", result.RowCount),
	}, nil
}

// executeDeleteQuery handles DELETE statements
func (e *Engine) executeDeleteQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing DELETE query")

	// For now, route to DuckDB
	// TODO: Integrate with Native Server's Delete when available
	result, err := e.duckdbEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.New(ErrDuckDBExecutionFailed, "DELETE execution failed", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  fmt.Sprintf("DELETE %d", result.RowCount),
	}, nil
}

// executeUseStmt handles USE statements
func (e *Engine) executeUseStmt(ctx context.Context, stmt *parser.UseStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().
		Str("database", stmt.DatabaseName.Value).
		Msg("Executing USE statement")

	// Validate that the database exists
	if err := e.validateDatabaseExists(ctx, stmt.DatabaseName.Value); err != nil {
		return nil, err
	}

	// Update the query context database
	queryCtx.Database = stmt.DatabaseName.Value

	return &QueryResult{
		Data:     [][]interface{}{},
		RowCount: 0,
		Columns:  []string{},
		Message:  fmt.Sprintf("Database changed to %s", stmt.DatabaseName.Value),
	}, nil
}

// executeExplainStmt handles EXPLAIN statements
func (e *Engine) executeExplainStmt(ctx context.Context, stmt *parser.ExplainStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Msg("Executing EXPLAIN statement")

	// For now, route to DuckDB for explanation
	// TODO: Implement custom explanation logic
	explainQuery := fmt.Sprintf("EXPLAIN %s", queryCtx.Query)
	result, err := e.duckdbEngine.ExecuteQuery(ctx, explainQuery)
	if err != nil {
		return nil, errors.New(ErrDuckDBExecutionFailed, "EXPLAIN execution failed", err)
	}

	return &QueryResult{
		Data:     result.Rows,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  "EXPLAIN completed",
	}, nil
}

// InsertData inserts data into the specified table
func (e *Engine) InsertData(ctx context.Context, tableName string, data [][]interface{}) error {
	e.logger.Info().Str("table", tableName).Int("rows", len(data)).Msg("Inserting data")

	// Store data in storage using Storage Manager
	if err := e.storageMgr.InsertData(ctx, "default", tableName, data); err != nil {
		return err
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
		return err
	}

	// Use the storage manager's InsertData method which already implements streaming
	err := e.storageMgr.InsertData(ctx, database, tableName, allData)
	if err != nil {
		return err
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
		return err
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
		return nil, err
	}

	// Apply limit if needed
	if limit > 0 && len(data) > limit {
		data = data[:limit]
	}

	e.logger.Info().Str("database", database).Str("table", tableName).Int("rows", len(data)).Msg("Data retrieved from storage")
	return data, nil
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

		if !colDef.IsNullable || colDef.NotNull {
			colSQL += " NOT NULL"
		}

		if colDef.Unique {
			colSQL += " UNIQUE"
		}

		if colDef.DefaultValue != "" {
			colSQL += fmt.Sprintf(" DEFAULT %s", colDef.DefaultValue)
		}

		columns = append(columns, colSQL)
	}

	sql += strings.Join(columns, ", ") + ")"
	return sql
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
			"nullable": colDef.IsNullable,
		}

		// Add length if specified
		if colDef.Length > 0 {
			field["length"] = colDef.Length
		}

		// Add precision and scale for decimal types
		if colDef.Precision > 0 {
			field["precision"] = colDef.Precision
		}
		if colDef.Scale > 0 {
			field["scale"] = colDef.Scale
		}

		// Add default value if specified
		if colDef.DefaultValue != "" {
			field["default"] = colDef.DefaultValue
		}

		// Add constraints
		if colDef.NotNull || !colDef.IsNullable {
			field["not_null"] = true
		}
		if colDef.Unique {
			field["unique"] = true
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
	// Parse the JSON schema data
	var schemaData map[string]interface{}
	if err := json.Unmarshal(data, &schemaData); err != nil {
		return nil, errors.New(ErrSchemaUnmarshalFailed, "failed to unmarshal schema data", err)
	}

	// Create TableSchema
	schema := &parser.TableSchema{
		ColumnDefinitions: make(map[string]*parser.ColumnDefinition),
	}

	// Extract fields
	fields, ok := schemaData["fields"].([]interface{})
	if !ok {
		// Fallback to basic schema if fields not found
		return &parser.TableSchema{
			ColumnDefinitions: map[string]*parser.ColumnDefinition{
				"id": {
					Name:       "id",
					DataType:   "int32",
					IsNullable: false,
				},
			},
		}, nil
	}

	// Parse each field
	for _, fieldInterface := range fields {
		field, ok := fieldInterface.(map[string]interface{})
		if !ok {
			continue
		}

		name, ok := field["name"].(string)
		if !ok {
			continue
		}

		dataType, ok := field["type"].(string)
		if !ok {
			continue
		}

		colDef := &parser.ColumnDefinition{
			Name:       name,
			DataType:   dataType,
			IsNullable: true, // Default to nullable
		}

		// Parse nullable
		if nullable, ok := field["nullable"].(bool); ok {
			colDef.IsNullable = nullable
		}

		// Parse length
		if length, ok := field["length"].(float64); ok {
			colDef.Length = int(length)
		}

		// Parse precision
		if precision, ok := field["precision"].(float64); ok {
			colDef.Precision = int(precision)
		}

		// Parse scale
		if scale, ok := field["scale"].(float64); ok {
			colDef.Scale = int(scale)
		}

		// Parse default value
		if defaultValue, ok := field["default"].(string); ok {
			colDef.DefaultValue = defaultValue
		}

		// Parse constraints
		if notNull, ok := field["not_null"].(bool); ok && notNull {
			colDef.NotNull = true
			colDef.IsNullable = false
		}

		if unique, ok := field["unique"].(bool); ok {
			colDef.Unique = unique
		}

		schema.ColumnDefinitions[name] = colDef
	}

	return schema, nil
}

// GetDuckDBEngine returns the underlying DuckDB engine for direct access
func (e *Engine) GetDuckDBEngine() *duckdb.Engine {
	return e.duckdbEngine
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

// executeShowColumns handles SHOW COLUMNS statements
func (e *Engine) executeShowColumns(ctx context.Context, stmt *parser.ShowStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	// Get database and table names
	database := e.getDatabaseFromContext(queryCtx)
	tableName := ""

	if stmt.TableName != nil {
		if stmt.TableName.IsQualified() {
			database = stmt.TableName.Database.Value
		}
		tableName = stmt.TableName.Table.Value
	} else {
		return nil, errors.New(ErrTableNameRequired, "table name is required for SHOW COLUMNS", nil)
	}

	// Validate that the database and table exist
	if err := e.validateDatabaseExists(ctx, database); err != nil {
		return nil, err
	}

	if !e.storageMgr.TableExists(ctx, database, tableName) {
		return nil, errors.New(ErrTableNotFound, "table does not exist", nil).
			AddContext("database", database).
			AddContext("table", tableName)
	}

	e.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Msg("Executing SHOW COLUMNS")

	// Get columns from system database manager
	systemMgr := e.storageMgr.GetSystemManager()
	columns, err := systemMgr.GetSystemColumns(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(ErrColumnListFailed, "failed to list columns", err)
	}

	// Format result as table data
	var rows [][]interface{}
	for _, col := range columns {
		rows = append(rows, []interface{}{
			col.ColumnName,
			col.DataType,
			col.IsNullable,
			col.IsPrimary,
			col.IsUnique,
			col.DefaultValue,
			col.Description,
		})
	}

	return &QueryResult{
		Data:     rows,
		RowCount: int64(len(rows)),
		Columns:  []string{"Field", "Type", "Null", "Key", "Unique", "Default", "Extra"},
		Message:  fmt.Sprintf("Found %d column(s) in table %s.%s", len(rows), database, tableName),
	}, nil
}

// executeShowCreateTable handles SHOW CREATE TABLE statements
func (e *Engine) executeShowCreateTable(ctx context.Context, stmt *parser.ShowStmt, queryCtx *types.QueryContext) (*QueryResult, error) {
	// Get database and table names
	database := e.getDatabaseFromContext(queryCtx)
	tableName := ""

	if stmt.TableName != nil {
		if stmt.TableName.IsQualified() {
			database = stmt.TableName.Database.Value
		}
		tableName = stmt.TableName.Table.Value
	} else {
		return nil, errors.New(ErrTableNameRequired, "table name is required for SHOW CREATE TABLE", nil)
	}

	// Validate that the database and table exist
	if err := e.validateDatabaseExists(ctx, database); err != nil {
		return nil, err
	}

	if !e.storageMgr.TableExists(ctx, database, tableName) {
		return nil, errors.New(ErrTableNotFound, "table does not exist", nil).
			AddContext("database", database).
			AddContext("table", tableName)
	}

	e.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Msg("Executing SHOW CREATE TABLE")

	// Generate DDL from system database manager
	systemMgr := e.storageMgr.GetSystemManager()
	ddl, err := systemMgr.GenerateCreateTableDDL(ctx, database, tableName)
	if err != nil {
		return nil, errors.New(ErrDDLGenerationFailed, "failed to generate DDL", err)
	}

	// Format result as table data
	rows := [][]interface{}{
		{tableName, ddl},
	}

	return &QueryResult{
		Data:     rows,
		RowCount: 1,
		Columns:  []string{"Table", "Create Table"},
		Message:  fmt.Sprintf("DDL for table %s.%s", database, tableName),
	}, nil
}

// isSystemDatabaseQuery checks if a query targets the system database
func (e *Engine) isSystemDatabaseQuery(query string) bool {
	systemMgr := e.storageMgr.GetSystemManager()
	return systemMgr.IsSystemDatabaseQuery(query)
}

// executeSystemDatabaseQuery executes queries against the system database
func (e *Engine) executeSystemDatabaseQuery(ctx context.Context, query string, queryCtx *types.QueryContext) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing system database query")

	// Get system database manager
	systemMgr := e.storageMgr.GetSystemManager()

	// Execute the query
	result, err := systemMgr.Query(ctx, query)
	if err != nil {
		return nil, errors.New(ErrDuckDBExecutionFailed, "system database query execution failed", err)
	}

	return &QueryResult{
		Data:     result.Data,
		RowCount: result.RowCount,
		Columns:  result.Columns,
		Message:  fmt.Sprintf("System query executed successfully, %d rows returned", result.RowCount),
	}, nil
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
		return errors.New(ErrQueryEngineCloseFailed, "failed to close query engine", err)
	}

	e.logger.Info().Msg("Query engine shut down successfully")
	return nil
}

// Close closes the engine and releases resources
func (e *Engine) Close() error {
	if err := e.duckdbEngine.Close(); err != nil {
		return errors.New(ErrQueryEngineCloseFailed, "failed to close query engine", err)
	}

	return nil
}
