package query

import (
	"context"
	"fmt"

	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/query/duckdb"
	"github.com/TFMV/icebox/server/query/parser"
	"github.com/rs/zerolog"
)

// Engine represents the shared query engine service
type Engine struct {
	duckdbEngine *duckdb.Engine
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

// NewEngine creates a new shared query engine service
func NewEngine(cfg *config.Config, logger zerolog.Logger) (*Engine, error) {
	// Initialize catalog based on configuration
	catalogInstance, err := catalog.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	// Initialize DuckDB engine with the catalog
	duckdbEngine, err := duckdb.NewEngine(catalogInstance)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB engine: %w", err)
	}

	return &Engine{
		duckdbEngine: duckdbEngine,
		logger:       logger,
	}, nil
}

// ExecuteQuery routes and executes a query
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// Create a new parser with lexer for this specific query
	lexer := parser.NewLexer([]byte(query))
	p := parser.NewParser(lexer)

	// Parse the query to determine type
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Route based on statement type
	switch stmt.(type) {
	case *parser.SelectStmt:
		return e.executeReadQuery(ctx, query)
	case *parser.InsertStmt:
		return e.executeInsertQuery(ctx, query)
	case *parser.CreateTableStmt:
		return e.executeDDLQuery(ctx, query)
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

// executeDDLQuery handles DDL operations
func (e *Engine) executeDDLQuery(ctx context.Context, query string) (*QueryResult, error) {
	e.logger.Debug().Str("query", query).Msg("Executing DDL query")

	// Route to DuckDB for DDL
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

// GetDuckDBEngine returns the underlying DuckDB engine for direct access
func (e *Engine) GetDuckDBEngine() *duckdb.Engine {
	return e.duckdbEngine
}

// Close closes the engine and releases resources
func (e *Engine) Close() error {
	if e.duckdbEngine != nil {
		return e.duckdbEngine.Close()
	}
	return nil
}
