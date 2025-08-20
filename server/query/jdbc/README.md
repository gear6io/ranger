# SQL Parser Integration with Icebox JDBC Handler

This directory contains the enhanced JDBC handler that integrates SQL parsing and analysis capabilities with Icebox's existing DuckDB-based query execution engine.

## Overview

The integration enhances the existing JDBC handler with advanced SQL parsing capabilities, providing:

- **Query Analysis**: Detailed parsing and analysis of SQL statements
- **Security Validation**: Blocking of dangerous SQL operations
- **Performance Monitoring**: Query complexity assessment and optimization hints
- **Enhanced Logging**: Comprehensive query information for debugging and monitoring

## Architecture

### SQLParserMiddleware
The `SQLParserMiddleware` encapsulates all SQL parsing and analysis functionality:

```go
type SQLParserMiddleware struct {
	logger zerolog.Logger
}
```

**Key Methods:**
- `AnalyzeQuery(query string) (*QueryAnalysis, error)` - Parse and analyze SQL queries
- `ValidateQuery(analysis *QueryAnalysis) error` - Validate queries against security policies
- `GenerateCommandCompleteTag(analysis *QueryAnalysis, rowCount int) string` - Generate PostgreSQL-compatible response tags

### Enhanced JDBCHandler
The existing `JDBCHandler` is enhanced with middleware integration:

```go
type JDBCHandler struct {
	sqlMiddleware *SQLParserMiddleware
	engine        QueryEngineInterface
	logger        zerolog.Logger
	// ... other fields
}
```

## Usage

### Basic Integration
```go
import (
	"github.com/rs/zerolog"
	"github.com/TFMV/icebox/server/query/jdbc"
)

// Create logger
logger := zerolog.New(zerolog.NewConsoleWriter())

// Create the SQL parser middleware
sqlMiddleware := NewSQLParserMiddleware(logger)

// Create handler with SQL parser middleware
handler := NewJDBCHandler(sqlMiddleware, engine, logger)

// The handler now automatically analyzes and validates all queries
```

### Standalone Middleware Usage
```go
// Analyze a query
analysis, err := sqlMiddleware.AnalyzeQuery("SELECT * FROM users WHERE active = true")
if err != nil {
	logger.Error().Err(err).Msg("Query analysis failed")
	return
}

// Validate the query
err = sqlMiddleware.ValidateQuery(analysis)
if err != nil {
	logger.Error().Err(err).Msg("Query blocked")
	return
}

// Generate command complete tag
tag := sqlMiddleware.GenerateCommandCompleteTag(analysis, 42)
```

## Query Analysis

The middleware provides comprehensive query analysis:

```go
type QueryAnalysis struct {
	StatementType      string        // Type of SQL statement
	Tables            []string      // Tables referenced
	Columns           []string      // Columns referenced
	HasJoins          bool          // Contains JOIN operations
	HasSubqueries     bool          // Contains subqueries
	HasAggregations   bool          // Contains aggregate functions
	Complexity        string        // Query complexity level
	ParseTime         time.Duration // Time taken to parse
	OptimizationHints []string      // Performance optimization suggestions
	ValidationErrors  []string      // Validation issues found
	IsValid           bool          // Overall validity status
}
```

## Security Features

The middleware automatically blocks dangerous SQL operations:

- **DDL Operations**: `DROP`, `ALTER`, `CREATE` (configurable)
- **Privilege Operations**: `GRANT`, `REVOKE`
- **System Operations**: `SHUTDOWN`, `KILL` (if supported)

## Testing

Run the middleware tests:
```bash
go test ./server/query/jdbc/sql_parser_middleware_test.go ./server/query/jdbc/sql_parser_middleware.go
```

Run integration tests:
```bash
go test ./server/query/jdbc/integration_test.go
```

## Dependencies

- `github.com/rs/zerolog` - Structured logging
- `github.com/TFMV/icebox/server/query/parser` - SQL parser

## Examples

See `sql_parser_integration_example.go` for complete examples of:

- Basic middleware usage
- Integration with JDBCHandler
- Complex query analysis
- Error handling and validation
