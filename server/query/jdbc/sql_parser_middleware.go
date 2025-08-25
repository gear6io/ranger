package jdbc

import (
	"fmt"
	"strings"
	"time"

	"github.com/gear6io/ranger/server/query/parser"
	"github.com/rs/zerolog"
)

// SQLParserMiddleware provides SQL parsing and analysis capabilities
type SQLParserMiddleware struct {
	logger zerolog.Logger
}

// QueryAnalysis represents the analysis of a SQL query
type QueryAnalysis struct {
	StatementType     string
	Tables            []string
	Columns           []string
	Complexity        string
	HasJoins          bool
	HasSubqueries     bool
	HasAggregations   bool
	OptimizationHints []string
	ValidationErrors  []string
	ParseTime         time.Duration
	IsValid           bool
}

// NewSQLParserMiddleware creates a new SQL parser middleware
func NewSQLParserMiddleware(logger zerolog.Logger) *SQLParserMiddleware {
	return &SQLParserMiddleware{
		logger: logger,
	}
}

// AnalyzeQuery parses and analyzes a SQL query using the SQL parser
func (m *SQLParserMiddleware) AnalyzeQuery(query string) (*QueryAnalysis, error) {
	start := time.Now()

	// Create a new lexer and parser for the query
	lexer := parser.NewLexer([]byte(query))
	p := parser.NewParser(lexer)

	// Parse the query
	node, err := p.Parse()
	if err != nil {
		return &QueryAnalysis{
			StatementType:    "UNKNOWN",
			IsValid:          false,
			ValidationErrors: []string{err.Error()},
			ParseTime:        time.Since(start),
		}, fmt.Errorf("failed to parse query: %w", err)
	}

	// Initialize analysis
	analysis := &QueryAnalysis{
		StatementType:     "UNKNOWN",
		Tables:            []string{},
		Columns:           []string{},
		Complexity:        "SIMPLE",
		HasJoins:          false,
		HasSubqueries:     false,
		HasAggregations:   false,
		OptimizationHints: []string{},
		ValidationErrors:  []string{},
		ParseTime:         time.Since(start),
		IsValid:           true,
	}

	// Analyze based on statement type
	switch stmt := node.(type) {
	case *parser.SelectStmt:
		analysis = m.analyzeSelectStatement(stmt, analysis)
	case *parser.InsertStmt:
		analysis = m.analyzeInsertStatement(stmt, analysis)
	case *parser.UpdateStmt:
		analysis = m.analyzeUpdateStatement(stmt, analysis)
	case *parser.DeleteStmt:
		analysis = m.analyzeDeleteStatement(stmt, analysis)
	case *parser.CreateTableStmt:
		analysis = m.analyzeCreateTableStatement(stmt, analysis)
	case *parser.DropTableStmt:
		analysis = m.analyzeDropTableStatement(stmt, analysis)
	case *parser.AlterTableStmt:
		analysis = m.analyzeAlterTableStatement(stmt, analysis)
	case *parser.CreateIndexStmt:
		analysis = m.analyzeCreateIndexStatement(stmt, analysis)
	case *parser.GrantStmt:
		analysis = m.analyzeGrantStatement(stmt, analysis)
	case *parser.RevokeStmt:
		analysis = m.analyzeRevokeStatement(stmt, analysis)
	case *parser.ShowStmt:
		analysis = m.analyzeShowStatement(stmt, analysis)
	case *parser.ExplainStmt:
		analysis = m.analyzeExplainStatement(stmt, analysis)
	case *parser.CreateDatabaseStmt:
		analysis = m.analyzeCreateDatabaseStatement(stmt, analysis)
	case *parser.DropDatabaseStmt:
		analysis = m.analyzeDropDatabaseStatement(stmt, analysis)
	case *parser.UseStmt:
		analysis = m.analyzeUseStatement(stmt, analysis)
	case *parser.BeginStmt:
		analysis = m.analyzeBeginStatement(stmt, analysis)
	case *parser.CommitStmt:
		analysis = m.analyzeCommitStatement(stmt, analysis)
	case *parser.RollbackStmt:
		analysis = m.analyzeRollbackStatement(stmt, analysis)
	default:
		analysis.StatementType = "UNKNOWN"
	}

	// Determine complexity and generate optimization hints
	analysis.Complexity = m.determineComplexity(analysis)
	analysis.OptimizationHints = m.generateOptimizationHints(analysis)

	return analysis, nil
}

// ValidateQuery validates a query analysis for security and business rules
func (m *SQLParserMiddleware) ValidateQuery(analysis *QueryAnalysis) error {
	// Check for blocked statement types
	blockedTypes := []string{"DROP", "ALTER", "GRANT", "REVOKE"}
	for _, blocked := range blockedTypes {
		if strings.HasPrefix(analysis.StatementType, blocked) {
			return fmt.Errorf("statement type '%s' is not allowed", blocked)
		}
	}

	// Add more validation rules as needed
	if len(analysis.ValidationErrors) > 0 {
		return fmt.Errorf("query validation failed: %s", strings.Join(analysis.ValidationErrors, "; "))
	}

	return nil
}

// analyzeSelectStatement analyzes a SELECT statement
func (m *SQLParserMiddleware) analyzeSelectStatement(stmt *parser.SelectStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "SELECT"

	// Extract table names from FROM clause
	if stmt.TableExpression != nil && stmt.TableExpression.FromClause != nil {
		for _, table := range stmt.TableExpression.FromClause.Tables {
			if table.Name != nil {
				analysis.Tables = append(analysis.Tables, table.Name.Value)
			}
		}
		analysis.HasJoins = len(stmt.TableExpression.FromClause.Tables) > 1
	}

	// Extract column names from SELECT clause
	if stmt.SelectList != nil {
		for _, expr := range stmt.SelectList.Expressions {
			if col, ok := expr.Value.(*parser.ColumnSpecification); ok {
				if col.ColumnName != nil {
					analysis.Columns = append(analysis.Columns, col.ColumnName.Value)
				}
			}
		}
	}

	// Check for subqueries
	analysis.HasSubqueries = m.hasSubqueries(stmt)

	// Check for aggregations
	analysis.HasAggregations = m.hasAggregations(stmt)

	return analysis
}

// analyzeInsertStatement analyzes an INSERT statement
func (m *SQLParserMiddleware) analyzeInsertStatement(stmt *parser.InsertStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "INSERT"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeUpdateStatement analyzes an UPDATE statement
func (m *SQLParserMiddleware) analyzeUpdateStatement(stmt *parser.UpdateStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "UPDATE"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeDeleteStatement analyzes a DELETE statement
func (m *SQLParserMiddleware) analyzeDeleteStatement(stmt *parser.DeleteStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "DELETE"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeCreateTableStatement analyzes a CREATE TABLE statement
func (m *SQLParserMiddleware) analyzeCreateTableStatement(stmt *parser.CreateTableStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "CREATE TABLE"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeDropTableStatement analyzes a DROP TABLE statement
func (m *SQLParserMiddleware) analyzeDropTableStatement(stmt *parser.DropTableStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "DROP TABLE"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeAlterTableStatement analyzes an ALTER TABLE statement
func (m *SQLParserMiddleware) analyzeAlterTableStatement(stmt *parser.AlterTableStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "ALTER TABLE"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.Value)
	}

	return analysis
}

// analyzeCreateIndexStatement analyzes a CREATE INDEX statement
func (m *SQLParserMiddleware) analyzeCreateIndexStatement(stmt *parser.CreateIndexStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "CREATE INDEX"

	if stmt.TableName != nil {
		analysis.Tables = append(analysis.Tables, stmt.TableName.GetFullName())
	}

	return analysis
}

// analyzeGrantStatement analyzes a GRANT statement
func (m *SQLParserMiddleware) analyzeGrantStatement(stmt *parser.GrantStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "GRANT"

	if stmt.PrivilegeDefinition != nil && stmt.PrivilegeDefinition.Object != nil {
		analysis.Tables = append(analysis.Tables, stmt.PrivilegeDefinition.Object.Value)
	}

	return analysis
}

// analyzeRevokeStatement analyzes a REVOKE statement
func (m *SQLParserMiddleware) analyzeRevokeStatement(stmt *parser.RevokeStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "REVOKE"

	if stmt.PrivilegeDefinition != nil && stmt.PrivilegeDefinition.Object != nil {
		analysis.Tables = append(analysis.Tables, stmt.PrivilegeDefinition.Object.Value)
	}

	return analysis
}

// analyzeShowStatement analyzes a SHOW statement
func (m *SQLParserMiddleware) analyzeShowStatement(stmt *parser.ShowStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "SHOW"
	return analysis
}

// analyzeExplainStatement analyzes an EXPLAIN statement
func (m *SQLParserMiddleware) analyzeExplainStatement(stmt *parser.ExplainStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "EXPLAIN"
	return analysis
}

// analyzeCreateDatabaseStatement analyzes a CREATE DATABASE statement
func (m *SQLParserMiddleware) analyzeCreateDatabaseStatement(stmt *parser.CreateDatabaseStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "CREATE DATABASE"
	return analysis
}

// analyzeDropDatabaseStatement analyzes a DROP DATABASE statement
func (m *SQLParserMiddleware) analyzeDropDatabaseStatement(stmt *parser.DropDatabaseStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "DROP DATABASE"
	return analysis
}

// analyzeUseStatement analyzes a USE statement
func (m *SQLParserMiddleware) analyzeUseStatement(stmt *parser.UseStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "USE"
	return analysis
}

// analyzeBeginStatement analyzes a BEGIN statement
func (m *SQLParserMiddleware) analyzeBeginStatement(stmt *parser.BeginStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "BEGIN"
	return analysis
}

// analyzeCommitStatement analyzes a COMMIT statement
func (m *SQLParserMiddleware) analyzeCommitStatement(stmt *parser.CommitStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "COMMIT"
	return analysis
}

// analyzeRollbackStatement analyzes a ROLLBACK statement
func (m *SQLParserMiddleware) analyzeRollbackStatement(stmt *parser.RollbackStmt, analysis *QueryAnalysis) *QueryAnalysis {
	analysis.StatementType = "ROLLBACK"
	return analysis
}

// hasSubqueries checks if a SELECT statement contains subqueries
func (m *SQLParserMiddleware) hasSubqueries(stmt *parser.SelectStmt) bool {
	// This is a simplified check - the actual implementation would need to traverse the AST
	// For now, we'll return false as the parser might not expose this information easily
	return false
}

// hasAggregations checks if a SELECT statement contains aggregations
func (m *SQLParserMiddleware) hasAggregations(stmt *parser.SelectStmt) bool {
	// This is a simplified check - the actual implementation would need to traverse the AST
	// For now, we'll return false as the parser might not expose this information easily
	return false
}

// determineComplexity determines the complexity of a query
func (m *SQLParserMiddleware) determineComplexity(analysis *QueryAnalysis) string {
	if analysis.HasJoins || analysis.HasSubqueries || analysis.HasAggregations {
		return "COMPLEX"
	}

	if len(analysis.Tables) > 1 || len(analysis.Columns) > 10 {
		return "MODERATE"
	}

	return "SIMPLE"
}

// generateOptimizationHints generates optimization hints for a query
func (m *SQLParserMiddleware) generateOptimizationHints(analysis *QueryAnalysis) []string {
	var hints []string

	if analysis.HasJoins {
		hints = append(hints, "Consider adding indexes on join columns")
	}

	if analysis.HasSubqueries {
		hints = append(hints, "Consider rewriting subqueries as JOINs for better performance")
	}

	if analysis.HasAggregations {
		hints = append(hints, "Consider adding indexes on GROUP BY columns")
	}

	if len(analysis.Tables) > 3 {
		hints = append(hints, "Query involves many tables - consider query optimization")
	}

	if len(analysis.Columns) > 20 {
		hints = append(hints, "Query selects many columns - consider selecting only needed columns")
	}

	return hints
}

// GenerateCommandCompleteTag generates a command complete tag for PostgreSQL protocol
func (m *SQLParserMiddleware) GenerateCommandCompleteTag(analysis *QueryAnalysis, rowCount int) string {
	switch analysis.StatementType {
	case "SELECT":
		return fmt.Sprintf("SELECT %d", rowCount)
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	case "UPDATE":
		return fmt.Sprintf("UPDATE %d", rowCount)
	case "DELETE":
		return fmt.Sprintf("DELETE %d", rowCount)
	case "CREATE TABLE":
		return "CREATE TABLE"
	case "DROP TABLE":
		return "DROP TABLE"
	case "ALTER TABLE":
		return "ALTER TABLE"
	case "CREATE INDEX":
		return "CREATE INDEX"
	case "GRANT":
		return "GRANT"
	case "REVOKE":
		return "REVOKE"
	case "SHOW":
		return "SHOW"
	case "EXPLAIN":
		return "EXPLAIN"
	case "CREATE DATABASE":
		return "CREATE DATABASE"
	case "DROP DATABASE":
		return "DROP DATABASE"
	case "USE":
		return "USE"
	case "BEGIN":
		return "BEGIN"
	case "COMMIT":
		return "COMMIT"
	case "ROLLBACK":
		return "ROLLBACK"
	default:
		return "OK"
	}
}
