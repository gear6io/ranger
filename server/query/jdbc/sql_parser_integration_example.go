package jdbc

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ExampleIntegration demonstrates how to integrate SQLParserMiddleware with JDBCHandler
func ExampleIntegration() {
	// Create a logger
	logger := log.With().Str("component", "jdbc_handler").Logger()

	// Create the SQL parser middleware
	sqlMiddleware := NewSQLParserMiddleware(logger)

	// Example 1: Basic query analysis
	query := "SELECT * FROM users WHERE active = true"
	analysis, err := sqlMiddleware.AnalyzeQuery(query)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to analyze query")
		return
	}

	logger.Info().
		Str("statement_type", analysis.StatementType).
		Str("complexity", analysis.Complexity).
		Int("table_count", len(analysis.Tables)).
		Int("column_count", len(analysis.Columns)).
		Msg("Query analysis completed")

	// Example 2: Query validation
	err = sqlMiddleware.ValidateQuery(analysis)
	if err != nil {
		logger.Error().Err(err).Msg("Query blocked by security policy")
		return
	}

	logger.Info().Msg("Query validation passed")

	// Example 3: Generate command complete tag
	tag := sqlMiddleware.GenerateCommandCompleteTag(analysis, 42)
	logger.Info().Str("command_tag", tag).Msg("Command completed")

	// Example 4: Integration with JDBCHandler
	// This shows how the middleware would be used within the handler
	handler := &JDBCHandler{
		sqlMiddleware: sqlMiddleware,
		// ... other fields
	}

	// The handler can now use the middleware for all query processing
	_ = handler
}

// ExampleMiddlewareUsage demonstrates standalone middleware usage
func ExampleMiddlewareUsage() {
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create the SQL parser middleware
	sqlMiddleware := NewSQLParserMiddleware(logger)

	// Analyze a complex query
	complexQuery := `
		SELECT u.name, COUNT(p.id) as post_count
		FROM users u
		LEFT JOIN posts p ON u.id = p.user_id
		WHERE u.active = true
		GROUP BY u.id, u.name
		HAVING COUNT(p.id) > 5
		ORDER BY post_count DESC
		LIMIT 10
	`

	analysis, err := sqlMiddleware.AnalyzeQuery(complexQuery)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to analyze complex query")
		return
	}

	// Log detailed analysis
	logger.Info().
		Str("statement_type", analysis.StatementType).
		Str("complexity", analysis.Complexity).
		Bool("has_joins", analysis.HasJoins).
		Bool("has_aggregations", analysis.HasAggregations).
		Bool("has_subqueries", analysis.HasSubqueries).
		Strs("tables", analysis.Tables).
		Strs("columns", analysis.Columns).
		Strs("optimization_hints", analysis.OptimizationHints).
		Msg("Complex query analysis completed")

	// Validate the query
	err = sqlMiddleware.ValidateQuery(analysis)
	if err != nil {
		logger.Error().Err(err).Msg("Complex query blocked")
		return
	}

	logger.Info().Msg("Complex query is allowed")
}
