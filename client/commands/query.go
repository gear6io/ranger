package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/TFMV/icebox/client"
	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/query/parser"
	"github.com/rs/zerolog"
)

// QueryCommand handles SQL query execution
type QueryCommand struct {
	client *client.Client
	logger zerolog.Logger
}

// NewQueryCommand creates a new query command
func NewQueryCommand(client *client.Client, logger zerolog.Logger) *QueryCommand {
	return &QueryCommand{
		client: client,
		logger: logger,
	}
}

// Execute runs a SQL query
func (q *QueryCommand) Execute(ctx context.Context, query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New(ErrQueryEmpty, "query cannot be empty", nil)
	}

	// Auto-append semicolon if missing (internal only)
	originalQuery := query
	if !strings.HasSuffix(strings.TrimSpace(query), ";") {
		query = strings.TrimSpace(query) + ";"
		q.logger.Debug().Str("original_input", originalQuery).Str("modified_query", query).Msg("Auto-appended semicolon internally")
	}

	q.logger.Debug().Str("query", query).Msg("Executing SQL query")

	// Parse and format the query to show what will be executed
	formattedQuery, err := q.parseAndFormatQuery(query)
	if err != nil {
		q.logger.Warn().Err(err).Msg("Failed to parse/format query, proceeding with original")
		formattedQuery = query
	}

	// Show the formatted query that will be executed
	if formattedQuery != query {
		fmt.Printf("   %s\n\n", formattedQuery)
	}

	// Execute query via client
	result, err := q.client.ExecuteQuery(ctx, query)
	if err != nil {
		return errors.New(ErrQueryExecutionFailed, "failed to execute query", err)
	}

	// Display results
	return q.displayResults(result)
}

// displayResults displays query results in a formatted way
func (q *QueryCommand) displayResults(result *client.QueryResult) error {
	if result == nil {
		fmt.Println("Query executed successfully (no results)")
		return nil
	}

	// Calculate column widths for proper table formatting
	columnWidths := make([]int, len(result.Columns))
	for i, colName := range result.Columns {
		// Start with header width
		columnWidths[i] = len(colName)

		// Check data widths for this column
		for _, row := range result.Rows {
			if i < len(row) {
				var cellStr string
				if row[i] == nil {
					cellStr = "NULL"
				} else {
					cellStr = fmt.Sprintf("%v", row[i])
				}
				if len(cellStr) > columnWidths[i] {
					columnWidths[i] = len(cellStr)
				}
			}
		}
	}

	// Print column headers
	if len(result.Columns) > 0 {
		// Print top border
		fmt.Print("┌─")
		for i, width := range columnWidths {
			if i > 0 {
				fmt.Print("─┬─")
			}
			fmt.Print(strings.Repeat("─", width))
		}
		fmt.Println("─┐")

		// Print header row
		fmt.Print("│ ")
		for i, colName := range result.Columns {
			if i > 0 {
				fmt.Print(" │ ")
			}
			fmt.Printf("%-*s", columnWidths[i], colName)
		}
		fmt.Println(" │")

		// Print separator
		fmt.Print("├─")
		for i, width := range columnWidths {
			if i > 0 {
				fmt.Print("─┼─")
			}
			fmt.Print(strings.Repeat("─", width))
		}
		fmt.Println("─┤")
	}

	// Print data rows
	for _, row := range result.Rows {
		fmt.Print("│ ")
		for j, val := range row {
			if j > 0 {
				fmt.Print(" │ ")
			}
			var cellStr string
			if val == nil {
				cellStr = "NULL"
			} else {
				cellStr = fmt.Sprintf("%v", val)
			}
			fmt.Printf("%-*s", columnWidths[j], cellStr)
		}
		fmt.Println(" │")
	}

	// Print footer
	if len(result.Columns) > 0 {
		fmt.Print("└─")
		for i, width := range columnWidths {
			if i > 0 {
				fmt.Print("─┴─")
			}
			fmt.Print(strings.Repeat("─", width))
		}
		fmt.Println("─┘")
	}

	// Print summary
	fmt.Printf("\n📊 Query Results:\n")
	fmt.Printf("   Rows: %d\n", len(result.Rows))
	fmt.Printf("   Columns: %d\n", len(result.Columns))
	if result.Duration > 0 {
		fmt.Printf("   Duration: %v\n", result.Duration)
	}

	return nil
}

// parseAndFormatQuery parses the query and returns a formatted version
func (q *QueryCommand) parseAndFormatQuery(query string) (string, error) {
	// Parse the query
	ast, err := parser.Parse(query)
	if err != nil {
		return "", errors.New(ErrQueryParseFailed, "failed to parse query", err)
	}

	// Format the query
	formatted := parser.FormatQuery(ast)
	if formatted == "" {
		return "", errors.New(ErrQueryFormatFailed, "formatter returned empty string", nil)
	}

	return formatted, nil
}

// Explain shows query execution plan
func (q *QueryCommand) Explain(ctx context.Context, query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New(ErrQueryEmpty, "query cannot be empty", nil)
	}

	q.logger.Debug().Str("query", query).Msg("Explaining SQL query")

	// Execute explain query
	explainQuery := "EXPLAIN " + query
	result, err := q.client.ExecuteQuery(ctx, explainQuery)
	if err != nil {
		return errors.New(ErrQueryExplainFailed, "failed to explain query", err)
	}

	// Display explain results
	return q.displayResults(result)
}
