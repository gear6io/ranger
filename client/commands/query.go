package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/TFMV/icebox/client"
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
		return fmt.Errorf("query cannot be empty")
	}

	q.logger.Debug().Str("query", query).Msg("Executing SQL query")

	// Execute query via client
	result, err := q.client.ExecuteQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
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

	// Print column headers
	if len(result.Columns) > 0 {
		headers := strings.Join(result.Columns, " | ")
		fmt.Printf("┌─%s─┐\n", strings.Repeat("─", len(headers)))
		fmt.Printf("│ %s │\n", headers)
		fmt.Printf("├─%s─┤\n", strings.Repeat("─", len(headers)))
	}

	// Print data rows
	for _, row := range result.Rows {
		rowStr := make([]string, len(row))
		for j, val := range row {
			if val == nil {
				rowStr[j] = "NULL"
			} else {
				rowStr[j] = fmt.Sprintf("%v", val)
			}
		}
		fmt.Printf("│ %s │\n", strings.Join(rowStr, " | "))
	}

	// Print footer
	if len(result.Columns) > 0 {
		headers := strings.Join(result.Columns, " | ")
		fmt.Printf("└─%s─┘\n", strings.Repeat("─", len(headers)))
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

// Explain shows query execution plan
func (q *QueryCommand) Explain(ctx context.Context, query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query cannot be empty")
	}

	q.logger.Debug().Str("query", query).Msg("Explaining SQL query")

	// Execute explain query
	explainQuery := fmt.Sprintf("EXPLAIN %s", query)
	result, err := q.client.ExecuteQuery(ctx, explainQuery)
	if err != nil {
		return fmt.Errorf("failed to explain query: %w", err)
	}

	// Display explain results
	return q.displayResults(result)
}
