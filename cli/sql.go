package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/TFMV/icebox/engine/duckdb"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var sqlCmd = &cobra.Command{
	Use:   "sql [query]",
	Short: "Execute SQL queries against Iceberg tables",
	Long: `Execute SQL queries against your Iceberg tables using DuckDB.

The SQL engine automatically discovers and registers all tables from your
Icebox catalog, making them available for querying.

Table Naming:
  Tables can be referenced in two ways:
  1. Using just the table name: SELECT * FROM sales
  2. Using namespace_table format: SELECT * FROM default_sales
  Note: The dot notation (default.sales) is not supported by DuckDB.

Examples:
  icebox sql "SELECT COUNT(*) FROM sales"
  icebox sql "SELECT COUNT(*) FROM default_sales"
  icebox sql "SELECT region, SUM(amount) FROM sales GROUP BY region"
  icebox sql "SHOW TABLES"
  icebox sql "DESCRIBE sales"`,
	Args: cobra.ExactArgs(1),
	RunE: runSQL,
}

type sqlOptions struct {
	format       string
	maxRows      int
	showSchema   bool
	timing       bool
	autoRegister bool
}

var sqlOpts = &sqlOptions{}

func init() {
	rootCmd.AddCommand(sqlCmd)

	sqlCmd.Flags().StringVar(&sqlOpts.format, "format", "table", "output format: table, csv, json")
	sqlCmd.Flags().IntVar(&sqlOpts.maxRows, "max-rows", 1000, "maximum number of rows to display")
	sqlCmd.Flags().BoolVar(&sqlOpts.showSchema, "show-schema", false, "show column schema information")
	sqlCmd.Flags().BoolVar(&sqlOpts.timing, "timing", true, "show query execution time")
	sqlCmd.Flags().BoolVar(&sqlOpts.autoRegister, "auto-register", true, "automatically register catalog tables")
	sqlCmd.Flags().Bool("metrics", false, "show engine performance metrics after query")
}

func runSQL(cmd *cobra.Command, args []string) error {
	query := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "sql").Str("query", query).Msg("Starting SQL query execution")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration")
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd.Flag("verbose") != nil && cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		d.Info("Your catalog may be corrupted. Try backing up and running 'icebox init' again")
		return err
	}
	defer catalog.Close()

	// Create SQL engine with enhanced configuration
	engineConfig := duckdb.DefaultEngineConfig()
	if cmd.Flag("verbose").Value.String() == "true" {
		engineConfig.EnableQueryLog = true
	}

	engine, err := duckdb.NewEngineWithConfig(catalog, engineConfig)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Err(err).Msg("Failed to create SQL engine")
		}
		d.Error("Failed to create SQL engine: %v", err)
		d.Info("This might be a DuckDB installation issue")
		return err
	}
	defer engine.Close()

	// Auto-register tables if enabled
	if sqlOpts.autoRegister {
		if err := autoRegisterTablesWithDisplay(ctx, engine, catalog, d, logger); err != nil {
			// Don't fail the query if auto-registration fails, just warn
			if logger != nil {
				logger.Warn().Str("cmd", "sql").Err(err).Msg("Failed to auto-register some tables")
			}
			d.Warning("Failed to auto-register some tables: %v", err)
		}
	}

	// Execute the query
	start := time.Now()
	result, err := engine.ExecuteQuery(ctx, query)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Str("query", query).Err(err).Msg("Query execution failed")
		}
		// Enhanced error handling with helpful suggestions
		if strings.Contains(err.Error(), "timeout") {
			d.Error("Query timed out: %v", err)
			d.Info("Try simplifying your query or increasing --timeout")
		} else if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			d.Error("Table not found: %v", err)
			d.Info("Run 'icebox sql \"SHOW TABLES\"' to see available tables")
		} else {
			d.Error("Query failed: %v", err)
		}
		return err
	}
	duration := time.Since(start)

	if logger != nil {
		logger.Info().Str("cmd", "sql").Str("query", query).Dur("duration", duration).Int64("rows", result.RowCount).Msg("Query executed successfully")
	}

	// Display results
	if err := displayResultsWithDisplay(result, duration, d); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Err(err).Msg("Failed to display results")
		}
		d.Error("Failed to display results: %v", err)
		return err
	}

	// Show metrics if requested
	showMetrics, _ := cmd.Flags().GetBool("metrics")
	if showMetrics {
		metrics := engine.GetMetrics()
		d.Info("Engine Metrics:")
		d.Info("  Queries Executed: %d", metrics.QueriesExecuted)
		d.Info("  Tables Registered: %d", metrics.TablesRegistered)
		d.Info("  Cache Hits: %d", metrics.CacheHits)
		d.Info("  Cache Misses: %d", metrics.CacheMisses)
		d.Info("  Total Query Time: %v", metrics.TotalQueryTime)
		d.Info("  Error Count: %d", metrics.ErrorCount)
		if metrics.QueriesExecuted > 0 {
			avgTime := metrics.TotalQueryTime / time.Duration(metrics.QueriesExecuted)
			d.Info("  Average Query Time: %v", avgTime)
		}
	}

	return nil
}

// autoRegisterTablesWithDisplay automatically registers all catalog tables with the SQL engine
func autoRegisterTablesWithDisplay(ctx context.Context, engine *duckdb.Engine, catalog catalog.CatalogInterface, d display.Display, logger *zerolog.Logger) error {
	// Get all namespaces
	namespaces, err := catalog.ListNamespaces(ctx, nil)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "sql").Err(err).Msg("Failed to list namespaces")
		}
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	if len(namespaces) == 0 {
		d.Info("No namespaces found in catalog")
		d.Info("Try running 'icebox import <file.parquet> --table <table_name>' to create a table")
		return nil
	}

	d.Info("Found %d namespaces: %v", len(namespaces), namespaces)

	registeredCount := 0
	var errors []string

	for _, namespace := range namespaces {
		d.Info("Checking namespace '%s' for tables...", strings.Join(namespace, "."))

		// List tables in this namespace
		var tableCount int
		for identifier, err := range catalog.ListTables(ctx, namespace) {
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to list tables in namespace %v: %v", namespace, err))
				continue
			}

			tableCount++
			d.Info("Found table: %s", strings.Join(identifier, "."))

			// Load the table
			icebergTable, err := catalog.LoadTable(ctx, identifier, nil)
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to load table %v: %v", identifier, err))
				continue
			}

			// Register with the SQL engine
			if err := engine.RegisterTable(ctx, identifier, icebergTable); err != nil {
				errors = append(errors, fmt.Sprintf("failed to register table %v: %v", identifier, err))
				continue
			}

			d.Success("Successfully registered table: %s", strings.Join(identifier, "."))
			registeredCount++
		}

		if tableCount == 0 {
			d.Info("No tables found in namespace '%s'", strings.Join(namespace, "."))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("registration errors: %s", strings.Join(errors, "; "))
	}

	if registeredCount > 0 {
		d.Info("Registered %d tables for querying", registeredCount)
	} else {
		d.Info("No tables found to register")
		d.Info("Try running 'icebox table list' to see what tables exist in your catalog")
	}

	if logger != nil {
		logger.Info().Str("cmd", "sql").Int("registered_count", registeredCount).Msg("Auto-registration completed")
	}

	return nil
}

// Legacy function for backward compatibility
func autoRegisterTables(ctx context.Context, engine *duckdb.Engine, catalog catalog.CatalogInterface) error {
	d := display.New()
	return autoRegisterTablesWithDisplay(ctx, engine, catalog, d, nil)
}

// displayResultsWithDisplay displays query results in the specified format
func displayResultsWithDisplay(result *duckdb.QueryResult, duration time.Duration, d display.Display) error {
	// Show timing if enabled
	if sqlOpts.timing {
		d.Info("Query [%s] executed in %v", result.QueryID, result.Duration)
	}

	// Show row count
	if result.RowCount == 0 {
		d.Info("No rows returned")
		return nil
	}

	// Handle large result sets with user-friendly messaging
	if result.RowCount >= 100000 {
		d.Warning("Large result set detected (%d rows) - performance may vary", result.RowCount)
	}

	d.Info("%d rows returned", result.RowCount)

	// Show schema if requested
	if sqlOpts.showSchema {
		d.Info("Schema:")
		for i, col := range result.Columns {
			d.Info("  %d. %s", i+1, col)
		}
	}

	// Limit rows if necessary
	rows := result.Rows
	if int64(len(rows)) > int64(sqlOpts.maxRows) {
		rows = rows[:sqlOpts.maxRows]
		d.Warning("Showing first %d rows (use --max-rows to adjust)", sqlOpts.maxRows)
	}

	// Display results based on format
	switch sqlOpts.format {
	case "table":
		return displayTableFormatWithDisplay(result.Columns, rows, d)
	case "csv":
		return displayCSVFormatWithDisplay(result.Columns, rows, d)
	case "json":
		return displayJSONFormatWithDisplay(result.Columns, rows, d)
	default:
		d.Error("Unsupported format: %s", sqlOpts.format)
		return fmt.Errorf("unsupported format: %s", sqlOpts.format)
	}
}

// displayTableFormatWithDisplay displays results in a formatted table using display package
func displayTableFormatWithDisplay(columns []string, rows [][]interface{}, d display.Display) error {
	if len(rows) == 0 {
		return nil
	}

	tableData := display.TableData{
		Headers: columns,
		Rows:    rows,
	}

	return d.Table(tableData).Render()
}

// Legacy function for backward compatibility
func displayTableFormat(columns []string, rows [][]interface{}) error {
	d := display.New()
	return displayTableFormatWithDisplay(columns, rows, d)
}

// displayCSVFormatWithDisplay displays results in CSV format using display package
func displayCSVFormatWithDisplay(columns []string, rows [][]interface{}, d display.Display) error {
	tableData := display.TableData{
		Headers: columns,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatCSV).Render()
}

// Legacy function for backward compatibility
func displayCSVFormat(columns []string, rows [][]interface{}) error {
	d := display.New()
	return displayCSVFormatWithDisplay(columns, rows, d)
}

// displayJSONFormatWithDisplay displays results in JSON format using display package
func displayJSONFormatWithDisplay(columns []string, rows [][]interface{}, d display.Display) error {
	tableData := display.TableData{
		Headers: columns,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatJSON).Render()
}

// Legacy function for backward compatibility
func displayJSONFormat(columns []string, rows [][]interface{}) error {
	d := display.New()
	return displayJSONFormatWithDisplay(columns, rows, d)
}

// formatValue formats a value for display
func formatValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}

// formatValueCSV formats a value for CSV output
func formatValueCSV(value interface{}) string {
	if value == nil {
		return ""
	}
	str := fmt.Sprintf("%v", value)
	// Escape newlines and other special characters for cleaner CSV output
	str = strings.ReplaceAll(str, "\n", "\\n")
	str = strings.ReplaceAll(str, "\r", "\\r")
	str = strings.ReplaceAll(str, "\t", "\\t")
	// Escape quotes and wrap in quotes if contains comma or quotes
	if strings.Contains(str, ",") || strings.Contains(str, "\"") || strings.Contains(str, "\\") {
		str = strings.ReplaceAll(str, "\"", "\"\"")
		str = "\"" + str + "\""
	}
	return str
}

// truncateString truncates a string to the specified length
func truncateString(str string, maxLen int) string {
	if len(str) <= maxLen {
		return str
	}
	if maxLen <= 3 {
		return str[:maxLen]
	}
	return str[:maxLen-3] + "..."
}
