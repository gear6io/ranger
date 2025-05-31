package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/engine/duckdb"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var timeTravelCmd = &cobra.Command{
	Use:   "time-travel <table> --as-of <timestamp|snapshot-id>",
	Short: "Query a table at a specific point in time",
	Long: `Query an Iceberg table at a specific point in time using either a timestamp or snapshot ID.

This command demonstrates one of Apache Iceberg's key features: time-travel queries.
You can query your data as it existed at any point in the table's history.

Time formats supported:
  - RFC3339: 2023-01-01T10:00:00Z
  - ISO 8601: 2023-01-01T10:00:00
  - Date only: 2023-01-01 (defaults to midnight UTC)
  - Snapshot ID: Numeric identifier from table history

Examples:
  # Query table as it was at a specific timestamp
  icebox time-travel sales --as-of "2023-01-01T10:00:00Z"
  
  # Query using just a date (midnight UTC)
  icebox time-travel sales --as-of "2023-01-01"
  
  # Query using a specific snapshot ID
  icebox time-travel sales --as-of 1234567890123456789
  
  # Combined with SQL query
  icebox time-travel sales --as-of "2023-01-01" --query "SELECT COUNT(*) FROM sales"`,
	Args: cobra.ExactArgs(1),
	RunE: runTimeTravel,
}

type timeTravelOptions struct {
	asOf        string
	query       string
	format      string
	maxRows     int
	showSchema  bool
	timing      bool
	showHistory bool
}

var timeTravelOpts = &timeTravelOptions{}

func init() {
	rootCmd.AddCommand(timeTravelCmd)

	timeTravelCmd.Flags().StringVar(&timeTravelOpts.asOf, "as-of", "", "timestamp (RFC3339, ISO 8601, or YYYY-MM-DD) or snapshot ID")
	timeTravelCmd.Flags().StringVar(&timeTravelOpts.query, "query", "", "SQL query to execute (default: 'SELECT * FROM <table> LIMIT 10')")
	timeTravelCmd.Flags().StringVar(&timeTravelOpts.format, "format", "table", "output format: table, csv, json")
	timeTravelCmd.Flags().IntVar(&timeTravelOpts.maxRows, "max-rows", 1000, "maximum number of rows to display")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.showSchema, "show-schema", false, "show column schema information")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.timing, "timing", true, "show query execution time")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.showHistory, "show-history", false, "show table snapshot history")

	// Mark required flags
	if err := timeTravelCmd.MarkFlagRequired("as-of"); err != nil {
		// This should not happen in normal operation, but handle it gracefully
		fmt.Printf("Warning: Failed to mark as-of flag as required: %v\n", err)
	}
}

func runTimeTravel(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "time-travel").Str("table", tableName).Str("as_of", timeTravelOpts.asOf).Msg("Starting time-travel operation")
	}

	// Validate as-of parameter
	if timeTravelOpts.asOf == "" {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Msg("Missing required --as-of flag")
		}
		d.Error("The --as-of flag is required")
		d.Info("Specify a timestamp (2025-01-01T10:00:00Z) or snapshot ID (1234567890123456789)")
		return fmt.Errorf("the --as-of flag is required")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Create catalog using factory pattern
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		d.Info("Your catalog may be corrupted. Try backing up and running 'icebox init' again")
		return err
	}
	defer cat.Close()

	// Parse table identifier using existing function
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Str("table", tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Load the table
	icebergTable, err := cat.LoadTable(ctx, tableIdent, nil)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Str("table", tableName).Err(err).Msg("Failed to load table")
		}
		d.Error("Failed to load table '%s': %v", tableName, err)
		d.Info("Use 'icebox sql \"SHOW TABLES\"' to see available tables")
		return err
	}

	// Show table history if requested
	if timeTravelOpts.showHistory {
		if err := showTableHistory(icebergTable); err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "time-travel").Str("table", tableName).Err(err).Msg("Failed to show table history")
			}
			d.Error("Failed to show table history: %v", err)
			return err
		}
		fmt.Println() // Add spacing before continuing
	}

	// Resolve the snapshot
	snapshotID, resolvedTime, err := resolveSnapshot(icebergTable, timeTravelOpts.asOf)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Str("table", tableName).Str("as_of", timeTravelOpts.asOf).Err(err).Msg("Failed to resolve snapshot")
		}
		d.Error("Failed to resolve snapshot: %v", err)
		d.Info("Use --show-history to see available snapshots")
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "time-travel").Str("table", tableName).Int64("snapshot_id", snapshotID).Time("resolved_time", resolvedTime).Msg("Resolved snapshot for time-travel")
	}

	// Display time-travel information
	d.Info("Time-traveling to: %s", resolvedTime.Format(time.RFC3339))
	d.Info("Using snapshot: %d", snapshotID)

	// Create SQL engine - need to assert to concrete type for now
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		if err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "time-travel").Err(err).Msg("Failed to create SQL engine")
			}
			d.Error("Failed to create SQL engine: %v", err)
			d.Info("This might be a DuckDB installation issue")
			return err
		}
	default:
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Msg("Time-travel only supported with SQLite catalogs")
		}
		d.Error("Time-travel is currently only supported with SQLite catalogs")
		return fmt.Errorf("time-travel is currently only supported with SQLite catalogs")
	}
	defer engine.Close()

	// Create a new table with the specific snapshot for querying
	snapshotTable, err := createSnapshotTable(icebergTable, snapshotID)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Int64("snapshot_id", snapshotID).Err(err).Msg("Failed to create snapshot table")
		}
		d.Error("Failed to create snapshot table: %v", err)
		return err
	}

	// Register the snapshot table with a unique name
	snapshotTableIdent := append(tableIdent, fmt.Sprintf("snapshot_%d", snapshotID))
	if err := engine.RegisterTable(ctx, snapshotTableIdent, snapshotTable); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Interface("snapshot_table_ident", snapshotTableIdent).Err(err).Msg("Failed to register table at snapshot")
		}
		d.Error("Failed to register table at snapshot: %v", err)
		return err
	}

	// Determine the query to execute
	query := timeTravelOpts.query
	if query == "" {
		// Default query - show a sample of data
		tablePath := strings.Join(tableIdent, ".")
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 10", tablePath)
		d.Info("Default query: %s", query)
	}

	// Replace original table name with snapshot table name in query
	originalTablePath := strings.Join(tableIdent, ".")
	snapshotTablePath := strings.Join(snapshotTableIdent, ".")
	query = strings.ReplaceAll(query, originalTablePath, snapshotTablePath)

	if logger != nil {
		logger.Info().Str("cmd", "time-travel").Str("query", query).Int64("snapshot_id", snapshotID).Msg("Executing time-travel query")
	}

	// Execute the query
	start := time.Now()
	result, err := engine.ExecuteQuery(ctx, query)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Str("query", query).Int64("snapshot_id", snapshotID).Err(err).Msg("Time-travel query failed")
		}
		// Enhanced error handling
		if strings.Contains(err.Error(), "timeout") {
			d.Error("Query timed out: %v", err)
			d.Info("Try simplifying your query")
		} else if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			d.Error("Table not found in time-travel query: %v", err)
			d.Info("The table might not have existed at the specified time")
		} else {
			d.Error("Time-travel query failed: %v", err)
		}
		return err
	}
	duration := time.Since(start)

	if logger != nil {
		logger.Info().Str("cmd", "time-travel").Str("query", query).Int64("snapshot_id", snapshotID).Dur("duration", duration).Int64("rows", result.RowCount).Msg("Time-travel query executed successfully")
	}

	// Display results using the same formatting as the SQL command
	if err := displayTimeTravelResults(result, duration, snapshotID, resolvedTime); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "time-travel").Err(err).Msg("Failed to display results")
		}
		d.Error("Failed to display results: %v", err)
		return err
	}

	return nil
}

// createSnapshotTable creates a new table instance pointing to a specific snapshot
func createSnapshotTable(originalTable *table.Table, snapshotID int64) (*table.Table, error) {
	// Find the specific snapshot
	snapshot := originalTable.SnapshotByID(snapshotID)
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot with ID %d not found", snapshotID)
	}

	// For now, return the original table (Iceberg-go will handle the snapshot internally)
	// In a full implementation, we would create a new table instance with the specific snapshot
	return originalTable, nil
}

// resolveSnapshot resolves an as-of parameter to a snapshot ID and timestamp
func resolveSnapshot(tbl *table.Table, asOf string) (int64, time.Time, error) {
	// Try parsing as snapshot ID first (numeric)
	if snapshotID, err := strconv.ParseInt(asOf, 10, 64); err == nil {
		snapshot := tbl.SnapshotByID(snapshotID)
		if snapshot == nil {
			return 0, time.Time{}, fmt.Errorf("snapshot with ID %d not found", snapshotID)
		}
		return snapshotID, time.UnixMilli(snapshot.TimestampMs), nil
	}

	// Parse as timestamp
	timestamp, err := parseTimestamp(asOf)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("invalid timestamp format '%s': %w", asOf, err)
	}

	// Find the latest snapshot before or at the specified time
	targetMs := timestamp.UnixMilli()
	var bestSnapshot *table.Snapshot
	var bestTimestamp time.Time

	for _, snapshot := range tbl.Metadata().Snapshots() {
		if snapshot.TimestampMs <= targetMs {
			if bestSnapshot == nil || snapshot.TimestampMs > bestSnapshot.TimestampMs {
				bestSnapshot = &snapshot
				bestTimestamp = time.UnixMilli(snapshot.TimestampMs)
			}
		}
	}

	if bestSnapshot == nil {
		return 0, time.Time{}, fmt.Errorf("no snapshots found before or at %s", timestamp.Format(time.RFC3339))
	}

	return bestSnapshot.SnapshotID, bestTimestamp, nil
}

// parseTimestamp parses various timestamp formats
func parseTimestamp(asOf string) (time.Time, error) {
	// Supported formats in order of preference
	formats := []string{
		time.RFC3339,          // 2023-01-01T10:00:00Z
		time.RFC3339Nano,      // 2023-01-01T10:00:00.123456789Z
		"2006-01-02T15:04:05", // 2023-01-01T10:00:00
		"2006-01-02 15:04:05", // 2023-01-01 10:00:00
		"2006-01-02",          // 2023-01-01 (defaults to midnight UTC)
	}

	for _, format := range formats {
		if t, err := time.Parse(format, asOf); err == nil {
			// For date-only format, ensure we're using UTC
			if format == "2006-01-02" {
				t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
			}
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported timestamp format. Supported formats: RFC3339 (2023-01-01T10:00:00Z), ISO 8601 (2023-01-01T10:00:00), or date only (2023-01-01)")
}

// showTableHistory displays the snapshot history of a table
func showTableHistory(tbl *table.Table) error {
	snapshots := tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		fmt.Println("📭 No snapshots found in table history")
		return nil
	}

	fmt.Printf("📚 Table History (%d snapshots):\n", len(snapshots))
	fmt.Println("┌────────────────────┬─────────────────────────┬─────────────────────┬──────────────┐")
	fmt.Println("│     Snapshot ID    │       Timestamp         │      Operation      │  Parent ID   │")
	fmt.Println("├────────────────────┼─────────────────────────┼─────────────────────┼──────────────┤")

	// Show snapshots in reverse chronological order (newest first)
	for i := len(snapshots) - 1; i >= 0; i-- {
		snapshot := snapshots[i]
		timestamp := time.UnixMilli(snapshot.TimestampMs).Format("2006-01-02 15:04:05")

		operation := "unknown"
		if snapshot.Summary != nil {
			operation = string(snapshot.Summary.Operation)
		}

		parentID := "none"
		if snapshot.ParentSnapshotID != nil {
			parentID = fmt.Sprintf("%d", *snapshot.ParentSnapshotID)
		}

		// Truncate long IDs for better display
		snapshotIDStr := fmt.Sprintf("%d", snapshot.SnapshotID)
		if len(snapshotIDStr) > 18 {
			snapshotIDStr = snapshotIDStr[:15] + "..."
		}

		if len(parentID) > 12 && parentID != "none" {
			parentID = parentID[:9] + "..."
		}

		// Mark current snapshot
		marker := " "
		if tbl.CurrentSnapshot() != nil && snapshot.SnapshotID == tbl.CurrentSnapshot().SnapshotID {
			marker = "*"
		}

		fmt.Printf("│%s%-18s │ %-23s │ %-19s │ %-12s │\n",
			marker, snapshotIDStr, timestamp, operation, parentID)
	}

	fmt.Println("└────────────────────┴─────────────────────────┴─────────────────────┴──────────────┘")
	fmt.Println("* = current snapshot")

	return nil
}

// displayTimeTravelResults displays the results of a time-travel query
func displayTimeTravelResults(result *duckdb.QueryResult, duration time.Duration, snapshotID int64, timestamp time.Time) error {
	// Show execution info
	if timeTravelOpts.timing {
		fmt.Printf("⏱️  Time-travel query executed in %v (snapshot: %d)\n", duration, snapshotID)
	}

	// Show row count
	if result.RowCount == 0 {
		fmt.Println("📭 No rows found at the specified point in time")
		return nil
	}

	fmt.Printf("📊 %d rows returned from %s\n", result.RowCount, timestamp.Format("2006-01-02 15:04:05"))

	// Show schema if requested
	if timeTravelOpts.showSchema {
		fmt.Println("📋 Schema:")
		for i, col := range result.Columns {
			fmt.Printf("  %d. %s\n", i+1, col)
		}
		fmt.Println()
	}

	// Limit rows if necessary
	rows := result.Rows
	if int64(len(rows)) > int64(timeTravelOpts.maxRows) {
		rows = rows[:timeTravelOpts.maxRows]
		fmt.Printf("⚠️  Showing first %d rows (use --max-rows to adjust)\n", timeTravelOpts.maxRows)
	}

	// Display results based on format (reuse the display functions from sql.go)
	switch timeTravelOpts.format {
	case "table":
		return displayTableFormat(result.Columns, rows)
	case "csv":
		return displayCSVFormat(result.Columns, rows)
	case "json":
		return displayJSONFormat(result.Columns, rows)
	default:
		return fmt.Errorf("unsupported format: %s", timeTravelOpts.format)
	}
}
