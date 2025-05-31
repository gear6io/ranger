package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/TFMV/icebox/engine/duckdb"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Start an interactive SQL shell",
	Long: `Start an interactive SQL shell for querying your Iceberg tables.

The shell provides a REPL (Read-Eval-Print Loop) interface with:
- Command history
- Multi-line queries  
- Built-in shortcuts and help
- Automatic table registration
- Query timing and statistics

Special commands:
  \help, \h     - Show help
  \tables, \t   - List all tables
  \schema <tbl> - Show table schema  
  \history      - Show command history
  \timing       - Toggle query timing
  \clear        - Clear screen
  \quit, \q     - Exit shell

Examples:
  icebox> SELECT COUNT(*) FROM sales;
  icebox> \t
  icebox> DESCRIBE sales;`,
	RunE: runShell,
}

type shellState struct {
	engine       *duckdb.Engine
	catalog      *sqlite.Catalog
	history      []string
	showTiming   bool
	multiLine    bool
	buffer       []string
	config       *config.Config
	sessionStart time.Time
	display      display.Display
	logger       *zerolog.Logger
}

func init() {
	rootCmd.AddCommand(shellCmd)

	shellCmd.Flags().BoolP("timing", "t", true, "show query execution time")
	shellCmd.Flags().Bool("metrics", false, "show engine metrics on startup")
	shellCmd.Flags().Bool("query-log", false, "enable query logging")
}

func runShell(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "shell").Msg("Starting interactive SQL shell")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "shell").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "shell").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		d.Info("Your catalog may be corrupted. Try backing up and running 'icebox init' again")
		return err
	}
	defer catalog.Close()

	// Create SQL engine with enhanced configuration
	engineConfig := duckdb.DefaultEngineConfig()
	if cmd.Flag("verbose").Value.String() == "true" || cmd.Flag("query-log").Value.String() == "true" {
		engineConfig.EnableQueryLog = true
	}

	engine, err := duckdb.NewEngineWithConfig(catalog, engineConfig)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "shell").Err(err).Msg("Failed to create SQL engine")
		}
		d.Error("Failed to create SQL engine: %v", err)
		d.Info("This might be a DuckDB installation issue")
		return err
	}
	defer engine.Close()

	// Initialize shell state
	state := &shellState{
		engine:       engine,
		catalog:      catalog,
		history:      make([]string, 0),
		showTiming:   cmd.Flag("timing").Value.String() == "true",
		multiLine:    false,
		buffer:       make([]string, 0),
		config:       cfg,
		sessionStart: time.Now(),
		display:      d,
		logger:       logger,
	}

	// Welcome message with ASCII art
	fmt.Println(iceboxBanner())
	fmt.Println("🧊 Icebox SQL Shell v0.1.0")
	fmt.Println("Enterprise-grade SQL querying for Apache Iceberg")
	fmt.Println("Type \\help for help, \\quit to exit")

	// Auto-register tables
	if err := autoRegisterTables(ctx, engine, catalog); err != nil {
		fmt.Printf("⚠️  Warning: Failed to auto-register some tables: %v\n", err)
	}

	// Show metrics if requested
	showMetrics, _ := cmd.Flags().GetBool("metrics")
	if showMetrics {
		displayShellMetrics(state)
	}

	fmt.Println()

	// Start the shell loop
	return runShellLoop(state)
}

// runShellLoop runs the main shell REPL loop
func runShellLoop(state *shellState) error {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Show prompt
		prompt := "icebox> "
		if state.multiLine {
			prompt = "     -> "
		}
		fmt.Print(prompt)

		// Read input
		if !scanner.Scan() {
			if scanner.Err() != nil {
				return fmt.Errorf("error reading input: %w", scanner.Err())
			}
			// EOF (Ctrl+D)
			fmt.Println("Goodbye! 👋")
			return nil
		}

		line := strings.TrimSpace(scanner.Text())

		// Handle empty lines
		if line == "" {
			if state.multiLine && len(state.buffer) > 0 {
				// Empty line in multi-line mode - execute the query
				query := strings.Join(state.buffer, " ")
				state.buffer = state.buffer[:0]
				state.multiLine = false

				if err := executeShellCommand(state, query); err != nil {
					if state.logger != nil {
						state.logger.Error().Str("cmd", "shell").Str("query", query).Err(err).Msg("Query execution failed")
					}
					state.display.Error("Error: %v", err)
				}
			}
			continue
		}

		// Handle special commands
		if strings.HasPrefix(line, "\\") {
			if err := handleSpecialCommand(state, line); err != nil {
				if state.logger != nil {
					state.logger.Error().Str("cmd", "shell").Str("command", line).Err(err).Msg("Special command failed")
				}
				state.display.Error("Error: %v", err)
			}
			continue
		}

		// Add line to buffer
		state.buffer = append(state.buffer, line)

		// Check if query is complete (ends with semicolon)
		if strings.HasSuffix(line, ";") {
			query := strings.Join(state.buffer, " ")
			state.buffer = state.buffer[:0]
			state.multiLine = false

			if err := executeShellCommand(state, query); err != nil {
				if state.logger != nil {
					state.logger.Error().Str("cmd", "shell").Str("query", query).Err(err).Msg("Query execution failed")
				}
				state.display.Error("Error: %v", err)
			}
		} else {
			// Enter multi-line mode
			state.multiLine = true
		}
	}
}

// executeShellCommand executes a SQL query in the shell
func executeShellCommand(state *shellState, query string) error {
	// Remove trailing semicolon
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	if query == "" {
		return nil
	}

	// Add to history
	state.history = append(state.history, query)

	if state.logger != nil {
		state.logger.Info().Str("cmd", "shell").Str("query", query).Msg("Executing SQL query")
	}

	// Execute query
	start := time.Now()
	result, err := state.engine.ExecuteQuery(context.Background(), query)
	if err != nil {
		return err
	}
	duration := time.Since(start)

	if state.logger != nil {
		state.logger.Info().Str("cmd", "shell").Str("query", query).Dur("duration", duration).Int64("rows", result.RowCount).Msg("Query executed successfully")
	}

	// Display results
	if state.showTiming {
		fmt.Printf("⏱️  Query executed in %v\n", duration)
	}

	if result.RowCount == 0 {
		fmt.Println("📭 No rows returned")
		return nil
	}

	fmt.Printf("📊 %d rows returned\n", result.RowCount)

	// Display results in table format (limited for shell)
	rows := result.Rows
	maxRows := 100 // Limit for shell display
	if int64(len(rows)) > int64(maxRows) {
		rows = rows[:maxRows]
		fmt.Printf("⚠️  Showing first %d rows\n", maxRows)
	}

	return displayTableFormat(result.Columns, rows)
}

// handleSpecialCommand handles special shell commands starting with backslash
func handleSpecialCommand(state *shellState, command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "\\help", "\\h":
		showShellHelp()

	case "\\tables", "\\t":
		return showTables(state)

	case "\\schema", "\\s":
		if len(args) == 0 {
			return fmt.Errorf("usage: \\schema <table_name>")
		}
		return showTableSchema(state, args[0])

	case "\\history":
		showHistory(state)

	case "\\metrics", "\\m":
		displayShellMetrics(state)

	case "\\cache":
		return handleCacheCommand(state, args)

	case "\\performance", "\\perf":
		showPerformanceStats(state)

	case "\\timing":
		state.showTiming = !state.showTiming
		status := map[bool]string{true: "ON", false: "OFF"}[state.showTiming]
		fmt.Printf("Query timing is now %s\n", status)
		if state.logger != nil {
			state.logger.Info().Str("cmd", "shell").Str("operation", "toggle-timing").Bool("timing_enabled", state.showTiming).Msg("Toggled query timing")
		}

	case "\\clear", "\\c":
		// Clear screen (ANSI escape sequence)
		fmt.Print("\033[2J\033[H")

	case "\\status":
		showEngineStatus(state)

	case "\\quit", "\\q", "\\exit":
		// Show session summary before exit
		sessionDuration := time.Since(state.sessionStart)
		metrics := state.engine.GetMetrics()

		if state.logger != nil {
			state.logger.Info().Str("cmd", "shell").Str("operation", "exit").Dur("session_duration", sessionDuration).Int("commands_executed", len(state.history)).Int64("queries_executed", metrics.QueriesExecuted).Msg("Exiting shell session")
		}

		fmt.Printf("📊 Session Summary:\n")
		fmt.Printf("  Duration: %v\n", sessionDuration)
		fmt.Printf("  Commands executed: %d\n", len(state.history))
		fmt.Printf("  Queries executed: %d\n", metrics.QueriesExecuted)
		fmt.Println("Goodbye! 👋")
		os.Exit(0)

	default:
		return fmt.Errorf("unknown command: %s (type \\help for help)", cmd)
	}

	return nil
}

// showShellHelp displays help information
func showShellHelp() {
	fmt.Println(`🧊 Icebox SQL Shell Help

SQL Commands:
  SELECT * FROM table_name;     Query a table
  SHOW TABLES;                  List all available tables
  DESCRIBE table_name;          Show table schema
  
Special Commands:
  \help, \h                     Show this help
  \tables, \t                   List all tables
  \schema <table>               Show table schema
  \history                      Show command history
  \metrics, \m                  Show engine performance metrics
  \cache [clear|status]         Manage table cache
  \performance, \perf           Show detailed performance statistics
  \status                       Show engine status and configuration
  \timing                       Toggle query timing display
  \clear, \c                    Clear screen
  \quit, \q, \exit              Exit shell with session summary

Tips:
  - End SQL statements with semicolon (;)
  - Multi-line queries are supported
  - Press Enter on empty line to execute multi-line query
  - Use Ctrl+D to exit
  - Query IDs are shown for tracking and debugging
  - Cache improves performance for repeated table access`)
}

// showTables displays all available tables
func showTables(state *shellState) error {
	tables, err := state.engine.ListTables(context.Background())
	if err != nil {
		if state.logger != nil {
			state.logger.Error().Str("cmd", "shell").Str("operation", "list-tables").Err(err).Msg("Failed to list tables")
		}
		return fmt.Errorf("failed to list tables: %w", err)
	}

	if len(tables) == 0 {
		fmt.Println("📭 No tables found")
		return nil
	}

	fmt.Printf("📋 Available tables (%d):\n", len(tables))
	for i, table := range tables {
		fmt.Printf("  %d. %s\n", i+1, table)
	}

	if state.logger != nil {
		state.logger.Info().Str("cmd", "shell").Str("operation", "list-tables").Int("table_count", len(tables)).Msg("Listed tables")
	}

	return nil
}

// showTableSchema displays schema for a specific table
func showTableSchema(state *shellState, tableName string) error {
	result, err := state.engine.DescribeTable(context.Background(), tableName)
	if err != nil {
		if state.logger != nil {
			state.logger.Error().Str("cmd", "shell").Str("operation", "describe-table").Str("table", tableName).Err(err).Msg("Failed to describe table")
		}
		return fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	if state.logger != nil {
		state.logger.Info().Str("cmd", "shell").Str("operation", "describe-table").Str("table", tableName).Msg("Described table schema")
	}

	fmt.Printf("📋 Schema for table '%s':\n", tableName)
	return displayTableFormat(result.Columns, result.Rows)
}

// showHistory displays command history
func showHistory(state *shellState) {
	if len(state.history) == 0 {
		fmt.Println("📭 No command history")
		return
	}

	fmt.Printf("📜 Command history (%d commands):\n", len(state.history))
	for i, cmd := range state.history {
		fmt.Printf("  %d. %s\n", i+1, cmd)
	}
}

// displayShellMetrics shows engine performance metrics in the shell
func displayShellMetrics(state *shellState) {
	metrics := state.engine.GetMetrics()
	sessionDuration := time.Since(state.sessionStart)

	fmt.Printf("📈 Engine Metrics:\n")
	fmt.Printf("  Session Duration: %v\n", sessionDuration)
	fmt.Printf("  Queries Executed: %d\n", metrics.QueriesExecuted)
	fmt.Printf("  Tables Registered: %d\n", metrics.TablesRegistered)
	fmt.Printf("  Cache Hits: %d\n", metrics.CacheHits)
	fmt.Printf("  Cache Misses: %d\n", metrics.CacheMisses)
	fmt.Printf("  Total Query Time: %v\n", metrics.TotalQueryTime)
	fmt.Printf("  Error Count: %d\n", metrics.ErrorCount)

	if metrics.QueriesExecuted > 0 {
		avgTime := metrics.TotalQueryTime / time.Duration(metrics.QueriesExecuted)
		fmt.Printf("  Average Query Time: %v\n", avgTime)
		cacheHitRate := float64(metrics.CacheHits) / float64(metrics.CacheHits+metrics.CacheMisses) * 100
		fmt.Printf("  Cache Hit Rate: %.1f%%\n", cacheHitRate)
	}
}

// handleCacheCommand handles cache-related commands
func handleCacheCommand(state *shellState, args []string) error {
	if len(args) == 0 {
		// Show cache status
		metrics := state.engine.GetMetrics()
		fmt.Printf("🗃️  Cache Status:\n")
		fmt.Printf("  Cache Hits: %d\n", metrics.CacheHits)
		fmt.Printf("  Cache Misses: %d\n", metrics.CacheMisses)

		if metrics.CacheHits+metrics.CacheMisses > 0 {
			hitRate := float64(metrics.CacheHits) / float64(metrics.CacheHits+metrics.CacheMisses) * 100
			fmt.Printf("  Hit Rate: %.1f%%\n", hitRate)
		}
		return nil
	}

	switch args[0] {
	case "clear":
		state.engine.ClearTableCache()
		fmt.Println("✅ Table cache cleared")
		if state.logger != nil {
			state.logger.Info().Str("cmd", "shell").Str("operation", "cache-clear").Msg("Cleared table cache")
		}
	case "status":
		// Same as no args
		return handleCacheCommand(state, []string{})
	default:
		return fmt.Errorf("unknown cache command: %s (use: clear, status)", args[0])
	}

	return nil
}

// showPerformanceStats displays detailed performance statistics
func showPerformanceStats(state *shellState) {
	metrics := state.engine.GetMetrics()
	sessionDuration := time.Since(state.sessionStart)

	fmt.Printf("🚀 Performance Statistics:\n")
	fmt.Printf("  Session Duration: %v\n", sessionDuration)
	fmt.Printf("  Total Queries: %d\n", metrics.QueriesExecuted)
	fmt.Printf("  Query Errors: %d\n", metrics.ErrorCount)
	fmt.Printf("  Total Query Time: %v\n", metrics.TotalQueryTime)

	if metrics.QueriesExecuted > 0 {
		avgTime := metrics.TotalQueryTime / time.Duration(metrics.QueriesExecuted)
		errorRate := float64(metrics.ErrorCount) / float64(metrics.QueriesExecuted) * 100
		queriesPerSec := float64(metrics.QueriesExecuted) / sessionDuration.Seconds()

		fmt.Printf("  Average Query Time: %v\n", avgTime)
		fmt.Printf("  Error Rate: %.1f%%\n", errorRate)
		fmt.Printf("  Queries per Second: %.2f\n", queriesPerSec)
	}

	fmt.Printf("\nCache Performance:\n")
	fmt.Printf("  Tables Registered: %d\n", metrics.TablesRegistered)
	fmt.Printf("  Cache Hits: %d\n", metrics.CacheHits)
	fmt.Printf("  Cache Misses: %d\n", metrics.CacheMisses)

	if metrics.CacheHits+metrics.CacheMisses > 0 {
		hitRate := float64(metrics.CacheHits) / float64(metrics.CacheHits+metrics.CacheMisses) * 100
		fmt.Printf("  Cache Hit Rate: %.1f%%\n", hitRate)
	}
}

// showEngineStatus displays current engine status and configuration
func showEngineStatus(state *shellState) {
	fmt.Printf("⚙️  Engine Status:\n")
	fmt.Printf("  Engine: DuckDB with Arrow integration\n")

	// Display catalog information
	if state.config.Catalog.SQLite != nil {
		fmt.Printf("  Catalog: SQLite (%s)\n", state.config.Catalog.SQLite.Path)
	} else {
		fmt.Printf("  Catalog: %s\n", state.config.Catalog.Type)
	}

	fmt.Printf("  Query Timing: %s\n", map[bool]string{true: "ON", false: "OFF"}[state.showTiming])
	fmt.Printf("  Multi-line Mode: %s\n", map[bool]string{true: "ON", false: "OFF"}[state.multiLine])

	if len(state.buffer) > 0 {
		fmt.Printf("  Current Buffer: %d lines\n", len(state.buffer))
	}

	fmt.Printf("  Command History: %d commands\n", len(state.history))

	// Show available tables count
	tables, err := state.engine.ListTables(context.Background())
	if err == nil {
		fmt.Printf("  Available Tables: %d\n", len(tables))
	}
}

// iceboxBanner returns the ASCII art banner for Icebox
func iceboxBanner() string {
	return `
  _________
 /        /|
/________/ |
|        | |
|   🧊   | /
|________|/
I C E B O X`
}
