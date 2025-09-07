package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/c-bata/go-prompt"
	"github.com/gear6io/ranger/client"
	"github.com/gear6io/ranger/client/config"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func main() {
	// Initialize logger
	logger := setupLogger()

	// Set up global signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in a goroutine
	go func() {
		sig := <-sigChan
		logger.Info().Str("signal", sig.String()).Msg("Received signal, shutting down gracefully")
		os.Exit(0)
	}()

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "ranger-client",
		Short: "Data lakehouse client for connecting to data lakehouse server",
		Long: `Data lakehouse client provides command-line interface for connecting to data lakehouse server
and executing queries, importing data, and managing tables.

Examples:
ranger-client query "SELECT * FROM my_table"
ranger-client import data.parquet --table sales
ranger-client shell
ranger-client --server localhost:2849 query "SHOW TABLES"`,
	}

	// Add global flags
	var serverAddr string
	var username string
	var password string
	var database string
	var sslMode string

	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "localhost:2849", "server address")
	rootCmd.PersistentFlags().StringVar(&username, "user", "", "username")
	rootCmd.PersistentFlags().StringVar(&password, "password", "", "password")
	rootCmd.PersistentFlags().StringVar(&database, "database", "default", "database name")
	rootCmd.PersistentFlags().StringVar(&sslMode, "ssl", "disable", "SSL mode (disable, require, verify-ca, verify-full)")

	// Load client configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to load config, using defaults")
		cfg = config.DefaultConfig()
	}

	// Create client
	client, err := client.New(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create client")
		os.Exit(1)
	}
	defer client.Close()

	// Add subcommands
	rootCmd.AddCommand(
		createQueryCommand(client),
		createShellCommand(client),
		createImportCommand(client),
		createTableCommand(client),
		createCatalogCommand(client),
	)

	// Execute command
	if err := rootCmd.Execute(); err != nil {
		logger.Error().Err(err).Msg("Command failed")
		os.Exit(1)
	}
}

func setupLogger() zerolog.Logger {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("component", "ranger-client").
		Logger()

	return logger
}

func createQueryCommand(client *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Execute a SQL query",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := args[0]

			// Validate server connection before executing query
			ctx := context.Background()
			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect to server: %w", err)
			}

			// Execute query with context (no timeout)
			result, err := client.ExecuteQuery(ctx, query)
			if err != nil {
				// Provide better error categorization
				errMsg := err.Error()
				if strings.Contains(errMsg, "server exception") {
					return fmt.Errorf("server error: %s", errMsg)
				} else if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "network") {
					return fmt.Errorf("connection error: %s", errMsg)
				} else if strings.Contains(errMsg, "syntax") || strings.Contains(errMsg, "parse") {
					return fmt.Errorf("syntax error: %s", errMsg)
				} else if strings.Contains(errMsg, "table") || strings.Contains(errMsg, "database") {
					return fmt.Errorf("object error: %s", errMsg)
				} else {
					return fmt.Errorf("query execution failed: %s", errMsg)
				}
			}

			// Display results
			return displayQueryResults(result)
		},
	}

	return cmd
}

func createShellCommand(client *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Start interactive SQL shell",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Connect to server
			ctx := context.Background()
			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect to server: %w", err)
			}

			// Start interactive shell
			return startInteractiveShell(client)
		},
	}

	return cmd
}

// displayQueryResults displays query results in a formatted way
func displayQueryResults(result *client.QueryResult) error {
	if result == nil {
		fmt.Println("Query executed successfully (no results)")
		return nil
	}

	// Check if this is an empty result set (successful query with no data)
	if len(result.Columns) == 0 && len(result.Rows) == 0 {
		fmt.Println("✅ Query executed successfully")
		fmt.Println("📊 Result: Empty result set (no data returned)")
		if result.Duration > 0 {
			fmt.Printf("⏱️  Duration: %v\n", result.Duration)
		}
		return nil
	}

	// Check if we have columns but no rows (successful query with structure but no data)
	if len(result.Columns) > 0 && len(result.Rows) == 0 {
		fmt.Println("✅ Query executed successfully")
		fmt.Printf("📊 Result: Query returned %d columns but no rows\n", len(result.Columns))
		fmt.Printf("📋 Columns: %s\n", strings.Join(result.Columns, ", "))
		if result.Duration > 0 {
			fmt.Printf("⏱️  Duration: %v\n", result.Duration)
		}
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

	// Print column headers for data results
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

	// Print footer for data results
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

	// Print summary with clean formatting
	fmt.Println()
	fmt.Println("📊 Query Results:")
	fmt.Printf("   Status: ✅ Success\n")
	fmt.Printf("   Rows: %d\n", len(result.Rows))
	fmt.Printf("   Columns: %d\n", len(result.Columns))
	if result.Duration > 0 {
		fmt.Printf("   Duration: %v\n", result.Duration)
	}

	return nil
}

// startInteractiveShell starts an interactive SQL shell with proper arrow key support and command history
func startInteractiveShell(client *client.Client) error {
	fmt.Println("🧊 Data Lakehouse Interactive Shell")
	fmt.Println("==========================")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Type 'help' for available commands")
	fmt.Println("Use ↑↓ arrow keys to navigate command history")
	fmt.Println("Use ←→ arrow keys to edit current line")
	fmt.Println("Press Ctrl+C to cancel long-running queries")
	fmt.Println()

	// Set up signal handling for Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Command history
	var history []string

	// Create a completer for basic SQL commands
	completer := func(d prompt.Document) []prompt.Suggest {
		suggestions := []prompt.Suggest{
			{Text: "SHOW", Description: "Show databases, tables, or columns"},
			{Text: "CREATE", Description: "Create database, table, or other objects"},
			{Text: "DROP", Description: "Drop database, table, or other objects"},
			{Text: "SELECT", Description: "Query data from tables"},
			{Text: "INSERT", Description: "Insert data into tables"},
			{Text: "UPDATE", Description: "Update data in tables"},
			{Text: "DELETE", Description: "Delete data from tables"},
			{Text: "DESCRIBE", Description: "Describe table structure"},
			{Text: "EXPLAIN", Description: "Explain query execution plan"},
			{Text: "help", Description: "Show available commands"},
			{Text: "history", Description: "Show command history"},
			{Text: "clear", Description: "Clear screen"},
			{Text: "exit", Description: "Exit the shell"},
			{Text: "quit", Description: "Exit the shell"},
		}
		return prompt.FilterHasPrefix(suggestions, d.GetWordBeforeCursor(), true)
	}

	// Create the prompt
	p := prompt.New(
		executor(client, &history, sigChan),
		completer,
		prompt.OptionTitle("Data Lakehouse SQL Shell"),
		prompt.OptionPrefix("ranger> "),
		prompt.OptionInputTextColor(prompt.Yellow),
		prompt.OptionPrefixTextColor(prompt.Blue),
		prompt.OptionSuggestionTextColor(prompt.Green),
		prompt.OptionSuggestionBGColor(prompt.DarkGray),
		prompt.OptionSelectedSuggestionTextColor(prompt.Black),
		prompt.OptionSelectedSuggestionBGColor(prompt.Turquoise),
		prompt.OptionDescriptionBGColor(prompt.DarkGray),
		prompt.OptionDescriptionTextColor(prompt.White),
		prompt.OptionSelectedDescriptionTextColor(prompt.Black),
		prompt.OptionSelectedDescriptionBGColor(prompt.Turquoise),
		prompt.OptionScrollbarThumbColor(prompt.DarkGray),
		prompt.OptionScrollbarBGColor(prompt.LightGray),
		prompt.OptionMaxSuggestion(20),
	)

	p.Run()
	return nil
}

// executor handles command execution and maintains history
func executor(client *client.Client, history *[]string, sigChan chan os.Signal) func(string) {
	return func(input string) {
		input = strings.TrimSpace(input)
		if input == "" {
			return
		}

		// Handle special commands
		switch strings.ToLower(input) {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			os.Exit(0)
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  exit, quit - Exit the shell")
			fmt.Println("  help       - Show this help")
			fmt.Println("  history    - Show command history")
			fmt.Println("  clear      - Clear screen")
			fmt.Println("  <SQL>      - Execute SQL query")
			return
		case "history":
			fmt.Println("Command History:")
			for i, cmd := range *history {
				fmt.Printf("  %d: %s\n", i+1, cmd)
			}
			return
		case "clear":
			// Clear screen
			fmt.Print("\033[H\033[2J")
			return
		}

		// Auto-append semicolon if missing for SQL queries (internal only)
		query := input
		if !strings.HasSuffix(strings.TrimSpace(input), ";") {
			query = strings.TrimSpace(input) + ";"
		}

		// Add to history (avoid duplicates) - store the original user input
		if len(*history) == 0 || (*history)[len(*history)-1] != input {
			*history = append(*history, input)
		}

		// Create a cancellable context for the query (no timeout)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Set up signal handling for this query
		go func() {
			select {
			case <-sigChan:
				fmt.Println("\n⚠️  Cancelling query...")
				cancel()
			case <-ctx.Done():
				// Context was cancelled by other means
			}
		}()

		// Parse and format the query to show what will be executed
		formattedQuery, err := parseAndFormatQuery(query)
		if err != nil {
			// Log warning but continue with original query
			fmt.Printf("❌ Warning: invalid query (%v)\n", err)
			return
		}

		// Show the formatted query that will be executed
		if formattedQuery != query {
			fmt.Printf("   %s\n\n", formattedQuery)
		}

		// Execute SQL query with context
		result, err := client.ExecuteQuery(ctx, query)
		if err != nil {
			if ctx.Err() == context.Canceled {
				fmt.Println("❌ Query cancelled")
			} else {
				// Provide better error categorization
				errMsg := err.Error()
				if strings.Contains(errMsg, "server exception") {
					fmt.Printf("❌ Server Error: %s\n", errMsg)
				} else if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "network") {
					fmt.Printf("❌ Connection Error: %s\n", errMsg)
				} else if strings.Contains(errMsg, "syntax") || strings.Contains(errMsg, "parse") {
					fmt.Printf("❌ Syntax Error: %s\n", errMsg)
				} else if strings.Contains(errMsg, "table") || strings.Contains(errMsg, "database") {
					fmt.Printf("❌ Object Error: %s\n", errMsg)
				} else {
					fmt.Printf("❌ Query Error: %s\n", errMsg)
				}
			}
			return
		}

		// Display results with proper formatting
		if err := displayQueryResults(result); err != nil {
			fmt.Printf("❌ Error displaying results: %v\n", err)
		}

		// Ensure proper spacing after results
		fmt.Println()
	}
}

func createImportCommand(client *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import [file]",
		Short: "Import data from file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file := args[0]
			table, _ := cmd.Flags().GetString("table")
			namespace, _ := cmd.Flags().GetString("namespace")
			overwrite, _ := cmd.Flags().GetBool("overwrite")

			// Use simple context for import operation
			ctx := context.Background()
			return client.ImportFile(ctx, file, table, namespace, overwrite)
		},
	}

	cmd.Flags().String("table", "", "target table name")
	cmd.Flags().String("namespace", "default", "namespace")
	cmd.Flags().Bool("overwrite", false, "overwrite existing table")

	return cmd
}

func createTableCommand(client *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table",
		Short: "Table management commands",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List tables",
			RunE: func(cmd *cobra.Command, args []string) error {
				// Use simple context
				ctx := context.Background()
				tables, err := client.ListTables(ctx)
				if err != nil {
					return err
				}

				// Display tables
				fmt.Printf("📋 Tables:\n")
				if len(tables) == 0 {
					fmt.Printf("   No tables found\n")
				} else {
					for _, table := range tables {
						fmt.Printf("   - %s\n", table)
					}
				}
				return nil
			},
		},
		&cobra.Command{
			Use:   "describe [table]",
			Short: "Describe table structure",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				table := args[0]

				// Use simple context
				ctx := context.Background()
				schema, err := client.DescribeTable(ctx, table)
				if err != nil {
					return err
				}

				// Display schema
				fmt.Printf("📋 Table: %s\n", table)
				fmt.Printf("📊 Schema:\n")
				if len(schema.Columns) == 0 {
					fmt.Printf("   No columns found\n")
				} else {
					for _, column := range schema.Columns {
						fmt.Printf("   - %s: %s\n", column.Name, column.Type)
					}
				}
				return nil
			},
		},
		&cobra.Command{
			Use:   "drop [table]",
			Short: "Drop table",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				table := args[0]

				// Use simple context
				ctx := context.Background()
				return client.DropTable(ctx, table)
			},
		},
	)

	return cmd
}

func createCatalogCommand(client *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Catalog management commands",
	}

	cmd.AddCommand(
		&cobra.Command{
			Use:   "namespaces",
			Short: "List namespaces",
			RunE: func(cmd *cobra.Command, args []string) error {
				// Use simple context
				ctx := context.Background()
				namespaces, err := client.ListNamespaces(ctx)
				if err != nil {
					return err
				}

				// Display namespaces
				fmt.Printf("📋 Namespaces:\n")
				if len(namespaces) == 0 {
					fmt.Printf("   No namespaces found\n")
				} else {
					for _, namespace := range namespaces {
						fmt.Printf("   - %s\n", namespace)
					}
				}
				return nil
			},
		},
		&cobra.Command{
			Use:   "create-namespace [name]",
			Short: "Create namespace",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				namespace := args[0]

				// Use simple context
				ctx := context.Background()
				return client.CreateNamespace(ctx, namespace)
			},
		},
		&cobra.Command{
			Use:   "drop-namespace [name]",
			Short: "Drop namespace",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				namespace := args[0]

				// Use simple context
				ctx := context.Background()
				return client.DropNamespace(ctx, namespace)
			},
		},
	)

	return cmd
}

// parseAndFormatQuery parses the query and returns a formatted version
func parseAndFormatQuery(query string) (string, error) {
	// Parse the query
	ast, err := parser.Parse(query)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	// Format the query
	formatted := parser.FormatQuery(ast)
	if formatted == "" {
		return "", fmt.Errorf("formatter returned empty string")
	}

	return formatted, nil
}
