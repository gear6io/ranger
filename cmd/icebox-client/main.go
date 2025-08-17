package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/TFMV/icebox/client"
	"github.com/TFMV/icebox/client/config"
	"github.com/c-bata/go-prompt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func main() {
	// Initialize logger
	logger := setupLogger()

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "icebox-client",
		Short: "Icebox client for connecting to icebox server",
		Long: `Icebox client provides command-line interface for connecting to icebox server
and executing SQL queries, managing tables, and importing data.

Examples:
  icebox-client query "SELECT * FROM my_table"
  icebox-client import data.parquet --table sales
  icebox-client shell
  icebox-client --server localhost:2849 query "SHOW TABLES"`,
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
		Str("component", "icebox-client").
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

			result, err := client.ExecuteQuery(ctx, query)
			if err != nil {
				return err
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
		Short: "Start interactive shell",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate server connection before starting shell
			ctx := context.Background()
			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect to server: %w", err)
			}

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

	// Print column headers for data results
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

	// Print footer for data results
	if len(result.Columns) > 0 {
		headers := strings.Join(result.Columns, " | ")
		fmt.Printf("└─%s─┘\n", strings.Repeat("─", len(headers)))
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
	fmt.Println("🧊 Icebox Interactive Shell")
	fmt.Println("==========================")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Type 'help' for available commands")
	fmt.Println("Use ↑↓ arrow keys to navigate command history")
	fmt.Println("Use ←→ arrow keys to edit current line")
	fmt.Println()

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
		executor(client, &history),
		completer,
		prompt.OptionTitle("Icebox SQL Shell"),
		prompt.OptionPrefix("icebox> "),
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
func executor(client *client.Client, history *[]string) func(string) {
	return func(input string) {
		input = strings.TrimSpace(input)
		if input == "" {
			return
		}

		// Add to history (avoid duplicates)
		if len(*history) == 0 || (*history)[len(*history)-1] != input {
			*history = append(*history, input)
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

		// Execute SQL query
		result, err := client.ExecuteQuery(context.Background(), input)
		if err != nil {
			fmt.Printf("❌ Error: %v\n", err)
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

			return client.ImportFile(context.Background(), file, table, namespace, overwrite)
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
				tables, err := client.ListTables(context.Background())
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
				schema, err := client.DescribeTable(context.Background(), table)
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
				return client.DropTable(context.Background(), table)
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
				namespaces, err := client.ListNamespaces(context.Background())
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
				return client.CreateNamespace(context.Background(), namespace)
			},
		},
		&cobra.Command{
			Use:   "drop-namespace [name]",
			Short: "Drop namespace",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				namespace := args[0]
				return client.DropNamespace(context.Background(), namespace)
			},
		},
	)

	return cmd
}
