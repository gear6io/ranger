package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"bufio"

	"github.com/TFMV/icebox/client"
	"github.com/TFMV/icebox/client/config"
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
  icebox-client --server localhost:8080 query "SHOW TABLES"`,
	}

	// Add global flags
	var serverAddr string
	var username string
	var password string
	var database string
	var sslMode string

	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "localhost:8080", "server address")
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
			result, err := client.ExecuteQuery(context.Background(), query)
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

// startInteractiveShell starts an interactive SQL shell
func startInteractiveShell(client *client.Client) error {
	fmt.Println("🧊 Icebox Interactive Shell")
	fmt.Println("==========================")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Type 'help' for available commands")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("icebox> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Handle special commands
		switch strings.ToLower(line) {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return nil
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  exit, quit - Exit the shell")
			fmt.Println("  help       - Show this help")
			fmt.Println("  <SQL>      - Execute SQL query")
			continue
		}

		// Execute SQL query
		result, err := client.ExecuteQuery(context.Background(), line)
		if err != nil {
			fmt.Printf("❌ Error: %v\n", err)
			continue
		}

		if err := displayQueryResults(result); err != nil {
			fmt.Printf("❌ Error displaying results: %v\n", err)
		}
	}

	return scanner.Err()
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
				return client.ListTables(context.Background())
			},
		},
		&cobra.Command{
			Use:   "describe [table]",
			Short: "Describe table structure",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				table := args[0]
				return client.DescribeTable(context.Background(), table)
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
				return client.ListNamespaces(context.Background())
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
