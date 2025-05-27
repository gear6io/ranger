package cli

import (
	"context"

	"github.com/TFMV/icebox/display"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "icebox",
	Short: "A single-binary playground for Apache Iceberg",
	Long: `Icebox is a single-binary playground for Apache Iceberg that provides
a "five-minutes-to-first-query" experience for table format experimentation.

It includes an embedded SQLite catalog, local filesystem storage,
and DuckDB integration for SQL queries.`,
	Version: "0.1.0",
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

// ExecuteWithContext runs the root command with context containing display and logger
func ExecuteWithContext(ctx context.Context) error {
	// Set context on root command so it's available to all subcommands
	rootCmd.SetContext(ctx)

	// Log the command execution
	if logger := getLoggerFromContext(ctx); logger != nil {
		logger.Info().Str("cmd", "root").Msg("Executing root command")
	}

	return rootCmd.Execute()
}

// getLoggerFromContext retrieves the logger from context
func getLoggerFromContext(ctx context.Context) *zerolog.Logger {
	if logger, ok := ctx.Value("logger").(zerolog.Logger); ok {
		return &logger
	}
	return nil
}

// getDisplayFromContext retrieves the display instance from context
func getDisplayFromContext(ctx context.Context) display.Display {
	return display.GetDisplayOrDefault(ctx)
}

func init() {
	// Global flags can be added here
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "verbose output")
}
