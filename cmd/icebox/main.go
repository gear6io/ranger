package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/TFMV/icebox/cli"
	"github.com/TFMV/icebox/display"
	"github.com/rs/zerolog"
)

// Context key types to avoid collisions
type contextKey string

const (
	loggerKey contextKey = "logger"
)

func main() {
	// Initialize logger
	logger := setupLogger()

	// Initialize display system
	displayInstance := setupDisplay()

	// Create context with display and logger
	ctx := context.Background()
	ctx = display.WithDisplay(ctx, displayInstance)
	ctx = context.WithValue(ctx, loggerKey, logger)

	// Log application start
	logger.Info().Str("cmd", "main").Msg("Starting Icebox CLI")

	// Execute CLI with context
	if err := cli.ExecuteWithContext(ctx); err != nil {
		logger.Error().Str("cmd", "main").Err(err).Msg("CLI execution failed")
		os.Exit(1)
	}

	logger.Info().Str("cmd", "main").Msg("Icebox CLI completed successfully")
}

// setupLogger initializes zerolog with file output
func setupLogger() zerolog.Logger {
	// Determine log file location
	logFile := getLogFilePath()

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create log directory: %v\n", err)
		os.Exit(1)
	}

	// Open log file for appending
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
		os.Exit(1)
	}

	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger := zerolog.New(file).With().
		Timestamp().
		Str("app", "icebox").
		Logger()

	return logger
}

// setupDisplay initializes the display system with configuration
func setupDisplay() display.Display {
	// Load display configuration
	config, err := display.LoadConfig()
	if err != nil {
		// Use default config if loading fails
		config = display.DefaultConfig()
	}

	// Create display instance with config
	return display.NewWithConfig(config)
}

// getLogFilePath determines the log file path
func getLogFilePath() string {
	// First try to find project root by looking for .icebox.yml
	if projectRoot := findProjectRoot(); projectRoot != "" {
		return filepath.Join(projectRoot, "icebox.log")
	}

	// Fallback to current directory
	cwd, err := os.Getwd()
	if err != nil {
		// Last resort: use temp directory
		return filepath.Join(os.TempDir(), "icebox.log")
	}

	return filepath.Join(cwd, "icebox.log")
}

// findProjectRoot searches for .icebox.yml to determine project root
func findProjectRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		configPath := filepath.Join(dir, ".icebox.yml")
		if _, err := os.Stat(configPath); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			break
		}
		dir = parent
	}

	return ""
}
