package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TFMV/icebox/server"
	"github.com/TFMV/icebox/server/config"
)

func main() {
	// Load server configuration first
	cfg, err := config.LoadConfig("icebox-server.yml")
	if err != nil {
		// Try default config if file not found
		cfg = config.LoadDefaultConfig()
	}

	// Initialize logger with configuration
	logger, err := config.SetupLogger(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to setup logger: %v", err))
	}

	if err != nil {
		logger.Info().Msg("Using default configuration")
	}

	// Create server instance
	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create server")
		os.Exit(1)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		logger.Info().Msg("Shutting down icebox server...")
		cancel()
	}()

	// Start server
	logger.Info().Msg("Starting icebox server...")
	if err := srv.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Server failed")
		os.Exit(1)
	}

	// Wait for shutdown signal
	<-ctx.Done()

	// Graceful shutdown
	if err := srv.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("Error during shutdown")
	}

	logger.Info().Msg("Server stopped gracefully")
}
