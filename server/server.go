package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/protocols/http"
	"github.com/TFMV/icebox/server/protocols/jdbc"
	"github.com/rs/zerolog"
)

// Server represents the main icebox server
type Server struct {
	config     *config.Config
	logger     zerolog.Logger
	httpServer *http.Server
	jdbcServer *jdbc.Server
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// New creates a new icebox server
func New(cfg *config.Config, logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create HTTP server
	httpServer, err := http.NewServer(&cfg.HTTP, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create HTTP server: %w", err)
	}

	// Create JDBC server
	jdbcServer, err := jdbc.NewServer(&cfg.JDBC, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create JDBC server: %w", err)
	}

	return &Server{
		config:     cfg,
		logger:     logger,
		httpServer: httpServer,
		jdbcServer: jdbcServer,
		wg:         sync.WaitGroup{},
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting icebox server...")

	// Start HTTP server if enabled
	if s.config.HTTP.Enabled {
		if err := s.startHTTPServer(); err != nil {
			return fmt.Errorf("failed to start HTTP server: %w", err)
		}
	}

	// Start JDBC server if enabled
	if s.config.JDBC.Enabled {
		if err := s.startJDBCServer(); err != nil {
			return fmt.Errorf("failed to start JDBC server: %w", err)
		}
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	s.logger.Info().Msg("Shutting down server...")
	return s.Shutdown()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() error {
	s.logger.Info().Msg("Initiating graceful shutdown...")

	// Cancel context
	s.cancel()

	// Stop HTTP server
	if s.httpServer != nil {
		if err := s.httpServer.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to stop HTTP server")
		}
	}

	// Stop JDBC server
	if s.jdbcServer != nil {
		if err := s.jdbcServer.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to stop JDBC server")
		}
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info().Msg("Graceful shutdown completed")
	case <-time.After(30 * time.Second):
		s.logger.Warn().Msg("Graceful shutdown timed out")
	}

	return nil
}

// startHTTPServer starts the HTTP server
func (s *Server) startHTTPServer() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Start(s.ctx); err != nil {
			s.logger.Error().Err(err).Msg("HTTP server error")
		}
	}()
	return nil
}

// startJDBCServer starts the JDBC server
func (s *Server) startJDBCServer() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.jdbcServer.Start(s.ctx); err != nil {
			s.logger.Error().Err(err).Msg("JDBC server error")
		}
	}()
	return nil
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"start_time": time.Now(),
		"uptime":     time.Since(time.Now()),
	}

	if s.httpServer != nil {
		status["http"] = s.httpServer.GetStatus()
	}

	if s.jdbcServer != nil {
		status["jdbc"] = s.jdbcServer.GetStatus()
	}

	return status
}
