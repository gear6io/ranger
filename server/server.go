package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/protocols/http"
	"github.com/TFMV/icebox/server/protocols/jdbc"
	"github.com/TFMV/icebox/server/protocols/native"
	"github.com/rs/zerolog"
)

// Server represents the main server that manages all protocol servers
type Server struct {
	config       *config.Config
	logger       zerolog.Logger
	httpServer   *http.Server
	jdbcServer   *jdbc.Server
	nativeServer *native.Server
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	startTime    time.Time
}

// New creates a new server instance
func New(cfg *config.Config, logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create HTTP server
	httpServer, err := http.NewServer(logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create HTTP server: %w", err)
	}

	// Create JDBC server
	jdbcServer, err := jdbc.NewServer(logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create JDBC server: %w", err)
	}

	// Create native server
	nativeServer, err := native.NewServer(logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create native server: %w", err)
	}

	return &Server{
		config:       cfg,
		logger:       logger.With().Str("component", "server").Logger(),
		httpServer:   httpServer,
		jdbcServer:   jdbcServer,
		nativeServer: nativeServer,
		wg:           sync.WaitGroup{},
		ctx:          ctx,
		cancel:       cancel,
		startTime:    time.Now(),
	}, nil
}

// Start starts all protocol servers
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting Icebox server...")

	// Start HTTP server if enabled
	if config.HTTP_SERVER_ENABLED {
		s.logger.Info().Msg("Starting HTTP server")
		if err := s.httpServer.Start(ctx); err != nil {
			return fmt.Errorf("failed to start HTTP server: %w", err)
		}
		s.logger.Info().Msg("HTTP server started")
	}

	// Start JDBC server if enabled
	if config.JDBC_SERVER_ENABLED {
		s.logger.Info().Msg("Starting JDBC server")
		if err := s.jdbcServer.Start(ctx); err != nil {
			return fmt.Errorf("failed to start JDBC server: %w", err)
		}
		s.logger.Info().Msg("JDBC server started")
	}

	// Start native protocol server if enabled
	if config.NATIVE_SERVER_ENABLED {
		s.logger.Info().Msg("Starting native protocol server")
		if err := s.nativeServer.Start(ctx); err != nil {
			return fmt.Errorf("failed to start native protocol server: %w", err)
		}
		s.logger.Info().Msg("Native protocol server started")
	}

	// Log all server statuses
	s.logger.Info().
		Bool("http_enabled", config.HTTP_SERVER_ENABLED).
		Str("http_address", config.DEFAULT_SERVER_ADDRESS).
		Int("http_port", s.config.GetHTTPPort()).
		Bool("jdbc_enabled", config.JDBC_SERVER_ENABLED).
		Str("jdbc_address", config.DEFAULT_SERVER_ADDRESS).
		Int("jdbc_port", s.config.GetJDBCPort()).
		Bool("native_enabled", config.NATIVE_SERVER_ENABLED).
		Str("native_address", config.DEFAULT_SERVER_ADDRESS).
		Int("native_port", s.config.GetNativePort()).
		Msg("All servers started")

	return nil
}

// Shutdown gracefully shuts down all servers
func (s *Server) Shutdown() error {
	s.logger.Info().Msg("Shutting down server...")

	s.cancel()

	// Stop HTTP server
	if s.httpServer != nil {
		if err := s.httpServer.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Error stopping HTTP server")
		}
	}

	// Stop JDBC server
	if s.jdbcServer != nil {
		if err := s.jdbcServer.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Error stopping JDBC server")
		}
	}

	// Stop native server
	if s.nativeServer != nil {
		if err := s.nativeServer.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Error stopping native server")
		}
	}

	// Wait for all servers to stop
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info().Msg("Graceful shutdown completed")
	case <-time.After(30 * time.Second):
		s.logger.Warn().Msg("Shutdown timeout, forcing close")
	}

	return nil
}

// GetUptime returns the server uptime
func (s *Server) GetUptime() time.Duration {
	return time.Since(s.startTime)
}

// GetStatus returns the server status
func (s *Server) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"uptime":         s.GetUptime().String(),
		"start_time":     s.startTime,
		"http_enabled":   config.HTTP_SERVER_ENABLED,
		"jdbc_enabled":   config.JDBC_SERVER_ENABLED,
		"native_enabled": config.NATIVE_SERVER_ENABLED,
	}
}
