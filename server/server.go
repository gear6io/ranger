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

	// Create native server
	nativeServer, err := native.NewServer(&cfg.Native, logger)
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

	// Start HTTP server
	if s.config.HTTP.Enabled {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.httpServer.Start(ctx); err != nil {
				s.logger.Error().Err(err).Msg("HTTP server error")
			}
		}()
	}

	// Start JDBC server
	if s.config.JDBC.Enabled {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.jdbcServer.Start(ctx); err != nil {
				s.logger.Error().Err(err).Msg("JDBC server error")
			}
		}()
	}

	// Start native server
	if s.config.Native.Enabled {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.nativeServer.Start(ctx); err != nil {
				s.logger.Error().Err(err).Msg("Native server error")
			}
		}()
	}

	s.logger.Info().
		Bool("http_enabled", s.config.HTTP.Enabled).
		Bool("jdbc_enabled", s.config.JDBC.Enabled).
		Bool("native_enabled", s.config.Native.Enabled).
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
		"http_enabled":   s.config.HTTP.Enabled,
		"jdbc_enabled":   s.config.JDBC.Enabled,
		"native_enabled": s.config.Native.Enabled,
	}
}
