package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/loader"
	"github.com/rs/zerolog"
)

// Server represents the main Icebox server
type Server struct {
	config    *config.Config
	loader    *loader.Loader
	logger    zerolog.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	startTime time.Time
}

// New creates a new server instance
func New(cfg *config.Config, logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create Loader which initializes all components
	loaderInstance, err := loader.NewLoader(cfg, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create loader: %w", err)
	}

	return &Server{
		config:    cfg,
		loader:    loaderInstance,
		logger:    logger.With().Str("component", "server").Logger(),
		wg:        sync.WaitGroup{},
		ctx:       ctx,
		cancel:    cancel,
		startTime: time.Now(),
	}, nil
}

// Start starts all protocol servers
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting Icebox server...")

	// Start the Loader which will start all components including servers
	if err := s.loader.Start(); err != nil {
		return fmt.Errorf("failed to start loader: %w", err)
	}

	s.logger.Info().Msg("Icebox server started successfully")
	return nil
}

// Shutdown gracefully shuts down all servers
func (s *Server) Shutdown() error {
	s.logger.Info().Msg("Shutting down server...")

	s.cancel()

	// Stop the Loader which will stop all components including servers
	if s.loader != nil {
		if err := s.loader.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Error stopping loader")
		}
	}

	// Wait for all components to stop
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
	status := map[string]interface{}{
		"uptime":     s.GetUptime().String(),
		"start_time": s.startTime,
	}

	// Add Loader status which includes all component statuses
	if s.loader != nil {
		status["loader"] = s.loader.GetStatus()
	}

	return status
}
