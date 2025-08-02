package http

import (
	"context"
	"fmt"
	"net"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

// Server represents an HTTP server for icebox
type Server struct {
	config *config.HTTPConfig
	logger zerolog.Logger
	server *net.Listener
}

// NewServer creates a new HTTP server
func NewServer(cfg *config.HTTPConfig, logger zerolog.Logger) (*Server, error) {
	return &Server{
		config: cfg,
		logger: logger,
	}, nil
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	if !s.config.Enabled {
		s.logger.Info().Msg("HTTP server disabled")
		return nil
	}

	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	s.logger.Info().Str("address", addr).Msg("Starting HTTP server")

	// TODO: Implement actual HTTP server using Fiber
	// For now, just log that we would start the server
	s.logger.Info().Msg("HTTP server would start here (implementation pending)")

	return nil
}

// Stop stops the HTTP server
func (s *Server) Stop() error {
	s.logger.Info().Msg("Stopping HTTP server")
	// TODO: Implement actual server shutdown
	return nil
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled": s.config.Enabled,
		"address": s.config.Address,
		"port":    s.config.Port,
	}
}
