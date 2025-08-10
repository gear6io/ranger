package jdbc

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

// Server represents a JDBC server for icebox
type Server struct {
	logger zerolog.Logger
	server net.Listener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new JDBC server
func NewServer(logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		logger: logger.With().Str("component", "jdbc-server").Logger(),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start starts the JDBC server
func (s *Server) Start(ctx context.Context) error {
	if !config.JDBC_SERVER_ENABLED {
		s.logger.Info().Msg("JDBC server is disabled")
		return nil
	}

	// Use fixed port and address from config constants
	port := config.JDBC_SERVER_PORT
	addr := fmt.Sprintf("%s:%d", config.DEFAULT_SERVER_ADDRESS, port)
	s.logger.Info().Str("address", addr).Msg("Starting JDBC server")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.server = listener

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	s.logger.Info().Msg("JDBC server started successfully")
	return nil
}

// Stop stops the JDBC server
func (s *Server) Stop() error {
	s.logger.Info().Msg("Stopping JDBC server")

	s.cancel()

	if s.server != nil {
		if err := s.server.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing server listener")
		}
	}

	// Wait for all connections to close
	s.wg.Wait()

	s.logger.Info().Msg("JDBC server stopped")
	return nil
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled": config.JDBC_SERVER_ENABLED,
		"address": config.DEFAULT_SERVER_ADDRESS,
		"port":    config.JDBC_SERVER_PORT,
	}
}

// acceptConnections accepts incoming connections
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := s.server.Accept()
			if err != nil {
				if s.ctx.Err() != nil {
					// Server is shutting down
					return
				}
				s.logger.Error().Err(err).Msg("Error accepting connection")
				continue
			}

			// Handle connection in a new goroutine
			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

// handleConnection handles a JDBC connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.wg.Done()
		conn.Close()
	}()

	s.logger.Debug().Str("remote_addr", conn.RemoteAddr().String()).Msg("New JDBC connection")

	// TODO: Implement actual JDBC protocol handling
	// For now, just log the connection and close it
	s.logger.Info().Str("remote_addr", conn.RemoteAddr().String()).Msg("JDBC connection established (protocol handling pending)")
}
