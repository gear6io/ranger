package native

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

// Server represents a native protocol server
type Server struct {
	logger zerolog.Logger
	server net.Listener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new native protocol server
func NewServer(logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		logger: logger.With().Str("component", "native-server").Logger(),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start starts the native protocol server
func (s *Server) Start(ctx context.Context) error {
	if !config.NATIVE_SERVER_ENABLED {
		s.logger.Info().Msg("Native server is disabled")
		return nil
	}

	// Use fixed port and address from config constants
	port := config.NATIVE_SERVER_PORT
	addr := fmt.Sprintf("%s:%d", config.DEFAULT_SERVER_ADDRESS, port)
	s.logger.Info().Str("address", addr).Msg("Starting native protocol server")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.server = listener

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	s.logger.Info().Msg("Native protocol server started successfully")
	return nil
}

// Stop stops the native protocol server
func (s *Server) Stop() error {
	s.logger.Info().Msg("Stopping native protocol server")

	s.cancel()

	if s.server != nil {
		if err := s.server.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing server listener")
		}
	}

	// Wait for all connections to close
	s.wg.Wait()

	s.logger.Info().Msg("Native protocol server stopped")
	return nil
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

			// Handle each connection in a goroutine
			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.wg.Done()
	}()

	clientAddr := conn.RemoteAddr().String()
	s.logger.Debug().Str("client", clientAddr).Msg("New client connected")

	// Create a new connection handler
	handler := NewConnectionHandler(conn, s.logger)

	// Handle the connection
	if err := handler.Handle(); err != nil {
		s.logger.Error().Err(err).Str("client", clientAddr).Msg("Error handling connection")
	}

	s.logger.Debug().Str("client", clientAddr).Msg("Client disconnected")
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled": config.NATIVE_SERVER_ENABLED,
		"address": config.DEFAULT_SERVER_ADDRESS,
		"port":    config.NATIVE_SERVER_PORT,
	}
}
