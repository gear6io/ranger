package native

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/protocols/native/middleware"
	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// Server represents the native protocol server
type Server struct {
	queryEngine *query.Engine
	logger      zerolog.Logger
	server      net.Listener
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	// Middleware system
	middlewareChain *middleware.Chain
	connectionPool  *middleware.ConnectionPool
	authMiddleware  *middleware.AuthMiddleware
	circuitBreaker  *middleware.CircuitBreaker
}

// NewServer creates a new native server instance
func NewServer(queryEngine *query.Engine, logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create middleware components
	connectionPool := middleware.NewConnectionPool(
		100,           // max connections
		5*time.Minute, // idle timeout
		1*time.Minute, // cleanup interval
		logger,
	)

	// Create simple auth provider
	authProvider := middleware.NewSimpleAuthProvider(
		1*time.Hour, // token TTL
		logger,
	)

	authMiddleware := middleware.NewAuthMiddleware(
		authProvider,
		true,           // enabled
		false,          // require auth (false for development)
		1*time.Hour,    // token timeout
		30*time.Minute, // cache TTL
		logger,
	)

	// Create circuit breaker with resource thresholds
	thresholds := middleware.ResourceThresholds{
		MaxMemoryBytes:       100 * 1024 * 1024, // 100MB
		MaxCPUTime:           30 * time.Second,  // 30 seconds
		MaxQueryDuration:     60 * time.Second,  // 1 minute
		MaxRowsProcessed:     1000000,           // 1M rows
		MaxConcurrentQueries: 10,                // 10 concurrent queries per connection
	}

	circuitBreaker := middleware.NewCircuitBreaker(
		thresholds,
		5,              // failure threshold
		30*time.Second, // recovery timeout
		logger,
	)

	// Create middleware chain
	middlewareChain := middleware.NewChain(
		connectionPool,
		authMiddleware,
		circuitBreaker,
	)

	server := &Server{
		queryEngine:     queryEngine,
		logger:          logger.With().Str("component", "native-server").Logger(),
		ctx:             ctx,
		cancel:          cancel,
		middlewareChain: middlewareChain,
		connectionPool:  connectionPool,
		authMiddleware:  authMiddleware,
		circuitBreaker:  circuitBreaker,
	}

	return server, nil
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

	// Close middleware components
	if s.connectionPool != nil {
		if err := s.connectionPool.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing connection pool")
		}
	}

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

	// Create a new connection handler with the QueryEngine and middleware chain
	handler := NewConnectionHandler(conn, s.queryEngine, s.logger, s.middlewareChain)

	// Handle the connection
	if err := handler.Handle(); err != nil {
		s.logger.Error().Err(err).Str("client", clientAddr).Msg("Error handling connection")
	}

	s.logger.Debug().Str("client", clientAddr).Msg("Client disconnected")
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"enabled": config.NATIVE_SERVER_ENABLED,
		"address": config.DEFAULT_SERVER_ADDRESS,
		"port":    config.NATIVE_SERVER_PORT,
	}

	// Add middleware statistics
	if s.connectionPool != nil {
		status["connection_pool"] = s.connectionPool.GetStats()
	}

	if s.circuitBreaker != nil {
		status["circuit_breaker"] = s.circuitBreaker.GetStats()
	}

	return status
}
