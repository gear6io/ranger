package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/protocols/http"
	"github.com/TFMV/icebox/server/protocols/jdbc"
	"github.com/TFMV/icebox/server/protocols/native"
	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// Gateway manages the lifecycle of all protocol servers
type Gateway struct {
	queryEngine  *query.Engine
	httpServer   *http.Server
	jdbcServer   *jdbc.Server
	nativeServer *native.Server
	logger       zerolog.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	started      bool
	mu           sync.RWMutex

	// Connection management
	maxConnections    int
	activeConnections int32
	connectionMutex   sync.RWMutex
}

// NewGateway creates a new gateway instance
func NewGateway(queryEngine *query.Engine, logger zerolog.Logger) (*Gateway, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create all servers with the shared QueryEngine
	httpServer, err := http.NewServer(queryEngine, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create HTTP server: %w", err)
	}

	jdbcServer, err := jdbc.NewServer(queryEngine, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create JDBC server: %w", err)
	}

	nativeServer, err := native.NewServer(queryEngine, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create native server: %w", err)
	}

	return &Gateway{
		queryEngine:       queryEngine,
		httpServer:        httpServer,
		jdbcServer:        jdbcServer,
		nativeServer:      nativeServer,
		logger:            logger.With().Str("component", "gateway").Logger(),
		ctx:               ctx,
		cancel:            cancel,
		maxConnections:    1000, // Default max connections
		activeConnections: 0,
	}, nil
}

// Start starts all enabled servers
func (g *Gateway) Start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.started {
		return fmt.Errorf("gateway is already started")
	}

	g.logger.Info().Msg("Starting Gateway...")

	// Track which servers started successfully
	startedServers := 0
	totalServers := 0

	// Start HTTP server if enabled
	if g.isHTTPServerEnabled() {
		totalServers++
		if err := g.httpServer.Start(ctx); err != nil {
			g.logger.Error().Err(err).Msg("HTTP server failed to start, continuing with other servers")
		} else {
			startedServers++
			g.logger.Info().Msg("HTTP server started successfully")
		}
	}

	// Start JDBC server if enabled
	if g.isJDBCServerEnabled() {
		totalServers++
		if err := g.jdbcServer.Start(ctx); err != nil {
			g.logger.Error().Err(err).Msg("JDBC server failed to start, continuing with other servers")
		} else {
			startedServers++
			g.logger.Info().Msg("JDBC server started successfully")
		}
	}

	// Start native server if enabled
	if g.isNativeServerEnabled() {
		totalServers++
		if err := g.nativeServer.Start(ctx); err != nil {
			g.logger.Error().Err(err).Msg("Native server failed to start, continuing with other servers")
		} else {
			startedServers++
			g.logger.Info().Msg("Native server started successfully")
		}
	}

	// Check if any servers started successfully
	if startedServers == 0 && totalServers > 0 {
		g.logger.Error().Msg("All servers failed to start, shutting down gateway")
		g.cancel()
		return fmt.Errorf("all servers failed to start")
	}

	if startedServers < totalServers {
		g.logger.Warn().Int("started", startedServers).Int("total", totalServers).Msg("Some servers failed to start, but gateway is operational")
	} else {
		g.logger.Info().Int("started", startedServers).Msg("All servers started successfully")
	}

	g.started = true
	g.logger.Info().Msg("Gateway started successfully")
	return nil
}

// Stop gracefully shuts down all servers
func (g *Gateway) Stop() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.started {
		return nil
	}

	g.logger.Info().Msg("Stopping Gateway...")
	g.cancel()

	// Stop all servers concurrently
	var wg sync.WaitGroup

	if g.httpServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.httpServer.Stop(); err != nil {
				g.logger.Error().Err(err).Msg("Error stopping HTTP server")
			}
		}()
	}

	if g.jdbcServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.jdbcServer.Stop(); err != nil {
				g.logger.Error().Err(err).Msg("Error stopping JDBC server")
			}
		}()
	}

	if g.nativeServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.nativeServer.Stop(); err != nil {
				g.logger.Error().Err(err).Msg("Error stopping native server")
			}
		}()
	}

	// Wait for all servers to stop
	wg.Wait()

	g.started = false
	g.logger.Info().Msg("Gateway stopped successfully")
	return nil
}

// GetStatus returns the combined status of all servers
func (g *Gateway) GetStatus() map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()

	status := map[string]interface{}{
		"started": g.started,
		"servers": map[string]interface{}{
			"http":   g.httpServer.GetStatus(),
			"jdbc":   g.jdbcServer.GetStatus(),
			"native": g.nativeServer.GetStatus(),
		},
		"connections": g.GetConnectionStats(),
	}

	return status
}

// GetQueryEngine returns the shared QueryEngine instance
func (g *Gateway) GetQueryEngine() *query.Engine {
	return g.queryEngine
}

// Helper methods to check server enabled states
func (g *Gateway) isHTTPServerEnabled() bool {
	return config.HTTP_SERVER_ENABLED
}

func (g *Gateway) isJDBCServerEnabled() bool {
	return config.JDBC_SERVER_ENABLED
}

func (g *Gateway) isNativeServerEnabled() bool {
	return config.NATIVE_SERVER_ENABLED
}

// Connection management methods
func (g *Gateway) canAcceptConnection() bool {
	g.connectionMutex.RLock()
	defer g.connectionMutex.RUnlock()
	return int(g.activeConnections) < g.maxConnections
}

func (g *Gateway) incrementConnection() bool {
	g.connectionMutex.Lock()
	defer g.connectionMutex.Unlock()

	if int(g.activeConnections) >= g.maxConnections {
		return false
	}

	g.activeConnections++
	return true
}

func (g *Gateway) decrementConnection() {
	g.connectionMutex.Lock()
	defer g.connectionMutex.Unlock()

	if g.activeConnections > 0 {
		g.activeConnections--
	}
}

func (g *Gateway) GetConnectionStats() map[string]interface{} {
	g.connectionMutex.RLock()
	defer g.connectionMutex.RUnlock()

	return map[string]interface{}{
		"active_connections": g.activeConnections,
		"max_connections":    g.maxConnections,
		"available":          g.maxConnections - int(g.activeConnections),
	}
}
