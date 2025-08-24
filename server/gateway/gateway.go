package gateway

import (
	"context"
	"sync"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/protocols/http"
	"github.com/gear6io/ranger/server/protocols/jdbc"
	"github.com/gear6io/ranger/server/protocols/native"
	"github.com/gear6io/ranger/server/query"
	"github.com/rs/zerolog"
)

// ComponentType defines the gateway component type identifier
const ComponentType = "gateway"

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
		return nil, errors.New(ErrHTTPServerCreationFailed, "failed to create HTTP server", err)
	}

	jdbcServer, err := jdbc.NewServer(queryEngine, logger)
	if err != nil {
		cancel()
		return nil, errors.New(ErrJDBCServerCreationFailed, "failed to create JDBC server", err)
	}

	nativeServer, err := native.NewServer(queryEngine, logger)
	if err != nil {
		cancel()
		return nil, errors.New(ErrNativeServerCreationFailed, "failed to create native server", err)
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
		return errors.New(ErrGatewayAlreadyStarted, "gateway is already started", nil)
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
		return errors.New(ErrAllServersFailedToStart, "all servers failed to start", nil)
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

// GetType returns the component type identifier
func (g *Gateway) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the gateway
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.logger.Info().Msg("Shutting down gateway")

	// Stop gateway
	if err := g.Stop(); err != nil {
		return errors.New(ErrGatewayStopFailed, "failed to stop gateway", err)
	}

	g.logger.Info().Msg("Gateway shut down successfully")
	return nil
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
