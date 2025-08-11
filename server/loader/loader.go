package loader

import (
	"context"
	"fmt"

	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/gateway"
	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// Loader initializes and manages all core components
type Loader struct {
	config      *config.Config
	catalog     catalog.CatalogInterface
	queryEngine *query.Engine
	gateway     *gateway.Gateway
	logger      zerolog.Logger
}

// NewLoader creates a new Loader instance
func NewLoader(cfg *config.Config, logger zerolog.Logger) (*Loader, error) {
	// Initialize catalog based on configuration
	catalogInstance, err := catalog.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	// Initialize QueryEngine with catalog
	queryEngine, err := query.NewEngine(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create query engine: %w", err)
	}

	// Initialize Gateway with QueryEngine
	gatewayInstance, err := gateway.NewGateway(queryEngine, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}

	return &Loader{
		config:      cfg,
		catalog:     catalogInstance,
		queryEngine: queryEngine,
		gateway:     gatewayInstance,
		logger:      logger.With().Str("component", "loader").Logger(),
	}, nil
}

// Start initializes and starts all components
func (l *Loader) Start() error {
	l.logger.Info().Msg("Starting Loader...")

	// Start the Gateway (which manages all servers)
	if err := l.gateway.Start(l.logger.WithContext(context.Background())); err != nil {
		return fmt.Errorf("failed to start gateway: %w", err)
	}

	l.logger.Info().Msg("Loader started successfully")
	return nil
}

// Stop gracefully shuts down all components
func (l *Loader) Stop() error {
	l.logger.Info().Msg("Stopping Loader...")

	// Stop the Gateway (which will stop all servers)
	if err := l.gateway.Stop(); err != nil {
		l.logger.Error().Err(err).Msg("Error stopping gateway")
	}

	l.logger.Info().Msg("Loader stopped successfully")
	return nil
}

// GetConfig returns the configuration
func (l *Loader) GetConfig() *config.Config {
	return l.config
}

// GetCatalog returns the catalog instance
func (l *Loader) GetCatalog() catalog.CatalogInterface {
	return l.catalog
}

// GetQueryEngine returns the QueryEngine instance
func (l *Loader) GetQueryEngine() *query.Engine {
	return l.queryEngine
}

// GetGateway returns the Gateway instance
func (l *Loader) GetGateway() *gateway.Gateway {
	return l.gateway
}

// GetStatus returns the status of all components
func (l *Loader) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"config": l.config,
	}

	if l.gateway != nil {
		status["gateway"] = l.gateway.GetStatus()
	}

	return status
}
