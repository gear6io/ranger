package loader

import (
	"context"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/gateway"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/paths"
	"github.com/gear6io/ranger/server/query"
	"github.com/gear6io/ranger/server/shared"
	"github.com/gear6io/ranger/server/storage"
	"github.com/rs/zerolog"
)

// InitFunction defines the function type for initializing components
type InitFunction func(loader LoaderInterface) (shared.Component, error)

// LoaderInterface defines the interface that components can use to access other components
type LoaderInterface interface {
	GetStorage() *storage.Manager
	GetCatalog() catalog.CatalogInterface
	GetQueryEngine() *query.Engine
	GetGateway() *gateway.Gateway
	GetPathManager() paths.PathManager
	GetMetadataManager() metadata.MetadataManagerInterface
	GetConfig() *config.Config
	GetLogger() zerolog.Logger
}

// Loader initializes and manages all core components
type Loader struct {
	config *config.Config
	logger zerolog.Logger

	// Component registry for the new initialization system
	initFunctions []InitFunction
	components    map[string]shared.Component
	initOrder     []string // Store initialization order for shutdown
}

// NewLoader creates a new Loader instance
func NewLoader(cfg *config.Config, logger zerolog.Logger) (*Loader, error) {
	loader := &Loader{
		config:        cfg,
		logger:        logger.With().Str("component", "loader").Logger(),
		initFunctions: make([]InitFunction, 0),
		components:    make(map[string]shared.Component),
		initOrder:     make([]string, 0),
	}

	// Register components in initialization order
	loader.registerComponents()

	// Initialize all components
	if err := loader.Initialize(); err != nil {
		return nil, errors.New(ErrComponentInitializationFailed, "failed to initialize components", err)
	}

	return loader, nil
}

// RegisterComponent registers a component initialization function with its name
func (l *Loader) RegisterComponent(name string, initFunc InitFunction) {
	l.initFunctions = append(l.initFunctions, initFunc)
	l.initOrder = append(l.initOrder, name)
}

// registerComponents registers all components in the correct initialization order
func (l *Loader) registerComponents() {
	// This order determines initialization sequence
	l.RegisterComponent("paths", func(loader LoaderInterface) (shared.Component, error) {
		return paths.NewManager(loader.GetConfig().GetStoragePath()), nil
	})

	l.RegisterComponent("catalog", func(loader LoaderInterface) (shared.Component, error) {
		return catalog.NewCatalog(loader.GetConfig(), loader.GetPathManager())
	})

	l.RegisterComponent("metadata", func(loader LoaderInterface) (shared.Component, error) {
		return metadata.NewMetadataManager(loader.GetCatalog(),
			loader.GetPathManager().GetInternalMetadataDBPath(),
			loader.GetConfig().GetStoragePath(),
			loader.GetLogger())
	})

	l.RegisterComponent("storage", func(loader LoaderInterface) (shared.Component, error) {
		return storage.NewManager(loader.GetConfig(), loader.GetLogger())
	})

	l.RegisterComponent("query", func(loader LoaderInterface) (shared.Component, error) {
		return query.NewEngine(loader.GetConfig(), loader.GetStorage(), loader.GetLogger())
	})

	l.RegisterComponent("gateway", func(loader LoaderInterface) (shared.Component, error) {
		return gateway.NewGateway(loader.GetQueryEngine(), loader.GetLogger())
	})
}

// Initialize initializes all components in registration order
func (l *Loader) Initialize() error {
	l.logger.Info().Msg("Initializing components...")

	for i, initFunc := range l.initFunctions {
		component, err := initFunc(l)
		if err != nil {
			return errors.New(ErrComponentInitFailed, "failed to initialize component", err).AddContext("component_index", i)
		}

		// Store component by its type
		componentType := component.GetType()
		l.components[componentType] = component
		l.logger.Info().Str("type", componentType).Msg("Component initialized successfully")
	}

	l.logger.Info().Msg("All components initialized successfully")
	return nil
}

// Start initializes and starts all components
func (l *Loader) Start() error {
	l.logger.Info().Msg("Starting Loader...")

	// Start the Gateway (which manages all servers)
	gateway := l.GetGateway()
	if gateway != nil {
		if err := gateway.Start(l.logger.WithContext(context.Background())); err != nil {
			return errors.New(ErrGatewayStartFailed, "failed to start gateway", err)
		}
	}

	l.logger.Info().Msg("Loader started successfully")
	return nil
}

// Shutdown gracefully shuts down all components in reverse initialization order
func (l *Loader) Shutdown(ctx context.Context) error {
	l.logger.Info().Msg("Shutting down components...")

	// Shutdown components in reverse order (LIFO - Last In, First Out)
	for i := len(l.initOrder) - 1; i >= 0; i-- {
		componentType := l.initOrder[i]

		if component, exists := l.components[componentType]; exists {
			l.logger.Info().Str("type", componentType).Msg("Shutting down component")
			if err := component.Shutdown(ctx); err != nil {
				// Log error but continue with other components
				l.logger.Error().Err(err).Str("type", componentType).Msg("Component shutdown failed")
			} else {
				l.logger.Info().Str("type", componentType).Msg("Component shut down successfully")
			}
		} else {
			l.logger.Warn().Str("type", componentType).Msg("Component not found during shutdown")
		}
	}

	l.logger.Info().Msg("All components shut down")
	return nil
}

// GetConfig returns the configuration
func (l *Loader) GetConfig() *config.Config {
	return l.config
}

// GetLogger returns the logger instance
func (l *Loader) GetLogger() zerolog.Logger {
	return l.logger
}

// GetCatalog returns the catalog instance
func (l *Loader) GetCatalog() catalog.CatalogInterface {
	if comp, exists := l.components[storage.ComponentType]; exists {
		// Catalog comes from storage manager
		storageManager := comp.(*storage.Manager)
		return storageManager.GetCatalog()
	}
	return nil
}

// GetQueryEngine returns the QueryEngine instance
func (l *Loader) GetQueryEngine() *query.Engine {
	if comp, exists := l.components[query.ComponentType]; exists {
		return comp.(*query.Engine)
	}
	return nil
}

// GetGateway returns the Gateway instance
func (l *Loader) GetGateway() *gateway.Gateway {
	if comp, exists := l.components[gateway.ComponentType]; exists {
		return comp.(*gateway.Gateway)
	}
	return nil
}

// GetStorage returns the storage manager
func (l *Loader) GetStorage() *storage.Manager {
	if comp, exists := l.components[storage.ComponentType]; exists {
		return comp.(*storage.Manager)
	}
	return nil
}

// GetPathManager returns the path manager from component registry
func (l *Loader) GetPathManager() paths.PathManager {
	if comp, exists := l.components[paths.ComponentType]; exists {
		return comp.(*paths.Manager)
	}
	return nil
}

// GetMetadataManager returns the metadata manager from component registry
func (l *Loader) GetMetadataManager() metadata.MetadataManagerInterface {
	if comp, exists := l.components[metadata.ComponentType]; exists {
		return comp.(*metadata.MetadataManager)
	}
	return nil
}

// GetStatus returns the status of all components
func (l *Loader) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"config": l.config,
	}

	gateway := l.GetGateway()
	if gateway != nil {
		status["gateway"] = gateway.GetStatus()
	}

	return status
}
