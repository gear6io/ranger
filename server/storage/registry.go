package storage

import (
	"fmt"
	"sync"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/TFMV/icebox/server/storage/s3"
	"github.com/rs/zerolog"
)

// StorageEngineRegistry manages multiple storage engines
type StorageEngineRegistry struct {
	engines       map[string]FileSystem
	defaultEngine string
	mu            sync.RWMutex
	logger        zerolog.Logger
}

// NewStorageEngineRegistry creates a new storage engine registry
func NewStorageEngineRegistry(cfg *config.Config, logger zerolog.Logger) (*StorageEngineRegistry, error) {
	registry := &StorageEngineRegistry{
		engines:       make(map[string]FileSystem),
		defaultEngine: filesystem.Type, // Default fallback
		logger:        logger,
	}

	// Initialize all available storage engines
	if err := registry.initializeEngines(cfg); err != nil {
		return nil, fmt.Errorf("failed to initialize storage engines: %w", err)
	}

	logger.Info().
		Str("default_engine", registry.defaultEngine).
		Int("total_engines", len(registry.engines)).
		Msg("Storage engine registry initialized")

	return registry, nil
}

// initializeEngines initializes all available storage engines
func (r *StorageEngineRegistry) initializeEngines(cfg *config.Config) error {
	// Initialize filesystem engine
	fsEngine := filesystem.NewFileStorage()
	r.RegisterEngine(filesystem.Type, fsEngine)

	// Initialize memory engine
	memEngine, err := memory.NewMemoryStorage()
	if err != nil {
		return fmt.Errorf("failed to initialize memory engine: %w", err)
	}
	r.RegisterEngine(memory.Type, memEngine)

	// Initialize S3 engine (if credentials are available)
	if s3Engine, err := s3.NewS3FileSystem(cfg); err == nil {
		r.RegisterEngine(s3.Type, s3Engine)
		r.logger.Info().Msg("S3 storage engine initialized successfully")
	} else {
		r.logger.Warn().Err(err).Msg("S3 storage engine not available (credentials missing or invalid)")
	}

	// Set default engine based on available engines
	if _, exists := r.engines[filesystem.Type]; exists {
		r.defaultEngine = filesystem.Type
	} else if _, exists := r.engines[memory.Type]; exists {
		r.defaultEngine = memory.Type
	} else {
		return fmt.Errorf("no storage engines available")
	}

	return nil
}

// RegisterEngine registers a storage engine with the registry
func (r *StorageEngineRegistry) RegisterEngine(name string, engine FileSystem) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.engines[name] = engine
}

// GetEngine returns a storage engine by name
func (r *StorageEngineRegistry) GetEngine(engineName string) (FileSystem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if engine, exists := r.engines[engineName]; exists {
		return engine, nil
	}

	return nil, fmt.Errorf("storage engine '%s' not found", engineName)
}

// GetDefaultEngine returns the default storage engine
func (r *StorageEngineRegistry) GetDefaultEngine() (FileSystem, error) {
	return r.GetEngine(r.defaultEngine)
}

// ListEngines returns a list of all available engine names
func (r *StorageEngineRegistry) ListEngines() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	engines := make([]string, 0, len(r.engines))
	for name := range r.engines {
		engines = append(engines, name)
	}
	return engines
}

// EngineExists checks if a storage engine exists
func (r *StorageEngineRegistry) EngineExists(engineName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.engines[engineName]
	return exists
}

// GetEngineStatus returns the status of all engines
func (r *StorageEngineRegistry) GetEngineStatus() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := make(map[string]interface{})
	for name, engine := range r.engines {
		status[name] = map[string]interface{}{
			"available": true,
			"type":      fmt.Sprintf("%T", engine),
		}
	}

	status["default_engine"] = r.defaultEngine
	status["total_engines"] = len(r.engines)

	return status
}
