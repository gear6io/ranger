package storage

import (
	"fmt"
	"sync"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/rs/zerolog"
)

// Package-specific error codes for storage registry
var (
	StorageRegistryEngineNotFound = errors.MustNewCode("storage.engine_not_found")
)

// StorageEngineRegistry manages multiple storage engines
type StorageEngineRegistry struct {
	engines       map[string]FileSystem
	defaultEngine string
	mu            sync.RWMutex
	logger        zerolog.Logger
}

// NewStorageEngineRegistry creates a new storage engine registry
func NewStorageEngineRegistry(logger zerolog.Logger) *StorageEngineRegistry {
	return &StorageEngineRegistry{
		engines:       make(map[string]FileSystem),
		defaultEngine: "",
		logger:        logger,
	}
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

	return nil, errors.New(StorageRegistryEngineNotFound, "storage engine not found", nil).AddContext("engine_name", engineName)
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
