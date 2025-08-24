package storage

import (
	"testing"

	"github.com/gear6io/ranger/server/storage/filesystem"
	"github.com/gear6io/ranger/server/storage/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngineRegistry(t *testing.T) {
	// Create test configuration
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create registry
	registry := NewStorageEngineRegistry(logger)
	require.NotNil(t, registry)

	// Manually register engines for testing
	fsEngine := &filesystem.FileStorage{} // Mock engine for testing
	memEngine, err := memory.NewMemoryStorage()
	require.NoError(t, err)

	registry.RegisterEngine(filesystem.Type, fsEngine)
	registry.RegisterEngine(memory.Type, memEngine)

	// Set default engine
	registry.defaultEngine = filesystem.Type

	// Test that engines are available
	engines := registry.ListEngines()
	assert.GreaterOrEqual(t, len(engines), 2, "Should have at least filesystem and memory engines")

	// Test filesystem engine
	fsEngineInterface, err := registry.GetEngine(filesystem.Type)
	require.NoError(t, err)
	assert.NotNil(t, fsEngineInterface)

	// Test memory engine
	memEngineInterface, err := registry.GetEngine(memory.Type)
	require.NoError(t, err)
	assert.NotNil(t, memEngineInterface)

	// Test default engine
	defaultEngine, err := registry.GetDefaultEngine()
	require.NoError(t, err)
	assert.NotNil(t, defaultEngine)

	// Test engine existence
	assert.True(t, registry.EngineExists(filesystem.Type))
	assert.True(t, registry.EngineExists(memory.Type))
	assert.False(t, registry.EngineExists("nonexistent"))

	// Test engine status
	status := registry.GetEngineStatus()
	assert.Contains(t, status, "FILESYSTEM")
	assert.Contains(t, status, "MEMORY")
	assert.Contains(t, status, "default_engine")
	assert.Contains(t, status, "total_engines")
}

func TestStorageEngineRegistryWithInvalidEngine(t *testing.T) {
	// Create test configuration
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create registry
	registry := NewStorageEngineRegistry(logger)

	// Test getting non-existent engine
	engine, err := registry.GetEngine("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, engine)
	assert.Contains(t, err.Error(), "storage engine not found")
}
