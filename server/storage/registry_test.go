package storage

import (
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngineRegistry(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create registry
	registry, err := NewStorageEngineRegistry(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, registry)

	// Test that engines are available
	engines := registry.ListEngines()
	assert.GreaterOrEqual(t, len(engines), 2, "Should have at least filesystem and memory engines")

	// Test filesystem engine
	fsEngine, err := registry.GetEngine(FILESYSTEM)
	require.NoError(t, err)
	assert.NotNil(t, fsEngine)

	// Test memory engine
	memEngine, err := registry.GetEngine(MEMORY)
	require.NoError(t, err)
	assert.NotNil(t, memEngine)

	// Test default engine
	defaultEngine, err := registry.GetDefaultEngine()
	require.NoError(t, err)
	assert.NotNil(t, defaultEngine)

	// Test engine existence
	assert.True(t, registry.EngineExists(FILESYSTEM))
	assert.True(t, registry.EngineExists(MEMORY))
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
	cfg := config.LoadDefaultConfig()
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create registry
	registry, err := NewStorageEngineRegistry(cfg, logger)
	require.NoError(t, err)

	// Test getting non-existent engine
	engine, err := registry.GetEngine(EngineType("nonexistent"))
	assert.Error(t, err)
	assert.Nil(t, engine)
	assert.Contains(t, err.Error(), "storage engine 'nonexistent' not found")
}
