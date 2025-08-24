package storage

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageEnginesDirectly tests storage engines without query engine integration
// This avoids import cycles while testing storage functionality
func TestStorageEnginesDirectly(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/icebox_test"
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create storage engine registry directly
	engineRegistry := NewStorageEngineRegistry(logger)
	require.NotNil(t, engineRegistry)

	// Manually register engines for testing
	fsEngine := &filesystem.FileStorage{} // Mock engine for testing
	memEngine, err := memory.NewMemoryStorage()
	require.NoError(t, err)

	engineRegistry.RegisterEngine(filesystem.Type, fsEngine)
	engineRegistry.RegisterEngine(memory.Type, memEngine)

	// Set default engine
	engineRegistry.defaultEngine = filesystem.Type

	t.Run("AvailableEngines", func(t *testing.T) {
		// List available engines
		engines := engineRegistry.ListEngines()
		t.Logf("Available storage engines: %v", engines)

		// Verify we have the expected engines
		assert.Contains(t, engines, "MEMORY", "MEMORY engine should be available")
		assert.Contains(t, engines, "FILESYSTEM", "FILESYSTEM engine should be available")

		// S3 might not be available in test environment
		if contains(engines, "S3") {
			t.Log("S3 engine is available")
		} else {
			t.Log("S3 engine not available (expected in test environment)")
		}
	})

	t.Run("EngineTypes", func(t *testing.T) {
		// Test MEMORY engine
		memoryEngine, err := engineRegistry.GetEngine("MEMORY")
		require.NoError(t, err, "Should get MEMORY engine")

		// Use type assertion to access concrete method
		if memEngine, ok := memoryEngine.(interface{ GetStorageType() string }); ok {
			assert.Equal(t, "MEMORY", memEngine.GetStorageType(), "MEMORY engine type should match")
		}

		// Test FILESYSTEM engine
		filesystemEngine, err := engineRegistry.GetEngine("FILESYSTEM")
		require.NoError(t, err, "Should get FILESYSTEM engine")

		// Use type assertion to access concrete method
		if fsEngine, ok := filesystemEngine.(interface{ GetStorageType() string }); ok {
			assert.Equal(t, "FILESYSTEM", fsEngine.GetStorageType(), "FILESYSTEM engine type should match")
		}

		// Test S3 engine if available
		if s3Engine, err := engineRegistry.GetEngine("S3"); err == nil {
			if s3EngineImpl, ok := s3Engine.(interface{ GetStorageType() string }); ok {
				assert.Equal(t, "S3", s3EngineImpl.GetStorageType(), "S3 engine type should match")
			}
		}
	})

	t.Run("DefaultEngine", func(t *testing.T) {
		// Get default engine
		defaultEngine, err := engineRegistry.GetDefaultEngine()
		require.NoError(t, err, "Should get default engine")
		assert.NotNil(t, defaultEngine, "Default engine should not be nil")

		// Verify default engine is one of the expected types
		if memEngine, ok := defaultEngine.(interface{ GetStorageType() string }); ok {
			engineType := memEngine.GetStorageType()
			assert.Contains(t, []string{"MEMORY", "FILESYSTEM"}, engineType,
				"Default engine should be MEMORY or FILESYSTEM, got %s", engineType)
		}
	})

	t.Run("StorageManagerIntegration", func(t *testing.T) {
		// Test storage manager creation and initialization
		storageMgr, err := NewManager(cfg, logger)
		require.NoError(t, err)
		defer storageMgr.Close()

		// Initialize storage
		ctx := context.Background()
		err = storageMgr.Initialize(ctx)
		require.NoError(t, err)

		// Check storage status
		status := storageMgr.GetStatus()
		assert.Contains(t, status, "total_engines")
		assert.Contains(t, status, "default_engine")
		assert.Contains(t, status, "base_path")
		assert.Contains(t, status, "catalog_path")
		assert.Contains(t, status, "data_path")
		assert.Contains(t, status, "metadata_path")
	})
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
