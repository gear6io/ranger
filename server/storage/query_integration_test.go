package storage

import (
	"context"
	"testing"

	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/paths"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageEnginesDirectly tests storage engines without query engine integration
// This avoids import cycles while testing storage functionality
func TestStorageEnginesDirectly(t *testing.T) {
	// Create test configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/ranger_test"
	cfg.Storage.Catalog.Type = "json"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create path manager
	pathManager := paths.NewManager(cfg.GetStoragePath())

	// Create catalog
	catalogInstance, err := catalog.NewCatalog(cfg, pathManager)
	require.NoError(t, err)

	// Create metadata manager
	metadataMgr, err := metadata.NewMetadataManager(catalogInstance, pathManager.GetInternalMetadataDBPath(), cfg.GetStoragePath(), logger)
	require.NoError(t, err)

	// Create storage manager
	storageMgr, err := NewManager(cfg, logger, metadataMgr)
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
