package storage

import (
	"context"
	"testing"

	"github.com/gear6io/ranger/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageManagerWithCatalog(t *testing.T) {
	// Load default configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/ranger_test"
	cfg.Storage.Catalog.Type = "json"
	// Storage engine is now specified per-table, not globally

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create storage manager (this will initialize the catalog)
	manager, err := NewManager(cfg, logger)
	require.NoError(t, err)
	defer manager.Close()

	// Verify catalog was initialized
	catalog := manager.GetCatalog()
	assert.NotNil(t, catalog, "Catalog should be initialized")
	assert.Equal(t, "ranger-json-catalog", catalog.Name())

	// Verify path manager was initialized
	pathManager := manager.GetPathManager()
	assert.NotNil(t, pathManager, "PathManager should be initialized")

	// Test catalog URI generation
	catalogURI := pathManager.GetCatalogURI(cfg.GetCatalogType())
	assert.Contains(t, catalogURI, "/tmp/ranger_test/catalog/catalog.json")

	// Initialize storage
	ctx := context.Background()
	err = manager.Initialize(ctx)
	require.NoError(t, err)

	// Get status
	status := manager.GetStatus()
	assert.Equal(t, "/tmp/ranger_test", status["base_path"])
	assert.Equal(t, "/tmp/ranger_test/catalog", status["catalog_path"])
	assert.Equal(t, "/tmp/ranger_test/data", status["data_path"])
	assert.Equal(t, "/tmp/ranger_test/.ranger/metadata.db", status["metadata_path"])
	assert.Contains(t, status, "total_engines")
	assert.Contains(t, status, "default_engine")
}
