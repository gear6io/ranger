package storage

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageManagerWithCatalog(t *testing.T) {
	// Load default configuration
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/icebox_test"
	cfg.Storage.Catalog.Type = "json"
	cfg.Storage.Data.Type = "memory"

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create storage manager (this will initialize the catalog)
	manager, err := NewManager(cfg, logger)
	require.NoError(t, err)
	defer manager.Close()

	// Verify catalog was initialized
	catalog := manager.GetCatalog()
	assert.NotNil(t, catalog, "Catalog should be initialized")
	assert.Equal(t, "icebox-json-catalog", catalog.Name())

	// Verify path manager was initialized
	pathManager := manager.GetPathManager()
	assert.NotNil(t, pathManager, "PathManager should be initialized")

	// Test catalog URI generation
	catalogURI := pathManager.GetCatalogURI(cfg.GetCatalogType())
	assert.Contains(t, catalogURI, "/tmp/icebox_test/catalog/catalog.json")

	// Initialize storage
	ctx := context.Background()
	err = manager.Initialize(ctx)
	require.NoError(t, err)

	// Get status
	status := manager.GetStatus()
	assert.Equal(t, "memory", status["type"])
	assert.Equal(t, "/tmp/icebox_test", status["base_path"])
	assert.Equal(t, "/tmp/icebox_test/catalog", status["catalog_path"])
	assert.Equal(t, "/tmp/icebox_test/data", status["data_path"])
	assert.Equal(t, "/tmp/icebox_test/.icebox/metadata.db", status["metadata_path"])
}
