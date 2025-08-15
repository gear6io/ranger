package loader

import (
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

func TestNewLoader(t *testing.T) {
	// Create a test config
	cfg := &config.Config{
		Log: config.LogConfig{Level: "info"},
		Storage: config.StorageConfig{
			Catalog: config.CatalogConfig{
				Type: "json",
				URI:  ".icebox/catalog.json",
			},
			Config: config.DataConfig{
				Type: "memory",
			},
		},
	}

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Test creating a new loader
	loader, err := NewLoader(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create loader: %v", err)
	}

	// Verify loader was created
	if loader == nil {
		t.Fatal("Loader should not be nil")
	}

	// Verify components were created
	if loader.GetConfig() == nil {
		t.Error("Config should not be nil")
	}

	if loader.GetCatalog() == nil {
		t.Error("Catalog should not be nil")
	}

	if loader.GetQueryEngine() == nil {
		t.Error("QueryEngine should not be nil")
	}

	if loader.GetGateway() == nil {
		t.Error("Gateway should not be nil")
	}

	if loader.GetStorage() == nil {
		t.Error("Storage should not be nil")
	}
}
