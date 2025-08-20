package gateway

import (
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/query"
	"github.com/TFMV/icebox/server/storage"
	"github.com/rs/zerolog"
)

func TestNewGateway(t *testing.T) {
	// Create a test config
	cfg := &config.Config{
		Log: config.LogConfig{Level: "info"},
		Storage: config.StorageConfig{
			DataPath: ".icebox/warehouse",
			Catalog: config.CatalogConfig{
				Type: "json",
			},
		},
	}

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create storage manager
	storageMgr, err := storage.NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}

	// Create a mock QueryEngine (we'll use a simple one for testing)
	queryEngine, err := query.NewEngine(cfg, storageMgr, logger)
	if err != nil {
		t.Fatalf("Failed to create query engine: %v", err)
	}

	// Test creating a new gateway
	gateway, err := NewGateway(queryEngine, logger)
	if err != nil {
		t.Fatalf("Failed to create gateway: %v", err)
	}

	// Verify gateway was created
	if gateway == nil {
		t.Fatal("Gateway should not be nil")
	}

	// Verify QueryEngine was set
	if gateway.GetQueryEngine() == nil {
		t.Error("QueryEngine should not be nil")
	}

	// Verify servers were created
	if gateway.GetStatus() == nil {
		t.Error("Status should not be nil")
	}
}
