package storage

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

func TestMetadataUpdateAfterInsertion(t *testing.T) {
	// Create a test configuration
	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataPath: "/tmp/test-storage",
			Catalog: config.CatalogConfig{
				Type: "json",
			},
		},
	}

	// Create a logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create storage manager
	manager, err := NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	// Test context
	ctx := context.Background()

	// Test data (not used in this test but shows the expected format)
	_ = [][]interface{}{
		{"user1", "John Doe", 25},
		{"user2", "Jane Smith", 30},
		{"user3", "Bob Johnson", 35},
	}

	// Test database and table names
	database := "testdb"
	tableName := "users"

	// Create a test table first (this would normally be done by the metadata manager)
	// For this test, we'll just verify that the metadata update methods can be called

	// Test the single metadata update method

	t.Run("Test updateMetadataAfterInsertion", func(t *testing.T) {
		err := manager.updateMetadataAfterInsertion(ctx, database, tableName, 3, "memory")
		if err != nil {
			t.Errorf("updateMetadataAfterInsertion failed: %v", err)
		}
	})
}

func TestMetadataUpdateMethodSignatures(t *testing.T) {
	// This test verifies that the method signatures are correct
	// and that the methods can be called with the right parameters

	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataPath: "/tmp/test-storage",
			Catalog: config.CatalogConfig{
				Type: "json",
			},
		},
	}

	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	manager, err := NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// Test that the single metadata update method has the correct signature
	// and can be called without compilation errors

	// Test updateMetadataAfterInsertion with all parameters
	err = manager.updateMetadataAfterInsertion(ctx, "db", "table", 100, "memory")
	if err != nil {
		t.Errorf("updateMetadataAfterInsertion signature test failed: %v", err)
	}
}
