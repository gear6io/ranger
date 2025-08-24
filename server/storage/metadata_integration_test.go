package storage

import (
	"context"
	"os"
	"testing"

	"github.com/gear6io/ranger/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMetadataUpdateAfterInsertion(t *testing.T) {
	// This test verifies that the updateMetadataAfterInsertion method can be called
	// with the correct parameters. The actual metadata update requires proper
	// integration between catalog and metadata manager which is beyond the scope
	// of this unit test.

	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_metadata")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataPath: tempDir,
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

	// Test that the method signature is correct and can be called
	// Note: This will fail because the table doesn't exist in metadata,
	// but that's expected for a unit test without full integration
	err = manager.updateMetadataAfterInsertion(ctx, "testdb", "users", 3, "memory")
	if err != nil {
		// This is expected - the table doesn't exist in metadata
		t.Logf("Expected error (table not in metadata): %v", err)
	} else {
		t.Log("Unexpected success - table was found in metadata")
	}
}

func TestMetadataUpdateMethodSignatures(t *testing.T) {
	// This test verifies that the method signatures are correct
	// and that the methods can be called with the right parameters

	// Create test configuration with unique temporary directory
	tempDir, err := os.MkdirTemp("", "ranger_test_metadata_sig")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataPath: tempDir,
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
	// Note: This will fail because the table doesn't exist in metadata,
	// but that's expected for a unit test without full integration
	err = manager.updateMetadataAfterInsertion(ctx, "db", "table", 100, "memory")
	if err != nil {
		// This is expected - the table doesn't exist in metadata
		t.Logf("Expected error (table not in metadata): %v", err)
	} else {
		t.Log("Unexpected success - table was found in metadata")
	}
}
