package storage

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
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

	// Test database and table names
	database := "testdb"
	tableName := "users"

	// Create a test table first using the catalog
	err = manager.GetCatalog().CreateNamespace(ctx, table.Identifier{database}, nil)
	if err != nil {
		t.Fatalf("Failed to create database namespace: %v", err)
	}

	// Create a simple schema for the table
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	// Create the table
	_, err = manager.GetCatalog().CreateTable(ctx, table.Identifier{database, tableName}, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test the single metadata update method
	t.Run("Test updateMetadataAfterInsertion", func(t *testing.T) {
		err := manager.updateMetadataAfterInsertion(ctx, database, tableName, 3, "memory")
		if err != nil {
			t.Errorf("updateMetadataAfterInsertion failed: %v", err)
		}
	})

	// Clean up
	_ = manager.GetCatalog().DropTable(ctx, table.Identifier{database, tableName})
	_ = manager.GetCatalog().DropNamespace(ctx, table.Identifier{database})
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
