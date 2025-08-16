package storage

import (
	"context"
	"fmt"
	"log"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
)

// ExampleUsage demonstrates the new standardized storage structure
func ExampleUsage() {
	// Load configuration (data_path is set to "./data" by default)
	cfg := config.LoadDefaultConfig()

	// Optionally override the data path
	// cfg.Storage.DataPath = "./my-custom-data"

	// Set storage types (these are also set by default)
	cfg.Storage.Data.Type = "filesystem" // Default: filesystem
	cfg.Storage.Catalog.Type = "json"    // Default: json
	// Metadata type is always SQLite (not configurable)

	fmt.Printf("📁 Data path: %s\n", cfg.GetStoragePath())
	fmt.Printf("🗄️  Catalog type: %s\n", cfg.GetCatalogType())
	fmt.Printf("💾 Storage type: %s\n", cfg.GetStorageType())
	fmt.Printf("📊 Metadata type: SQLite (always)\n")

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Create storage manager
	manager, err := NewManager(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	// Initialize storage (creates directory structure)
	ctx := context.Background()
	if err := manager.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// Get status to see the created structure
	status := manager.GetStatus()
	fmt.Printf("\nStorage Status:\n")
	fmt.Printf("  Type: %s\n", status["type"])
	fmt.Printf("  Base Path: %s\n", status["base_path"])
	fmt.Printf("  Catalog Path: %s\n", status["catalog_path"])
	fmt.Printf("  Data Path: %s\n", status["data_path"])
	fmt.Printf("  Metadata Path: %s\n", status["metadata_path"])

	// Create a table with database.table syntax
	tableSchema := []byte(`{"fields":[{"name":"id","type":"int","nullable":false},{"name":"name","type":"string","nullable":true}]}`)

	if err := manager.CreateTable("analytics.users", tableSchema); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Printf("\n✅ Created table 'analytics.users'\n")

	// Create another table in default database
	if err := manager.CreateTable("sales", tableSchema); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Printf("✅ Created table 'default.sales'\n")

	// Insert some data
	data := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	if err := manager.InsertData("analytics.users", data); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	fmt.Printf("✅ Inserted %d rows into 'analytics.users'\n", len(data))

	// Check if tables exist
	if manager.TableExists("analytics.users") {
		fmt.Printf("✅ Table 'analytics.users' exists\n")
	}

	if manager.TableExists("sales") {
		fmt.Printf("✅ Table 'default.sales' exists\n")
	}

	fmt.Printf("\n🎉 Storage structure created successfully!\n")
	fmt.Printf("📁 Check the '%s' directory to see the new structure\n", cfg.GetStoragePath())
}
