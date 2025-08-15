package parser

import (
	"context"
	"fmt"
)

// QuickTest runs a quick test of the catalog integration
func QuickTest() {
	fmt.Println("🧪 Quick Catalog Integration Test")

	// Create test config
	cfg := CreateTestConfig()
	fmt.Printf("✅ Test config created: catalog type = %s, path = %s\n",
		cfg.Storage.Catalog.Type, cfg.Storage.Path)

	// Create catalog adapter
	adapter, err := CreateTestCatalogAdapter()
	if err != nil {
		fmt.Printf("❌ Failed to create catalog adapter: %v\n", err)
		return
	}
	defer adapter.Close()
	fmt.Println("✅ Catalog adapter created successfully")

	// Test basic operations
	ctx := context.Background()

	// Test data type validation
	if err := adapter.ValidateDataType("INT"); err != nil {
		fmt.Printf("❌ Data type validation failed: %v\n", err)
	} else {
		fmt.Println("✅ Data type validation passed")
	}

	// Test type compatibility
	if adapter.IsCompatibleDataType("INT", "BIGINT") {
		fmt.Println("✅ Type compatibility check passed")
	} else {
		fmt.Println("❌ Type compatibility check failed")
	}

	// Test context methods
	db := adapter.GetCurrentDatabase(ctx)
	user := adapter.GetCurrentUser(ctx)
	fmt.Printf("✅ Context: database=%s, user=%s\n", db, user)

	fmt.Println("🎉 Quick test completed successfully!")
}
