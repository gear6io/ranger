package parser

import (
	"context"
	"fmt"
)

// RunIntegrationDemo demonstrates the full catalog integration
func RunIntegrationDemo() error {
	fmt.Println("🚀 Starting Icebox Catalog Integration Demo")
	fmt.Println("==========================================")

	// Create test catalog adapter
	catalogAdapter, err := CreateTestCatalogAdapter()
	if err != nil {
		return fmt.Errorf("failed to create catalog adapter: %w", err)
	}
	defer catalogAdapter.Close()

	// Create enhanced parser with catalog integration
	enhancedParser := NewEnhancedParser(catalogAdapter)

	// Test basic catalog operations
	fmt.Println("\n📋 Testing Basic Catalog Operations:")
	
	ctx := context.Background()
	
	// Test database validation
	if err := catalogAdapter.ValidateDatabase(ctx, "default"); err != nil {
		fmt.Printf("❌ Database validation failed: %v\n", err)
	} else {
		fmt.Printf("✅ Database 'default' validation passed\n")
	}

	// Test data type validation
	if err := catalogAdapter.ValidateDataType("INT"); err != nil {
		fmt.Printf("❌ Data type validation failed: %v\n", err)
	} else {
		fmt.Printf("✅ Data type 'INT' validation passed\n")
	}

	// Test type compatibility
	if catalogAdapter.IsCompatibleDataType("INT", "BIGINT") {
		fmt.Printf("✅ INT is compatible with BIGINT\n")
	} else {
		fmt.Printf("❌ INT is not compatible with BIGINT\n")
	}

	// Test parser with catalog integration
	fmt.Println("\n🔍 Testing Parser with Catalog Integration:")
	
	// Test simple SELECT query
	selectQuery := "SELECT * FROM users WHERE age > 18;"
	fmt.Printf("Parsing query: %s\n", selectQuery)
	
	node, err := enhancedParser.ParseAndValidate(ctx, selectQuery, catalogAdapter)
	if err != nil {
		fmt.Printf("❌ SELECT parsing failed: %v\n", err)
	} else {
		fmt.Printf("✅ SELECT parsing successful, node type: %T\n", node)
	}

	// Test CREATE TABLE query
	createQuery := "CREATE TABLE test_table (id INT, name VARCHAR(255));"
	fmt.Printf("Parsing query: %s\n", createQuery)
	
	ddlNode, err := enhancedParser.ParseAndValidate(ctx, createQuery, catalogAdapter)
	if err != nil {
		fmt.Printf("❌ CREATE TABLE parsing failed: %v\n", err)
	} else {
		fmt.Printf("✅ CREATE TABLE parsing successful, node type: %T\n", ddlNode)
	}

	// Test engine integration
	fmt.Println("\n⚙️ Testing Engine Integration:")
	
	engineIntegration := NewEngineIntegration(catalogAdapter)
	
	// Test query execution flow
	queries := []string{
		"SELECT * FROM users;",
		"CREATE TABLE demo (id INT);",
		"INSERT INTO demo VALUES (1);",
	}

	for _, query := range queries {
		fmt.Printf("Executing: %s\n", query)
		
		err := engineIntegration.ExecuteQueryWithValidation(ctx, query)
		if err != nil {
			fmt.Printf("❌ Execution failed: %v\n", err)
		} else {
			fmt.Printf("✅ Execution successful\n")
		}
	}

	fmt.Println("\n🎉 Integration Demo Completed Successfully!")
	return nil
}

// SimpleCatalogTest runs a basic catalog test
func SimpleCatalogTest() error {
	fmt.Println("🧪 Running Simple Catalog Test")
	
	adapter, err := CreateTestCatalogAdapter()
	if err != nil {
		return fmt.Errorf("failed to create adapter: %w", err)
	}
	defer adapter.Close()

	ctx := context.Background()
	
	// Test current context
	db := adapter.GetCurrentDatabase(ctx)
	user := adapter.GetCurrentUser(ctx)
	fmt.Printf("Current database: %s\n", db)
	fmt.Printf("Current user: %s\n", user)

	// Test privilege checking (should always pass in permissive mode)
	if err := adapter.CheckPrivilege(ctx, user, db, "any_table", "SELECT"); err != nil {
		fmt.Printf("❌ Privilege check failed: %v\n", err)
	} else {
		fmt.Printf("✅ Privilege check passed\n")
	}

	return nil
}
