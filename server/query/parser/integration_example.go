package parser

import (
	"context"
	"fmt"
	"log"
)

// IntegrationExample demonstrates how to use the catalog adapter with the parser
func IntegrationExample() {
	// Create a catalog adapter (using default for now)
	catalogAdapter := NewDefaultCatalogAdapter()

	// Create an enhanced parser with catalog integration
	enhancedParser := NewEnhancedParser(catalogAdapter)

	// Example SQL query
	query := "SELECT * FROM users WHERE age > 18;"

	// Parse and validate with catalog
	ctx := context.Background()
	node, err := enhancedParser.ParseAndValidate(ctx, query, catalogAdapter)
	if err != nil {
		log.Printf("Parsing failed: %v", err)
		return
	}

	fmt.Printf("Successfully parsed and validated query: %s\n", query)
	fmt.Printf("AST Node type: %T\n", node)

	// Example DDL query
	ddlQuery := "CREATE TABLE test_table (id INT, name VARCHAR(255));"

	ddlNode, err := enhancedParser.ParseAndValidate(ctx, ddlQuery, catalogAdapter)
	if err != nil {
		log.Printf("DDL parsing failed: %v", err)
		return
	}

	fmt.Printf("Successfully parsed and validated DDL: %s\n", ddlQuery)
	fmt.Printf("DDL AST Node type: %T\n", ddlNode)
}

// SimpleCatalogIntegration shows basic catalog integration
func SimpleCatalogIntegration() {
	// Create catalog adapter
	catalogAdapter := NewDefaultCatalogAdapter()

	// Test basic validation
	ctx := context.Background()

	// Validate table
	if err := catalogAdapter.ValidateTable(ctx, "users"); err != nil {
		log.Printf("Table validation failed: %v", err)
	} else {
		fmt.Println("Table 'users' validation passed")
	}

	// Validate column
	if err := catalogAdapter.ValidateColumn(ctx, "users", "id"); err != nil {
		log.Printf("Column validation failed: %v", err)
	} else {
		fmt.Println("Column 'users.id' validation passed")
	}

	// Validate data type
	if err := catalogAdapter.ValidateDataType("INT"); err != nil {
		log.Printf("Data type validation failed: %v", err)
	} else {
		fmt.Println("Data type 'INT' validation passed")
	}

	// Check type compatibility
	if catalogAdapter.IsCompatibleDataType("INT", "BIGINT") {
		fmt.Println("INT is compatible with BIGINT")
	} else {
		fmt.Println("INT is not compatible with BIGINT")
	}
}
