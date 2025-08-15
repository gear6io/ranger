package parser

import (
	"context"
	"fmt"
	"log"
)

// ExampleUsage demonstrates how to use the catalog-aware parser
func ExampleUsage() {
	// Create a catalog adapter (you can use either default or Icebox-specific)
	catalogAdapter := NewDefaultCatalogAdapter()

	// Create an enhanced parser with catalog integration
	parser := NewEnhancedParser(catalogAdapter)

	ctx := context.Background()

	// Example 1: Parse and validate a SELECT statement
	selectQuery := "SELECT id, name FROM users WHERE age > 18;"
	node, err := parser.ParseAndValidate(ctx, selectQuery, catalogAdapter)
	if err != nil {
		log.Printf("SELECT validation failed: %v", err)
	} else {
		fmt.Printf("SELECT statement parsed and validated successfully\n")
		// You can now use the parsed AST node for execution
		_ = node
	}

	// Example 2: Parse and validate a CREATE TABLE statement
	createQuery := "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2));"
	node, err = parser.ParseAndValidate(ctx, createQuery, catalogAdapter)
	if err != nil {
		log.Printf("CREATE TABLE validation failed: %v", err)
	} else {
		fmt.Printf("CREATE TABLE statement parsed and validated successfully\n")
		// You can now use the parsed AST node for execution
		_ = node
	}

	// Example 3: Parse and validate an INSERT statement
	insertQuery := "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99);"
	node, err = parser.ParseAndValidate(ctx, insertQuery, catalogAdapter)
	if err != nil {
		log.Printf("INSERT validation failed: %v", err)
	} else {
		fmt.Printf("INSERT statement parsed and validated successfully\n")
		// You can now use the parsed AST node for execution
		_ = node
	}

	// Example 4: Parse and validate an UPDATE statement
	updateQuery := "UPDATE products SET price = 899.99 WHERE id = 1;"
	node, err = parser.ParseAndValidate(ctx, updateQuery, catalogAdapter)
	if err != nil {
		log.Printf("UPDATE validation failed: %v", err)
	} else {
		fmt.Printf("UPDATE statement parsed and validated successfully\n")
		// You can now use the parsed AST node for execution
		_ = node
	}

	// Example 5: Parse and validate a DELETE statement
	deleteQuery := "DELETE FROM products WHERE id = 1;"
	node, err = parser.ParseAndValidate(ctx, deleteQuery, catalogAdapter)
	if err != nil {
		log.Printf("DELETE validation failed: %v", err)
	} else {
		fmt.Printf("DELETE statement parsed and validated successfully\n")
		// You can now use the parsed AST node for execution
		_ = node
	}
}

// ExampleWithIceboxCatalog demonstrates using the Icebox-specific catalog adapter
func ExampleWithIceboxCatalog() {
	// This would require a config object
	// cfg := &config.Config{...}
	// catalogAdapter, err := NewIceboxCatalogAdapter(cfg)
	// if err != nil {
	//     log.Fatalf("Failed to create Icebox catalog adapter: %v", err)
	// }

	fmt.Println("Icebox catalog adapter example would go here")
	fmt.Println("(Requires config object to be implemented)")
}
