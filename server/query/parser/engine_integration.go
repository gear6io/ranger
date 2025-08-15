package parser

import (
	"context"
	"fmt"
	"log"
)

// EngineIntegration shows how to integrate the catalog-aware parser with the query engine
type EngineIntegration struct {
	parser  *EnhancedParser
	catalog CatalogAdapter
}

// NewEngineIntegration creates a new engine integration
func NewEngineIntegration(catalog CatalogAdapter) *EngineIntegration {
	return &EngineIntegration{
		parser:  NewEnhancedParser(catalog),
		catalog: catalog,
	}
}

// ParseAndValidateQuery parses and validates a query using catalog information
func (ei *EngineIntegration) ParseAndValidateQuery(ctx context.Context, query string) (Node, error) {
	return ei.parser.ParseAndValidate(ctx, query, ei.catalog)
}

// ExecuteQueryWithValidation demonstrates the full flow from parsing to execution
func (ei *EngineIntegration) ExecuteQueryWithValidation(ctx context.Context, query string) error {
	// Step 1: Parse and validate with catalog
	node, err := ei.ParseAndValidateQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("parsing and validation failed: %w", err)
	}

	// Step 2: Analyze the parsed statement
	stmtType, err := ei.analyzeStatement(node)
	if err != nil {
		return fmt.Errorf("statement analysis failed: %w", err)
	}

	// Step 3: Route to appropriate execution path
	switch stmtType {
	case "SELECT":
		return ei.executeReadQuery(ctx, node)
	case "INSERT":
		return ei.executeWriteQuery(ctx, node)
	case "CREATE":
		return ei.executeDDLQuery(ctx, node)
	case "UPDATE":
		return ei.executeWriteQuery(ctx, node)
	case "DELETE":
		return ei.executeWriteQuery(ctx, node)
	default:
		return fmt.Errorf("unsupported statement type: %s", stmtType)
	}
}

// analyzeStatement determines the type of SQL statement
func (ei *EngineIntegration) analyzeStatement(node Node) (string, error) {
	switch node.(type) {
	case *SelectStmt:
		return "SELECT", nil
	case *InsertStmt:
		return "INSERT", nil
	case *UpdateStmt:
		return "UPDATE", nil
	case *DeleteStmt:
		return "DELETE", nil
	case *CreateTableStmt:
		return "CREATE", nil
	case *CreateDatabaseStmt:
		return "CREATE", nil
	case *CreateIndexStmt:
		return "CREATE", nil
	case *DropTableStmt:
		return "DROP", nil
	case *DropDatabaseStmt:
		return "DROP", nil
	case *DropIndexStmt:
		return "DROP", nil
	case *AlterTableStmt:
		return "ALTER", nil
	default:
		return "UNKNOWN", fmt.Errorf("unknown statement type: %T", node)
	}
}

// executeReadQuery handles read operations (SELECT)
func (ei *EngineIntegration) executeReadQuery(ctx context.Context, node Node) error {
	log.Printf("Executing read query: %T", node)
	// TODO: Route to DuckDB or native execution engine
	return nil
}

// executeWriteQuery handles write operations (INSERT, UPDATE, DELETE)
func (ei *EngineIntegration) executeWriteQuery(ctx context.Context, node Node) error {
	log.Printf("Executing write query: %T", node)
	// TODO: Route to native execution engine or DuckDB
	return nil
}

// executeDDLQuery handles DDL operations (CREATE, DROP, ALTER)
func (ei *EngineIntegration) executeDDLQuery(ctx context.Context, node Node) error {
	log.Printf("Executing DDL query: %T", node)
	// TODO: Execute DDL operations using catalog
	return nil
}

// EngineIntegrationExample demonstrates how to use the engine integration
func EngineIntegrationExample() {
	// Create catalog adapter
	catalogAdapter := NewDefaultCatalogAdapter()

	// Create engine integration
	integration := NewEngineIntegration(catalogAdapter)

	// Example queries
	queries := []string{
		"SELECT * FROM users WHERE age > 18;",
		"CREATE TABLE test_table (id INT, name VARCHAR(255));",
		"INSERT INTO test_table VALUES (1, 'John');",
		"UPDATE test_table SET name = 'Jane' WHERE id = 1;",
		"DELETE FROM test_table WHERE id = 1;",
	}

	ctx := context.Background()

	for _, query := range queries {
		fmt.Printf("Processing query: %s\n", query)

		err := integration.ExecuteQueryWithValidation(ctx, query)
		if err != nil {
			log.Printf("Query execution failed: %v", err)
		} else {
			fmt.Printf("Query executed successfully\n")
		}
		fmt.Println()
	}
}
