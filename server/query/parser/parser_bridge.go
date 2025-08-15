package parser

import (
	"context"
	"fmt"
)

// ParserCatalogBridge defines the interface for integrating parser with catalog validation
type ParserCatalogBridge interface {
	// ParseWithCatalog parses a query with catalog validation
	ParseWithCatalog(ctx context.Context, query string, catalog CatalogAdapter) (Node, error)

	// ValidateAST validates an AST against catalog constraints
	ValidateAST(ctx context.Context, node Node, catalog CatalogAdapter) error

	// ParseAndValidate combines parsing and validation in one step
	ParseAndValidate(ctx context.Context, query string, catalog CatalogAdapter) (Node, error)
}

// EnhancedParser extends the basic parser with catalog integration capabilities
type EnhancedParser struct {
	*Parser
	catalog CatalogAdapter
}

// NewEnhancedParser creates a new enhanced parser with catalog integration
func NewEnhancedParser(catalog CatalogAdapter) *EnhancedParser {
	return &EnhancedParser{
		Parser:  nil, // Will be set when parsing
		catalog: catalog,
	}
}

// ParseWithCatalog parses a query with catalog validation
func (ep *EnhancedParser) ParseWithCatalog(ctx context.Context, query string, catalog CatalogAdapter) (Node, error) {
	// Create lexer and parser
	lexer := NewLexer([]byte(query))
	parser := NewParser(lexer)
	ep.Parser = parser

	// Parse the query
	node, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	// Validate against catalog
	if err := ep.ValidateAST(ctx, node, catalog); err != nil {
		return nil, fmt.Errorf("catalog validation failed: %w", err)
	}

	return node, nil
}

// ValidateAST validates an AST against catalog constraints
func (ep *EnhancedParser) ValidateAST(ctx context.Context, node Node, catalog CatalogAdapter) error {
	if catalog == nil {
		// No catalog available, skip validation
		return nil
	}

	// Perform validation based on node type
	switch stmt := node.(type) {
	case *SelectStmt:
		return ep.validateSelectStmt(ctx, stmt, catalog)
	case *InsertStmt:
		return ep.validateInsertStmt(ctx, stmt, catalog)
	case *UpdateStmt:
		return ep.validateUpdateStmt(ctx, stmt, catalog)
	case *DeleteStmt:
		return ep.validateDeleteStmt(ctx, stmt, catalog)
	case *CreateTableStmt:
		return ep.validateCreateTableStmt(ctx, stmt, catalog)
	case *AlterTableStmt:
		return ep.validateAlterTableStmt(ctx, stmt, catalog)
	case *DropTableStmt:
		return ep.validateDropTableStmt(ctx, stmt, catalog)
	case *UseStmt:
		return ep.validateUseStmt(ctx, stmt, catalog)
	default:
		// Unknown statement type, skip validation
		return nil
	}
}

// ParseAndValidate combines parsing and validation in one step
func (ep *EnhancedParser) ParseAndValidate(ctx context.Context, query string, catalog CatalogAdapter) (Node, error) {
	return ep.ParseWithCatalog(ctx, query, catalog)
}

// Validation methods for different statement types
func (ep *EnhancedParser) validateSelectStmt(ctx context.Context, stmt *SelectStmt, catalog CatalogAdapter) error {
	if stmt.TableExpression == nil || stmt.TableExpression.FromClause == nil {
		return nil
	}

	// Validate tables in FROM clause
	for _, table := range stmt.TableExpression.FromClause.Tables {
		if err := catalog.ValidateTable(ctx, table.Name.Value); err != nil {
			return fmt.Errorf("invalid table '%s': %w", table.Name.Value, err)
		}
	}

	// Validate columns in SELECT list
	if stmt.SelectList != nil {
		for _, expr := range stmt.SelectList.Expressions {
			if err := ep.validateValueExpression(ctx, expr, catalog); err != nil {
				return err
			}
		}
	}

	return nil
}

func (ep *EnhancedParser) validateInsertStmt(ctx context.Context, stmt *InsertStmt, catalog CatalogAdapter) error {
	// Validate target table
	if err := catalog.ValidateTable(ctx, stmt.TableName.Value); err != nil {
		return fmt.Errorf("invalid table '%s': %w", stmt.TableName.Value, err)
	}

	// Validate columns if specified
	if stmt.ColumnNames != nil {
		for _, col := range stmt.ColumnNames {
			if err := catalog.ValidateColumn(ctx, stmt.TableName.Value, col.Value); err != nil {
				return fmt.Errorf("invalid column '%s' in table '%s': %w", col.Value, stmt.TableName.Value, err)
			}
		}
	}

	return nil
}

func (ep *EnhancedParser) validateUpdateStmt(ctx context.Context, stmt *UpdateStmt, catalog CatalogAdapter) error {
	// Validate target table
	if err := catalog.ValidateTable(ctx, stmt.TableName.Value); err != nil {
		return fmt.Errorf("invalid table '%s': %w", stmt.TableName.Value, err)
	}

	// Validate columns in SET clause
	for _, set := range stmt.SetClause {
		if err := catalog.ValidateColumn(ctx, stmt.TableName.Value, set.Column.Value); err != nil {
			return fmt.Errorf("invalid column '%s' in table '%s': %w", set.Column.Value, stmt.TableName.Value, err)
		}
	}

	return nil
}

func (ep *EnhancedParser) validateDeleteStmt(ctx context.Context, stmt *DeleteStmt, catalog CatalogAdapter) error {
	// Validate target table
	if err := catalog.ValidateTable(ctx, stmt.TableName.Value); err != nil {
		return fmt.Errorf("invalid table '%s': %w", stmt.TableName.Value, err)
	}

	return nil
}

func (ep *EnhancedParser) validateCreateTableStmt(ctx context.Context, stmt *CreateTableStmt, catalog CatalogAdapter) error {
	// Validate data types for columns
	if stmt.TableSchema != nil {
		for colName, colDef := range stmt.TableSchema.ColumnDefinitions {
			if err := catalog.ValidateDataType(colDef.DataType); err != nil {
				return fmt.Errorf("invalid data type '%s' for column '%s': %w", colDef.DataType, colName, err)
			}
		}
	}

	return nil
}

func (ep *EnhancedParser) validateAlterTableStmt(ctx context.Context, stmt *AlterTableStmt, catalog CatalogAdapter) error {
	// Validate target table exists
	if err := catalog.ValidateTable(ctx, stmt.TableName.Value); err != nil {
		return fmt.Errorf("invalid table '%s': %w", stmt.TableName.Value, err)
	}

	// Validate column exists if altering column
	if stmt.ColumnName != nil {
		if err := catalog.ValidateColumn(ctx, stmt.TableName.Value, stmt.ColumnName.Value); err != nil {
			return fmt.Errorf("invalid column '%s' in table '%s': %w", stmt.ColumnName.Value, stmt.TableName.Value, err)
		}
	}

	// Validate new column definition if provided
	if stmt.ColumnDefinition != nil {
		if err := catalog.ValidateDataType(stmt.ColumnDefinition.DataType); err != nil {
			return fmt.Errorf("invalid data type '%s': %w", stmt.ColumnDefinition.DataType, err)
		}
	}

	return nil
}

func (ep *EnhancedParser) validateDropTableStmt(ctx context.Context, stmt *DropTableStmt, catalog CatalogAdapter) error {
	// Validate target table exists
	if err := catalog.ValidateTable(ctx, stmt.TableName.Value); err != nil {
		return fmt.Errorf("invalid table '%s': %w", stmt.TableName.Value, err)
	}

	return nil
}

func (ep *EnhancedParser) validateUseStmt(ctx context.Context, stmt *UseStmt, catalog CatalogAdapter) error {
	// Validate database exists
	if err := catalog.ValidateDatabase(ctx, stmt.DatabaseName.Value); err != nil {
		return fmt.Errorf("invalid database '%s': %w", stmt.DatabaseName.Value, err)
	}

	return nil
}

func (ep *EnhancedParser) validateValueExpression(ctx context.Context, expr *ValueExpression, catalog CatalogAdapter) error {
	// This is a simplified validation - can be enhanced based on expression types
	// For now, just return success
	return nil
}
