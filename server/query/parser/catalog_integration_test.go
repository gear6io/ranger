package parser

import (
	"context"
	"testing"
)

func TestCatalogAdapterInterface(t *testing.T) {
	// Test that DefaultCatalogAdapter implements CatalogAdapter interface
	var _ CatalogAdapter = &DefaultCatalogAdapter{}
}

func TestDefaultCatalogAdapter(t *testing.T) {
	adapter := NewDefaultCatalogAdapter()
	ctx := context.Background()

	// Test basic validation methods
	if err := adapter.ValidateTable(ctx, "test_table"); err != nil {
		t.Errorf("ValidateTable failed: %v", err)
	}

	if err := adapter.ValidateColumn(ctx, "test_table", "test_column"); err != nil {
		t.Errorf("ValidateColumn failed: %v", err)
	}

	if err := adapter.ValidateDatabase(ctx, "test_db"); err != nil {
		t.Errorf("ValidateDatabase failed: %v", err)
	}

	if err := adapter.ValidateDataType("INT"); err != nil {
		t.Errorf("ValidateDataType failed: %v", err)
	}

	if !adapter.IsCompatibleDataType("INT", "BIGINT") {
		t.Error("IsCompatibleDataType should return true for INT and BIGINT")
	}

	// Test context methods
	if db := adapter.GetCurrentDatabase(ctx); db != "default" {
		t.Errorf("GetCurrentDatabase returned %s, expected 'default'", db)
	}

	if user := adapter.GetCurrentUser(ctx); user != "default" {
		t.Errorf("GetCurrentUser returned %s, expected 'default'", user)
	}
}

func TestEnhancedParserWithCatalog(t *testing.T) {
	adapter := NewDefaultCatalogAdapter()
	enhancedParser := NewEnhancedParser(adapter)
	ctx := context.Background()

	// Test simple SELECT query
	query := "SELECT * FROM users;"
	node, err := enhancedParser.ParseAndValidate(ctx, query, adapter)
	if err != nil {
		t.Errorf("ParseAndValidate failed: %v", err)
	}

	if node == nil {
		t.Error("ParseAndValidate returned nil node")
	}

	// Test CREATE TABLE query
	ddlQuery := "CREATE TABLE test (id INT);"
	ddlNode, err := enhancedParser.ParseAndValidate(ctx, ddlQuery, adapter)
	if err != nil {
		t.Errorf("DDL ParseAndValidate failed: %v", err)
	}

	if ddlNode == nil {
		t.Error("DDL ParseAndValidate returned nil node")
	}
}

func TestCatalogOperationTypes(t *testing.T) {
	// Test operation type constants
	if OpCreateTable != 0 {
		t.Errorf("OpCreateTable should be 0, got %d", OpCreateTable)
	}

	if OpDropTable != 1 {
		t.Errorf("OpDropTable should be 1, got %d", OpDropTable)
	}

	if OpAlterTable != 2 {
		t.Errorf("OpAlterTable should be 2, got %d", OpAlterTable)
	}
}

func TestAlterOperationTypes(t *testing.T) {
	// Test alter operation type constants
	if AlterAddColumn != 0 {
		t.Errorf("AlterAddColumn should be 0, got %d", AlterAddColumn)
	}

	if AlterDropColumn != 1 {
		t.Errorf("AlterDropColumn should be 1, got %d", AlterDropColumn)
	}

	if AlterModifyColumn != 2 {
		t.Errorf("AlterModifyColumn should be 2, got %d", AlterModifyColumn)
	}
}
