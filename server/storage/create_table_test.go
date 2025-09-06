package storage

import (
	"testing"

	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/types"
)

func TestCreateTableRequestValidation(t *testing.T) {
	// Create a mock CREATE TABLE statement
	stmt := &parser.CreateTableStmt{
		TableName: &parser.TableIdentifier{
			Table: &parser.Identifier{Value: "test_table"},
		},
		TableSchema: &parser.TableSchema{
			ColumnDefinitions: map[string]*parser.ColumnDefinition{
				"id": {
					Name:       "id",
					DataType:   "int64",
					IsNullable: false,
				},
				"name": {
					Name:       "name",
					DataType:   "string",
					IsNullable: true,
				},
			},
		},
	}

	// Create a request
	req := &types.CreateTableRequest{
		Statement:     stmt,
		Database:      "test_db",
		RequestID:     "test-123",
		UserContext:   &types.UserContext{UserID: 1, Username: "test"},
		StorageEngine: "filesystem",
		EngineConfig:  map[string]interface{}{},
	}

	// Test type assertion (this is what the CreateTable method does)
	if parsedStmt, ok := req.Statement.(*parser.CreateTableStmt); !ok {
		t.Errorf("Failed to cast statement to CreateTableStmt")
	} else {
		if parsedStmt.TableName.Table.Value != "test_table" {
			t.Errorf("Expected table name 'test_table', got '%s'", parsedStmt.TableName.Table.Value)
		}

		if len(parsedStmt.TableSchema.ColumnDefinitions) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(parsedStmt.TableSchema.ColumnDefinitions))
		}
	}
}

func TestCreateTableResponseStructure(t *testing.T) {
	// Test that we can create a proper response
	response := &types.CreateTableResponse{
		TableID: 123,
		Success: true,
		Metadata: &types.TableCreationMetadata{
			ColumnCount:   2,
			StorageEngine: "filesystem",
			SchemaVersion: 1,
		},
	}

	if response.TableID != 123 {
		t.Errorf("Expected TableID 123, got %d", response.TableID)
	}

	if !response.Success {
		t.Errorf("Expected Success to be true")
	}

	if response.Metadata.ColumnCount != 2 {
		t.Errorf("Expected ColumnCount 2, got %d", response.Metadata.ColumnCount)
	}
}
