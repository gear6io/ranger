package jdbc

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestSQLParserMiddlewareCreation(t *testing.T) {
	logger := zerolog.Nop()
	middleware := NewSQLParserMiddleware(logger)

	if middleware == nil {
		t.Fatal("Failed to create SQLParserMiddleware")
	}

	// Note: We can't directly compare zerolog.Logger instances
	// as they contain unexported fields that can't be compared
}

func TestSQLParserMiddlewareAnalyzeQuery(t *testing.T) {
	logger := zerolog.Nop()
	middleware := NewSQLParserMiddleware(logger)

	// Test a simple SELECT query
	query := "SELECT * FROM users;"
	analysis, err := middleware.AnalyzeQuery(query)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if analysis == nil {
		t.Fatal("Expected analysis result, got nil")
	}

	if analysis.StatementType != "SELECT" {
		t.Errorf("Expected statement type 'SELECT', got: %s", analysis.StatementType)
	}

	if !analysis.IsValid {
		t.Error("Expected query to be valid")
	}
}

func TestSQLParserMiddlewareValidateQuery(t *testing.T) {
	logger := zerolog.Nop()
	middleware := NewSQLParserMiddleware(logger)

	// Test a valid query
	validQuery := "SELECT * FROM users;"
	analysis, err := middleware.AnalyzeQuery(validQuery)
	if err != nil {
		t.Fatalf("Failed to analyze valid query: %v", err)
	}

	err = middleware.ValidateQuery(analysis)
	if err != nil {
		t.Errorf("Expected no validation error for valid query, got: %v", err)
	}

	// Test a blocked query (DROP TABLE)
	blockedQuery := "DROP TABLE users;"
	blockedAnalysis, err := middleware.AnalyzeQuery(blockedQuery)
	if err != nil {
		t.Fatalf("Failed to analyze blocked query: %v", err)
	}

	err = middleware.ValidateQuery(blockedAnalysis)
	if err == nil {
		t.Error("Expected validation error for blocked query, got none")
	}
}

func TestSQLParserMiddlewareGenerateCommandCompleteTag(t *testing.T) {
	logger := zerolog.Nop()
	middleware := NewSQLParserMiddleware(logger)

	// Test SELECT statement
	selectQuery := "SELECT * FROM users;"
	selectAnalysis, err := middleware.AnalyzeQuery(selectQuery)
	if err != nil {
		t.Fatalf("Failed to analyze SELECT query: %v", err)
	}

	tag := middleware.GenerateCommandCompleteTag(selectAnalysis, 5)
	expectedTag := "SELECT 5"
	if tag != expectedTag {
		t.Errorf("Expected tag '%s', got: %s", expectedTag, tag)
	}

	// Test INSERT statement
	insertQuery := "INSERT INTO users (name) VALUES ('test');"
	insertAnalysis, err := middleware.AnalyzeQuery(insertQuery)
	if err != nil {
		t.Fatalf("Failed to analyze INSERT query: %v", err)
	}

	tag = middleware.GenerateCommandCompleteTag(insertAnalysis, 1)
	expectedTag = "INSERT 0 1"
	if tag != expectedTag {
		t.Errorf("Expected tag '%s', got: %s", expectedTag, tag)
	}
}
