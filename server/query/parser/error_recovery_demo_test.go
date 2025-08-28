package parser

import (
	"strings"
	"testing"
)

// TestErrorRecoveryComparison demonstrates the difference between regular parsing and error recovery
func TestErrorRecoveryComparison(t *testing.T) {
	// SQL with multiple errors: missing closing parenthesis and semicolon
	sql := "CREATE TABLE test (id int32, name string"

	t.Run("Regular Parse - Returns First Error", func(t *testing.T) {
		lexer := NewLexer([]byte(sql))
		parser := NewParser(lexer)

		_, err := parser.Parse()

		// Should return the first error (missing semicolon)
		if err == nil {
			t.Error("Expected an error")
		}

		// Should be a single error, not multiple
		if strings.Contains(err.Error(), "multiple parsing errors") {
			t.Error("Regular Parse should not return multiple errors")
		}

		// Error collector should be empty since we didn't use recovery
		if parser.errorCollector.HasErrors() {
			t.Error("Regular Parse should not collect errors")
		}
	})

	t.Run("Parse With Recovery - Collects Multiple Errors", func(t *testing.T) {
		lexer := NewLexer([]byte(sql))
		parser := NewParser(lexer)

		_, err := parser.ParseWithRecovery()

		// Should return an error
		if err == nil {
			t.Error("Expected an error")
		}

		// Error collector should have multiple errors
		if !parser.errorCollector.HasErrors() {
			t.Error("ParseWithRecovery should collect errors")
		}

		if parser.errorCollector.GetErrorCount() < 1 {
			t.Error("Expected at least 1 error in collector")
		}

		// Should have attempted recovery
		if !parser.errorCollector.WasRecovered() {
			t.Error("Expected error recovery to be attempted")
		}
	})
}

// TestErrorRecoveryFormatting demonstrates error formatting capabilities
func TestErrorRecoveryFormatting(t *testing.T) {
	sql := "CREATE TABLE test (id int32, name string"
	lexer := NewLexer([]byte(sql))
	parser := NewParser(lexer)

	_, err := parser.ParseWithRecovery()

	if err == nil {
		t.Error("Expected an error")
	}

	// Test formatted output
	formatted := parser.errorCollector.FormatAllErrors()
	if formatted == "" {
		t.Error("Expected formatted error output")
	}

	// Test error summary
	summary := parser.errorCollector.GetErrorSummary()
	if summary == "" {
		t.Error("Expected error summary")
	}

	t.Logf("Error Summary: %s", summary)
	t.Logf("Formatted Errors:\n%s", formatted)
}

// TestErrorRecoveryLimits tests that error recovery respects limits
func TestErrorRecoveryLimits(t *testing.T) {
	// Create a SQL with many potential errors
	sql := "INVALID KEYWORD test (id int32, name string"
	lexer := NewLexer([]byte(sql))
	parser := NewParser(lexer)

	_, err := parser.ParseWithRecovery()

	if err == nil {
		t.Error("Expected an error")
	}

	// Should not exceed the error limit
	if parser.errorCollector.GetErrorCount() > 10 {
		t.Errorf("Error count should not exceed 10, got %d", parser.errorCollector.GetErrorCount())
	}
}
