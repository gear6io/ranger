package parser

import (
	"strings"
	"testing"
)

// TestErrorCollectorBasicFunctionality tests the basic functionality of ErrorCollector
func TestErrorCollectorBasicFunctionality(t *testing.T) {
	collector := NewErrorCollector()

	// Test empty collector
	if collector.HasErrors() {
		t.Error("New collector should not have errors")
	}

	if collector.GetErrorCount() != 0 {
		t.Error("New collector should have 0 errors")
	}

	// Create some test errors
	tracker := NewPositionTracker([]byte("SELECT * FROM table WHERE"))

	err1 := &ParseError{
		Code:     ErrExpectedIdentifier,
		Message:  "expected identifier",
		Position: TokenPosition{Offset: 10, Length: 1},
		Expected: []string{"identifier"},
		Found:    "*",
		Category: SyntaxError,
		tracker:  tracker,
	}

	err2 := &ParseError{
		Code:     ErrMissingSemicolon,
		Message:  "expected ';'",
		Position: TokenPosition{Offset: 25, Length: 0},
		Expected: []string{";"},
		Found:    "end of input",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Add errors
	if !collector.AddError(err1) {
		t.Error("Should be able to add first error")
	}

	if !collector.AddError(err2) {
		t.Error("Should be able to add second error")
	}

	// Test collector state
	if !collector.HasErrors() {
		t.Error("Collector should have errors")
	}

	if collector.GetErrorCount() != 2 {
		t.Errorf("Expected 2 errors, got %d", collector.GetErrorCount())
	}

	// Test error ordering
	sortedErrors := collector.GetErrors()
	if len(sortedErrors) != 2 {
		t.Errorf("Expected 2 sorted errors, got %d", len(sortedErrors))
	}

	// First error should be at offset 10, second at offset 25
	if sortedErrors[0].Position.Offset != 10 {
		t.Errorf("First error should be at offset 10, got %d", sortedErrors[0].Position.Offset)
	}

	if sortedErrors[1].Position.Offset != 25 {
		t.Errorf("Second error should be at offset 25, got %d", sortedErrors[1].Position.Offset)
	}
}

// TestErrorCollectorLimitReached tests the error limit functionality
func TestErrorCollectorLimitReached(t *testing.T) {
	collector := NewErrorCollector()
	tracker := NewPositionTracker([]byte("SELECT * FROM table"))

	// Add errors up to the limit
	for i := 0; i < 10; i++ {
		err := &ParseError{
			Code:     ErrExpectedIdentifier,
			Message:  "test error",
			Position: TokenPosition{Offset: i, Length: 1},
			Category: SyntaxError,
			tracker:  tracker,
		}

		canAddMore := collector.AddError(err)
		if i < 9 && !canAddMore {
			t.Errorf("Should be able to add error %d", i)
		}
	}

	// Try to add one more error (should fail)
	extraErr := &ParseError{
		Code:     ErrExpectedIdentifier,
		Message:  "extra error",
		Position: TokenPosition{Offset: 100, Length: 1},
		Category: SyntaxError,
		tracker:  tracker,
	}

	if collector.AddError(extraErr) {
		t.Error("Should not be able to add error beyond limit")
	}

	if !collector.IsLimitReached() {
		t.Error("Collector should report limit reached")
	}
}

// TestErrorCollectorGrouping tests error grouping functionality
func TestErrorCollectorGrouping(t *testing.T) {
	collector := NewErrorCollector()
	tracker := NewPositionTracker([]byte("SELECT * FROM table WHERE col = 'value'"))

	// Add errors of different categories
	syntaxErr := &ParseError{
		Code:     ErrExpectedIdentifier,
		Message:  "syntax error",
		Position: TokenPosition{Offset: 10, Length: 1},
		Category: SyntaxError,
		tracker:  tracker,
	}

	typeErr := &ParseError{
		Code:     ErrInvalidIcebergType,
		Message:  "type error",
		Position: TokenPosition{Offset: 20, Length: 5},
		Category: TypeError,
		tracker:  tracker,
	}

	collector.AddError(syntaxErr)
	collector.AddError(typeErr)

	// Test grouping by category
	categoryGroups := collector.GetErrorsByCategory()
	if len(categoryGroups) != 2 {
		t.Errorf("Expected 2 category groups, got %d", len(categoryGroups))
	}

	if len(categoryGroups[SyntaxError]) != 1 {
		t.Errorf("Expected 1 syntax error, got %d", len(categoryGroups[SyntaxError]))
	}

	if len(categoryGroups[TypeError]) != 1 {
		t.Errorf("Expected 1 type error, got %d", len(categoryGroups[TypeError]))
	}

	// Test grouping by line
	lineGroups := collector.GetErrorsByLine()
	if len(lineGroups) != 1 {
		t.Errorf("Expected 1 line group, got %d", len(lineGroups))
	}

	if len(lineGroups[1]) != 2 {
		t.Errorf("Expected 2 errors on line 1, got %d", len(lineGroups[1]))
	}
}

// TestErrorCollectorSummary tests error summary functionality
func TestErrorCollectorSummary(t *testing.T) {
	collector := NewErrorCollector()
	tracker := NewPositionTracker([]byte("SELECT * FROM table"))

	// Test empty summary
	summary := collector.GetErrorSummary()
	if summary != "No errors" {
		t.Errorf("Expected 'No errors', got '%s'", summary)
	}

	// Add one error
	err := &ParseError{
		Code:     ErrExpectedIdentifier,
		Message:  "test error",
		Position: TokenPosition{Offset: 10, Length: 1},
		Category: SyntaxError,
		tracker:  tracker,
	}
	collector.AddError(err)

	summary = collector.GetErrorSummary()
	if !strings.Contains(summary, "1 error at line") {
		t.Errorf("Expected single error summary, got '%s'", summary)
	}

	// Add another error of different category
	typeErr := &ParseError{
		Code:     ErrInvalidIcebergType,
		Message:  "type error",
		Position: TokenPosition{Offset: 15, Length: 1},
		Category: TypeError,
		tracker:  tracker,
	}
	collector.AddError(typeErr)

	summary = collector.GetErrorSummary()
	if !strings.Contains(summary, "2 total errors") {
		t.Errorf("Expected multi-error summary, got '%s'", summary)
	}
}

// TestErrorRecoveryBasic tests basic error recovery functionality
func TestErrorRecoveryBasic(t *testing.T) {
	// Test with a SQL statement that has multiple errors
	sql := "CREATE TABLE test (id int32, name string"
	lexer := NewLexer([]byte(sql))
	parser := NewParser(lexer)

	// Use ParseWithRecovery to collect multiple errors
	_, err := parser.ParseWithRecovery()

	// Should have errors due to missing closing parenthesis and semicolon
	if err == nil {
		t.Error("Expected parsing errors")
	}

	// Check if error collector has multiple errors
	if !parser.errorCollector.HasErrors() {
		t.Error("Expected error collector to have errors")
	}

	// Should have attempted recovery
	if !parser.errorCollector.WasRecovered() {
		t.Error("Expected error recovery to be attempted")
	}
}

// TestMultiErrorFormatting tests the formatting of multiple errors
func TestMultiErrorFormatting(t *testing.T) {
	collector := NewErrorCollector()
	tracker := NewPositionTracker([]byte("SELECT * FROM table WHERE"))

	// Add multiple errors
	err1 := &ParseError{
		Code:     ErrExpectedIdentifier,
		Message:  "expected identifier",
		Position: TokenPosition{Offset: 7, Length: 1},
		Expected: []string{"identifier"},
		Found:    "*",
		Category: SyntaxError,
		tracker:  tracker,
	}

	err2 := &ParseError{
		Code:     ErrMissingSemicolon,
		Message:  "expected ';'",
		Position: TokenPosition{Offset: 25, Length: 0},
		Expected: []string{";"},
		Found:    "end of input",
		Category: SyntaxError,
		tracker:  tracker,
	}

	collector.AddError(err1)
	collector.AddError(err2)

	// Test formatted output
	formatted := collector.FormatAllErrors()
	if !strings.Contains(formatted, "Found 2 parsing errors") {
		t.Error("Formatted output should mention 2 errors")
	}

	if !strings.Contains(formatted, "Error 1:") {
		t.Error("Formatted output should have Error 1")
	}

	if !strings.Contains(formatted, "Error 2:") {
		t.Error("Formatted output should have Error 2")
	}
}
