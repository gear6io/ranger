package parser

import (
	stderrors "errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gear6io/ranger/pkg/errors"
)

func TestParseError_BasicFunctionality(t *testing.T) {
	source := []byte("SELECT * FROM users WHERE id = 123;")
	tracker := NewPositionTracker(source)

	// Create a sample token at position 14 (the "users" token)
	token := &Token{
		tokenT: IDENT_TOK,
		value:  "users",
		Position: TokenPosition{
			Offset: 14,
			Length: 5,
		},
	}

	// Create a ParseError
	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected table name",
		Position: token.Position,
		Token:    token,
		Expected: []string{"table_name", "identifier"},
		Found:    "users",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test Error() method
	errorMsg := parseErr.Error()
	if !strings.Contains(errorMsg, "parse error at line 1, column 15") {
		t.Errorf("Error() should contain position info, got: %s", errorMsg)
	}
	if !strings.Contains(errorMsg, "expected table name") {
		t.Errorf("Error() should contain message, got: %s", errorMsg)
	}

	// Test DetailedError() method
	detailedMsg := parseErr.DetailedError()
	if !strings.Contains(detailedMsg, "Error at line 1, column 15 (syntax)") {
		t.Errorf("DetailedError() should contain position and category, got: %s", detailedMsg)
	}
	if !strings.Contains(detailedMsg, "SELECT * FROM users WHERE id = 123;") {
		t.Errorf("DetailedError() should contain source line, got: %s", detailedMsg)
	}
	if !strings.Contains(detailedMsg, "Expected: table_name, identifier") {
		t.Errorf("DetailedError() should contain expected tokens, got: %s", detailedMsg)
	}
	if !strings.Contains(detailedMsg, "Found: users") {
		t.Errorf("DetailedError() should contain found token, got: %s", detailedMsg)
	}
}

func TestParseError_InternalErrorInterface(t *testing.T) {
	source := []byte("CREATE TABLE test (id int32);")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected semicolon",
		Position: TokenPosition{Offset: 25, Length: 1},
		Expected: []string{";"},
		Found:    ")",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test that it implements InternalError interface
	var internalErr errors.InternalError = parseErr
	if internalErr == nil {
		t.Fatal("ParseError should implement InternalError interface")
	}

	// Test Transform() method
	transformed := parseErr.Transform()
	if transformed == nil {
		t.Fatal("Transform() should return non-nil error")
	}

	// Check that context is preserved
	context := errors.GetContext(transformed)
	if context == nil {
		t.Fatal("Transform() should preserve context")
	}

	if context["line"] != "1" {
		t.Errorf("Expected line=1, got line=%s", context["line"])
	}
	if context["column"] != "26" {
		t.Errorf("Expected column=26, got column=%s", context["column"])
	}
	if context["category"] != "syntax" {
		t.Errorf("Expected category=syntax, got category=%s", context["category"])
	}
	if context["expected"] != ";" {
		t.Errorf("Expected expected=';', got expected=%s", context["expected"])
	}
	if context["found"] != ")" {
		t.Errorf("Expected found=')', got found=%s", context["found"])
	}
}

func TestParseError_VisualContext(t *testing.T) {
	source := []byte("SELECT name, age\nFROM users\nWHERE age > 18;")
	tracker := NewPositionTracker(source)

	// Error at "users" token on line 2
	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected table name",
		Position: TokenPosition{Offset: 22, Length: 5}, // "users" position
		Category: SyntaxError,
		tracker:  tracker,
	}

	visualContext := parseErr.GetVisualContext()
	if !strings.Contains(visualContext, "FROM users") {
		t.Errorf("Visual context should contain source line, got: %s", visualContext)
	}
	if !strings.Contains(visualContext, "^^^^^") {
		t.Errorf("Visual context should contain caret pointer for token length, got: %s", visualContext)
	}
}

func TestParseError_LineTruncation(t *testing.T) {
	// Create a very long line to test truncation
	longLine := "SELECT " + strings.Repeat("very_long_column_name, ", 20) + "FROM users;"
	source := []byte(longLine)
	tracker := NewPositionTracker(source)

	// Error near the end of the line
	errorPos := len(longLine) - 10
	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected semicolon",
		Position: TokenPosition{Offset: errorPos, Length: 1},
		Category: SyntaxError,
		tracker:  tracker,
	}

	visualContext := parseErr.GetVisualContext()
	// Should contain truncation indicators
	if !strings.Contains(visualContext, "...") {
		t.Errorf("Visual context should contain truncation indicators for long lines, got: %s", visualContext)
	}
}

func TestIsParseError(t *testing.T) {
	source := []byte("SELECT * FROM users;")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "test error",
		Position: TokenPosition{Offset: 0, Length: 6},
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test with ParseError
	if pe, ok := IsParseError(parseErr); !ok || pe != parseErr {
		t.Error("IsParseError should return true and the same ParseError for ParseError input")
	}

	// Test with regular error
	regularErr := errors.New(errors.CommonInternal, "regular error", nil)
	if pe, ok := IsParseError(regularErr); ok || pe != nil {
		t.Error("IsParseError should return false and nil for non-ParseError input")
	}
}

func TestParseError_AsErrorIntegration(t *testing.T) {
	source := []byte("SELECT * FROM users;")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "test error",
		Position: TokenPosition{Offset: 0, Length: 6},
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test AsError utility function
	internalErr := errors.AsError(parseErr)
	if internalErr == nil {
		t.Fatal("AsError should return non-nil error")
	}

	// Should use Transform() method
	context := errors.GetContext(internalErr)
	if context == nil {
		t.Fatal("AsError should preserve context from Transform()")
	}

	if context["category"] != "syntax" {
		t.Errorf("Expected category=syntax, got category=%s", context["category"])
	}
}

// TestErrorHandlingMigrationPattern demonstrates the migration from direct type assertions to AsError
func TestErrorHandlingMigrationPattern(t *testing.T) {
	// Create different types of errors for testing
	tracker := NewPositionTracker([]byte("SELECT * FROM users"))
	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected semicolon",
		Position: TokenPosition{Offset: 15, Length: 1},
		Expected: []string{";"},
		Found:    "end of input",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Create a standard internal error
	internalErr := errors.New(errors.CommonInternal, "standard error", nil)

	// Create a standard Go error
	standardErr := fmt.Errorf("standard go error")

	testCases := []struct {
		name string
		err  error
	}{
		{"ParseError", parseErr},
		{"InternalError", internalErr},
		{"StandardError", standardErr},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// NEW PATTERN: Use AsError utility for consistent error handling
			result := errors.AsError(tc.err)

			if result == nil {
				t.Fatal("AsError should never return nil for non-nil input")
			}

			// Verify that we get a proper internal error
			if !errors.IsRangerError(result) {
				t.Error("AsError should always return a Ranger internal error")
			}

			// Verify error message is preserved
			if result.Error() == "" {
				t.Error("AsError should preserve error message")
			}

			// For ParseError, verify that context is preserved
			if tc.name == "ParseError" {
				context := errors.GetContext(result)
				if context == nil {
					t.Error("AsError should preserve ParseError context")
				}
				if context["line"] == "" || context["column"] == "" {
					t.Error("AsError should preserve position information from ParseError")
				}
			}
		})
	}
}

// TestMigrationDocumentation provides examples of the migration pattern
func TestMigrationDocumentation(t *testing.T) {
	// This test documents the migration pattern for future reference

	// Example error that might come from various sources
	var err error = errors.New(errors.CommonInternal, "example error", nil)

	// OLD PATTERN (deprecated - don't use this):
	// if internalErr, ok := err.(errors.Error); ok {
	//     return internalErr
	// }
	// return errors.New(errors.CommonInternal, err.Error(), err)

	// NEW PATTERN (recommended):
	result := errors.AsError(err)

	// AsError handles all error types consistently:
	// - InternalError types: uses Transform() method
	// - *errors.Error types: returns as-is
	// - Standard errors: wraps in internal error

	if result == nil {
		t.Fatal("AsError should handle all error types")
	}

	// Verify the result is always a proper internal error
	if !errors.IsRangerError(result) {
		t.Error("AsError should always return internal error type")
	}
}

// TestEnhancedErrorMessageClarity tests error message clarity and actionability
func TestEnhancedErrorMessageClarity(t *testing.T) {
	source := []byte("CREATE TABLE users (id int32, name string")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name             string
		parseError       *ParseError
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name: "Missing semicolon error",
			parseError: &ParseError{
				Code:     ErrMissingSemicolon,
				Message:  "expected ';' at end of statement",
				Position: TokenPosition{Offset: 40, Length: 0},
				Expected: []string{";"},
				Found:    "end of input",
				Category: SyntaxError,
				tracker:  tracker,
			},
			shouldContain: []string{
				"expected ';'",
				"end of statement",
				"line 1, column 41",
			},
			shouldNotContain: []string{
				"unexpected error",
				"internal error",
			},
		},
		{
			name: "Invalid data type error",
			parseError: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "invalid data type 'varchar', use 'string' for Iceberg tables",
				Position: TokenPosition{Offset: 25, Length: 7},
				Expected: []string{"string", "int32", "int64", "boolean"},
				Found:    "varchar",
				Category: TypeError,
				tracker:  tracker,
			},
			shouldContain: []string{
				"invalid data type",
				"use 'string'",
				"Iceberg tables",
				"Expected: string, int32, int64, boolean",
				"Found: varchar",
			},
			shouldNotContain: []string{
				"syntax error",
				"unknown error",
			},
		},
		{
			name: "Missing identifier error",
			parseError: &ParseError{
				Code:     ErrExpectedIdentifier,
				Message:  "expected column name after data type",
				Position: TokenPosition{Offset: 30, Length: 1},
				Expected: []string{"identifier", "column_name"},
				Found:    ",",
				Category: SyntaxError,
				tracker:  tracker,
			},
			shouldContain: []string{
				"expected column name",
				"after data type",
				"Expected: identifier, column_name",
				"Found: ,",
			},
			shouldNotContain: []string{
				"type error",
				"validation error",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test DetailedError() method for comprehensive information
			detailedMsg := test.parseError.DetailedError()
			for _, should := range test.shouldContain {
				if !strings.Contains(detailedMsg, should) {
					t.Errorf("Detailed error message should contain '%s', got: %s", should, detailedMsg)
				}
			}
			for _, shouldNot := range test.shouldNotContain {
				if strings.Contains(detailedMsg, shouldNot) {
					t.Errorf("Detailed error message should not contain '%s', got: %s", shouldNot, detailedMsg)
				}
			}

			// Detailed error should always contain position and category
			if !strings.Contains(detailedMsg, fmt.Sprintf("(%s)", test.parseError.Category)) {
				t.Errorf("Detailed error should contain category (%s), got: %s", test.parseError.Category, detailedMsg)
			}

			// Test Error() method for basic clarity (should contain position and basic message)
			errorMsg := test.parseError.Error()
			basicRequirements := []string{
				"line 1, column",
				test.parseError.Message,
			}
			for _, should := range basicRequirements {
				if !strings.Contains(errorMsg, should) {
					t.Errorf("Basic error message should contain '%s', got: %s", should, errorMsg)
				}
			}
		})
	}
}

// TestExpectedVsFoundTokenReporting tests expected vs found token reporting accuracy
func TestExpectedVsFoundTokenReporting(t *testing.T) {
	source := []byte("SELECT * FROM users WHERE id = 123 AND name LIKE 'test'")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name         string
		error        *ParseError
		wantExpected string
		wantFound    string
	}{
		{
			name: "Single expected token",
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected FROM keyword",
				Position: TokenPosition{Offset: 9, Length: 1},
				Expected: []string{"FROM"},
				Found:    "*",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantExpected: "Expected: FROM",
			wantFound:    "Found: *",
		},
		{
			name: "Multiple expected tokens",
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected comparison operator",
				Position: TokenPosition{Offset: 30, Length: 3},
				Expected: []string{"=", "<>", "<", ">", "<=", ">=", "LIKE", "IN"},
				Found:    "AND",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantExpected: "Expected: =, <>, <, >, <=, >=, LIKE, IN",
			wantFound:    "Found: AND",
		},
		{
			name: "End of input error",
			error: &ParseError{
				Code:     ErrMissingSemicolon,
				Message:  "expected semicolon",
				Position: TokenPosition{Offset: 56, Length: 0},
				Expected: []string{";"},
				Found:    "end of input",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantExpected: "Expected: ;",
			wantFound:    "Found: end of input",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			detailedMsg := test.error.DetailedError()

			if !strings.Contains(detailedMsg, test.wantExpected) {
				t.Errorf("Expected to find '%s' in detailed message, got: %s", test.wantExpected, detailedMsg)
			}

			if !strings.Contains(detailedMsg, test.wantFound) {
				t.Errorf("Expected to find '%s' in detailed message, got: %s", test.wantFound, detailedMsg)
			}

			// Test GetFormattedExpectedFound method
			formatted := test.error.GetFormattedExpectedFound()
			if !strings.Contains(formatted, test.wantExpected) {
				t.Errorf("GetFormattedExpectedFound should contain '%s', got: %s", test.wantExpected, formatted)
			}
			if !strings.Contains(formatted, test.wantFound) {
				t.Errorf("GetFormattedExpectedFound should contain '%s', got: %s", test.wantFound, formatted)
			}
		})
	}
}

// TestErrorCategorizationAccuracy tests error categorization accuracy
func TestErrorCategorizationAccuracy(t *testing.T) {
	source := []byte("CREATE TABLE test (id varchar(50), name int);")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name          string
		error         *ParseError
		wantCategory  ErrorCategory
		wantInMessage bool
	}{
		{
			name: "Syntax error categorization",
			error: &ParseError{
				Code:     ErrExpectedLeftParen,
				Message:  "expected '(' after table name",
				Position: TokenPosition{Offset: 18, Length: 1},
				Expected: []string{"("},
				Found:    "test",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantCategory:  SyntaxError,
			wantInMessage: true,
		},
		{
			name: "Type error categorization",
			error: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "invalid Iceberg type 'varchar', use 'string' instead",
				Position: TokenPosition{Offset: 22, Length: 7},
				Expected: []string{"string", "binary"},
				Found:    "varchar",
				Category: TypeError,
				tracker:  tracker,
			},
			wantCategory:  TypeError,
			wantInMessage: true,
		},
		{
			name: "Validation error categorization",
			error: &ParseError{
				Code:     ErrDuplicateColumnName,
				Message:  "duplicate column name 'id'",
				Position: TokenPosition{Offset: 19, Length: 2},
				Expected: []string{"unique column name"},
				Found:    "id",
				Category: ValidationError,
				tracker:  tracker,
			},
			wantCategory:  ValidationError,
			wantInMessage: true,
		},
		{
			name: "Lexical error categorization",
			error: &ParseError{
				Code:     ErrExpectedLiteral,
				Message:  "unterminated string literal",
				Position: TokenPosition{Offset: 25, Length: 10},
				Expected: []string{"'", "\""},
				Found:    "unterminated string",
				Category: LexicalError,
				tracker:  tracker,
			},
			wantCategory:  LexicalError,
			wantInMessage: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test category field
			if test.error.Category != test.wantCategory {
				t.Errorf("Expected category %s, got %s", test.wantCategory, test.error.Category)
			}

			// Test that category appears in detailed error message
			if test.wantInMessage {
				detailedMsg := test.error.DetailedError()
				categoryStr := fmt.Sprintf("(%s)", test.wantCategory)
				if !strings.Contains(detailedMsg, categoryStr) {
					t.Errorf("Detailed message should contain category %s, got: %s", categoryStr, detailedMsg)
				}
			}

			// Test that Transform() preserves category in context
			transformed := test.error.Transform()
			context := errors.GetContext(transformed)
			if context["category"] != string(test.wantCategory) {
				t.Errorf("Transform should preserve category, expected %s, got %s",
					test.wantCategory, context["category"])
			}
		})
	}
}

// TestErrorMessageActionability tests that error messages provide actionable guidance
func TestErrorMessageActionability(t *testing.T) {
	source := []byte("CREATE TABLE users (id int, name varchar(100));")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name          string
		error         *ParseError
		shouldProvide []string // What actionable guidance should be provided
	}{
		{
			name: "Iceberg type suggestion",
			error: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "invalid type 'varchar', use 'string' for text data in Iceberg tables",
				Position: TokenPosition{Offset: 28, Length: 7},
				Expected: []string{"string", "binary"},
				Found:    "varchar",
				Category: TypeError,
				tracker:  tracker,
			},
			shouldProvide: []string{
				"use 'string'",   // Specific alternative
				"Iceberg tables", // Context about why
				"text data",      // What this type is for
			},
		},
		{
			name: "Missing punctuation guidance",
			error: &ParseError{
				Code:     ErrExpectedComma,
				Message:  "expected ',' between column definitions",
				Position: TokenPosition{Offset: 23, Length: 1},
				Expected: []string{","},
				Found:    "name",
				Category: SyntaxError,
				tracker:  tracker,
			},
			shouldProvide: []string{
				"between column definitions", // Context about where comma is needed
				"expected ','",               // Specific fix
			},
		},
		{
			name: "Keyword suggestion",
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected 'CREATE', 'DROP', 'SELECT', 'INSERT', 'UPDATE', or 'DELETE' to start statement",
				Position: TokenPosition{Offset: 0, Length: 6},
				Expected: []string{"CREATE", "DROP", "SELECT", "INSERT", "UPDATE", "DELETE"},
				Found:    "CREAT",
				Category: SyntaxError,
				tracker:  tracker,
			},
			shouldProvide: []string{
				"to start statement", // Context about when these keywords are used
				"CREATE",             // Likely intended keyword
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			detailedMsg := test.error.DetailedError()

			for _, guidance := range test.shouldProvide {
				if !strings.Contains(detailedMsg, guidance) {
					t.Errorf("Error message should provide actionable guidance '%s', got: %s",
						guidance, detailedMsg)
				}
			}

			// Test that the error message is not just technical jargon
			technicalJargon := []string{
				"parse error",
				"token mismatch",
				"unexpected symbol",
				"syntax violation",
			}

			for _, jargon := range technicalJargon {
				if strings.Contains(test.error.Message, jargon) {
					t.Errorf("Error message should avoid technical jargon '%s', got: %s",
						jargon, test.error.Message)
				}
			}
		})
	}
}

// TestErrorMessageConsistency tests consistency across different error types
func TestErrorMessageConsistency(t *testing.T) {
	source := []byte("SELECT * FROM users WHERE id = 123;")
	tracker := NewPositionTracker(source)

	// Create errors of different types but similar contexts
	parseErrors := []*ParseError{
		{
			Code:     ErrExpectedKeyword,
			Message:  "expected keyword 'FROM'",
			Position: TokenPosition{Offset: 9, Length: 1},
			Expected: []string{"FROM"},
			Found:    "*",
			Category: SyntaxError,
			tracker:  tracker,
		},
		{
			Code:     ErrExpectedIdentifier,
			Message:  "expected table name",
			Position: TokenPosition{Offset: 14, Length: 5},
			Expected: []string{"identifier"},
			Found:    "users",
			Category: SyntaxError,
			tracker:  tracker,
		},
		{
			Code:     ErrInvalidIcebergType,
			Message:  "invalid data type 'text'",
			Position: TokenPosition{Offset: 20, Length: 4},
			Expected: []string{"string", "binary"},
			Found:    "text",
			Category: TypeError,
			tracker:  tracker,
		},
	}

	// Test that all errors follow consistent formatting patterns
	for i, err := range parseErrors {
		t.Run(fmt.Sprintf("Error_%d", i), func(t *testing.T) {
			detailedMsg := err.DetailedError()

			// All detailed messages should have consistent structure
			requiredElements := []string{
				"Error at line",                   // Position header
				fmt.Sprintf("(%s)", err.Category), // Category
				"Expected:",                       // Expected section
				"Found:",                          // Found section
			}

			for _, element := range requiredElements {
				if !strings.Contains(detailedMsg, element) {
					t.Errorf("Detailed error should contain '%s', got: %s", element, detailedMsg)
				}
			}

			// All errors should have proper Transform() behavior
			transformed := err.Transform()
			if transformed == nil {
				t.Error("Transform() should not return nil")
			}

			context := errors.GetContext(transformed)
			if context == nil {
				t.Error("Transform() should preserve context")
			}

			// Check for consistent context fields
			requiredContextFields := []string{"line", "column", "category"}
			for _, field := range requiredContextFields {
				if context[field] == "" {
					t.Errorf("Context should contain '%s' field", field)
				}
			}
		})
	}
}

// TestVisualErrorContextDisplay tests source line display with caret positioning
func TestVisualErrorContextDisplay(t *testing.T) {
	tests := []struct {
		name            string
		source          []byte
		error           *ParseError
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:   "Single line error with caret",
			source: []byte("SELECT * FROM users WHERE id = 123;"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected table name",
				Position: TokenPosition{Offset: 14, Length: 5}, // "users"
				Expected: []string{"table_name"},
				Found:    "users",
				Category: SyntaxError,
			},
			wantContains: []string{
				"SELECT * FROM users WHERE id = 123;", // Source line
				"^^^^^",                               // Caret for token length
				"   1 |",                              // Line number
			},
			wantNotContains: []string{
				"   2 |", // Should not have line 2
			},
		},
		{
			name: "Multi-line error with context",
			source: []byte(`CREATE TABLE users (
    id int32,
    name string,
    email varchar(100)
);`),
			error: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "invalid type 'varchar'",
				Position: TokenPosition{Offset: 55, Length: 7}, // "varchar"
				Expected: []string{"string", "binary"},
				Found:    "varchar",
				Category: TypeError,
			},
			wantContains: []string{
				"email varchar(100)", // Source line with error
				"^^^^^^^",            // Caret for "varchar"
				"   4 |",             // Line number
			},
			wantNotContains: []string{
				"CREATE TABLE", // Should not show distant context
			},
		},
		{
			name:   "Error at end of line",
			source: []byte("SELECT * FROM users"),
			error: &ParseError{
				Code:     ErrMissingSemicolon,
				Message:  "expected ';'",
				Position: TokenPosition{Offset: 19, Length: 0}, // End of input
				Expected: []string{";"},
				Found:    "end of input",
				Category: SyntaxError,
			},
			wantContains: []string{
				"SELECT * FROM users", // Source line
				"^",                   // Single caret at end
				"   1 |",              // Line number
			},
			wantNotContains: []string{
				"^^^^^", // Should not have multiple carets
			},
		},
		{
			name:   "Error at beginning of line",
			source: []byte("SELECT * FROM users;"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected statement keyword",
				Position: TokenPosition{Offset: 0, Length: 6}, // "SELECT"
				Expected: []string{"CREATE", "DROP", "INSERT"},
				Found:    "SELECT",
				Category: SyntaxError,
			},
			wantContains: []string{
				"SELECT * FROM users;", // Source line
				"^^^^^^",               // Caret for "SELECT"
				"   1 |",               // Line number
			},
			wantNotContains: []string{
				"   0 |", // Should not have line 0
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := NewPositionTracker(test.source)
			test.error.tracker = tracker

			visualContext := test.error.GetVisualContext()

			for _, want := range test.wantContains {
				if !strings.Contains(visualContext, want) {
					t.Errorf("Visual context should contain '%s', got:\n%s", want, visualContext)
				}
			}

			for _, wantNot := range test.wantNotContains {
				if strings.Contains(visualContext, wantNot) {
					t.Errorf("Visual context should not contain '%s', got:\n%s", wantNot, visualContext)
				}
			}
		})
	}
}

// TestVisualErrorContextTruncation tests context line generation and truncation
func TestVisualErrorContextTruncation(t *testing.T) {
	// Create a very long line to test truncation
	longPrefix := "SELECT " + strings.Repeat("very_long_column_name, ", 10)
	longSuffix := strings.Repeat(", another_very_long_column_name", 10) + " FROM users;"
	longLine := longPrefix + "target_column" + longSuffix
	source := []byte(longLine)

	// Error at "target_column" in the middle of the long line
	errorOffset := len(longPrefix)
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected column name",
		Position: TokenPosition{Offset: errorOffset, Length: 13}, // "target_column"
		Expected: []string{"column_name"},
		Found:    "target_column",
		Category: SyntaxError,
		tracker:  tracker,
	}

	visualContext := parseErr.GetVisualContext()

	// Should contain truncation indicators
	if !strings.Contains(visualContext, "...") {
		t.Errorf("Visual context should contain truncation indicators for long lines, got:\n%s", visualContext)
	}

	// Should contain the target error location
	if !strings.Contains(visualContext, "target_column") {
		t.Errorf("Visual context should contain the error location, got:\n%s", visualContext)
	}

	// Should contain caret pointer
	if !strings.Contains(visualContext, "^") {
		t.Errorf("Visual context should contain caret pointer, got:\n%s", visualContext)
	}

	// Should not be excessively long
	lines := strings.Split(visualContext, "\n")
	for _, line := range lines {
		if len(line) > 150 { // Reasonable limit for display
			t.Errorf("Visual context line too long (%d chars): %s", len(line), line)
		}
	}
}

// TestVisualErrorContextFormatting tests error formatting for various scenarios
func TestVisualErrorContextFormatting(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		error     *ParseError
		checkFunc func(t *testing.T, visualContext string)
	}{
		{
			name: "Empty line handling",
			source: []byte(`CREATE TABLE test (
    id int32

    name string
);`),
			error: &ParseError{
				Code:     ErrExpectedComma,
				Message:  "expected ','",
				Position: TokenPosition{Offset: 31, Length: 0}, // After "int32"
				Expected: []string{","},
				Found:    "newline",
				Category: SyntaxError,
			},
			checkFunc: func(t *testing.T, visualContext string) {
				// Should handle empty lines gracefully
				if !strings.Contains(visualContext, "id int32") {
					t.Error("Should contain the error line")
				}
				// Should show line numbers correctly
				if !strings.Contains(visualContext, "   2 |") {
					t.Error("Should show correct line number")
				}
			},
		},
		{
			name:   "Tab character handling",
			source: []byte("SELECT\t*\tFROM\tusers;"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected identifier",
				Position: TokenPosition{Offset: 7, Length: 1}, // "*"
				Expected: []string{"column_name"},
				Found:    "*",
				Category: SyntaxError,
			},
			checkFunc: func(t *testing.T, visualContext string) {
				// Should handle tabs in source display
				if !strings.Contains(visualContext, "SELECT") {
					t.Error("Should contain source with tabs")
				}
				// Should position caret correctly despite tabs
				if !strings.Contains(visualContext, "^") {
					t.Error("Should contain caret pointer")
				}
			},
		},
		{
			name:   "UTF-8 character handling",
			source: []byte("SELECT 'café' AS naïve FROM tàble;"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected table name",
				Position: TokenPosition{Offset: 28, Length: 5}, // "tàble"
				Expected: []string{"table_name"},
				Found:    "tàble",
				Category: SyntaxError,
			},
			checkFunc: func(t *testing.T, visualContext string) {
				// Should handle UTF-8 characters correctly
				if !strings.Contains(visualContext, "tàble") {
					t.Error("Should contain UTF-8 characters")
				}
				// Should position caret correctly for UTF-8
				if !strings.Contains(visualContext, "^^^^^") {
					t.Error("Should have correct caret length for UTF-8 token")
				}
			},
		},
		{
			name: "Multiple context lines",
			source: []byte(`-- Comment line
CREATE TABLE users (
    id int32,
    name string
);
-- Another comment`),
			error: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "expected valid type",
				Position: TokenPosition{Offset: 45, Length: 5}, // "int32"
				Expected: []string{"string", "boolean"},
				Found:    "int32",
				Category: TypeError,
			},
			checkFunc: func(t *testing.T, visualContext string) {
				// Should show context lines when helpful
				lines := strings.Split(visualContext, "\n")
				lineCount := 0
				for _, line := range lines {
					if strings.Contains(line, " | ") {
						lineCount++
					}
				}
				// Should have the error line plus some context
				if lineCount < 1 {
					t.Error("Should show at least the error line")
				}
				// Should not show too many context lines
				if lineCount > 5 {
					t.Error("Should not show excessive context lines")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := NewPositionTracker(test.source)
			test.error.tracker = tracker

			visualContext := test.error.GetVisualContext()
			test.checkFunc(t, visualContext)
		})
	}
}

// TestDetailedErrorIntegration tests integration of visual context with detailed error
func TestDetailedErrorIntegration(t *testing.T) {
	source := []byte(`CREATE TABLE users (
    id int32 NOT NULL,
    name varchar(100),
    email string UNIQUE
);`)

	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrInvalidIcebergType,
		Message:  "invalid Iceberg type 'varchar', use 'string' for text data",
		Position: TokenPosition{Offset: 49, Length: 7}, // "varchar"
		Expected: []string{"string", "binary"},
		Found:    "varchar",
		Category: TypeError,
		tracker:  tracker,
	}

	detailedError := parseErr.DetailedError()

	// Should contain all components of a detailed error
	requiredComponents := []string{
		"Error at line 3, column 6 (type):", // Header with position and category (corrected)
		"invalid Iceberg type 'varchar'",    // Error message
		"name varchar(100)",                 // Source line
		"^^^^^^^",                           // Caret pointer
		"Expected: string, binary",          // Expected tokens
		"Found: varchar",                    // Found token
	}

	for _, component := range requiredComponents {
		if !strings.Contains(detailedError, component) {
			t.Errorf("Detailed error should contain '%s', got:\n%s", component, detailedError)
		}
	}

	// Should be well-formatted and readable
	lines := strings.Split(detailedError, "\n")
	if len(lines) < 5 {
		t.Error("Detailed error should have multiple lines for readability")
	}

	// Should not have excessive blank lines
	blankLineCount := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blankLineCount++
		}
	}
	if blankLineCount > 3 {
		t.Error("Detailed error should not have excessive blank lines")
	}
}

// TestVisualContextEdgeCases tests edge cases in visual context generation
func TestVisualContextEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		source      []byte
		error       *ParseError
		wantNoError bool
	}{
		{
			name:   "Empty source",
			source: []byte(""),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected statement",
				Position: TokenPosition{Offset: 0, Length: 0},
				Expected: []string{"CREATE", "SELECT"},
				Found:    "end of input",
				Category: SyntaxError,
			},
			wantNoError: true,
		},
		{
			name:   "Single character source",
			source: []byte(";"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected statement before ';'",
				Position: TokenPosition{Offset: 0, Length: 1},
				Expected: []string{"CREATE", "SELECT"},
				Found:    ";",
				Category: SyntaxError,
			},
			wantNoError: true,
		},
		{
			name:   "Error beyond source length",
			source: []byte("SELECT"),
			error: &ParseError{
				Code:     ErrMissingSemicolon,
				Message:  "expected ';'",
				Position: TokenPosition{Offset: 10, Length: 0}, // Beyond source
				Expected: []string{";"},
				Found:    "end of input",
				Category: SyntaxError,
			},
			wantNoError: true,
		},
		{
			name:   "Very long token",
			source: []byte("SELECT " + strings.Repeat("very_long_identifier_name", 10) + " FROM users;"),
			error: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected column name",
				Position: TokenPosition{Offset: 7, Length: 260}, // Very long token
				Expected: []string{"column_name"},
				Found:    "very_long_identifier",
				Category: SyntaxError,
			},
			wantNoError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := NewPositionTracker(test.source)
			test.error.tracker = tracker

			// Should not panic or error
			visualContext := test.error.GetVisualContext()

			if test.wantNoError {
				// Should return some context, even if minimal (except for empty source)
				if visualContext == "" && len(test.source) > 0 {
					t.Error("Visual context should not be empty for non-empty edge cases")
				}

				// Should not contain invalid line numbers
				if strings.Contains(visualContext, "   0 |") {
					t.Error("Visual context should not contain line 0")
				}

				// Should handle the case gracefully
				if strings.Contains(visualContext, "panic") || strings.Contains(visualContext, "error") {
					t.Errorf("Visual context should handle edge case gracefully, got: %s", visualContext)
				}
			}
		})
	}
}

// TestInternalErrorInterfaceImplementation tests InternalError interface implementation
func TestInternalErrorInterfaceImplementation(t *testing.T) {
	source := []byte("CREATE TABLE users (id int32);")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected semicolon",
		Position: TokenPosition{Offset: 25, Length: 1},
		Expected: []string{";"},
		Found:    ")",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test that ParseError implements InternalError interface
	var internalErr errors.InternalError = parseErr
	if internalErr == nil {
		t.Fatal("ParseError should implement InternalError interface")
	}

	// Test Error() method (from error interface)
	errorMsg := internalErr.Error()
	if errorMsg == "" {
		t.Error("Error() should return non-empty string")
	}
	if !strings.Contains(errorMsg, "line 1") {
		t.Error("Error() should contain position information")
	}

	// Test Transform() method (from InternalError interface)
	transformed := internalErr.Transform()
	if transformed == nil {
		t.Fatal("Transform() should return non-nil error")
	}

	// Verify Transform() returns proper internal error
	if !errors.IsRangerError(transformed) {
		t.Error("Transform() should return Ranger internal error")
	}

	// Verify Transform() preserves error code
	if errors.GetCode(transformed) != parseErr.Code.String() {
		t.Errorf("Transform() should preserve error code, expected %s, got %s",
			parseErr.Code.String(), errors.GetCode(transformed))
	}

	// Verify Transform() preserves message
	if !strings.Contains(transformed.Error(), parseErr.Message) {
		t.Error("Transform() should preserve error message")
	}
}

// TestTransformMethodAccuracy tests Transform() method accuracy
func TestTransformMethodAccuracy(t *testing.T) {
	source := []byte("SELECT * FROM users WHERE id = 123;")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name        string
		parseError  *ParseError
		wantContext map[string]string
	}{
		{
			name: "Syntax error transformation",
			parseError: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected FROM keyword",
				Position: TokenPosition{Offset: 9, Length: 1},
				Expected: []string{"FROM"},
				Found:    "*",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantContext: map[string]string{
				"category": "syntax",
				"expected": "FROM",
				"found":    "*",
				"line":     "1",
				"column":   "10",
			},
		},
		{
			name: "Type error transformation",
			parseError: &ParseError{
				Code:     ErrInvalidIcebergType,
				Message:  "invalid type 'varchar'",
				Position: TokenPosition{Offset: 20, Length: 7},
				Expected: []string{"string", "binary"},
				Found:    "varchar",
				Category: TypeError,
				tracker:  tracker,
			},
			wantContext: map[string]string{
				"category": "type",
				"expected": "string, binary",
				"found":    "varchar",
				"line":     "1",
				"column":   "21",
			},
		},
		{
			name: "Validation error transformation",
			parseError: &ParseError{
				Code:     ErrDuplicateColumnName,
				Message:  "duplicate column 'id'",
				Position: TokenPosition{Offset: 14, Length: 2},
				Expected: []string{"unique column name"},
				Found:    "id",
				Category: ValidationError,
				tracker:  tracker,
			},
			wantContext: map[string]string{
				"category": "validation",
				"expected": "unique column name",
				"found":    "id",
				"line":     "1",
				"column":   "15",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transformed := test.parseError.Transform()
			context := errors.GetContext(transformed)

			if context == nil {
				t.Fatal("Transform() should preserve context")
			}

			for key, expectedValue := range test.wantContext {
				if context[key] != expectedValue {
					t.Errorf("Context[%s]: expected %s, got %s", key, expectedValue, context[key])
				}
			}

			// Verify all required context fields are present
			requiredFields := []string{"line", "column", "category", "offset"}
			for _, field := range requiredFields {
				if context[field] == "" {
					t.Errorf("Context should contain non-empty '%s' field", field)
				}
			}
		})
	}
}

// TestAsErrorUtilityFunction tests AsError() utility function behavior
func TestAsErrorUtilityFunction(t *testing.T) {
	source := []byte("SELECT * FROM users;")
	tracker := NewPositionTracker(source)

	tests := []struct {
		name      string
		input     error
		wantType  string
		checkFunc func(t *testing.T, result *errors.Error)
	}{
		{
			name: "ParseError input",
			input: &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "test parse error",
				Position: TokenPosition{Offset: 0, Length: 6},
				Expected: []string{"CREATE"},
				Found:    "SELECT",
				Category: SyntaxError,
				tracker:  tracker,
			},
			wantType: "ParseError",
			checkFunc: func(t *testing.T, result *errors.Error) {
				context := errors.GetContext(result)
				if context["category"] != "syntax" {
					t.Error("AsError should preserve ParseError category")
				}
				if context["line"] == "" || context["column"] == "" {
					t.Error("AsError should preserve ParseError position")
				}
			},
		},
		{
			name: "Internal Error input",
			input: errors.New(errors.CommonInternal, "internal error", nil).
				AddContext("test", "value"),
			wantType: "InternalError",
			checkFunc: func(t *testing.T, result *errors.Error) {
				context := errors.GetContext(result)
				if context["test"] != "value" {
					t.Error("AsError should preserve existing internal error context")
				}
			},
		},
		{
			name:     "Standard Go error input",
			input:    fmt.Errorf("standard error: %s", "test"),
			wantType: "StandardError",
			checkFunc: func(t *testing.T, result *errors.Error) {
				if !strings.Contains(result.Error(), "standard error: test") {
					t.Error("AsError should preserve standard error message")
				}
				if errors.GetCode(result) != errors.CommonInternal.String() {
					t.Error("AsError should use CommonInternal code for standard errors")
				}
			},
		},
		{
			name:     "Nil error input",
			input:    nil,
			wantType: "Nil",
			checkFunc: func(t *testing.T, result *errors.Error) {
				if result != nil {
					t.Error("AsError should return nil for nil input")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := errors.AsError(test.input)

			if test.wantType == "Nil" {
				if result != nil {
					t.Error("AsError should return nil for nil input")
				}
				return
			}

			if result == nil {
				t.Fatal("AsError should not return nil for non-nil input")
			}

			// Verify result is always a Ranger internal error
			if !errors.IsRangerError(result) {
				t.Error("AsError should always return Ranger internal error")
			}

			// Run specific checks
			test.checkFunc(t, result)
		})
	}
}

// TestBackwardCompatibilityWithExistingErrorHandling tests backward compatibility
func TestBackwardCompatibilityWithExistingErrorHandling(t *testing.T) {
	source := []byte("CREATE TABLE users (id int32);")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected semicolon",
		Position: TokenPosition{Offset: 25, Length: 1},
		Expected: []string{";"},
		Found:    ")",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test that existing error handling patterns still work

	// 1. Standard error interface
	var stdErr error = parseErr
	if stdErr.Error() == "" {
		t.Error("ParseError should implement standard error interface")
	}

	// 2. Type assertion to InternalError
	if internalErr, ok := stdErr.(errors.InternalError); ok {
		transformed := internalErr.Transform()
		if transformed == nil {
			t.Error("Type assertion to InternalError should work")
		}
	} else {
		t.Error("ParseError should be type-assertable to InternalError")
	}

	// 3. AsError utility function (new pattern)
	result := errors.AsError(stdErr)
	if result == nil {
		t.Error("AsError should handle ParseError")
	}

	// 4. Error wrapping and unwrapping
	wrappedErr := fmt.Errorf("wrapped: %w", parseErr)
	unwrapped := stderrors.Unwrap(wrappedErr)
	if unwrapped != parseErr {
		t.Error("Error wrapping/unwrapping should work with ParseError")
	}

	// 5. Error chain handling
	chainedErr := errors.AsError(wrappedErr)
	if chainedErr == nil {
		t.Error("AsError should handle wrapped ParseError")
	}
}

// TestErrorInterfaceConsistency tests consistency across different error interface methods
func TestErrorInterfaceConsistency(t *testing.T) {
	source := []byte("SELECT * FROM users WHERE id = 123;")
	tracker := NewPositionTracker(source)

	parseErr := &ParseError{
		Code:     ErrExpectedKeyword,
		Message:  "expected table name",
		Position: TokenPosition{Offset: 14, Length: 5},
		Expected: []string{"table_name"},
		Found:    "users",
		Category: SyntaxError,
		tracker:  tracker,
	}

	// Test consistency between different interface methods

	// 1. Error() vs DetailedError() consistency
	basicError := parseErr.Error()
	detailedError := parseErr.DetailedError()

	// Both should contain the core message
	if !strings.Contains(basicError, parseErr.Message) {
		t.Error("Error() should contain core message")
	}
	if !strings.Contains(detailedError, parseErr.Message) {
		t.Error("DetailedError() should contain core message")
	}

	// Both should contain position information
	if !strings.Contains(basicError, "line 1") {
		t.Error("Error() should contain position information")
	}
	if !strings.Contains(detailedError, "line 1") {
		t.Error("DetailedError() should contain position information")
	}

	// 2. Transform() vs AsError() consistency
	transformed := parseErr.Transform()
	asErrorResult := errors.AsError(parseErr)

	// Should produce equivalent results
	if transformed.Error() != asErrorResult.Error() {
		t.Error("Transform() and AsError() should produce consistent results")
	}

	transformedContext := errors.GetContext(transformed)
	asErrorContext := errors.GetContext(asErrorResult)

	// Context should be identical
	for key, value := range transformedContext {
		if asErrorContext[key] != value {
			t.Errorf("Context consistency: Transform()[%s]=%s, AsError()[%s]=%s",
				key, value, key, asErrorContext[key])
		}
	}

	// 3. Multiple Transform() calls should be idempotent
	transformed1 := parseErr.Transform()
	transformed2 := parseErr.Transform()

	if transformed1.Error() != transformed2.Error() {
		t.Error("Multiple Transform() calls should produce identical results")
	}
}
