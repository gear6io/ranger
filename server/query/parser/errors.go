package parser

import (
	"fmt"
	"strings"

	"github.com/gear6io/ranger/pkg/errors"
)

// ErrorCategory represents the type of parsing error
// This categorization helps users understand the nature of the error
// and provides context for debugging and error handling.
type ErrorCategory string

const (
	// SyntaxError represents basic syntax violations in SQL statements
	// Examples: missing semicolons, unexpected tokens, malformed statements
	SyntaxError ErrorCategory = "syntax"

	// TypeError represents invalid data type specifications or type mismatches
	// Examples: invalid Iceberg types, type conversion errors, unsupported types
	TypeError ErrorCategory = "type"

	// ValidationError represents semantic validation failures
	// Examples: duplicate column names, missing required fields, constraint violations
	ValidationError ErrorCategory = "validation"

	// LexicalError represents tokenization failures during lexical analysis
	// Examples: unterminated strings, invalid characters, malformed literals
	LexicalError ErrorCategory = "lexical"
)

// Parser-specific error codes
var (
	// Syntax errors
	ErrEmptyStatement              = errors.MustNewCode("parser.syntax.empty_statement")
	ErrInvalidStatement            = errors.MustNewCode("parser.syntax.invalid_statement")
	ErrMissingSemicolon            = errors.MustNewCode("parser.syntax.missing_semicolon")
	ErrExpectedKeyword             = errors.MustNewCode("parser.syntax.expected_keyword")
	ErrExpectedIdentifier          = errors.MustNewCode("parser.syntax.expected_identifier")
	ErrExpectedLiteral             = errors.MustNewCode("parser.syntax.expected_literal")
	ErrExpectedAtSymbol            = errors.MustNewCode("parser.syntax.expected_at_symbol")
	ErrExpectedFetchStatus         = errors.MustNewCode("parser.syntax.expected_fetch_status")
	ErrExpectedEquals              = errors.MustNewCode("parser.syntax.expected_equals")
	ErrExpectedZero                = errors.MustNewCode("parser.syntax.expected_zero")
	ErrExpectedBegin               = errors.MustNewCode("parser.syntax.expected_begin")
	ErrExpectedEnd                 = errors.MustNewCode("parser.syntax.expected_end")
	ErrExpectedNext                = errors.MustNewCode("parser.syntax.expected_next")
	ErrExpectedFrom                = errors.MustNewCode("parser.syntax.expected_from")
	ErrExpectedInto                = errors.MustNewCode("parser.syntax.expected_into")
	ErrExpectedCursorVar           = errors.MustNewCode("parser.syntax.expected_cursor_variable")
	ErrExpectedDataType            = errors.MustNewCode("parser.syntax.expected_data_type")
	ErrExpectedCursor              = errors.MustNewCode("parser.syntax.expected_cursor")
	ErrExpectedFor                 = errors.MustNewCode("parser.syntax.expected_for")
	ErrExpectedUserOrTable         = errors.MustNewCode("parser.syntax.expected_user_or_table")
	ErrExpectedLeftParen           = errors.MustNewCode("parser.syntax.expected_left_paren")
	ErrExpectedRightParen          = errors.MustNewCode("parser.syntax.expected_right_paren")
	ErrExpectedComma               = errors.MustNewCode("parser.syntax.expected_comma")
	ErrExpectedAddDropSet          = errors.MustNewCode("parser.syntax.expected_add_drop_set")
	ErrExpectedPasswordUser        = errors.MustNewCode("parser.syntax.expected_password_username")
	ErrExpectedSet                 = errors.MustNewCode("parser.syntax.expected_set")
	ErrExpectedDatabases           = errors.MustNewCode("parser.syntax.expected_databases_tables_users")
	ErrExpectedSelectInsert        = errors.MustNewCode("parser.syntax.expected_select_insert_update_delete")
	ErrExpectedPrivilege           = errors.MustNewCode("parser.syntax.expected_privilege")
	ErrExpectedOn                  = errors.MustNewCode("parser.syntax.expected_on")
	ErrExpectedStarOrTable         = errors.MustNewCode("parser.syntax.expected_star_or_table")
	ErrExpectedDotAfterDB          = errors.MustNewCode("parser.syntax.expected_dot_after_database")
	ErrExpectedDatabaseOrStar      = errors.MustNewCode("parser.syntax.expected_database_or_star")
	ErrExpectedStarOrTableAfterDot = errors.MustNewCode("parser.syntax.expected_star_or_table_after_dot")
	ErrExpectedDatabaseOrTable     = errors.MustNewCode("parser.syntax.expected_database_or_table")
	ErrExpectedIdentifierAfterDot  = errors.MustNewCode("parser.syntax.expected_identifier_after_dot")
	ErrExpectedExistsAfterIf       = errors.MustNewCode("parser.syntax.expected_exists_after_if")
	ErrExpectedValues              = errors.MustNewCode("parser.syntax.expected_values")
	ErrExpectedLiteralOrNull       = errors.MustNewCode("parser.syntax.expected_literal_or_null")

	// Validation errors
	ErrTableNameRequired            = errors.MustNewCode("parser.validation.table_name_required")
	ErrTableNameEmpty               = errors.MustNewCode("parser.validation.table_name_empty")
	ErrDatabaseNameEmpty            = errors.MustNewCode("parser.validation.database_name_empty")
	ErrDatabaseNameEmptyIfSpecified = errors.MustNewCode("parser.validation.database_name_empty_if_specified")

	// Parsing errors
	ErrUnexpectedDataType = errors.MustNewCode("parser.parse.unexpected_data_type")

	// Iceberg type validation errors
	ErrInvalidIcebergType    = errors.MustNewCode("parser.iceberg.invalid_type")
	ErrUnsupportedSQLType    = errors.MustNewCode("parser.iceberg.unsupported_sql_type")
	ErrComplexTypeParseError = errors.MustNewCode("parser.iceberg.complex_type_parse_error")
	ErrDuplicateColumnName   = errors.MustNewCode("parser.iceberg.duplicate_column_name")
	ErrEmptyColumnName       = errors.MustNewCode("parser.iceberg.empty_column_name")
	ErrNoColumnsSpecified    = errors.MustNewCode("parser.iceberg.no_columns_specified")

	// Multi-error reporting
	ErrMultipleParseErrors = errors.MustNewCode("parser.syntax.multiple_parse_errors")
)

// ParseError provides rich error information and integrates with internal error system
// This structure implements the InternalError interface to provide seamless integration
// with the existing error handling system while offering enhanced debugging capabilities.
type ParseError struct {
	// Core error information
	Code    errors.Code // Existing error code system
	Message string      // Human-readable message

	// Position information
	Position TokenPosition // Where the error occurred
	Token    *Token        // The problematic token (if any)

	// Context information
	Expected []string // What tokens were expected
	Found    string   // What was actually found

	// Source context
	tracker *PositionTracker // For generating context

	// Error categorization
	Category ErrorCategory // syntax, type, validation, lexical
}

// Ensure ParseError implements InternalError interface
var _ errors.InternalError = (*ParseError)(nil)

// Error implements the standard Go error interface
// Returns a formatted error message with position information for basic error handling
func (pe *ParseError) Error() string {
	if pe.tracker != nil {
		line, column := pe.tracker.GetLineColumn(pe.Position.Offset)
		return fmt.Sprintf("parse error at line %d, column %d: %s", line, column, pe.Message)
	}
	return fmt.Sprintf("parse error: %s", pe.Message)
}

// DetailedError provides rich error formatting for debugging
// Generates a comprehensive error report with source context, position information,
// and expected vs found token details for enhanced debugging experience.
func (pe *ParseError) DetailedError() string {
	if pe.tracker == nil {
		return pe.Error()
	}

	line, column := pe.tracker.GetLineColumn(pe.Position.Offset)
	sourceLine := pe.tracker.GetSourceLine(line)

	var buf strings.Builder

	// Error header with position and category
	buf.WriteString(fmt.Sprintf("Error at line %d, column %d (%s):\n",
		line, column, pe.Category))
	buf.WriteString(fmt.Sprintf("  %s\n\n", pe.Message))

	// Source context with caret pointer
	if sourceLine != "" {
		buf.WriteString(fmt.Sprintf("%4d | %s\n", line, sourceLine))

		// Generate caret pointer, handling potential truncation
		caretPos := column - 1
		if caretPos < 0 {
			caretPos = 0
		}
		if caretPos > len(sourceLine) {
			caretPos = len(sourceLine)
		}

		buf.WriteString(fmt.Sprintf("     | %s", strings.Repeat(" ", caretPos)))

		// Show token length if available
		if pe.Position.Length > 1 && caretPos+pe.Position.Length <= len(sourceLine) {
			buf.WriteString(strings.Repeat("^", pe.Position.Length))
		} else {
			buf.WriteString("^")
		}
		buf.WriteString("\n")
	}

	// Expected vs found information
	if len(pe.Expected) > 0 {
		buf.WriteString(fmt.Sprintf("\nExpected: %s\n",
			strings.Join(pe.Expected, ", ")))
	}
	if pe.Found != "" {
		buf.WriteString(fmt.Sprintf("Found: %s\n", pe.Found))
	}

	return buf.String()
}

// Transform converts ParseError to internal errors.Error format
// This method implements the InternalError interface, providing seamless integration
// with the existing error handling system while preserving all debugging information
// as structured context data.
func (pe *ParseError) Transform() *errors.Error {
	// Create base internal error
	internalErr := errors.New(pe.Code, pe.Message, nil)

	// Add rich context using errors.AddContext for structured debugging information
	if pe.tracker != nil {
		line, column := pe.tracker.GetLineColumn(pe.Position.Offset)
		internalErr = internalErr.AddContext("line", line).
			AddContext("column", column).
			AddContext("offset", pe.Position.Offset)
	}

	// Add error categorization
	internalErr = internalErr.AddContext("category", string(pe.Category))

	// Add expected vs found context
	if len(pe.Expected) > 0 {
		internalErr = internalErr.AddContext("expected", strings.Join(pe.Expected, ", "))
	}
	if pe.Found != "" {
		internalErr = internalErr.AddContext("found", pe.Found)
	}

	// Add token information if available
	if pe.Token != nil {
		internalErr = internalErr.AddContext("token_type", fmt.Sprintf("%d", pe.Token.tokenT)).
			AddContext("token_value", fmt.Sprintf("%v", pe.Token.value))
	}

	// Add position length for token span information
	if pe.Position.Length > 0 {
		internalErr = internalErr.AddContext("token_length", pe.Position.Length)
	}

	return internalErr
}

// GetVisualContext generates visual error context with source line display and caret pointer
// This method creates a formatted display showing the problematic source line with a visual
// indicator pointing to the exact error location, including surrounding context when helpful.
func (pe *ParseError) GetVisualContext() string {
	if pe.tracker == nil {
		return ""
	}

	line, column := pe.tracker.GetLineColumn(pe.Position.Offset)
	return pe.generateVisualContext(line, column)
}

// generateVisualContext creates the visual context display with line truncation and context lines
func (pe *ParseError) generateVisualContext(line, column int) string {
	var buf strings.Builder

	// Get the main error line
	sourceLine := pe.tracker.GetSourceLine(line)
	if sourceLine == "" {
		return ""
	}

	// Handle very long lines by truncating appropriately while preserving error context
	const maxLineLength = 120
	const contextWindow = 20

	truncatedLine, adjustedColumn := pe.truncateLineForDisplay(sourceLine, column, maxLineLength, contextWindow)

	// Add surrounding context lines when helpful (1 line before and after)
	pe.addContextLines(&buf, line, maxLineLength)

	// Add the main error line with line number
	buf.WriteString(fmt.Sprintf("%4d | %s\n", line, truncatedLine))

	// Add caret pointer
	pe.addCaretPointer(&buf, adjustedColumn)

	return buf.String()
}

// truncateLineForDisplay handles line truncation for very long lines while preserving error context
func (pe *ParseError) truncateLineForDisplay(sourceLine string, column, maxLength, contextWindow int) (string, int) {
	if len(sourceLine) <= maxLength {
		return sourceLine, column
	}

	// Calculate truncation window around the error position
	errorPos := column - 1 // Convert to 0-based
	if errorPos < 0 {
		errorPos = 0
	}

	// Determine start and end positions for truncation
	start := errorPos - contextWindow
	if start < 0 {
		start = 0
	}

	end := start + maxLength
	if end > len(sourceLine) {
		end = len(sourceLine)
		start = end - maxLength
		if start < 0 {
			start = 0
		}
	}

	// Adjust if we're too close to the beginning
	if errorPos < contextWindow {
		start = 0
		end = maxLength
		if end > len(sourceLine) {
			end = len(sourceLine)
		}
	}

	truncated := sourceLine[start:end]
	adjustedColumn := errorPos - start + 1 // Convert back to 1-based

	// Add truncation indicators
	if start > 0 {
		truncated = "..." + truncated[3:]
		adjustedColumn += 3
	}
	if end < len(sourceLine) {
		if len(truncated) > 3 {
			truncated = truncated[:len(truncated)-3] + "..."
		}
	}

	return truncated, adjustedColumn
}

// addContextLines adds surrounding context lines when helpful
func (pe *ParseError) addContextLines(buf *strings.Builder, errorLine, maxLength int) {
	// Add one line before if available
	if errorLine > 1 {
		prevLine := pe.tracker.GetSourceLine(errorLine - 1)
		if prevLine != "" {
			if len(prevLine) > maxLength {
				prevLine = prevLine[:maxLength-3] + "..."
			}
			buf.WriteString(fmt.Sprintf("%4d | %s\n", errorLine-1, prevLine))
		}
	}
}

// addCaretPointer adds the caret pointer line showing the exact error position
func (pe *ParseError) addCaretPointer(buf *strings.Builder, column int) {
	// Generate caret pointer
	caretPos := column - 1
	if caretPos < 0 {
		caretPos = 0
	}

	buf.WriteString(fmt.Sprintf("     | %s", strings.Repeat(" ", caretPos)))

	// Show token length if available and reasonable
	if pe.Position.Length > 1 && pe.Position.Length <= 10 {
		buf.WriteString(strings.Repeat("^", pe.Position.Length))
	} else {
		buf.WriteString("^")
	}
	buf.WriteString("\n")
}

// GetFormattedExpectedFound returns a formatted string showing expected vs found information
func (pe *ParseError) GetFormattedExpectedFound() string {
	var parts []string

	if len(pe.Expected) > 0 {
		expected := strings.Join(pe.Expected, ", ")
		parts = append(parts, fmt.Sprintf("Expected: %s", expected))
	}

	if pe.Found != "" {
		parts = append(parts, fmt.Sprintf("Found: %s", pe.Found))
	}

	return strings.Join(parts, "\n")
}

// ErrorCollector manages multiple parsing errors with recovery capabilities
// Provides error collection, ordering, and limiting functionality for multi-error reporting
type ErrorCollector struct {
	errors    []*ParseError // Collected parsing errors
	maxErrors int           // Maximum number of errors to collect (default 10)
	recovered bool          // Whether error recovery was attempted
}

// NewErrorCollector creates a new error collector with default settings
func NewErrorCollector() *ErrorCollector {
	return &ErrorCollector{
		errors:    make([]*ParseError, 0),
		maxErrors: 10, // Limit to prevent overwhelming output
		recovered: false,
	}
}

// AddError adds a parsing error to the collection
// Returns true if more errors can be collected, false if limit reached
func (ec *ErrorCollector) AddError(err *ParseError) bool {
	if len(ec.errors) >= ec.maxErrors {
		return false
	}

	ec.errors = append(ec.errors, err)
	return len(ec.errors) < ec.maxErrors
}

// HasErrors returns true if any errors have been collected
func (ec *ErrorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}

// GetErrors returns all collected errors, sorted by position
func (ec *ErrorCollector) GetErrors() []*ParseError {
	if len(ec.errors) <= 1 {
		return ec.errors
	}

	// Sort errors by line and column position
	sorted := make([]*ParseError, len(ec.errors))
	copy(sorted, ec.errors)

	// Sort by position (line first, then column)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			iLine, iCol := sorted[i].getLineColumn()
			jLine, jCol := sorted[j].getLineColumn()

			if iLine > jLine || (iLine == jLine && iCol > jCol) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// GetFirstError returns the first error (by position) or nil if no errors
func (ec *ErrorCollector) GetFirstError() *ParseError {
	errors := ec.GetErrors()
	if len(errors) == 0 {
		return nil
	}
	return errors[0]
}

// GetErrorCount returns the number of collected errors
func (ec *ErrorCollector) GetErrorCount() int {
	return len(ec.errors)
}

// IsLimitReached returns true if the error limit has been reached
func (ec *ErrorCollector) IsLimitReached() bool {
	return len(ec.errors) >= ec.maxErrors
}

// SetRecovered marks that error recovery was attempted
func (ec *ErrorCollector) SetRecovered() {
	ec.recovered = true
}

// WasRecovered returns true if error recovery was attempted
func (ec *ErrorCollector) WasRecovered() bool {
	return ec.recovered
}

// GroupRelatedErrors groups errors that are likely related
// Returns a map where keys are group identifiers and values are error slices
func (ec *ErrorCollector) GroupRelatedErrors() map[string][]*ParseError {
	groups := make(map[string][]*ParseError)

	for _, err := range ec.GetErrors() {
		groupKey := ec.getErrorGroupKey(err)
		groups[groupKey] = append(groups[groupKey], err)
	}

	return groups
}

// getErrorGroupKey determines the group key for error grouping
func (ec *ErrorCollector) getErrorGroupKey(err *ParseError) string {
	// Group by error category and general location
	line, _ := err.getLineColumn()
	lineGroup := (line-1)/5*5 + 1 // Group by 5-line blocks

	return fmt.Sprintf("%s_line_%d", err.Category, lineGroup)
}

// CreateMultiError creates a single error that represents multiple parsing errors
func (ec *ErrorCollector) CreateMultiError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	if len(ec.errors) == 1 {
		return ec.errors[0]
	}

	// Create a multi-error representation
	firstError := ec.GetFirstError()
	message := fmt.Sprintf("multiple parsing errors (%d total)", len(ec.errors))

	multiError := &ParseError{
		Code:     ErrMultipleParseErrors,
		Message:  message,
		Position: firstError.Position,
		Token:    firstError.Token,
		Expected: []string{"valid SQL syntax"},
		Found:    "multiple syntax errors",
		Category: SyntaxError,
		tracker:  firstError.tracker,
	}

	return multiError
}

// FormatAllErrors returns a formatted string containing all collected errors
// Errors are sorted by position and grouped when appropriate
func (ec *ErrorCollector) FormatAllErrors() string {
	if len(ec.errors) == 0 {
		return ""
	}

	if len(ec.errors) == 1 {
		return ec.errors[0].DetailedError()
	}

	var buf strings.Builder
	sortedErrors := ec.GetErrors()

	buf.WriteString(fmt.Sprintf("Found %d parsing errors:\n\n", len(sortedErrors)))

	// Group related errors
	groups := ec.GroupRelatedErrors()

	if len(groups) == 1 {
		// All errors are in one group, display them sequentially
		for i, err := range sortedErrors {
			buf.WriteString(fmt.Sprintf("Error %d:\n", i+1))
			buf.WriteString(err.DetailedError())
			if i < len(sortedErrors)-1 {
				buf.WriteString("\n" + strings.Repeat("-", 50) + "\n\n")
			}
		}
	} else {
		// Multiple groups, display by group
		groupNum := 1
		for groupKey, groupErrors := range groups {
			if len(groupErrors) > 1 {
				buf.WriteString(fmt.Sprintf("Error Group %d (%s):\n", groupNum, groupKey))
			}

			for i, err := range groupErrors {
				if len(groupErrors) > 1 {
					buf.WriteString(fmt.Sprintf("  %d.%d: ", groupNum, i+1))
				} else {
					buf.WriteString(fmt.Sprintf("Error %d: ", groupNum))
				}
				buf.WriteString(err.Error())
				buf.WriteString("\n")
			}

			if groupNum < len(groups) {
				buf.WriteString("\n")
			}
			groupNum++
		}
	}

	// Add recovery information if applicable
	if ec.recovered {
		buf.WriteString("\nNote: Parser attempted error recovery to find additional issues.\n")
	}

	return buf.String()
}

// GetErrorSummary returns a brief summary of all collected errors
func (ec *ErrorCollector) GetErrorSummary() string {
	if len(ec.errors) == 0 {
		return "No errors"
	}

	if len(ec.errors) == 1 {
		line, column := ec.errors[0].getLineColumn()
		return fmt.Sprintf("1 error at line %d, column %d", line, column)
	}

	// Count errors by category
	categoryCounts := make(map[ErrorCategory]int)
	for _, err := range ec.errors {
		categoryCounts[err.Category]++
	}

	var parts []string
	for category, count := range categoryCounts {
		if count == 1 {
			parts = append(parts, fmt.Sprintf("1 %s error", category))
		} else {
			parts = append(parts, fmt.Sprintf("%d %s errors", count, category))
		}
	}

	return fmt.Sprintf("%d total errors: %s", len(ec.errors), strings.Join(parts, ", "))
}

// GetErrorsByLine returns errors grouped by line number
func (ec *ErrorCollector) GetErrorsByLine() map[int][]*ParseError {
	lineGroups := make(map[int][]*ParseError)

	for _, err := range ec.errors {
		line, _ := err.getLineColumn()
		lineGroups[line] = append(lineGroups[line], err)
	}

	return lineGroups
}

// GetErrorsByCategory returns errors grouped by category
func (ec *ErrorCollector) GetErrorsByCategory() map[ErrorCategory][]*ParseError {
	categoryGroups := make(map[ErrorCategory][]*ParseError)

	for _, err := range ec.errors {
		categoryGroups[err.Category] = append(categoryGroups[err.Category], err)
	}

	return categoryGroups
}

// HasErrorsOfCategory returns true if there are errors of the specified category
func (ec *ErrorCollector) HasErrorsOfCategory(category ErrorCategory) bool {
	for _, err := range ec.errors {
		if err.Category == category {
			return true
		}
	}
	return false
}

// GetMostSevereError returns the error with the highest severity
// Priority: LexicalError > SyntaxError > TypeError > ValidationError
func (ec *ErrorCollector) GetMostSevereError() *ParseError {
	if len(ec.errors) == 0 {
		return nil
	}

	severityOrder := map[ErrorCategory]int{
		LexicalError:    4,
		SyntaxError:     3,
		TypeError:       2,
		ValidationError: 1,
	}

	mostSevere := ec.errors[0]
	highestSeverity := severityOrder[mostSevere.Category]

	for _, err := range ec.errors[1:] {
		if severity := severityOrder[err.Category]; severity > highestSeverity {
			mostSevere = err
			highestSeverity = severity
		}
	}

	return mostSevere
}

// getLineColumn helper method for ParseError
func (pe *ParseError) getLineColumn() (int, int) {
	if pe.tracker != nil {
		return pe.tracker.GetLineColumn(pe.Position.Offset)
	}
	return 1, 1
}

// IsParseError allows type assertion for enhanced error details
// This utility function provides a safe way to check if an error is a ParseError
// and extract enhanced debugging information when available.
func IsParseError(err error) (*ParseError, bool) {
	if pe, ok := err.(*ParseError); ok {
		return pe, true
	}
	return nil, false
}
