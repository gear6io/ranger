package schema_manager

import (
	"fmt"

	"github.com/gear6io/ranger/pkg/errors"
)

// Package-specific error codes for schema manager
var (
	SchemaManagerCacheError      = errors.MustNewCode("schema_manager.cache_error")
	SchemaManagerRetrievalError  = errors.MustNewCode("schema_manager.retrieval_error")
	SchemaManagerParsingError    = errors.MustNewCode("schema_manager.parsing_error")
	SchemaManagerValidationError = errors.MustNewCode("schema_manager.validation_error")
	SchemaManagerNotFound        = errors.MustNewCode("schema_manager.not_found")
	SchemaManagerInvalidConfig   = errors.MustNewCode("schema_manager.invalid_config")
	SchemaManagerUnhealthy       = errors.MustNewCode("schema_manager.unhealthy")
	SchemaManagerStale           = errors.MustNewCode("schema_manager.stale")
	SchemaManagerUnresponsive    = errors.MustNewCode("schema_manager.unresponsive")
	SchemaManagerFormatError     = errors.MustNewCode("schema_manager.format_error")
	SchemaManagerCacheError      = errors.MustNewCode("schema_manager.cache_error")
)

// ValidationError represents a detailed validation error with context
// This implements Requirement 4.1, 4.2, 4.3 for detailed error information
type ValidationError struct {
	RowIndex     int         `json:"row_index"`
	ColumnIndex  int         `json:"column_index"`
	ColumnName   string      `json:"column_name"`
	ExpectedType string      `json:"expected_type"`
	ActualType   string      `json:"actual_type"`
	Value        interface{} `json:"value"`
	Message      string      `json:"message"`
	TableName    string      `json:"table_name"`
	Database     string      `json:"database"`
}

// Error implements the error interface
func (ve ValidationError) Error() string {
	return ve.Message
}

// NewValidationError creates a new validation error with comprehensive context
// Requirement 4.1, 4.2, 4.3: Include specific validation failure reason, row/column info, and type information
func NewValidationError(rowIndex, columnIndex int, columnName, expectedType, actualType string, value interface{}, database, tableName string) *ValidationError {
	message := fmt.Sprintf("validation failed at row %d, column %d (%s): expected %s but got %s (value: %v)",
		rowIndex, columnIndex, columnName, expectedType, actualType, value)

	return &ValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  columnIndex,
		ColumnName:   columnName,
		ExpectedType: expectedType,
		ActualType:   actualType,
		Value:        value,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// NewColumnCountValidationError creates a validation error for column count mismatches
// Requirement 4.1, 4.2: Include specific validation failure reason and row information
func NewColumnCountValidationError(rowIndex, actualColumns, expectedColumns int, database, tableName string) *ValidationError {
	message := fmt.Sprintf("validation failed at row %d: expected %d columns but got %d columns",
		rowIndex, expectedColumns, actualColumns)

	return &ValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  -1, // Not applicable for column count errors
		ColumnName:   "",
		ExpectedType: fmt.Sprintf("%d columns", expectedColumns),
		ActualType:   fmt.Sprintf("%d columns", actualColumns),
		Value:        actualColumns,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// NewNullValueValidationError creates a validation error for null value violations
// Requirement 4.1, 4.2, 4.3: Include specific validation failure reason, row/column info, and field requirements
func NewNullValueValidationError(rowIndex, columnIndex int, columnName, database, tableName string) *ValidationError {
	message := fmt.Sprintf("validation failed at row %d, column %d (%s): field cannot be null",
		rowIndex, columnIndex, columnName)

	return &ValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  columnIndex,
		ColumnName:   columnName,
		ExpectedType: "non-null value",
		ActualType:   "null",
		Value:        nil,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// BatchValidationError represents a batch validation failure with detailed context
// Requirement 4.6: Clear indication of batch rejection reason
type BatchValidationError struct {
	FirstError    *ValidationError `json:"first_error"`
	BatchSize     int              `json:"batch_size"`
	Database      string           `json:"database"`
	TableName     string           `json:"table_name"`
	Message       string           `json:"message"`
	RejectedCount int              `json:"rejected_count"`
}

// Error implements the error interface
func (bve BatchValidationError) Error() string {
	return bve.Message
}

// NewBatchValidationError creates a batch validation error
// Requirement 4.6: Clear indication that entire batch was rejected
func NewBatchValidationError(firstError *ValidationError, batchSize int, database, tableName string) *BatchValidationError {
	message := fmt.Sprintf("batch validation failed for table %s.%s: entire batch of %d rows rejected due to validation error: %s",
		database, tableName, batchSize, firstError.Message)

	return &BatchValidationError{
		FirstError:    firstError,
		BatchSize:     batchSize,
		Database:      database,
		TableName:     tableName,
		Message:       message,
		RejectedCount: batchSize,
	}
}

// SchemaRetrievalError represents detailed schema retrieval errors
// Requirement 4.4, 4.5: Indicate whether it's a cache miss or database error, and schema parsing failures
type SchemaRetrievalError struct {
	Database   string `json:"database"`
	TableName  string `json:"table_name"`
	ErrorType  string `json:"error_type"` // "cache_miss", "database_error", "parsing_error"
	Message    string `json:"message"`
	Cause      error  `json:"-"`
	ColumnName string `json:"column_name,omitempty"` // For parsing errors
	DataType   string `json:"data_type,omitempty"`   // For parsing errors
}

// Error implements the error interface
func (sre SchemaRetrievalError) Error() string {
	return sre.Message
}

// NewSchemaRetrievalError creates a schema retrieval error
// Requirement 4.4: Indicate whether it's a cache miss or database error
func NewSchemaRetrievalError(database, tableName, errorType string, cause error) *SchemaRetrievalError {
	var message string
	switch errorType {
	case "cache_miss":
		message = fmt.Sprintf("schema cache miss for table %s.%s", database, tableName)
	case "database_error":
		message = fmt.Sprintf("failed to retrieve schema for table %s.%s from database: %v", database, tableName, cause)
	case "parsing_error":
		message = fmt.Sprintf("failed to parse schema for table %s.%s: %v", database, tableName, cause)
	default:
		message = fmt.Sprintf("schema retrieval error for table %s.%s: %v", database, tableName, cause)
	}

	return &SchemaRetrievalError{
		Database:  database,
		TableName: tableName,
		ErrorType: errorType,
		Message:   message,
		Cause:     cause,
	}
}

// NewSchemaParsingError creates a schema parsing error with column details
// Requirement 4.5: Include the problematic column and DataType value
func NewSchemaParsingError(database, tableName, columnName, dataType string, cause error) *SchemaRetrievalError {
	message := fmt.Sprintf("failed to parse schema for table %s.%s: invalid DataType '%s' for column '%s': %v",
		database, tableName, dataType, columnName, cause)

	return &SchemaRetrievalError{
		Database:   database,
		TableName:  tableName,
		ErrorType:  "parsing_error",
		Message:    message,
		Cause:      cause,
		ColumnName: columnName,
		DataType:   dataType,
	}
}
