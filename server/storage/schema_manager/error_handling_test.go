package schema_manager

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gear6io/ranger/server/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetailedValidationErrors tests the enhanced validation error types
// Requirement 4.1, 4.2, 4.3: Create detailed validation error types with context
func TestDetailedValidationErrors(t *testing.T) {
	t.Run("ValidationErrorWithContext", func(t *testing.T) {
		// Test creating a detailed validation error
		validationErr := schema.NewDetailedValidationError(
			1,        // rowIndex
			2,        // columnIndex
			"email",  // columnName
			"string", // expectedType
			"int",    // actualType
			123,      // value
			"testdb", // database
			"users",  // tableName
		)

		assert.Equal(t, 1, validationErr.RowIndex)
		assert.Equal(t, 2, validationErr.ColumnIndex)
		assert.Equal(t, "email", validationErr.ColumnName)
		assert.Equal(t, "string", validationErr.ExpectedType)
		assert.Equal(t, "int", validationErr.ActualType)
		assert.Equal(t, 123, validationErr.Value)
		assert.Equal(t, "testdb", validationErr.Database)
		assert.Equal(t, "users", validationErr.TableName)

		// Verify error message contains all relevant information
		errorMsg := validationErr.Error()
		assert.Contains(t, errorMsg, "validation failed at row 1, column 2 (email)")
		assert.Contains(t, errorMsg, "expected string but got int")
		assert.Contains(t, errorMsg, "value: 123")
	})

	t.Run("ColumnCountValidationError", func(t *testing.T) {
		// Test creating a column count validation error
		validationErr := schema.NewColumnCountValidationError(
			0,        // rowIndex
			2,        // actualColumns
			3,        // expectedColumns
			"testdb", // database
			"users",  // tableName
		)

		assert.Equal(t, 0, validationErr.RowIndex)
		assert.Equal(t, -1, validationErr.ColumnIndex) // Not applicable
		assert.Equal(t, "", validationErr.ColumnName)
		assert.Equal(t, "3 columns", validationErr.ExpectedType)
		assert.Equal(t, "2 columns", validationErr.ActualType)
		assert.Equal(t, 2, validationErr.Value)

		// Verify error message
		errorMsg := validationErr.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0")
		assert.Contains(t, errorMsg, "expected 3 columns but got 2 columns")
	})

	t.Run("NullValueValidationError", func(t *testing.T) {
		// Test creating a null value validation error
		validationErr := schema.NewNullValueValidationError(
			5,        // rowIndex
			1,        // columnIndex
			"name",   // columnName
			"testdb", // database
			"users",  // tableName
		)

		assert.Equal(t, 5, validationErr.RowIndex)
		assert.Equal(t, 1, validationErr.ColumnIndex)
		assert.Equal(t, "name", validationErr.ColumnName)
		assert.Equal(t, "non-null value", validationErr.ExpectedType)
		assert.Equal(t, "null", validationErr.ActualType)
		assert.Nil(t, validationErr.Value)

		// Verify error message
		errorMsg := validationErr.Error()
		assert.Contains(t, errorMsg, "validation failed at row 5, column 1 (name)")
		assert.Contains(t, errorMsg, "field cannot be null")
	})
}

// TestSchemaRetrievalErrors tests the enhanced schema retrieval error types
// Requirement 4.4, 4.5: Indicate whether it's a cache miss or database error, and schema parsing failures
func TestSchemaRetrievalErrors(t *testing.T) {
	t.Run("DatabaseError", func(t *testing.T) {
		// Test creating a database error
		cause := assert.AnError
		retrievalErr := NewSchemaRetrievalError("testdb", "users", "database_error", cause)

		assert.Equal(t, "testdb", retrievalErr.Database)
		assert.Equal(t, "users", retrievalErr.TableName)
		assert.Equal(t, "database_error", retrievalErr.ErrorType)
		assert.Equal(t, cause, retrievalErr.Cause)

		// Verify error message
		errorMsg := retrievalErr.Error()
		assert.Contains(t, errorMsg, "failed to retrieve schema for table testdb.users from database")
		assert.Contains(t, errorMsg, cause.Error())
	})

	t.Run("CacheMissError", func(t *testing.T) {
		// Test creating a cache miss error
		retrievalErr := NewSchemaRetrievalError("testdb", "users", "cache_miss", nil)

		assert.Equal(t, "testdb", retrievalErr.Database)
		assert.Equal(t, "users", retrievalErr.TableName)
		assert.Equal(t, "cache_miss", retrievalErr.ErrorType)
		assert.Nil(t, retrievalErr.Cause)

		// Verify error message
		errorMsg := retrievalErr.Error()
		assert.Contains(t, errorMsg, "schema cache miss for table testdb.users")
	})

	t.Run("ParsingError", func(t *testing.T) {
		// Test creating a parsing error with column details
		cause := assert.AnError
		parsingErr := NewSchemaParsingError("testdb", "users", "email", "invalid_type", cause)

		assert.Equal(t, "testdb", parsingErr.Database)
		assert.Equal(t, "users", parsingErr.TableName)
		assert.Equal(t, "parsing_error", parsingErr.ErrorType)
		assert.Equal(t, "email", parsingErr.ColumnName)
		assert.Equal(t, "invalid_type", parsingErr.DataType)
		assert.Equal(t, cause, parsingErr.Cause)

		// Verify error message
		errorMsg := parsingErr.Error()
		assert.Contains(t, errorMsg, "failed to parse schema for table testdb.users")
		assert.Contains(t, errorMsg, "invalid DataType 'invalid_type' for column 'email'")
		assert.Contains(t, errorMsg, cause.Error())
	})
}

// TestBatchValidationError tests the batch validation error type
// Requirement 4.6: Clear indication that entire batch was rejected
func TestBatchValidationError(t *testing.T) {
	t.Run("BatchRejectionError", func(t *testing.T) {
		// Create a first validation error
		firstError := NewValidationError(
			2,        // rowIndex
			1,        // columnIndex
			"age",    // columnName
			"int32",  // expectedType
			"string", // actualType
			"abc",    // value
			"testdb", // database
			"users",  // tableName
		)

		// Create batch validation error
		batchErr := NewBatchValidationError(firstError, 100, "testdb", "users")

		assert.Equal(t, firstError, batchErr.FirstError)
		assert.Equal(t, 100, batchErr.BatchSize)
		assert.Equal(t, "testdb", batchErr.Database)
		assert.Equal(t, "users", batchErr.TableName)
		assert.Equal(t, 100, batchErr.RejectedCount)

		// Verify error message
		errorMsg := batchErr.Error()
		assert.Contains(t, errorMsg, "batch validation failed for table testdb.users")
		assert.Contains(t, errorMsg, "entire batch of 100 rows rejected")
		assert.Contains(t, errorMsg, "validation failed at row 2, column 1 (age)")
	})
}

// TestEnhancedValidationWithContext tests the enhanced validation with context
// Requirement 4.1, 4.2, 4.3: Include database and table context in validation errors
func TestEnhancedValidationWithContext(t *testing.T) {
	t.Run("TypeMismatchWithContext", func(t *testing.T) {
		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create Arrow schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create invalid data (wrong type for id field)
		invalidData := [][]interface{}{
			{"not_an_int", "Alice"}, // id should be int64, not string
		}

		// Validate with context
		err := schemaValidator.ValidateDataWithContext(invalidData, arrowSchema, "testdb", "users")

		// Verify error occurred
		require.Error(t, err)

		// Verify error message contains context information
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0, column 0 (id)")
		assert.Contains(t, errorMsg, "expected int64 but got string")
		assert.Contains(t, errorMsg, "value: not_an_int")
	})

	t.Run("NullConstraintViolationWithContext", func(t *testing.T) {
		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create Arrow schema with non-nullable field
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false}, // Non-nullable
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create invalid data (null value for non-nullable field)
		invalidData := [][]interface{}{
			{int64(1), nil}, // name cannot be null
		}

		// Validate with context
		err := schemaValidator.ValidateDataWithContext(invalidData, arrowSchema, "testdb", "users")

		// Verify error occurred
		require.Error(t, err)

		// Verify error message contains context information
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0, column 1 (name)")
		assert.Contains(t, errorMsg, "field cannot be null")
	})

	t.Run("ColumnCountMismatchWithContext", func(t *testing.T) {
		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create Arrow schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create invalid data (wrong number of columns)
		invalidData := [][]interface{}{
			{int64(1), "Alice"}, // Missing email column
		}

		// Validate with context
		err := schemaValidator.ValidateDataWithContext(invalidData, arrowSchema, "testdb", "users")

		// Verify error occurred
		require.Error(t, err)

		// Verify error message contains context information
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0")
		assert.Contains(t, errorMsg, "expected 3 columns but got 2 columns")
	})
}

// TestFailFastValidation tests that validation fails on the first error
// Requirement 3.4: Return immediately on first validation failure
func TestFailFastValidation(t *testing.T) {
	t.Run("FailOnFirstError", func(t *testing.T) {
		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create Arrow schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create batch with multiple errors (first row has error, second row is valid)
		invalidData := [][]interface{}{
			{"not_an_int", "Alice"}, // First error - should fail here
			{int64(2), "Bob"},       // This row is valid but should not be processed
		}

		// Validate with context
		err := schemaValidator.ValidateDataWithContext(invalidData, arrowSchema, "testdb", "users")

		// Verify error occurred
		require.Error(t, err)

		// Verify error is about the first row (fail-fast behavior)
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0") // Should fail on first row
		assert.Contains(t, errorMsg, "expected int64 but got string")
	})
}
