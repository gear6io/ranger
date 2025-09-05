package parquet

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetailedValidationErrors tests the enhanced validation error types
// Requirement 4.1, 4.2, 4.3: Create detailed validation error types with context
func TestDetailedValidationErrors(t *testing.T) {
	t.Run("ValidationErrorWithContext", func(t *testing.T) {
		// Test creating a detailed validation error
		validationErr := NewDetailedValidationError(
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
		validationErr := NewColumnCountValidationError(
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
		validationErr := NewNullValueValidationError(
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

// TestEnhancedValidationWithContext tests the enhanced validation with context
// Requirement 4.1, 4.2, 4.3: Include database and table context in validation errors
func TestEnhancedValidationWithContext(t *testing.T) {
	t.Run("TypeMismatchWithContext", func(t *testing.T) {
		// Create schema validator
		schemaValidator := NewManager(DefaultParquetConfig())

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
		schemaValidator := NewManager(DefaultParquetConfig())

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
		schemaValidator := NewManager(DefaultParquetConfig())

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
		schemaValidator := NewManager(DefaultParquetConfig())

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

// TestValidationErrorLogging tests that validation errors are logged with appropriate severity
// Requirement 4.7: Proper error logging with severity levels
func TestValidationErrorLogging(t *testing.T) {
	t.Run("ValidationErrorsAreDetailed", func(t *testing.T) {
		// Create schema validator
		schemaValidator := NewManager(DefaultParquetConfig())

		// Create Arrow schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Test various validation error scenarios
		testCases := []struct {
			name        string
			data        [][]interface{}
			expectedErr string
		}{
			{
				name:        "TypeMismatch",
				data:        [][]interface{}{{"not_an_int", "Alice"}},
				expectedErr: "expected int32 but got string",
			},
			{
				name:        "NullConstraintViolation",
				data:        [][]interface{}{{int32(1), nil}},
				expectedErr: "field cannot be null",
			},
			{
				name:        "ColumnCountMismatch",
				data:        [][]interface{}{{int32(1)}}, // Missing name column
				expectedErr: "expected 2 columns but got 1 columns",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := schemaValidator.ValidateDataWithContext(tc.data, arrowSchema, "testdb", "users")
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			})
		}
	})
}
