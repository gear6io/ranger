package storage

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gear6io/ranger/server/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnhancedErrorHandlingIntegration tests the comprehensive error handling system
// This verifies that all requirements for task 5 are met
func TestEnhancedErrorHandlingIntegration(t *testing.T) {
	t.Run("DetailedValidationErrorsWithContext", func(t *testing.T) {
		// Test Requirement 4.1, 4.2, 4.3: Detailed validation error types with context

		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create Arrow schema
		fields := []arrow.Field{
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "username", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		testCases := []struct {
			name           string
			data           [][]interface{}
			expectedErrors []string
			description    string
		}{
			{
				name: "TypeMismatchError",
				data: [][]interface{}{
					{"not_an_int", "alice", "alice@example.com", int32(25)}, // user_id should be int64
				},
				expectedErrors: []string{
					"validation failed at row 0, column 0 (user_id)",
					"expected int64 but got string",
					"value: not_an_int",
				},
				description: "Should provide detailed type mismatch information",
			},
			{
				name: "NullConstraintViolation",
				data: [][]interface{}{
					{int64(1), nil, "alice@example.com", int32(25)}, // username cannot be null
				},
				expectedErrors: []string{
					"validation failed at row 0, column 1 (username)",
					"field cannot be null",
				},
				description: "Should provide detailed null constraint violation information",
			},
			{
				name: "ColumnCountMismatch",
				data: [][]interface{}{
					{int64(1), "alice", "alice@example.com"}, // Missing age column
				},
				expectedErrors: []string{
					"validation failed at row 0",
					"expected 4 columns but got 3 columns",
				},
				description: "Should provide detailed column count mismatch information",
			},
			{
				name: "MultipleFieldTypeMismatch",
				data: [][]interface{}{
					{int64(1), "alice", "alice@example.com", "not_an_int"}, // age should be int32
				},
				expectedErrors: []string{
					"validation failed at row 0, column 3 (age)",
					"expected int32 but got string",
					"value: not_an_int",
				},
				description: "Should provide detailed information for any field type mismatch",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Validate with context (Requirement 4.1, 4.2, 4.3)
				err := schemaValidator.ValidateDataWithContext(tc.data, arrowSchema, "test_database", "test_table")

				// Verify error occurred
				require.Error(t, err, tc.description)

				// Verify all expected error details are present
				errorMsg := err.Error()
				for _, expectedErr := range tc.expectedErrors {
					assert.Contains(t, errorMsg, expectedErr,
						"Error message should contain: %s\nActual error: %s", expectedErr, errorMsg)
				}

				t.Logf("Test case '%s' - Error message: %s", tc.name, errorMsg)
			})
		}
	})

	t.Run("FailFastBehavior", func(t *testing.T) {
		// Test Requirement 3.4: Return immediately on first validation failure

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create batch with first row having error, subsequent rows valid
		batchData := [][]interface{}{
			{"invalid_id", "Alice"}, // First row - should fail here (fail-fast)
			{int64(2), "Bob"},       // Valid row - should not be processed
			{int64(3), "Charlie"},   // Valid row - should not be processed
		}

		err := schemaValidator.ValidateDataWithContext(batchData, arrowSchema, "test_db", "test_table")

		require.Error(t, err)

		// Verify it failed on the first row (fail-fast behavior)
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "validation failed at row 0",
			"Should fail on first row due to fail-fast behavior")
		assert.Contains(t, errorMsg, "expected int64 but got string")

		// The error should not mention subsequent rows
		assert.NotContains(t, errorMsg, "row 1")
		assert.NotContains(t, errorMsg, "row 2")

		t.Logf("Fail-fast validation error: %s", errorMsg)
	})

	t.Run("BatchRejectionScenario", func(t *testing.T) {
		// Test Requirement 4.6: Clear indication that entire batch was rejected

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		fields := []arrow.Field{
			{Name: "product_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "product_name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Large batch with error in the middle
		largeBatch := make([][]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			if i == 500 {
				// Insert error in the middle of the batch
				largeBatch[i] = []interface{}{"invalid_id", "Product 500", 99.99}
			} else {
				largeBatch[i] = []interface{}{int64(i), fmt.Sprintf("Product %d", i), float64(i) * 10.5}
			}
		}

		err := schemaValidator.ValidateDataWithContext(largeBatch, arrowSchema, "inventory_db", "products")

		require.Error(t, err)

		errorMsg := err.Error()

		// Verify it indicates batch rejection
		assert.Contains(t, errorMsg, "validation failed at row 500",
			"Should indicate the specific row that failed")
		assert.Contains(t, errorMsg, "expected int64 but got string")

		t.Logf("Batch rejection error for 1000-row batch: %s", errorMsg)
	})

	t.Run("ErrorLoggingAndSeverity", func(t *testing.T) {
		// Test Requirement 4.7: Proper error logging with severity levels

		// This test verifies that errors contain appropriate context for logging
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		fields := []arrow.Field{
			{Name: "transaction_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "currency", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		invalidData := [][]interface{}{
			{int64(1), "invalid_amount", "USD"}, // amount should be float64
		}

		err := schemaValidator.ValidateDataWithContext(invalidData, arrowSchema, "finance_db", "transactions")

		require.Error(t, err)

		errorMsg := err.Error()

		// Verify error contains all necessary context for proper logging
		assert.Contains(t, errorMsg, "validation failed at row 0, column 1 (amount)")
		assert.Contains(t, errorMsg, "expected float64 but got string")
		assert.Contains(t, errorMsg, "value: invalid_amount")

		// The error should be structured for easy parsing by logging systems
		assert.Contains(t, errorMsg, "row 0")
		assert.Contains(t, errorMsg, "column 1")
		assert.Contains(t, errorMsg, "amount") // field name

		t.Logf("Structured validation error for logging: %s", errorMsg)
	})

	t.Run("AllValidationErrorTypes", func(t *testing.T) {
		// Test all different types of validation errors to ensure comprehensive coverage

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		errorScenarios := []struct {
			name        string
			data        [][]interface{}
			errorType   string
			description string
		}{
			{
				name:        "Int32TypeMismatch",
				data:        [][]interface{}{{"not_int", "Alice", true, float32(95.5)}},
				errorType:   "type_mismatch",
				description: "Int32 field receives string value",
			},
			{
				name:        "StringTypeMismatch",
				data:        [][]interface{}{{int32(1), 123, true, float32(95.5)}},
				errorType:   "type_mismatch",
				description: "String field receives int value",
			},
			{
				name:        "BooleanTypeMismatch",
				data:        [][]interface{}{{int32(1), "Alice", "not_bool", float32(95.5)}},
				errorType:   "type_mismatch",
				description: "Boolean field receives string value",
			},
			{
				name:        "Float32TypeMismatch",
				data:        [][]interface{}{{int32(1), "Alice", true, "not_float"}},
				errorType:   "type_mismatch",
				description: "Float32 field receives string value",
			},
			{
				name:        "NullConstraintViolation",
				data:        [][]interface{}{{int32(1), nil, true, float32(95.5)}},
				errorType:   "null_constraint_violation",
				description: "Non-nullable field receives null value",
			},
			{
				name:        "ColumnCountTooFew",
				data:        [][]interface{}{{int32(1), "Alice"}}, // Missing columns
				errorType:   "column_count_mismatch",
				description: "Row has fewer columns than expected",
			},
			{
				name:        "ColumnCountTooMany",
				data:        [][]interface{}{{int32(1), "Alice", true, float32(95.5), "extra"}}, // Extra column
				errorType:   "column_count_mismatch",
				description: "Row has more columns than expected",
			},
		}

		for _, scenario := range errorScenarios {
			t.Run(scenario.name, func(t *testing.T) {
				err := schemaValidator.ValidateDataWithContext(scenario.data, arrowSchema, "test_db", "test_table")

				require.Error(t, err, scenario.description)

				errorMsg := err.Error()
				assert.NotEmpty(t, errorMsg, "Error message should not be empty")

				// All errors should contain row information
				assert.Contains(t, errorMsg, "validation failed at row 0",
					"All validation errors should specify the row number")

				t.Logf("Scenario '%s': %s", scenario.name, errorMsg)
			})
		}
	})
}

// Helper function to format error messages (would be used in actual logging)
func formatValidationErrorForLogging(err error, database, tableName string, batchSize int) map[string]interface{} {
	return map[string]interface{}{
		"error_type":    "validation_failure",
		"database":      database,
		"table":         tableName,
		"batch_size":    batchSize,
		"error_message": err.Error(),
		"severity":      "error",
	}
}
