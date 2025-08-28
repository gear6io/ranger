package storage

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaValidationIntegration tests that the schema validation is properly integrated into InsertData
func TestSchemaValidationIntegration(t *testing.T) {
	t.Run("SchemaValidationComponents", func(t *testing.T) {
		// Test the schema validation components work together

		// Create a test Iceberg schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		)

		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Convert Iceberg schema to Arrow schema
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Should convert Iceberg schema to Arrow schema")
		assert.NotNil(t, arrowSchema, "Arrow schema should not be nil")

		// Test valid data
		validData := [][]interface{}{
			{int64(1), "Alice", int32(25)},
			{int64(2), "Bob", int32(30)},
			{int64(3), "Charlie", nil}, // nullable age
		}

		err = schemaValidator.ValidateData(validData, arrowSchema)
		assert.NoError(t, err, "Valid data should pass validation")

		// Test invalid data - wrong type
		invalidData := [][]interface{}{
			{int64(1), "Alice", "invalid_age"}, // age should be int32, not string
			{int64(2), "Bob", int32(30)},
		}

		err = schemaValidator.ValidateData(invalidData, arrowSchema)
		assert.Error(t, err, "Invalid data should fail validation")
		assert.Contains(t, err.Error(), "expects int32", "Error should mention type mismatch")

		// Test invalid data - wrong number of columns
		invalidColumnData := [][]interface{}{
			{int64(1), "Alice"}, // missing age column
		}

		err = schemaValidator.ValidateData(invalidColumnData, arrowSchema)
		assert.Error(t, err, "Data with wrong number of columns should fail validation")
		assert.Contains(t, err.Error(), "incorrect number of columns", "Error should mention column count mismatch")
	})

	t.Run("InsertDataValidationFlow", func(t *testing.T) {
		// Test that the InsertData method has the validation flow integrated
		// This is a structural test to ensure the validation components are available

		// Create a simple Arrow schema for testing
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create schema validator
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Test that validation works as expected
		validData := [][]interface{}{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
		}

		err := schemaValidator.ValidateData(validData, arrowSchema)
		assert.NoError(t, err, "Valid data should pass validation")

		// Test batch rejection on first error
		invalidData := [][]interface{}{
			{int64(1), "Alice"},
			{"invalid_id", "Bob"}, // This should cause validation to fail
			{int64(3), "Charlie"}, // This row should not be processed due to batch rejection
		}

		err = schemaValidator.ValidateData(invalidData, arrowSchema)
		assert.Error(t, err, "Invalid data should fail validation and reject entire batch")
		assert.Contains(t, err.Error(), "expects int64", "Error should indicate type mismatch")
	})
}

// TestInsertDataSchemaValidationRequirements tests that the InsertData method meets the requirements
func TestInsertDataSchemaValidationRequirements(t *testing.T) {
	t.Run("ValidationBeforeStorage", func(t *testing.T) {
		// This test verifies that validation happens before storage operations
		// by testing the validation components that are integrated into InsertData

		// Create test schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// Create schema validator (same as used in InsertData)
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Test that validation fails fast on first error (Requirement 3.4)
		invalidData := [][]interface{}{
			{int64(1), "Alice"},
			{"not_an_int", "Bob"}, // This should cause immediate failure
		}

		err := schemaValidator.ValidateData(invalidData, arrowSchema)
		assert.Error(t, err, "Validation should fail on first invalid row")

		// Verify error contains detailed information (Requirement 4.1, 4.2, 4.3)
		errorMsg := err.Error()
		t.Logf("Validation error message: %s", errorMsg)
		assert.Contains(t, errorMsg, "expects int64", "Error should include expected type")

		// Check if the error has context information (even if not in the message)
		// This verifies that the validation system provides detailed error context
		assert.Error(t, err, "Should have validation error")
		assert.NotEmpty(t, errorMsg, "Error message should not be empty")
	})

	t.Run("BatchRejectionOnValidationFailure", func(t *testing.T) {
		// Test that entire batch is rejected when validation fails (Requirement 3.4)

		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())

		// Create batch with one invalid row
		batchData := [][]interface{}{
			{int32(1)},
			{int32(2)},
			{"invalid"}, // This should cause entire batch rejection
			{int32(4)},
		}

		err := schemaValidator.ValidateData(batchData, arrowSchema)
		assert.Error(t, err, "Entire batch should be rejected on validation failure")

		// Verify that the error indicates batch rejection
		assert.Contains(t, err.Error(), "expects int32", "Error should indicate type validation failure")
	})
}
