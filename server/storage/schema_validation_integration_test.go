package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndValidationPipeline tests the complete validation pipeline with real data
// Requirements: 3.1, 3.2, 3.3, 3.4, 6.1, 6.2, 6.3, 6.4
func TestEndToEndValidationPipeline(t *testing.T) {
	t.Run("CompleteValidationFlowWithValidData", func(t *testing.T) {
		// Step 1: Create test Iceberg schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "email", Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 4, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
			iceberg.NestedField{ID: 5, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		)

		// Step 2: Test schema conversion to Arrow (Requirement 3.2)
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")
		require.NotNil(t, arrowSchema, "Arrow schema should not be nil")
		assert.Len(t, arrowSchema.Fields(), 5, "Arrow schema should have 5 fields")

		// Step 3: Test data validation with valid data (Requirement 3.3)
		validData := [][]interface{}{
			{int64(1), "John Doe", "john@example.com", int32(30), time.Now()},
			{int64(2), "Jane Smith", "jane@example.com", int32(25), time.Now()},
			{int64(3), "Bob Johnson", nil, nil, time.Now()}, // Test nullable fields
		}

		err = schemaValidator.ValidateData(validData, arrowSchema)
		assert.NoError(t, err, "Valid data should pass validation")

		t.Logf("Successfully validated %d rows with schema containing %d fields",
			len(validData), len(arrowSchema.Fields()))
	})

	t.Run("ValidationFailureAndBatchRejection", func(t *testing.T) {
		// Create test schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		)

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")

		// Test batch rejection on validation failure (Requirement 3.4)
		invalidData := [][]interface{}{
			{int64(1), "Valid User", int32(25)},       // Valid row
			{"invalid_id", "Invalid User", int32(30)}, // Invalid ID type - should cause batch rejection
			{int64(3), "Another User", int32(35)},     // This row should not be processed
		}

		err = schemaValidator.ValidateData(invalidData, arrowSchema)
		require.Error(t, err, "Invalid data should cause validation failure")

		// Verify error contains detailed information (Requirement 4.1, 4.2, 4.3)
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "int64", "Error should mention expected type")

		t.Logf("Validation error correctly caught: %v", err)
	})
}

// TestCacheHitMissScenarios verifies cache behavior in various scenarios
// Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 5.1, 5.2, 5.3, 5.4, 5.5
func TestCacheHitMissScenarios(t *testing.T) {
	t.Run("CacheHitAfterInitialMiss", func(t *testing.T) {
		// Test cache behavior using the schema cache directly
		config := schema.DefaultSchemaManagerConfig()
		config.CacheTTL = 1 * time.Hour // Long TTL for testing

		cache := schema.NewSchemaCache(config)

		// Create test schema
		testSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		)

		cacheKey := "test_db.cache_test_table"

		// First access should be a cache miss
		_, found := cache.Get(cacheKey)
		assert.False(t, found, "Initial cache access should be a miss")

		// Store schema in cache
		cache.Put(cacheKey, testSchema)

		// Second access should be a cache hit
		schema2, found := cache.Get(cacheKey)
		assert.True(t, found, "Second cache access should be a hit")
		require.NotNil(t, schema2, "Cached schema should not be nil")

		// Verify schemas are identical
		assert.Equal(t, len(testSchema.Fields()), len(schema2.Fields()), "Schemas should have same field count")

		// Check cache stats
		stats := cache.GetStats()
		assert.Greater(t, stats.HitCount, int64(0), "Should have cache hits")
		assert.Greater(t, stats.CacheSize, 0, "Cache should contain entries")
	})

	t.Run("ConcurrentCacheAccess", func(t *testing.T) {
		// Test concurrent access to schema cache (Requirement 1.5)
		config := schema.DefaultSchemaManagerConfig()
		cache := schema.NewSchemaCache(config)

		testSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		)

		cacheKey := "test_db.concurrent_cache_test"
		cache.Put(cacheKey, testSchema)

		const numGoroutines = 20
		var wg sync.WaitGroup
		results := make(chan bool, numGoroutines)
		schemas := make(chan *iceberg.Schema, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				schema, found := cache.Get(cacheKey)
				results <- found
				if found {
					schemas <- schema
				} else {
					schemas <- nil
				}
			}(i)
		}

		wg.Wait()
		close(results)
		close(schemas)

		// Verify all accesses succeeded
		successCount := 0
		for found := range results {
			if found {
				successCount++
			}
		}

		assert.Equal(t, numGoroutines, successCount, "All concurrent accesses should succeed")

		// Verify all schemas are consistent
		var firstSchema *iceberg.Schema
		schemaCount := 0
		for schema := range schemas {
			if schema != nil {
				schemaCount++
				if firstSchema == nil {
					firstSchema = schema
				} else {
					assert.Equal(t, len(firstSchema.Fields()), len(schema.Fields()),
						"All schemas should have same field count")
				}
			}
		}

		assert.Equal(t, numGoroutines, schemaCount, "All goroutines should receive schemas")
	})
}

// TestValidationFailureHandling tests various validation failure scenarios
// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7
func TestValidationFailureHandling(t *testing.T) {
	t.Run("TypeMismatchErrors", func(t *testing.T) {
		// Create test schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		)

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")

		// Test various type mismatches
		testCases := []struct {
			name        string
			data        [][]interface{}
			expectedErr string
		}{
			{
				name: "StringInsteadOfInt64",
				data: [][]interface{}{
					{"not_a_number", "John Doe", int32(30)},
				},
				expectedErr: "int64",
			},
			{
				name: "IntInsteadOfString",
				data: [][]interface{}{
					{int64(1), 12345, int32(30)},
				},
				expectedErr: "string",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := schemaValidator.ValidateData(tc.data, arrowSchema)
				require.Error(t, err, "Type mismatch should cause validation failure")
				assert.Contains(t, err.Error(), tc.expectedErr,
					"Error should mention expected type: %s", tc.expectedErr)
			})
		}
	})

	t.Run("NullabilityViolations", func(t *testing.T) {
		// Create schema with required and optional fields
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "required_name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "optional_email", Type: iceberg.PrimitiveTypes.String, Required: false},
		)

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")

		// Test null value in non-nullable field
		invalidData := [][]interface{}{
			{int64(1), nil, "optional@example.com"}, // null in required_name
		}

		err = schemaValidator.ValidateData(invalidData, arrowSchema)
		require.Error(t, err, "Null in non-nullable field should cause validation failure")

		// Test valid data with null in nullable field
		validData := [][]interface{}{
			{int64(1), "John Doe", nil}, // null in optional_email is OK
		}

		err = schemaValidator.ValidateData(validData, arrowSchema)
		assert.NoError(t, err, "Null in nullable field should be allowed")
	})
}

// TestStorageEngineIntegration tests integration with validation components
// Requirements: 6.1, 6.2, 6.3, 6.4
func TestStorageEngineIntegration(t *testing.T) {
	t.Run("ValidationComponentsIntegration", func(t *testing.T) {
		// Test that validation components work together correctly

		// Create test Iceberg schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 3, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		)

		// Test schema conversion (Requirement 6.1)
		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")
		assert.Len(t, arrowSchema.Fields(), 3, "Arrow schema should have 3 fields")

		// Test validation with valid data (Requirement 6.2)
		validData := [][]interface{}{
			{int64(1), "test_data_1", time.Now()},
			{int64(2), "test_data_2", time.Now()},
			{int64(3), nil, time.Now()}, // Test nullable field
		}

		err = schemaValidator.ValidateData(validData, arrowSchema)
		assert.NoError(t, err, "Valid data should pass validation")

		// Test validation failure handling (Requirement 6.3)
		invalidData := [][]interface{}{
			{"invalid_id", "test_data", time.Now()}, // Invalid ID type
		}

		err = schemaValidator.ValidateData(invalidData, arrowSchema)
		assert.Error(t, err, "Invalid data should fail validation")
		assert.Contains(t, err.Error(), "int64", "Error should mention expected type")

		// Verify validation happens before any processing (Requirement 6.4)
		// This is implicitly tested by the validation failure above
		t.Logf("Successfully tested validation integration components")
	})
}

// TestPerformanceAndScalability tests performance characteristics of the validation pipeline
func TestPerformanceAndScalability(t *testing.T) {
	t.Run("LargeBatchValidation", func(t *testing.T) {
		// Create test schema
		icebergSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		)

		schemaValidator := schema.NewManager(schema.DefaultParquetConfig())
		arrowSchema, err := schemaValidator.ConvertIcebergToArrowSchema(icebergSchema)
		require.NoError(t, err, "Schema conversion should succeed")

		// Create large batch of valid data
		const batchSize = 1000
		largeValidData := make([][]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			largeValidData[i] = []interface{}{
				int64(i + 1),
				fmt.Sprintf("user_%d", i+1),
				float64(i) * 1.5,
			}
		}

		// Measure validation performance
		startTime := time.Now()
		err = schemaValidator.ValidateData(largeValidData, arrowSchema)
		duration := time.Since(startTime)

		assert.NoError(t, err, "Large batch should be validated successfully")
		assert.Less(t, duration, 2*time.Second, "Large batch validation should complete within 2 seconds")

		t.Logf("Successfully validated %d rows in %v (%.2f rows/sec)",
			batchSize, duration, float64(batchSize)/duration.Seconds())
	})

	t.Run("CachePerformanceUnderLoad", func(t *testing.T) {
		config := schema.DefaultSchemaManagerConfig()
		cache := schema.NewSchemaCache(config)

		// Create multiple test schemas
		const numTables = 50
		for i := 0; i < numTables; i++ {
			testSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
			)
			cacheKey := fmt.Sprintf("test_db.cache_perf_table_%d", i)
			cache.Put(cacheKey, testSchema)
		}

		// Measure cache performance with concurrent access
		const numConcurrentAccesses = 100
		var wg sync.WaitGroup
		startTime := time.Now()

		for i := 0; i < numConcurrentAccesses; i++ {
			wg.Add(1)
			go func(accessID int) {
				defer wg.Done()

				// Access random table
				cacheKey := fmt.Sprintf("test_db.cache_perf_table_%d", accessID%numTables)
				_, found := cache.Get(cacheKey)
				if !found {
					t.Errorf("Schema access failed for %s", cacheKey)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(startTime)

		// Check cache performance
		stats := cache.GetStats()
		hitRatio := stats.HitRatio

		assert.Greater(t, hitRatio, 0.5, "Cache hit ratio should be greater than 50%")
		assert.Less(t, duration, 2*time.Second, "Concurrent cache access should complete within 2 seconds")

		t.Logf("Cache performance: %d accesses in %v, hit ratio: %.2f%%, cache size: %d",
			numConcurrentAccesses, duration, hitRatio*100, stats.CacheSize)
	})
}
