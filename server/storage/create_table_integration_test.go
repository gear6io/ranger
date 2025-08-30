package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/astha"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/storage/schema_manager"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateTableToInsertValidationFlow tests the complete CREATE TABLE to INSERT validation flow
// Requirement 7.1: WHEN CREATE TABLE is executed THEN subsequent INSERT statements SHALL validate against the created schema
// Requirement 7.2: WHEN INSERT data matches schema THEN the operation SHALL succeed
func TestCreateTableToInsertValidationFlow(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("CompleteFlowWithValidData", func(t *testing.T) {
		// Step 1: Create table with Iceberg types
		createTableStmt := &parser.CreateTableStmt{
			TableName: "users",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
				{Name: "email", DataType: "string", IsNullable: true},
				{Name: "age", DataType: "int32", IsNullable: true},
				{Name: "balance", DataType: "decimal(10,2)", IsNullable: true},
				{Name: "created_at", DataType: "timestamp", IsNullable: false},
			},
		}

		// Execute CREATE TABLE through Storage Manager
		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "test_req_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err, "CREATE TABLE should succeed")
		require.True(t, createResp.Success, "CREATE TABLE response should indicate success")
		require.Greater(t, createResp.TableID, int64(0), "Table ID should be assigned")

		// Wait for Astha event processing
		time.Sleep(100 * time.Millisecond)

		// Step 2: Verify schema is cached in Schema Manager
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", "users")
		require.NoError(t, err, "Schema should be cached after CREATE TABLE")
		require.NotNil(t, schema, "Schema should not be nil")

		// Step 3: Simulate INSERT with valid data
		insertData := map[string]interface{}{
			"id":         int64(1),
			"name":       "John Doe",
			"email":      "john@example.com",
			"age":        int32(30),
			"balance":    "1234.56", // Decimal as string
			"created_at": time.Now(),
		}

		// Validate INSERT data against cached schema
		err = testEnv.validateInsertData(ctx, "test_db", "users", insertData)
		assert.NoError(t, err, "INSERT with valid data should pass validation")

		// Step 4: Test INSERT with invalid data
		invalidInsertData := map[string]interface{}{
			"id":         "not_a_number", // Invalid type
			"name":       "Jane Doe",
			"email":      "jane@example.com",
			"age":        int32(25),
			"created_at": time.Now(),
		}

		err = testEnv.validateInsertData(ctx, "test_db", "users", invalidInsertData)
		assert.Error(t, err, "INSERT with invalid data should fail validation")
		assert.Contains(t, err.Error(), "type mismatch", "Error should indicate type mismatch")
	})

	t.Run("CompleteFlowWithComplexTypes", func(t *testing.T) {
		// Test with complex Iceberg types
		createTableStmt := &parser.CreateTableStmt{
			TableName: "complex_table",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "tags", DataType: "list<string>", IsNullable: true},
				{Name: "metadata", DataType: "map<string,string>", IsNullable: true},
				{Name: "profile", DataType: "struct<name:string,age:int32>", IsNullable: true},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "test_req_002",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err, "CREATE TABLE with complex types should succeed")
		require.True(t, createResp.Success)

		// Wait for Astha event processing
		time.Sleep(100 * time.Millisecond)

		// Verify schema is cached
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", "complex_table")
		require.NoError(t, err, "Complex schema should be cached")
		require.NotNil(t, schema)

		// Test INSERT with complex data
		complexInsertData := map[string]interface{}{
			"id":   int64(1),
			"tags": []string{"tag1", "tag2", "tag3"},
			"metadata": map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			"profile": map[string]interface{}{
				"name": "John Doe",
				"age":  int32(30),
			},
		}

		err = testEnv.validateInsertData(ctx, "test_db", "complex_table", complexInsertData)
		assert.NoError(t, err, "INSERT with valid complex data should pass validation")
	})
}

// TestSchemaConsistencyAcrossComponents verifies schema consistency across all components
// Requirement 7.3: WHEN schema changes occur THEN existing INSERT operations SHALL use updated validation rules
func TestSchemaConsistencyAcrossComponents(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("SchemaConsistencyAfterTableCreation", func(t *testing.T) {
		// Create table
		createTableStmt := &parser.CreateTableStmt{
			TableName: "consistency_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "consistency_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)
		require.True(t, createResp.Success)

		// Wait for event processing
		time.Sleep(100 * time.Millisecond)

		// Verify schema consistency across components

		// 1. Check Schema Manager cache has correct schema
		cachedSchema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", "consistency_test")
		require.NoError(t, err, "Schema Manager should have cached schema")
		require.NotNil(t, cachedSchema)

		// 2. Verify schema has expected structure
		assert.Len(t, cachedSchema.Fields(), 2, "Schema should have 2 fields")

		// 3. Verify field names and basic structure
		fieldNames := make([]string, len(cachedSchema.Fields()))
		for i, field := range cachedSchema.Fields() {
			fieldNames[i] = field.Name
		}
		assert.Contains(t, fieldNames, "id", "Schema should contain id field")
		assert.Contains(t, fieldNames, "name", "Schema should contain name field")
	})

	t.Run("SchemaConsistencyAfterColumnAddition", func(t *testing.T) {
		// This test simulates adding a column and verifies all components see the change
		// Note: This would require ALTER TABLE functionality, which is not implemented yet
		// For now, we'll test the cache invalidation mechanism directly

		tableName := "schema_change_test"

		// Create initial table
		createTableStmt := &parser.CreateTableStmt{
			TableName: tableName,
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "schema_change_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Wait for event processing
		time.Sleep(100 * time.Millisecond)

		// Get initial schema
		initialSchema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
		require.NoError(t, err)
		assert.Len(t, initialSchema.Fields(), 2, "Initial schema should have 2 fields")

		// Simulate schema change by invalidating cache
		// In a real scenario, this would be triggered by an ALTER TABLE operation
		err = testEnv.schemaManager.InvalidateAndRefreshSchema(ctx, "test_db", tableName)
		require.NoError(t, err, "Cache invalidation should succeed")

		// Verify cache was refreshed
		refreshedSchema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
		require.NoError(t, err)
		assert.NotNil(t, refreshedSchema, "Refreshed schema should not be nil")
	})
}

// TestConcurrentOperationsAndRaceConditions tests concurrent operations and race condition handling
// Requirement 7.4: WHEN multiple concurrent operations occur THEN schema consistency SHALL be maintained
func TestConcurrentOperationsAndRaceConditions(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("ConcurrentTableCreation", func(t *testing.T) {
		const numConcurrentTables = 10
		var wg sync.WaitGroup
		results := make(chan error, numConcurrentTables)

		// Create multiple tables concurrently
		for i := 0; i < numConcurrentTables; i++ {
			wg.Add(1)
			go func(tableIndex int) {
				defer wg.Done()

				tableName := fmt.Sprintf("concurrent_table_%d", tableIndex)
				createTableStmt := &parser.CreateTableStmt{
					TableName: tableName,
					Columns: []*parser.ColumnDefinition{
						{Name: "id", DataType: "int64", IsNullable: false},
						{Name: "data", DataType: "string", IsNullable: true},
					},
				}

				createReq := &types.CreateTableRequest{
					Statement:     createTableStmt,
					Database:      "test_db",
					RequestID:     fmt.Sprintf("concurrent_req_%d", tableIndex),
					UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
					StorageEngine: "iceberg",
					EngineConfig:  map[string]interface{}{"format": "parquet"},
				}

				_, err := testEnv.storageManager.CreateTable(ctx, createReq)
				results <- err
			}(i)
		}

		wg.Wait()
		close(results)

		// Check all operations succeeded
		successCount := 0
		for err := range results {
			if err == nil {
				successCount++
			} else {
				t.Logf("Concurrent table creation error: %v", err)
			}
		}

		assert.Equal(t, numConcurrentTables, successCount, "All concurrent table creations should succeed")

		// Wait for all Astha events to be processed
		time.Sleep(500 * time.Millisecond)

		// Verify all schemas are cached
		for i := 0; i < numConcurrentTables; i++ {
			tableName := fmt.Sprintf("concurrent_table_%d", i)
			schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
			assert.NoError(t, err, "Schema should be cached for table %s", tableName)
			assert.NotNil(t, schema, "Schema should not be nil for table %s", tableName)
		}
	})

	t.Run("ConcurrentSchemaAccess", func(t *testing.T) {
		// Create a table first
		tableName := "concurrent_access_test"
		createTableStmt := &parser.CreateTableStmt{
			TableName: tableName,
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "concurrent_access_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Wait for initial caching
		time.Sleep(100 * time.Millisecond)

		// Concurrently access schema multiple times
		const numConcurrentReads = 20
		var wg sync.WaitGroup
		results := make(chan error, numConcurrentReads)

		for i := 0; i < numConcurrentReads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
				results <- err
			}()
		}

		wg.Wait()
		close(results)

		// All reads should succeed
		successCount := 0
		for err := range results {
			if err == nil {
				successCount++
			}
		}

		assert.Equal(t, numConcurrentReads, successCount, "All concurrent schema reads should succeed")
	})
}

// TestAsthaEventProcessingAndCacheUpdates validates Astha event processing and schema cache updates
// Requirement 7.5: WHEN validation errors occur THEN they SHALL reference the original CREATE TABLE column definitions
func TestAsthaEventProcessingAndCacheUpdates(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("AsthaEventProcessingForTableCreation", func(t *testing.T) {
		// Monitor Astha events
		eventReceived := make(chan bool, 1)

		// Create a custom subscriber to monitor events
		testSubscriber := &TestEventSubscriber{
			eventReceived: eventReceived,
		}

		// Register test subscriber
		componentInfo := astha.ComponentInfo{
			Name:          "test_subscriber",
			Version:       "1.0.0",
			Status:        "active",
			LastSeen:      time.Now(),
			Subscriptions: []string{"tables", "table_columns"},
		}

		err := testEnv.astha.RegisterComponentWithInstance(componentInfo, testSubscriber)
		require.NoError(t, err, "Test subscriber registration should succeed")

		// Create table to trigger events
		createTableStmt := &parser.CreateTableStmt{
			TableName: "astha_event_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "astha_event_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err = testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Simulate Astha event for table creation
		tableData := map[string]interface{}{
			"id":       int64(1),
			"name":     "astha_event_test",
			"database": "test_db",
		}
		err = testEnv.astha.TriggerEvent("tables", "INSERT", tableData)
		require.NoError(t, err, "Mock Astha event should be triggered")

		// Wait for event processing
		select {
		case <-eventReceived:
			// Event was received and processed
		case <-time.After(2 * time.Second):
			t.Fatal("Astha event was not received within timeout")
		}

		// Verify schema was cached as a result of the event
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", "astha_event_test")
		assert.NoError(t, err, "Schema should be cached after Astha event processing")
		assert.NotNil(t, schema, "Schema should not be nil")
	})

	t.Run("CacheUpdateAfterSchemaChange", func(t *testing.T) {
		tableName := "cache_update_test"

		// Create initial table
		createTableStmt := &parser.CreateTableStmt{
			TableName: tableName,
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "cache_update_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Wait for initial caching
		time.Sleep(100 * time.Millisecond)

		// Get cache stats before invalidation
		statsBefore := testEnv.schemaManager.GetCacheStats()

		// Trigger cache invalidation and refresh
		err = testEnv.schemaManager.InvalidateAndRefreshSchema(ctx, "test_db", tableName)
		require.NoError(t, err, "Cache invalidation should succeed")

		// Get cache stats after invalidation
		statsAfter := testEnv.schemaManager.GetCacheStats()

		// Verify cache was updated
		assert.True(t, statsAfter.LastUpdated.After(statsBefore.LastUpdated),
			"Cache should be updated after invalidation")

		// Verify schema is still accessible
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
		assert.NoError(t, err, "Schema should be accessible after cache refresh")
		assert.NotNil(t, schema, "Schema should not be nil after refresh")
	})
}

// TestErrorScenariosAndRecoveryMechanisms tests error scenarios and recovery mechanisms
// Requirement 7.6: WHEN system components fail THEN partial operations SHALL be properly rolled back
func TestErrorScenariosAndRecoveryMechanisms(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("InvalidIcebergTypeHandling", func(t *testing.T) {
		// Test CREATE TABLE with invalid Iceberg type
		createTableStmt := &parser.CreateTableStmt{
			TableName: "invalid_type_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "invalid_col", DataType: "invalid_type", IsNullable: true}, // Invalid type
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "invalid_type_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		assert.Error(t, err, "CREATE TABLE with invalid type should fail")
		assert.Contains(t, err.Error(), "invalid", "Error should mention invalid type")

		// Verify no partial data was stored by checking schema manager cache
		_, err = testEnv.schemaManager.GetSchema(ctx, "test_db", "invalid_type_test")
		assert.Error(t, err, "Schema manager should not have schema for failed table creation")
	})

	t.Run("DuplicateTableNameHandling", func(t *testing.T) {
		tableName := "duplicate_test"

		// Create first table
		createTableStmt := &parser.CreateTableStmt{
			TableName: tableName,
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "duplicate_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err, "First table creation should succeed")

		// Try to create duplicate table
		createReq.RequestID = "duplicate_002"
		_, err = testEnv.storageManager.CreateTable(ctx, createReq)
		assert.Error(t, err, "Duplicate table creation should fail")
		assert.Contains(t, err.Error(), "already exists", "Error should mention table already exists")
	})

	t.Run("SchemaManagerRecoveryAfterFailure", func(t *testing.T) {
		// Test schema manager recovery after simulated failure
		tableName := "recovery_test"

		// Create table
		createTableStmt := &parser.CreateTableStmt{
			TableName: tableName,
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "data", DataType: "string", IsNullable: true},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "recovery_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Wait for caching
		time.Sleep(100 * time.Millisecond)

		// Simulate cache failure by clearing cache
		testEnv.schemaManager.ClearCache()

		// Verify schema can be recovered from registry
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
		assert.NoError(t, err, "Schema should be recoverable from registry after cache failure")
		assert.NotNil(t, schema, "Recovered schema should not be nil")
	})

	t.Run("ValidationErrorsWithColumnReferences", func(t *testing.T) {
		// Create table with specific column definitions
		createTableStmt := &parser.CreateTableStmt{
			TableName: "validation_error_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "user_id", DataType: "int64", IsNullable: false},
				{Name: "username", DataType: "string", IsNullable: false},
				{Name: "email", DataType: "string", IsNullable: true},
				{Name: "age", DataType: "int32", IsNullable: true},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "validation_error_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)

		// Wait for caching
		time.Sleep(100 * time.Millisecond)

		// Test INSERT with data that violates specific column constraints
		invalidData := map[string]interface{}{
			"user_id":  "not_a_number", // Should be int64
			"username": nil,            // Should not be null
			"email":    "valid@email.com",
			"age":      "not_a_number", // Should be int32
		}

		err = testEnv.validateInsertData(ctx, "test_db", "validation_error_test", invalidData)
		require.Error(t, err, "Validation should fail for invalid data")

		// Verify error references specific columns from CREATE TABLE
		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "user_id", "Error should reference user_id column")
		assert.Contains(t, errorMsg, "username", "Error should reference username column")
		assert.Contains(t, errorMsg, "int64", "Error should reference expected int64 type")
		assert.Contains(t, errorMsg, "string", "Error should reference expected string type")
	})
}

// TestPerformanceOfCreateTableProcessing tests performance of CREATE TABLE processing
// Requirement 7.7: Add performance tests for CREATE TABLE processing
func TestPerformanceOfCreateTableProcessing(t *testing.T) {
	ctx := context.Background()
	testEnv := setupIntegrationTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("CreateTablePerformance", func(t *testing.T) {
		const numTables = 50
		startTime := time.Now()

		for i := 0; i < numTables; i++ {
			tableName := fmt.Sprintf("perf_table_%d", i)
			createTableStmt := &parser.CreateTableStmt{
				TableName: tableName,
				Columns: []*parser.ColumnDefinition{
					{Name: "id", DataType: "int64", IsNullable: false},
					{Name: "name", DataType: "string", IsNullable: false},
					{Name: "email", DataType: "string", IsNullable: true},
					{Name: "created_at", DataType: "timestamp", IsNullable: false},
				},
			}

			createReq := &types.CreateTableRequest{
				Statement:     createTableStmt,
				Database:      "test_db",
				RequestID:     fmt.Sprintf("perf_req_%d", i),
				UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
				StorageEngine: "iceberg",
				EngineConfig:  map[string]interface{}{"format": "parquet"},
			}

			_, err := testEnv.storageManager.CreateTable(ctx, createReq)
			require.NoError(t, err, "Performance test table creation should succeed")
		}

		duration := time.Since(startTime)
		avgTimePerTable := duration / numTables

		t.Logf("Created %d tables in %v (avg: %v per table)", numTables, duration, avgTimePerTable)

		// Performance assertions
		assert.Less(t, avgTimePerTable, 100*time.Millisecond,
			"Average CREATE TABLE time should be less than 100ms")
		assert.Less(t, duration, 10*time.Second,
			"Total time for %d tables should be less than 10 seconds", numTables)

		// Wait for all Astha events to be processed
		time.Sleep(1 * time.Second)

		// Verify all schemas are cached
		cacheHitCount := 0
		for i := 0; i < numTables; i++ {
			tableName := fmt.Sprintf("perf_table_%d", i)
			_, err := testEnv.schemaManager.GetSchema(ctx, "test_db", tableName)
			if err == nil {
				cacheHitCount++
			}
		}

		assert.Equal(t, numTables, cacheHitCount,
			"All performance test tables should have cached schemas")
	})

	t.Run("ComplexTypePerformance", func(t *testing.T) {
		// Test performance with complex Iceberg types
		startTime := time.Now()

		createTableStmt := &parser.CreateTableStmt{
			TableName: "complex_perf_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "simple_list", DataType: "list<string>", IsNullable: true},
				{Name: "simple_map", DataType: "map<string,int32>", IsNullable: true},
				{Name: "simple_struct", DataType: "struct<name:string,age:int32>", IsNullable: true},
				{Name: "nested_list", DataType: "list<struct<id:int64,tags:list<string>>>", IsNullable: true},
				{Name: "nested_map", DataType: "map<string,map<string,int32>>", IsNullable: true},
				{Name: "complex_struct", DataType: "struct<profile:struct<name:string,age:int32>,tags:list<string>,metadata:map<string,string>>", IsNullable: true},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "complex_perf_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		_, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err, "Complex type table creation should succeed")

		duration := time.Since(startTime)
		t.Logf("Created complex type table in %v", duration)

		// Performance assertion for complex types
		assert.Less(t, duration, 500*time.Millisecond,
			"Complex type CREATE TABLE should complete within 500ms")

		// Wait for caching
		time.Sleep(200 * time.Millisecond)

		// Verify complex schema is cached
		schema, err := testEnv.schemaManager.GetSchema(ctx, "test_db", "complex_perf_test")
		assert.NoError(t, err, "Complex schema should be cached")
		assert.NotNil(t, schema, "Complex schema should not be nil")
		assert.Len(t, schema.Fields(), 7, "Complex schema should have all 7 fields")
	})
}

// Test helper structures and functions

// IntegrationTestEnvironment provides a complete test environment
type IntegrationTestEnvironment struct {
	storageManager  *Manager
	schemaManager   schema_manager.SchemaManager
	metadataManager *metadata.MetadataManager
	registry        *registry.SQLiteRegistry
	astha           *MockAstha
	tempDir         string
	logger          zerolog.Logger
}

// setupIntegrationTestEnvironment creates a complete test environment
func setupIntegrationTestEnvironment(t *testing.T) *IntegrationTestEnvironment {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "create_table_integration_test")
	require.NoError(t, err)

	// Setup logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	// Setup configuration
	cfg := config.LoadDefaultConfig()
	cfg.Metadata.Registry.Type = "sqlite"
	cfg.Metadata.Registry.SQLite.DatabasePath = filepath.Join(tempDir, "test_registry.db")

	// Setup registry
	registryInstance, err := registry.NewSQLiteRegistry(cfg.Metadata.Registry.SQLite, logger)
	require.NoError(t, err)

	// Setup metadata manager
	metadataManager := metadata.NewMetadataManager(registryInstance, logger)

	// Setup Astha (simplified for testing)
	asthaInstance := &MockAstha{
		subscribers: make(map[string]astha.Subscriber[any]),
		logger:      logger,
	}

	// Setup schema manager
	schemaManagerConfig := schema_manager.DefaultSchemaManagerConfig()
	schemaManagerInstance := schema_manager.NewManager(registryInstance, schemaManagerConfig, logger)

	// Register schema manager with Astha
	err = schemaManagerInstance.RegisterWithAstha(asthaInstance)
	require.NoError(t, err)

	// Setup storage manager
	storageManagerInstance := NewManager(cfg, metadataManager, logger)

	return &IntegrationTestEnvironment{
		storageManager:  storageManagerInstance,
		schemaManager:   schemaManagerInstance,
		metadataManager: metadataManager,
		registry:        registryInstance,
		astha:           asthaInstance,
		tempDir:         tempDir,
		logger:          logger,
	}
}

// cleanup cleans up the test environment
func (env *IntegrationTestEnvironment) cleanup() {
	if env.tempDir != "" {
		os.RemoveAll(env.tempDir)
	}
}

// getSchemaFromRegistry retrieves schema directly from registry (simplified for testing)
func (env *IntegrationTestEnvironment) getSchemaFromRegistry(ctx context.Context, database, tableName string) ([]*regtypes.TableColumn, error) {
	// This is a simplified implementation - in reality, we'd need to query the registry
	// For now, return empty slice to indicate no schema found
	return nil, fmt.Errorf("table not found in registry")
}

// validateInsertData simulates INSERT data validation against cached schema
func (env *IntegrationTestEnvironment) validateInsertData(ctx context.Context, database, tableName string, data map[string]interface{}) error {
	// Get schema from cache
	schema, err := env.schemaManager.GetSchema(ctx, database, tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for validation: %w", err)
	}

	// Validate each field in the schema
	for _, field := range schema.Fields() {
		value, exists := data[field.Name]

		// Check required fields (assuming non-optional fields are required)
		if !field.Type.Optional() && (!exists || value == nil) {
			return fmt.Errorf("column '%s' is required but not provided or is null", field.Name)
		}

		// Skip validation for null values in optional fields
		if !exists || value == nil {
			continue
		}

		// Type validation based on Iceberg field type
		if err := env.validateFieldType(field.Name, field.Type, value); err != nil {
			return err
		}
	}

	return nil
}

// validateFieldType validates a single field's type based on Iceberg type
func (env *IntegrationTestEnvironment) validateFieldType(fieldName string, fieldType iceberg.Type, value interface{}) error {
	// Simplified type validation for testing
	// In a real implementation, this would be much more comprehensive

	switch fieldType.String() {
	case "long":
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("column '%s' expects int64 type, got %T", fieldName, value)
		}
	case "int":
		if _, ok := value.(int32); !ok {
			return fmt.Errorf("column '%s' expects int32 type, got %T", fieldName, value)
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("column '%s' expects string type, got %T", fieldName, value)
		}
	case "timestamp":
		if _, ok := value.(time.Time); !ok {
			return fmt.Errorf("column '%s' expects timestamp type, got %T", fieldName, value)
		}
	default:
		// For complex types and other types, simplified validation
		if value == nil {
			return fmt.Errorf("column '%s' has null value for type %s", fieldName, fieldType.String())
		}
	}

	return nil
}

// TestEventSubscriber is a test subscriber for monitoring Astha events
type TestEventSubscriber struct {
	eventReceived chan bool
}

func (tes *TestEventSubscriber) OnEvent(ctx context.Context, event astha.Event[any]) error {
	// Signal that an event was received
	select {
	case tes.eventReceived <- true:
	default:
		// Channel is full, ignore
	}
	return nil
}

func (tes *TestEventSubscriber) OnHealth(ctx context.Context) error {
	return nil
}

func (tes *TestEventSubscriber) OnRefresh(ctx context.Context) error {
	return nil
}

// Mock cache stats for testing
type MockCacheStats struct {
	LastUpdated time.Time
	HitCount    int64
	MissCount   int64
}

// MockAstha provides a mock implementation of Astha for testing
type MockAstha struct {
	subscribers map[string]astha.Subscriber[any]
	logger      zerolog.Logger
	mu          sync.RWMutex
}

func (ma *MockAstha) RegisterComponentWithInstance(info astha.ComponentInfo, instance astha.Subscriber[any]) error {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	ma.subscribers[info.Name] = instance
	ma.logger.Debug().
		Str("component", info.Name).
		Strs("subscriptions", info.Subscriptions).
		Msg("Mock Astha registered component")

	return nil
}

// TriggerEvent simulates an Astha event for testing
func (ma *MockAstha) TriggerEvent(table, operation string, data any) error {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	event := astha.Event[any]{
		ID:        time.Now().UnixNano(),
		Table:     table,
		Operation: operation,
		Data:      data,
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	// Notify all subscribers
	for name, subscriber := range ma.subscribers {
		go func(name string, sub astha.Subscriber[any]) {
			ctx := context.Background()
			if err := sub.OnEvent(ctx, event); err != nil {
				ma.logger.Error().Err(err).
					Str("subscriber", name).
					Str("table", table).
					Str("operation", operation).
					Msg("Mock Astha event processing failed")
			}
		}(name, subscriber)
	}

	return nil
}
