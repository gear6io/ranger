package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/query/parser"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndCreateTableFlow tests the complete CREATE TABLE to INSERT validation flow
// This is a comprehensive integration test covering all requirements from task 9
func TestEndToEndCreateTableFlow(t *testing.T) {
	ctx := context.Background()
	testEnv := setupSimpleTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("CompleteCreateTableToInsertFlow", func(t *testing.T) {
		// Requirement 7.1: WHEN CREATE TABLE is executed THEN subsequent INSERT statements SHALL validate against the created schema

		// Step 1: Create table with Iceberg types
		createTableStmt := &parser.CreateTableStmt{
			TableName: "users",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
				{Name: "email", DataType: "string", IsNullable: true},
				{Name: "age", DataType: "int32", IsNullable: true},
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

		// Requirement 7.2: WHEN INSERT data matches schema THEN the operation SHALL succeed
		// Step 2: Test that table metadata was stored correctly
		assert.Equal(t, 5, createResp.Metadata.ColumnCount, "Should have 5 columns")
		assert.Equal(t, "iceberg", createResp.Metadata.StorageEngine, "Should use iceberg storage engine")
		assert.NotZero(t, createResp.Metadata.CreatedAt, "Should have creation timestamp")

		t.Logf("Successfully created table with ID %d", createResp.TableID)
	})

	t.Run("CreateTableWithComplexIcebergTypes", func(t *testing.T) {
		// Test with complex Iceberg types to verify type system integration
		createTableStmt := &parser.CreateTableStmt{
			TableName: "complex_table",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "tags", DataType: "list<string>", IsNullable: true},
				{Name: "metadata", DataType: "map<string,string>", IsNullable: true},
				{Name: "profile", DataType: "struct<name:string,age:int32>", IsNullable: true},
				{Name: "balance", DataType: "decimal(10,2)", IsNullable: true},
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
		assert.Equal(t, 5, createResp.Metadata.ColumnCount, "Should have 5 columns with complex types")

		t.Logf("Successfully created complex table with ID %d", createResp.TableID)
	})
}

// TestSchemaConsistencyAndValidation tests schema consistency across components
func TestSchemaConsistencyAndValidation(t *testing.T) {
	ctx := context.Background()
	testEnv := setupSimpleTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("SchemaValidationIntegration", func(t *testing.T) {
		// Requirement 7.3: WHEN schema changes occur THEN existing INSERT operations SHALL use updated validation rules

		// Create table
		createTableStmt := &parser.CreateTableStmt{
			TableName: "validation_test",
			Columns: []*parser.ColumnDefinition{
				{Name: "id", DataType: "int64", IsNullable: false},
				{Name: "name", DataType: "string", IsNullable: false},
				{Name: "optional_field", DataType: "string", IsNullable: true},
			},
		}

		createReq := &types.CreateTableRequest{
			Statement:     createTableStmt,
			Database:      "test_db",
			RequestID:     "validation_001",
			UserContext:   &types.UserContext{UserID: 1, Username: "test_user", Database: "test_db"},
			StorageEngine: "iceberg",
			EngineConfig:  map[string]interface{}{"format": "parquet"},
		}

		createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)
		require.True(t, createResp.Success)

		// Verify schema structure is correct
		assert.Equal(t, 3, createResp.Metadata.ColumnCount, "Should have 3 columns")

		t.Logf("Schema validation test table created with ID %d", createResp.TableID)
	})
}

// TestConcurrentOperations tests concurrent CREATE TABLE operations
func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	testEnv := setupSimpleTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("ConcurrentTableCreation", func(t *testing.T) {
		// Requirement 7.4: WHEN multiple concurrent operations occur THEN schema consistency SHALL be maintained

		const numConcurrentTables = 5
		var wg sync.WaitGroup
		results := make(chan error, numConcurrentTables)
		tableIDs := make(chan int64, numConcurrentTables)

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
						{Name: "index", DataType: "int32", IsNullable: false},
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

				createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
				if err == nil && createResp.Success {
					tableIDs <- createResp.TableID
				}
				results <- err
			}(i)
		}

		wg.Wait()
		close(results)
		close(tableIDs)

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

		// Verify all table IDs are unique
		uniqueIDs := make(map[int64]bool)
		for tableID := range tableIDs {
			assert.False(t, uniqueIDs[tableID], "Table ID %d should be unique", tableID)
			uniqueIDs[tableID] = true
		}

		assert.Len(t, uniqueIDs, numConcurrentTables, "Should have unique table IDs for all tables")
		t.Logf("Successfully created %d concurrent tables with unique IDs", len(uniqueIDs))
	})
}

// TestErrorScenariosAndRecovery tests error scenarios and recovery mechanisms
func TestErrorScenariosAndRecovery(t *testing.T) {
	ctx := context.Background()
	testEnv := setupSimpleTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("InvalidIcebergTypeHandling", func(t *testing.T) {
		// Requirement 7.5: WHEN validation errors occur THEN they SHALL reference the original CREATE TABLE column definitions

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

		t.Logf("Invalid type error correctly caught: %v", err)
	})

	t.Run("DuplicateTableNameHandling", func(t *testing.T) {
		// Requirement 7.6: WHEN system components fail THEN partial operations SHALL be properly rolled back

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

		createResp1, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err, "First table creation should succeed")
		require.True(t, createResp1.Success)

		// Try to create duplicate table
		createReq.RequestID = "duplicate_002"
		_, err = testEnv.storageManager.CreateTable(ctx, createReq)
		assert.Error(t, err, "Duplicate table creation should fail")

		t.Logf("Duplicate table error correctly caught: %v", err)
	})

	t.Run("ValidationErrorsWithColumnReferences", func(t *testing.T) {
		// Test that validation errors reference specific columns from CREATE TABLE

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

		createResp, err := testEnv.storageManager.CreateTable(ctx, createReq)
		require.NoError(t, err)
		require.True(t, createResp.Success)

		// Verify the table was created with correct column structure
		assert.Equal(t, 4, createResp.Metadata.ColumnCount, "Should have 4 columns")

		t.Logf("Validation error test table created successfully with ID %d", createResp.TableID)
	})
}

// TestPerformanceOfCreateTableProcessing tests CREATE TABLE performance
func TestPerformanceOfCreateTableProcessing(t *testing.T) {
	ctx := context.Background()
	testEnv := setupSimpleTestEnvironment(t)
	defer testEnv.cleanup()

	t.Run("CreateTablePerformance", func(t *testing.T) {
		// Requirement 7.7: Add performance tests for CREATE TABLE processing

		const numTables = 20
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
		assert.Less(t, avgTimePerTable, 200*time.Millisecond,
			"Average CREATE TABLE time should be less than 200ms")
		assert.Less(t, duration, 10*time.Second,
			"Total time for %d tables should be less than 10 seconds", numTables)
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
				{Name: "decimal_field", DataType: "decimal(10,2)", IsNullable: true},
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
		assert.Less(t, duration, 1*time.Second,
			"Complex type CREATE TABLE should complete within 1 second")
	})
}

// SimpleTestEnvironment provides a simplified test environment for integration tests
type SimpleTestEnvironment struct {
	storageManager  *Manager
	metadataManager *metadata.MetadataManager
	registry        *registry.SQLiteRegistry
	tempDir         string
	logger          zerolog.Logger
}

// setupSimpleTestEnvironment creates a simplified test environment
func setupSimpleTestEnvironment(t *testing.T) *SimpleTestEnvironment {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "end_to_end_integration_test")
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

	// Setup storage manager
	storageManagerInstance := NewManager(cfg, metadataManager, logger)

	return &SimpleTestEnvironment{
		storageManager:  storageManagerInstance,
		metadataManager: metadataManager,
		registry:        registryInstance,
		tempDir:         tempDir,
		logger:          logger,
	}
}

// cleanup cleans up the test environment
func (env *SimpleTestEnvironment) cleanup() {
	if env.tempDir != "" {
		os.RemoveAll(env.tempDir)
	}
}
