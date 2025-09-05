package schema

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/astha"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockSchemaManagerForLifecycle for testing enhanced lifecycle methods
type MockSchemaManagerForLifecycle struct {
	mock.Mock
}

func (m *MockSchemaManagerForLifecycle) GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	args := m.Called(ctx, database, tableName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*iceberg.Schema), args.Error(1)
}

func (m *MockSchemaManagerForLifecycle) InvalidateSchema(database, tableName string) {
	m.Called(database, tableName)
}

func (m *MockSchemaManagerForLifecycle) ClearCache() {
	m.Called()
}

func (m *MockSchemaManagerForLifecycle) GetCacheStats() CacheStats {
	args := m.Called()
	return args.Get(0).(CacheStats)
}

func (m *MockSchemaManagerForLifecycle) CacheNewTableSchema(ctx context.Context, database, tableName string, tableID int64) error {
	args := m.Called(ctx, database, tableName, tableID)
	return args.Error(0)
}

func (m *MockSchemaManagerForLifecycle) InvalidateAndRefreshSchema(ctx context.Context, database, tableName string) error {
	args := m.Called(ctx, database, tableName)
	return args.Error(0)
}

func (m *MockSchemaManagerForLifecycle) CleanupDeletedTable(database, tableName string) {
	m.Called(database, tableName)
}

func (m *MockSchemaManagerForLifecycle) GetCacheMetrics() *CacheMetrics {
	args := m.Called()
	return args.Get(0).(*CacheMetrics)
}

func (m *MockSchemaManagerForLifecycle) Shutdown() {
	m.Called()
}

func TestAsthaSubscriberLifecycle(t *testing.T) {
	logger := zerolog.Nop()
	mockManager := &MockSchemaManagerForLifecycle{}
	subscriber := NewSchemaManagerSubscriber(mockManager, logger)

	ctx := context.Background()

	t.Run("HandleTableCreatedWithEnhancedCaching", func(t *testing.T) {
		// Test table creation event with table ID
		tableData := map[string]interface{}{
			"id":          int64(123),
			"name":        "new_table",
			"database":    "test_db",
			"database_id": int64(1),
		}

		event := astha.Event[any]{
			ID:        1,
			Table:     "tables",
			Operation: "INSERT",
			Data:      tableData,
			Timestamp: time.Now(),
		}

		// Mock the enhanced caching method
		mockManager.On("CacheNewTableSchema", ctx, "test_db", "new_table", int64(123)).Return(nil).Once()

		err := subscriber.OnEvent(ctx, event)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})

	t.Run("HandleTableCreatedFromRegtypes", func(t *testing.T) {
		// Test table creation event with regtypes.Table
		table := regtypes.Table{
			ID:         456,
			Name:       "another_table",
			DatabaseID: 2,
		}

		event := astha.Event[any]{
			ID:        2,
			Table:     "tables",
			Operation: "INSERT",
			Data:      table,
			Timestamp: time.Now(),
		}

		// Mock the enhanced caching method
		mockManager.On("CacheNewTableSchema", ctx, "db_2", "another_table", int64(456)).Return(nil).Once()

		err := subscriber.OnEvent(ctx, event)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})

	t.Run("HandleSchemaChangeWithEnhancedRefresh", func(t *testing.T) {
		// Test column update event
		columnData := map[string]interface{}{
			"table_id": int64(123),
			"name":     "updated_column",
		}

		event := astha.Event[any]{
			ID:        3,
			Table:     "table_columns",
			Operation: "UPDATE",
			Data:      columnData,
			Timestamp: time.Now(),
		}

		// Mock the enhanced refresh method (getTableInfo returns placeholder values)
		mockManager.On("InvalidateAndRefreshSchema", ctx, "db_123", "table_123").Return(nil).Once()

		err := subscriber.OnEvent(ctx, event)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})

	t.Run("HandleTableDeletionWithCleanup", func(t *testing.T) {
		// Test table deletion event
		tableData := map[string]interface{}{
			"name":     "deleted_table",
			"database": "test_db",
		}

		event := astha.Event[any]{
			ID:        4,
			Table:     "tables",
			Operation: "DELETE",
			Data:      tableData,
			Timestamp: time.Now(),
		}

		// Mock the cleanup method
		mockManager.On("CleanupDeletedTable", "test_db", "deleted_table").Once()

		err := subscriber.OnEvent(ctx, event)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})

	t.Run("RetryLogicWithExponentialBackoff", func(t *testing.T) {
		// Test retry logic with failures
		tableData := map[string]interface{}{
			"id":       int64(789),
			"name":     "retry_table",
			"database": "test_db",
		}

		event := astha.Event[any]{
			ID:        5,
			Table:     "tables",
			Operation: "INSERT",
			Data:      tableData,
			Timestamp: time.Now(),
		}

		// Mock failures followed by success
		mockManager.On("CacheNewTableSchema", ctx, "test_db", "retry_table", int64(789)).
			Return(assert.AnError).Twice() // Fail twice
		mockManager.On("CacheNewTableSchema", ctx, "test_db", "retry_table", int64(789)).
			Return(nil).Once() // Succeed on third try

		start := time.Now()
		err := subscriber.OnEvent(ctx, event)
		duration := time.Since(start)

		require.NoError(t, err)
		// Should take at least the initial delay + backoff delay
		assert.Greater(t, duration, 100*time.Millisecond, "Should have retry delays")

		mockManager.AssertExpectations(t)
	})

	t.Run("HealthCheckWithFailureTracking", func(t *testing.T) {
		// Test health check with failure tracking
		mockManager.On("GetCacheStats").Return(CacheStats{
			LastUpdated: time.Now(),
		}).Once()

		// Should be healthy initially
		err := subscriber.OnHealth(ctx)
		require.NoError(t, err)
		assert.True(t, subscriber.IsHealthy())

		// Simulate failures
		for i := 0; i < 6; i++ { // Exceed max failures (5)
			subscriber.healthChecker.failureCount++
		}

		// Should be unhealthy now
		err = subscriber.OnHealth(ctx)
		require.Error(t, err)
		assert.False(t, subscriber.IsHealthy())

		mockManager.AssertExpectations(t)
	})

	t.Run("ComponentInfoWithCorrectSubscriptions", func(t *testing.T) {
		info := subscriber.GetComponentInfo()

		assert.Equal(t, "schema_manager", info.Name)
		assert.Equal(t, "1.0.0", info.Version)
		assert.Contains(t, info.Subscriptions, "tables")
		assert.Contains(t, info.Subscriptions, "table_columns")
		assert.Contains(t, info.Subscriptions, "table_metadata")
	})

	t.Run("RefreshClearsCache", func(t *testing.T) {
		mockManager.On("ClearCache").Once()

		err := subscriber.OnRefresh(ctx)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})
}

func TestRetryConfiguration(t *testing.T) {
	logger := zerolog.Nop()
	mockManager := &MockSchemaManagerForLifecycle{}
	subscriber := NewSchemaManagerSubscriber(mockManager, logger)

	t.Run("RetryConfigurationDefaults", func(t *testing.T) {
		assert.Equal(t, 3, subscriber.retryConfig.MaxRetries)
		assert.Equal(t, 100*time.Millisecond, subscriber.retryConfig.InitialDelay)
		assert.Equal(t, 5*time.Second, subscriber.retryConfig.MaxDelay)
		assert.Equal(t, 2.0, subscriber.retryConfig.BackoffFactor)
	})

	t.Run("HealthCheckerDefaults", func(t *testing.T) {
		assert.True(t, subscriber.healthChecker.isHealthy)
		assert.Equal(t, 5, subscriber.healthChecker.maxFailures)
		assert.Equal(t, 0, subscriber.healthChecker.failureCount)
	})
}

func TestEventDataExtraction(t *testing.T) {
	logger := zerolog.Nop()
	mockManager := &MockSchemaManagerForLifecycle{}
	subscriber := NewSchemaManagerSubscriber(mockManager, logger)

	ctx := context.Background()

	t.Run("HandleInvalidEventDataFormat", func(t *testing.T) {
		// Test with invalid data format
		event := astha.Event[any]{
			ID:        1,
			Table:     "tables",
			Operation: "INSERT",
			Data:      "invalid_data_format", // String instead of map or struct
			Timestamp: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid event data format")
	})

	t.Run("HandleMissingRequiredFields", func(t *testing.T) {
		// Test with missing required fields
		tableData := map[string]interface{}{
			"id":       int64(123),
			"database": "test_db",
			// Missing "name" field
		}

		event := astha.Event[any]{
			ID:        2,
			Table:     "tables",
			Operation: "INSERT",
			Data:      tableData,
			Timestamp: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid table name field")
	})

	t.Run("HandleDatabaseIDFallback", func(t *testing.T) {
		// Test fallback to database_id when database name is not available
		tableData := map[string]interface{}{
			"id":          int64(123),
			"name":        "test_table",
			"database_id": int64(42),
			// No "database" field
		}

		event := astha.Event[any]{
			ID:        3,
			Table:     "tables",
			Operation: "INSERT",
			Data:      tableData,
			Timestamp: time.Now(),
		}

		// Mock the enhanced caching method with generated database name
		mockManager.On("CacheNewTableSchema", ctx, "db_42", "test_table", int64(123)).Return(nil).Once()

		err := subscriber.OnEvent(ctx, event)
		require.NoError(t, err)

		mockManager.AssertExpectations(t)
	})
}
