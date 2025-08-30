package schema_manager

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

// MockSchemaManager implements SchemaManager interface for testing
type MockSchemaManager struct {
	mock.Mock
}

func (m *MockSchemaManager) GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	args := m.Called(ctx, database, tableName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*iceberg.Schema), args.Error(1)
}

func (m *MockSchemaManager) InvalidateSchema(database, tableName string) {
	m.Called(database, tableName)
}

func (m *MockSchemaManager) InvalidateCache(database, tableName string) {
	m.Called(database, tableName)
}

func (m *MockSchemaManager) ClearCache() {
	m.Called()
}

func (m *MockSchemaManager) GetCacheStats() CacheStats {
	args := m.Called()
	return args.Get(0).(CacheStats)
}

func (m *MockSchemaManager) Shutdown() {
	m.Called()
}

// Enhanced lifecycle management methods
func (m *MockSchemaManager) CacheNewTableSchema(ctx context.Context, database, tableName string, tableID int64) error {
	args := m.Called(ctx, database, tableName, tableID)
	return args.Error(0)
}

func (m *MockSchemaManager) InvalidateAndRefreshSchema(ctx context.Context, database, tableName string) error {
	args := m.Called(ctx, database, tableName)
	return args.Error(0)
}

func (m *MockSchemaManager) CleanupDeletedTable(database, tableName string) {
	m.Called(database, tableName)
}

func (m *MockSchemaManager) GetCacheMetrics() *CacheMetrics {
	args := m.Called()
	return args.Get(0).(*CacheMetrics)
}

// Helper function to create a test schema
func createTestSchema() *iceberg.Schema {
	fields := []iceberg.NestedField{
		iceberg.NestedField{
			ID:       1,
			Name:     "id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "name",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
	}
	return iceberg.NewSchema(0, fields...)
}

func TestNewSchemaManagerSubscriber(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))

	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	assert.NotNil(t, subscriber)
	assert.Equal(t, mockSchemaManager, subscriber.schemaManager)
	assert.NotNil(t, subscriber.retryConfig)
	assert.NotNil(t, subscriber.healthChecker)
	assert.True(t, subscriber.healthChecker.isHealthy)
	assert.Equal(t, 5, subscriber.healthChecker.maxFailures)
}

func TestOnEvent_TableCreated(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Test with map[string]interface{} data
	t.Run("MapData", func(t *testing.T) {
		mockSchemaManager.On("CacheNewTableSchema", ctx, "test_db", "test_table", int64(456)).Return(nil).Once()

		event := astha.Event[any]{
			ID:        1,
			Table:     "tables",
			Operation: "INSERT",
			Data: map[string]interface{}{
				"id":       int64(456),
				"database": "test_db",
				"name":     "test_table",
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})

	// Test with regtypes.Table data
	t.Run("TableData", func(t *testing.T) {
		mockSchemaManager.On("CacheNewTableSchema", ctx, "db_123", "users", int64(123)).Return(nil).Once()

		table := regtypes.Table{
			ID:         123,
			DatabaseID: 123,
			Name:       "users",
		}

		event := astha.Event[any]{
			ID:        2,
			Table:     "tables",
			Operation: "INSERT",
			Data:      table,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})
}

func TestOnEvent_ColumnAdded(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Test with map[string]interface{} data
	t.Run("MapData", func(t *testing.T) {
		mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_456", "table_456").Return(nil).Once()

		event := astha.Event[any]{
			ID:        3,
			Table:     "table_columns",
			Operation: "INSERT",
			Data: map[string]interface{}{
				"table_id": int64(456),
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})

	// Test with regtypes.TableColumn data
	t.Run("ColumnData", func(t *testing.T) {
		mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_789", "table_789").Return(nil).Once()

		column := regtypes.TableColumn{
			ID:         1,
			TableID:    789,
			ColumnName: "email",
			DataType:   "string",
		}

		event := astha.Event[any]{
			ID:        4,
			Table:     "table_columns",
			Operation: "INSERT",
			Data:      column,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})
}

func TestOnEvent_SchemaChanged(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_101", "table_101").Return(nil).Once()

	event := astha.Event[any]{
		ID:        5,
		Table:     "table_columns",
		Operation: "UPDATE",
		Data: map[string]interface{}{
			"table_id": int64(101),
		},
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	err := subscriber.OnEvent(ctx, event)
	assert.NoError(t, err)
	mockSchemaManager.AssertExpectations(t)
}

func TestOnEvent_MetadataChanged(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_202", "table_202").Return(nil).Once()

	event := astha.Event[any]{
		ID:        6,
		Table:     "table_metadata",
		Operation: "UPDATE",
		Data: map[string]interface{}{
			"table_id": int64(202),
		},
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	err := subscriber.OnEvent(ctx, event)
	assert.NoError(t, err)
	mockSchemaManager.AssertExpectations(t)
}

func TestOnEvent_TableDeleted(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Test with map[string]interface{} data
	t.Run("MapData", func(t *testing.T) {
		mockSchemaManager.On("CleanupDeletedTable", "test_db", "deleted_table").Once()

		event := astha.Event[any]{
			ID:        7,
			Table:     "tables",
			Operation: "DELETE",
			Data: map[string]interface{}{
				"database": "test_db",
				"name":     "deleted_table",
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})

	// Test with regtypes.Table data
	t.Run("TableData", func(t *testing.T) {
		mockSchemaManager.On("CleanupDeletedTable", "db_303", "deleted_table").Once()

		table := regtypes.Table{
			ID:         303,
			DatabaseID: 303,
			Name:       "deleted_table",
		}

		event := astha.Event[any]{
			ID:        8,
			Table:     "tables",
			Operation: "DELETE",
			Data:      table,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, event)
		assert.NoError(t, err)
		mockSchemaManager.AssertExpectations(t)
	})
}

func TestOnEvent_ColumnDeleted(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_404", "table_404").Return(nil).Once()

	event := astha.Event[any]{
		ID:        9,
		Table:     "table_columns",
		Operation: "DELETE",
		Data: map[string]interface{}{
			"table_id": int64(404),
		},
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	err := subscriber.OnEvent(ctx, event)
	assert.NoError(t, err)
	mockSchemaManager.AssertExpectations(t)
}

func TestOnEvent_IrrelevantEvent(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	event := astha.Event[any]{
		ID:        10,
		Table:     "irrelevant_table",
		Operation: "INSERT",
		Data:      map[string]interface{}{},
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	err := subscriber.OnEvent(ctx, event)
	assert.NoError(t, err)
	// No expectations should be called on mockSchemaManager
	mockSchemaManager.AssertExpectations(t)
}

func TestOnHealth_Healthy(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("GetCacheStats").Return(CacheStats{
		LastUpdated: time.Now(),
	})

	err := subscriber.OnHealth(ctx)
	assert.NoError(t, err)
	assert.True(t, subscriber.IsHealthy())
	mockSchemaManager.AssertExpectations(t)
}

func TestOnHealth_TooManyFailures(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Simulate too many failures
	subscriber.healthChecker.failureCount = 10 // Exceeds maxFailures (5)

	err := subscriber.OnHealth(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeded maximum failure count")
	assert.False(t, subscriber.IsHealthy())
}

func TestOnHealth_StaleHealthCheck(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Since OnHealth updates lastHealthCheck at the beginning, we need to test the IsHealthy method
	// which checks for stale health checks
	subscriber.healthChecker.lastHealthCheck = time.Now().Add(-10 * time.Minute)

	// Test IsHealthy method directly for stale check
	assert.False(t, subscriber.IsHealthy())

	// Now test OnHealth - it should succeed since it updates the timestamp
	mockSchemaManager.On("GetCacheStats").Return(CacheStats{
		LastUpdated: time.Now(),
	})

	err := subscriber.OnHealth(ctx)
	assert.NoError(t, err)
	assert.True(t, subscriber.IsHealthy())
	mockSchemaManager.AssertExpectations(t)
}

func TestOnHealth_UnresponsiveSchemaManager(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("GetCacheStats").Return(CacheStats{
		LastUpdated: time.Time{}, // Zero time indicates unresponsive
	})

	err := subscriber.OnHealth(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cache stats are not available")
	assert.False(t, subscriber.IsHealthy())
	mockSchemaManager.AssertExpectations(t)
}

func TestOnRefresh(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("ClearCache").Once()

	err := subscriber.OnRefresh(ctx)
	assert.NoError(t, err)
	mockSchemaManager.AssertExpectations(t)
}

func TestRetryOperation_Success(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()
	callCount := 0

	operation := func() error {
		callCount++
		return nil // Success on first try
	}

	err := subscriber.retryOperation(ctx, operation)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestRetryOperation_SuccessAfterRetries(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()
	callCount := 0

	operation := func() error {
		callCount++
		if callCount < 3 {
			return assert.AnError // Fail first 2 times
		}
		return nil // Success on 3rd try
	}

	err := subscriber.retryOperation(ctx, operation)
	assert.NoError(t, err)
	assert.Equal(t, 3, callCount)
}

func TestRetryOperation_MaxRetriesExceeded(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()
	callCount := 0

	operation := func() error {
		callCount++
		return assert.AnError // Always fail
	}

	err := subscriber.retryOperation(ctx, operation)
	assert.Error(t, err)
	assert.Equal(t, subscriber.retryConfig.MaxRetries+1, callCount) // MaxRetries + initial attempt
}

func TestRetryOperation_ContextCanceled(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	operation := func() error {
		callCount++
		if callCount == 1 {
			cancel() // Cancel context after first failure
		}
		return assert.AnError // Always fail
	}

	err := subscriber.retryOperation(ctx, operation)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, 1, callCount)
}

func TestGetComponentInfo(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	// Set up the subscriber to be healthy by setting a recent health check
	subscriber.healthChecker.lastHealthCheck = time.Now()
	subscriber.healthChecker.isHealthy = true

	info := subscriber.GetComponentInfo()

	assert.Equal(t, "schema_manager", info.Name)
	assert.Equal(t, "1.0.0", info.Version)
	assert.Equal(t, "active", info.Status)
	assert.Contains(t, info.Subscriptions, "tables")
	assert.Contains(t, info.Subscriptions, "table_columns")
	assert.Contains(t, info.Subscriptions, "table_metadata")
	assert.True(t, time.Since(info.LastSeen) < time.Second)
}

func TestGetComponentInfo_ErrorStatus(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	// Make subscriber unhealthy
	subscriber.healthChecker.failureCount = 10
	subscriber.healthChecker.isHealthy = false

	info := subscriber.GetComponentInfo()

	assert.Equal(t, "schema_manager", info.Name)
	assert.Equal(t, "error", info.Status)
}

func TestProactivelyCacheSchema_Success(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()
	testSchema := createTestSchema()

	mockSchemaManager.On("GetSchema", ctx, "test_db", "test_table").Return(testSchema, nil).Once()

	err := subscriber.proactivelyCacheSchema(ctx, "test_db", "test_table", "test_reason")
	assert.NoError(t, err)
	assert.Equal(t, 0, subscriber.healthChecker.failureCount)
	assert.True(t, subscriber.healthChecker.isHealthy)
	mockSchemaManager.AssertExpectations(t)
}

func TestProactivelyCacheSchema_Failure(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Mock failure for all retry attempts
	mockSchemaManager.On("GetSchema", ctx, "test_db", "test_table").Return(nil, assert.AnError).Times(4) // Initial + 3 retries

	err := subscriber.proactivelyCacheSchema(ctx, "test_db", "test_table", "test_reason")
	assert.NoError(t, err) // Should not return error to avoid blocking other subscribers
	assert.Equal(t, 1, subscriber.healthChecker.failureCount)
	mockSchemaManager.AssertExpectations(t)
}

func TestInvalidateAndRefreshCache_Success(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "test_db", "test_table").Return(nil).Once()

	err := subscriber.invalidateAndRefreshCache(ctx, "test_db", "test_table", "test_reason")
	assert.NoError(t, err)
	assert.Equal(t, 0, subscriber.healthChecker.failureCount)
	assert.True(t, subscriber.healthChecker.isHealthy)
	mockSchemaManager.AssertExpectations(t)
}

func TestInvalidateAndRefreshCache_Failure(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()

	// Mock failure for all retry attempts
	mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "test_db", "test_table").Return(assert.AnError).Times(4) // Initial + 3 retries

	err := subscriber.invalidateAndRefreshCache(ctx, "test_db", "test_table", "test_reason")
	assert.NoError(t, err) // Should not return error to avoid blocking other subscribers
	assert.Equal(t, 1, subscriber.healthChecker.failureCount)
	mockSchemaManager.AssertExpectations(t)
}

// Integration test for the complete event handling flow
func TestIntegrationEventHandling(t *testing.T) {
	mockSchemaManager := &MockSchemaManager{}
	logger := zerolog.New(zerolog.NewTestWriter(t))
	subscriber := NewSchemaManagerSubscriber(mockSchemaManager, logger)

	ctx := context.Background()
	testSchema := createTestSchema()

	// Test complete flow: table created -> column added -> table deleted
	t.Run("CompleteFlow", func(t *testing.T) {
		// 1. Table created
		mockSchemaManager.On("GetSchema", ctx, "test_db", "users").Return(testSchema, nil).Once()

		createEvent := astha.Event[any]{
			ID:        1,
			Table:     "tables",
			Operation: "INSERT",
			Data: map[string]interface{}{
				"database": "test_db",
				"name":     "users",
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err := subscriber.OnEvent(ctx, createEvent)
		require.NoError(t, err)

		// 2. Column added
		mockSchemaManager.On("InvalidateAndRefreshSchema", ctx, "db_123", "table_123").Return(nil).Once()

		columnEvent := astha.Event[any]{
			ID:        2,
			Table:     "table_columns",
			Operation: "INSERT",
			Data: map[string]interface{}{
				"table_id": int64(123),
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err = subscriber.OnEvent(ctx, columnEvent)
		require.NoError(t, err)

		// 3. Table deleted
		mockSchemaManager.On("CleanupDeletedTable", "test_db", "users").Once()

		deleteEvent := astha.Event[any]{
			ID:        3,
			Table:     "tables",
			Operation: "DELETE",
			Data: map[string]interface{}{
				"database": "test_db",
				"name":     "users",
			},
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		err = subscriber.OnEvent(ctx, deleteEvent)
		require.NoError(t, err)

		mockSchemaManager.AssertExpectations(t)
	})
}
