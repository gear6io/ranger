package astha

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsthaCreation(t *testing.T) {
	// Create a mock database connection
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create Astha configuration
	cfg := &Config{
		Database:     db,
		Logger:       logger,
		BatchSize:    256,
		PollInterval: 100,
	}

	// Create Astha instance
	astha, err := NewAstha(cfg)
	require.NoError(t, err)
	assert.NotNil(t, astha)

	// Verify configuration
	assert.Equal(t, "__cdc_log", astha.GetCDCLogTable())
	assert.Equal(t, []string{"tables", "table_files", "table_statistics", "table_metadata"}, astha.GetMonitoredTables())
}

func TestComponentRegistration(t *testing.T) {
	// Create a mock database connection
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create Astha configuration
	cfg := &Config{
		Database:     db,
		Logger:       logger,
		BatchSize:    256,
		PollInterval: 100,
	}

	// Create Astha instance
	astha, err := NewAstha(cfg)
	require.NoError(t, err)

	// Create example component
	component := NewExampleComponent("test_component", "1.0.0", logger)

	// Register component
	err = astha.RegisterComponent(component.GetComponentInfo())
	require.NoError(t, err)

	// Verify component is registered
	info, exists := astha.GetComponentInfo("test_component")
	assert.True(t, exists)
	assert.Equal(t, "test_component", info.Name)
	assert.Equal(t, "1.0.0", info.Version)
	assert.Equal(t, "active", info.Status)

	// Verify table subscriptions
	subscribers := astha.GetTableSubscribers("tables")
	assert.Contains(t, subscribers, "test_component")

	// Unregister component
	err = astha.UnregisterComponent("test_component")
	require.NoError(t, err)

	// Verify component is unregistered
	_, exists = astha.GetComponentInfo("test_component")
	assert.False(t, exists)
}

func TestEventStore(t *testing.T) {
	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create event store
	eventStore := NewMemoryEventStore(logger)

	// Create test event
	event := Event[any]{
		ID:        1,
		Table:     "tables",
		Operation: "INSERT",
		Data:      map[string]interface{}{"id": 1, "name": "test_table"},
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	// Store event
	ctx := context.Background()
	err := eventStore.StoreEvent(ctx, event)
	require.NoError(t, err)

	// Verify event count
	counts := eventStore.GetEventCount()
	assert.Equal(t, 1, counts["tables"])

	// Get events
	events, err := eventStore.GetEvents(ctx, "tables", 10)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, int64(1), events[0].ID)

	// Mark event as processed
	err = eventStore.MarkEventProcessed(ctx, 1)
	require.NoError(t, err)

	// Verify processed count
	processedCount := eventStore.GetProcessedCount()
	assert.Equal(t, 1, processedCount)

	// Cleanup processed events
	eventStore.CleanupProcessedEvents()

	// Verify cleanup
	counts = eventStore.GetEventCount()
	assert.Equal(t, 0, counts["tables"])
}

func TestCDCConsumer(t *testing.T) {
	// Create a mock database connection
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create event store
	eventStore := NewMemoryEventStore(logger)

	// Create CDC consumer
	consumer := NewCDCConsumer(db, eventStore, logger)

	// Verify default configuration
	assert.Equal(t, "__cdc_log", consumer.logTable)
	assert.Equal(t, 256, consumer.batchSize)
	assert.Equal(t, 100*time.Millisecond, consumer.pollInterval)

	// Test configuration updates
	consumer.SetBatchSize(128)
	consumer.SetPollInterval(200 * time.Millisecond)

	assert.Equal(t, 128, consumer.batchSize)
	assert.Equal(t, 200*time.Millisecond, consumer.pollInterval)
}

func TestScheduler(t *testing.T) {
	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create event store
	eventStore := NewMemoryEventStore(logger)

	// Create mock CDC consumer
	consumer := &CDCConsumer{
		logTable:     "__cdc_log",
		batchSize:    256,
		pollInterval: 100 * time.Millisecond,
		logger:       logger,
		eventStore:   eventStore,
	}

	// Create scheduler
	scheduler := NewScheduler(eventStore, consumer, logger)

	// Verify initial state
	stats := scheduler.GetSchedulerStats()
	assert.Equal(t, 0, stats["component_count"])
	assert.Equal(t, 0, stats["table_count"])

	// Create and register component
	component := NewExampleComponent("test_component", "1.0.0", logger)
	err := scheduler.RegisterComponent(component.GetComponentInfo())
	require.NoError(t, err)

	// Verify component is registered
	stats = scheduler.GetSchedulerStats()
	assert.Equal(t, 1, stats["component_count"])
	assert.Equal(t, 4, stats["table_count"]) // 4 monitored tables

	// Get all components
	components := scheduler.GetAllComponents()
	assert.Len(t, components, 1)
	assert.Equal(t, "test_component", components[0].Name)

	// Get table subscribers
	subscribers := scheduler.GetTableSubscribers("tables")
	assert.Contains(t, subscribers, "test_component")

	// Unregister component
	err = scheduler.UnregisterComponent("test_component")
	require.NoError(t, err)

	// Verify component is unregistered
	stats = scheduler.GetSchedulerStats()
	assert.Equal(t, 0, stats["component_count"])
}

