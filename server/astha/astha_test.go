package astha

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockComponent is a simple mock implementation for testing
type MockComponent struct {
	name    string
	version string
	status  string
}

func (m *MockComponent) GetComponentInfo() ComponentInfo {
	return ComponentInfo{
		Name:          m.name,
		Version:       m.version,
		Status:        m.status,
		Subscriptions: []string{"tables"},
	}
}

func (m *MockComponent) OnEvent(ctx context.Context, event Event[any]) error {
	return nil
}

func (m *MockComponent) OnHealth(ctx context.Context) error {
	return nil
}

func (m *MockComponent) OnRefresh(ctx context.Context) error {
	return nil
}

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

func TestAsthaComponentRegistration(t *testing.T) {
	// Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)

	// Create mock database connection
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create the required tables that CDC setup expects
	ctx := context.Background()

	// Create tables table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE tables (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			database_id INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Create table_files table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_files (
			id INTEGER PRIMARY KEY,
			table_id INTEGER NOT NULL,
			file_path TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Create table_statistics table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_statistics (
			id INTEGER PRIMARY KEY,
			table_id INTEGER NOT NULL,
			stat_name TEXT NOT NULL,
			stat_value TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Create table_metadata table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_metadata (
			id INTEGER PRIMARY KEY,
			table_id INTEGER NOT NULL,
			metadata_key TEXT NOT NULL,
			metadata_value TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Create astha config
	cfg := &Config{
		Database:     db,
		Logger:       logger,
		PollInterval: 100,
	}
	astha, err := NewAstha(cfg)
	require.NoError(t, err)
	defer astha.Stop()

	// Start astha
	err = astha.Start()
	require.NoError(t, err)

	// Create mock component
	component := &MockComponent{
		name:    "test_component",
		version: "1.0.0",
		status:  "active",
	}

	// Register component
	err = astha.RegisterComponentWithInstance(component.GetComponentInfo(), component)
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

	// Create test event using supported type
	event := Event[regtypes.Table]{
		ID:        1,
		Table:     "tables",
		Operation: "INSERT",
		Data:      regtypes.Table{ID: 1, Name: "test_table"},
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

	// Type assert to access the ID field
	if eventData, ok := events[0].(Event[regtypes.Table]); ok {
		assert.Equal(t, int64(1), eventData.ID)
	} else {
		t.Error("Failed to type assert event")
	}

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

	// Test CDC consumer start
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = consumer.Start(ctx)
	require.NoError(t, err)

	// Test configuration updates
	consumer.SetBatchSize(128)
	consumer.SetPollInterval(200 * time.Millisecond)

	assert.Equal(t, 128, consumer.batchSize)
	assert.Equal(t, 200*time.Millisecond, consumer.pollInterval)
}
