# 🚀 Astha - CDC Scheduler for Data Lakehouse Platform

**Astha** (meaning "Trust and Reliability") is a Change Data Capture (CDC) scheduler that provides real-time event distribution for the Data Lakehouse Platform metadata registry system.

## 🎯 Overview

Astha monitors changes in the Registry database and distributes events to subscribed components in real-time. It uses SQLite triggers to capture changes and provides immediate cleanup for processed events.

## 🏗️ Architecture

```
Registry Database (SQLite)
    ↓ (Triggers)
__cdc_log Table
    ↓ (Polling)
Astha CDC Consumer
    ↓ (Event Store)
Astha Scheduler
    ↓ (Distribution)
Subscribed Components
```

## 🚀 Quick Start

### 1. Create Astha Instance

```go
import (
    "github.com/gear6io/ranger/server/astha"
    "github.com/rs/zerolog"
)

// Create configuration
cfg := &astha.Config{
    Database:     db,           // SQLite database connection
    Logger:       logger,        // Zerolog logger
    BatchSize:    256,          // CDC batch size
    PollInterval: 100,          // Poll interval in milliseconds
}

// Create Astha instance
astha, err := astha.NewAstha(cfg)
if err != nil {
    log.Fatal(err)
}
```

### 2. Start Astha

```go
// Start the scheduler and CDC infrastructure
if err := astha.Start(); err != nil {
    log.Fatal(err)
}

// Don't forget to stop when done
defer astha.Stop()
```

### 3. Create and Register Components

```go
// Create a component that implements the Subscriber interface
component := astha.NewExampleComponent("iceberg_updater", "1.0.0", logger)

// Register the component
err = astha.RegisterComponent(component.GetComponentInfo())
if err != nil {
    log.Fatal(err)
}
```

## 🔧 Component Implementation

### Subscriber Interface

All components must implement the `Subscriber[T]` interface:

```go
type Subscriber[T any] interface {
    // OnEvent handles incoming events for this component
    OnEvent(ctx context.Context, event Event[T]) error
    
    // OnHealth check for component health
    OnHealth(ctx context.Context) error
    
    // OnRefresh tells component to refresh from Registry
    OnRefresh(ctx context.Context) error
}
```

### Example Component

```go
type MyComponent struct {
    name string
    logger zerolog.Logger
}

func (c *MyComponent) OnEvent(ctx context.Context, event astha.Event[any]) error {
    c.logger.Info().
        Str("table", event.Table).
        Str("operation", event.Operation).
        Msg("Received event")
    
    // Process the event based on table and operation
    switch event.Table {
    case "tables":
        return c.handleTableEvent(event)
    case "table_files":
        return c.handleTableFileEvent(event)
    default:
        return nil
    }
}

func (c *MyComponent) OnHealth(ctx context.Context) error {
    // Implement health check logic
    return nil
}

func (c *MyComponent) OnRefresh(ctx context.Context) error {
    // Implement refresh logic
    return nil
}
```

## 📊 Event Types

### Generic Event Structure

```go
type Event[T any] struct {
    ID        int64     `json:"id"`
    Table     string    `json:"table"`
    Operation string    `json:"operation"` // INSERT, UPDATE, DELETE
    Data      T         `json:"data"`
    Timestamp time.Time `json:"timestamp"`
    CreatedAt time.Time `json:"created_at"`
}
```

### Supported Operations

- **INSERT**: New records added to tables
- **UPDATE**: Existing records modified
- **DELETE**: Records removed from tables

### Monitored Tables

Astha automatically monitors these Registry tables:

- `tables` - Table-level statistics
- `table_files` - File tracking information
- `table_statistics` - File-level statistics
- `table_metadata` - Schema and engine info

## ⚙️ Configuration

### CDC Configuration

```go
// Default settings
BatchSize:    256                    // Changes per batch
PollInterval: 100                    // Poll every 100ms
LogTable:     "__cdc_log"           // CDC log table name
```

### Performance Tuning

```go
// For high-throughput systems
cfg := &astha.Config{
    BatchSize:    512,              // Larger batches
    PollInterval: 50,               // Faster polling
}

// For low-latency systems
cfg := &astha.Config{
    BatchSize:    64,               // Smaller batches
    PollInterval: 200,              // Slower polling
}
```

## 🔍 Monitoring and Debugging

### Scheduler Statistics

```go
stats := astha.GetSchedulerStats()
fmt.Printf("Active components: %d\n", stats["active_components"])
fmt.Printf("Total events: %v\n", stats["event_counts"])
fmt.Printf("Processed events: %d\n", stats["processed_events"])
```

### Component Information

```go
// Get component details
info, exists := astha.GetComponentInfo("component_name")
if exists {
    fmt.Printf("Status: %s\n", info.Status)
    fmt.Printf("Subscriptions: %v\n", info.Subscriptions)
}

// List all components
components := astha.GetAllComponents()
for _, comp := range components {
    fmt.Printf("Component: %s (v%s) - %s\n", 
        comp.Name, comp.Version, comp.Status)
}
```

### Table Subscriptions

```go
// See which components subscribe to a table
subscribers := astha.GetTableSubscribers("tables")
fmt.Printf("Table 'tables' subscribers: %v\n", subscribers)
```

## 🚨 Error Handling

### Component Failures

Astha automatically detects component failures and provides recovery mechanisms:

1. **Health Monitoring**: Components must respond to health checks
2. **Event Retry**: Failed events are preserved for retry
3. **Circuit Breaker**: Prevents cascade failures
4. **State Recovery**: Components can refresh from Registry

### CDC Processing Failures

- Failed batches are not cleaned up, allowing retry
- Malformed CDC log entries are logged and skipped
- Database connection failures trigger automatic reconnection

## 🔄 Lifecycle Management

### Starting Astha

```go
// Start CDC infrastructure and scheduler
if err := astha.Start(); err != nil {
    log.Fatal("Failed to start Astha:", err)
}
```

### Stopping Astha

```go
// Graceful shutdown
if err := astha.Stop(); err != nil {
    log.Error("Error stopping Astha:", err)
}
```

### Component Lifecycle

```go
// Register component
err = astha.RegisterComponent(componentInfo)

// Update health status
err = astha.UpdateComponentHealth("component_name", "active")

// Unregister component
err = astha.UnregisterComponent("component_name")
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
go test ./server/astha

# Run with verbose output
go test ./server/astha -v

# Run specific test
go test ./server/astha -run TestComponentRegistration
```

### Test Coverage

```bash
# Generate coverage report
go test ./server/astha -coverprofile=coverage.out

# View coverage in browser
go tool cover -html=coverage.out
```

## 📚 Examples

### Complete Working Example

See `example_component.go` for a complete implementation example.

### Integration Example

```go
func main() {
    // Setup database and logger
    db := setupDatabase()
    logger := setupLogger()
    
    // Create Astha
    astha, err := astha.NewAstha(&astha.Config{
        Database:     db,
        Logger:       logger,
        BatchSize:    256,
        PollInterval: 100,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // Start Astha
    if err := astha.Start(); err != nil {
        log.Fatal(err)
    }
    defer astha.Stop()
    
    // Create and register components
    icebergUpdater := createIcebergUpdater(logger)
    compactor := createCompactor(logger)
    
    astha.RegisterComponent(icebergUpdater.GetComponentInfo())
    astha.RegisterComponent(compactor.GetComponentInfo())
    
    // Keep running
    select {}
}
```

## 🔮 Future Enhancements

- **Health Monitoring**: Advanced health checks and metrics
- **Circuit Breaker**: Sophisticated failure detection and recovery
- **Event Persistence**: Persistent event storage for recovery
- **Load Balancing**: Distribute events across multiple component instances
- **Authentication**: Secure component registration and communication

## 📖 API Reference

### Core Types

- `Astha` - Main CDC scheduler
- `Scheduler` - Event distribution engine
- `CDCConsumer` - CDC log processor
- `EventStore` - Event storage interface
- `Subscriber[T]` - Component interface

### Configuration

- `Config` - Astha configuration
- `ComponentInfo` - Component registration info
- `Event[T]` - Generic event structure

## 🤝 Contributing

1. Follow the existing code style
2. Add tests for new functionality
3. Update documentation for API changes
4. Ensure all tests pass before submitting

## 📄 License

This package is part of the Data Lakehouse Platform and follows the same license terms.

