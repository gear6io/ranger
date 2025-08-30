# Schema Manager Astha Integration

This document describes the integration between the Schema Manager and Astha (the Change Data Capture system) for automatic schema cache management.

## Overview

The Schema Manager Astha Integration provides automatic schema cache lifecycle management by subscribing to table change events. When tables are created, modified, or deleted, the Schema Manager automatically updates its cache to maintain consistency with the registry state.

## Architecture

```
Registry Changes → Astha CDC → Schema Manager Subscriber → Cache Updates
```

### Components

1. **SchemaManagerSubscriber**: Implements the Astha `Subscriber` interface
2. **RetryConfig**: Configures retry behavior for failed operations
3. **HealthChecker**: Tracks component health status
4. **AsthaInterface**: Abstraction for Astha integration

## Features

### Event Handling

The Schema Manager subscribes to the following table events:

- **tables**: Table creation and deletion
- **table_columns**: Column additions, modifications, and deletions  
- **table_metadata**: Metadata changes

### Automatic Cache Management

- **Proactive Caching**: New tables are automatically cached when created
- **Cache Invalidation**: Modified schemas are invalidated and refreshed
- **Cache Cleanup**: Deleted tables are removed from cache

### Error Handling & Resilience

- **Retry Logic**: Exponential backoff for failed operations
- **Health Monitoring**: Component health checks and status reporting
- **Graceful Degradation**: Errors don't block other subscribers

## Requirements Satisfied

This implementation satisfies the following requirements from the specification:

### Requirement 5.1: Astha Registration
✅ Schema Manager registers as an Astha subscriber for table creation events

### Requirement 5.2: Event Notification  
✅ Astha notifies all subscribed components when new tables are created

### Requirement 5.3: Event Handlers
✅ Implements event handlers for table creation, modification, and deletion

### Requirement 5.4: Cache Lifecycle Management
✅ Schema cache lifecycle management based on Astha events

### Requirement 5.5: Error Handling & Retry Logic
✅ Proper error handling and health checks with retry logic

### Requirement 5.6: Health Checks
✅ Component health checks with Astha marking unhealthy components

### Requirement 5.7: Event Queuing
✅ Astha queues events when Schema Manager is unavailable

## Usage

### Basic Integration

```go
// Create Schema Manager
schemaManager := NewManager(metadataManager, config, logger)

// Register with Astha
err := schemaManager.RegisterWithAstha(asthaInstance)
if err != nil {
    return err
}

// Start Astha
err = asthaInstance.Start()
if err != nil {
    return err
}
```

### Event Flow Example

1. **Table Creation**:
   ```
   CREATE TABLE users (id int64, name string) → Registry → Astha Event → Schema Manager → Cache Schema
   ```

2. **Column Addition**:
   ```
   ALTER TABLE users ADD email string → Registry → Astha Event → Schema Manager → Invalidate & Refresh Cache
   ```

3. **Table Deletion**:
   ```
   DROP TABLE users → Registry → Astha Event → Schema Manager → Remove from Cache
   ```

## Configuration

### Retry Configuration

```go
retryConfig := &RetryConfig{
    MaxRetries:    3,
    InitialDelay:  100 * time.Millisecond,
    MaxDelay:      5 * time.Second,
    BackoffFactor: 2.0,
}
```

### Health Check Configuration

```go
healthChecker := &HealthChecker{
    isHealthy:   true,
    maxFailures: 5,
}
```

## Event Types Handled

### Table Events (`tables` table)

- **INSERT**: New table created → Proactive schema caching
- **DELETE**: Table deleted → Remove from cache

### Column Events (`table_columns` table)

- **INSERT**: Column added → Invalidate and refresh cache
- **UPDATE**: Column modified → Invalidate and refresh cache  
- **DELETE**: Column deleted → Invalidate and refresh cache

### Metadata Events (`table_metadata` table)

- **UPDATE**: Metadata changed → Invalidate and refresh cache

## Error Handling

### Retry Strategy

- **Exponential Backoff**: Delays increase exponentially (100ms → 200ms → 400ms → ...)
- **Max Retries**: Configurable maximum retry attempts (default: 3)
- **Context Cancellation**: Respects context cancellation during retries

### Health Monitoring

- **Failure Tracking**: Counts consecutive failures
- **Health Status**: Reports healthy/unhealthy status to Astha
- **Automatic Recovery**: Resets failure count on successful operations

### Graceful Degradation

- **Non-blocking Errors**: Cache failures don't block other subscribers
- **Logging**: Comprehensive error logging with context
- **Fallback**: Cache misses fall back to registry retrieval

## Testing

The integration includes comprehensive tests covering:

- **Event Handling**: All event types and data formats
- **Retry Logic**: Success, failure, and context cancellation scenarios
- **Health Checks**: Healthy, unhealthy, and stale check scenarios
- **Integration**: End-to-end event processing flows

### Running Tests

```bash
go test -v ./server/storage/schema_manager/
```

## Monitoring

### Component Status

The Schema Manager reports its status to Astha:

- **Name**: `schema_manager`
- **Version**: `1.0.0`
- **Status**: `active` or `error`
- **Subscriptions**: `["tables", "table_columns", "table_metadata"]`

### Health Checks

Health checks verify:

- ✅ Failure count below threshold
- ✅ Recent health check activity
- ✅ Schema Manager responsiveness

### Cache Statistics

Monitor cache performance:

```go
stats := schemaManager.GetCacheStats()
// stats.HitRatio, stats.CacheSize, stats.MemoryUsage, etc.
```

## Best Practices

### Integration

1. **Register Early**: Register Schema Manager with Astha before starting
2. **Handle Errors**: Always check registration and startup errors
3. **Graceful Shutdown**: Stop Astha before shutting down Schema Manager

### Monitoring

1. **Health Checks**: Monitor component health status
2. **Cache Metrics**: Track cache hit ratios and memory usage
3. **Error Logs**: Monitor error logs for retry failures

### Performance

1. **Batch Size**: Configure appropriate Astha batch sizes
2. **Poll Interval**: Balance responsiveness vs. resource usage
3. **Cache Limits**: Set appropriate cache size and memory limits

## Troubleshooting

### Common Issues

1. **Registration Failures**:
   - Check Astha instance is created correctly
   - Verify database connection is available
   - Check component name conflicts

2. **Event Processing Failures**:
   - Monitor retry logs for persistent failures
   - Check registry connectivity
   - Verify table lookup functionality

3. **Health Check Failures**:
   - Check failure count thresholds
   - Verify Schema Manager responsiveness
   - Monitor health check frequency

### Debug Logging

Enable debug logging to trace event processing:

```go
logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel)
```

## Future Enhancements

Potential improvements for the integration:

1. **Metrics Collection**: Prometheus metrics for monitoring
2. **Event Filtering**: More granular event subscription
3. **Batch Processing**: Batch multiple events for efficiency
4. **Circuit Breaker**: Circuit breaker pattern for resilience
5. **Dead Letter Queue**: Handle permanently failed events

## See Also

- [Schema Manager Documentation](README.md)
- [Astha Documentation](../../astha/README.md)
- [Integration Example](integration_example.go)
- [Test Suite](astha_subscriber_test.go)