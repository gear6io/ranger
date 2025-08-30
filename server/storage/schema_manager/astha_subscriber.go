package schema_manager

import (
	"context"
	"fmt"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/astha"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/types"
	"github.com/rs/zerolog"
)

// SchemaManagerSubscriber implements Astha Subscriber interface for schema cache management
type SchemaManagerSubscriber struct {
	schemaManager SchemaManager
	logger        zerolog.Logger
	retryConfig   *RetryConfig
	healthChecker *HealthChecker
}

// RetryConfig defines retry behavior for failed operations
type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

// HealthChecker tracks component health status
type HealthChecker struct {
	lastHealthCheck time.Time
	isHealthy       bool
	failureCount    int
	maxFailures     int
}

// NewSchemaManagerSubscriber creates a new Astha subscriber for schema management
func NewSchemaManagerSubscriber(schemaManager SchemaManager, logger zerolog.Logger) *SchemaManagerSubscriber {
	return &SchemaManagerSubscriber{
		schemaManager: schemaManager,
		logger:        logger,
		retryConfig: &RetryConfig{
			MaxRetries:    3,
			InitialDelay:  100 * time.Millisecond,
			MaxDelay:      5 * time.Second,
			BackoffFactor: 2.0,
		},
		healthChecker: &HealthChecker{
			isHealthy:   true,
			maxFailures: 5,
		},
	}
}

// OnEvent handles incoming events for schema cache management with comprehensive error handling
// Requirement 5.3: Implement event handlers for table creation, modification, and deletion
func (sms *SchemaManagerSubscriber) OnEvent(ctx context.Context, event astha.Event[any]) error {
	startTime := time.Now()

	// Update health status
	sms.healthChecker.lastHealthCheck = time.Now()

	// Initialize diagnostic logger for this event
	eventID := fmt.Sprintf("event_%d", event.ID)
	diagLogger := types.NewDiagnosticLogger(sms.logger, "astha_subscriber", eventID)

	diagLogger.LogOperationStart("process_astha_event", map[string]interface{}{
		"event_id":  event.ID,
		"table":     event.Table,
		"operation": event.Operation,
		"timestamp": event.Timestamp,
	})

	sms.logger.Debug().
		Int64("event_id", event.ID).
		Str("table", event.Table).
		Str("operation", event.Operation).
		Time("timestamp", event.Timestamp).
		Msg("Processing Astha event")

	var err error
	var handled bool = true

	// Route events to appropriate handlers with comprehensive error handling
	switch event.Operation {
	case "INSERT":
		if event.Table == "tables" {
			err = sms.handleTableCreated(ctx, event)
		} else if event.Table == "table_columns" {
			err = sms.handleColumnAdded(ctx, event)
		} else {
			handled = false
		}
	case "UPDATE":
		if event.Table == "table_columns" {
			err = sms.handleSchemaChanged(ctx, event)
		} else if event.Table == "table_metadata" {
			err = sms.handleMetadataChanged(ctx, event)
		} else {
			handled = false
		}
	case "DELETE":
		if event.Table == "tables" {
			err = sms.handleTableDeleted(ctx, event)
		} else if event.Table == "table_columns" {
			err = sms.handleColumnDeleted(ctx, event)
		} else {
			handled = false
		}
	default:
		handled = false
	}

	if !handled {
		diagLogger.LogOperationEnd("process_astha_event", true, nil, map[string]interface{}{
			"handled": false,
			"reason":  "event not relevant for schema management",
		})

		sms.logger.Debug().
			Int64("event_id", event.ID).
			Str("table", event.Table).
			Str("operation", event.Operation).
			Msg("Event not relevant for schema management, skipping")

		return nil
	}

	// Handle errors with comprehensive logging and recovery
	if err != nil {
		sms.healthChecker.failureCount++

		// Create comprehensive Astha event processing error
		createTableErr := types.NewCreateTableEventProcessingError(
			fmt.Sprintf("failed to process Astha event: %s", err.Error()),
			"", // Table name will be extracted from event data
			"", // Database will be extracted from event data
			fmt.Sprintf("%s_%s", event.Operation, event.Table),
			err,
		).AddContext("event_id", event.ID).
			AddContext("event_table", event.Table).
			AddContext("event_operation", event.Operation).
			AddContext("failure_count", sms.healthChecker.failureCount)

		// Extract table and database info from event data if available
		if eventData, ok := event.Data.(map[string]interface{}); ok {
			if tableName, exists := eventData["name"].(string); exists {
				createTableErr.TableName = tableName
			}
			if database, exists := eventData["database"].(string); exists {
				createTableErr.Database = database
			}
		}

		diagLogger.LogOperationEnd("process_astha_event", false, createTableErr, map[string]interface{}{
			"failure_count": sms.healthChecker.failureCount,
			"duration":      time.Since(startTime),
		})

		sms.logger.Error().Err(err).
			Int64("event_id", event.ID).
			Str("table", event.Table).
			Str("operation", event.Operation).
			Int("failure_count", sms.healthChecker.failureCount).
			Dur("duration", time.Since(startTime)).
			Msg("Failed to process Astha event")

		// Check if we should mark component as unhealthy
		if sms.healthChecker.failureCount >= sms.healthChecker.maxFailures {
			sms.healthChecker.isHealthy = false
			sms.logger.Error().
				Int("failure_count", sms.healthChecker.failureCount).
				Int("max_failures", sms.healthChecker.maxFailures).
				Msg("Schema manager subscriber marked as unhealthy due to excessive failures")
		}

		// Don't return error to prevent blocking other subscribers (Requirement 5.4)
		return nil
	}

	// Success
	sms.healthChecker.failureCount = 0
	sms.healthChecker.isHealthy = true

	diagLogger.LogOperationEnd("process_astha_event", true, nil, map[string]interface{}{
		"duration": time.Since(startTime),
		"handled":  true,
	})

	sms.logger.Debug().
		Int64("event_id", event.ID).
		Str("table", event.Table).
		Str("operation", event.Operation).
		Dur("duration", time.Since(startTime)).
		Msg("Successfully processed Astha event")

	return nil
}

// handleTableCreated processes table creation events
// Requirement 5.4: Add schema cache lifecycle management based on Astha events
func (sms *SchemaManagerSubscriber) handleTableCreated(ctx context.Context, event astha.Event[any]) error {
	// Extract table information from event with proper error handling
	tableData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.Table if it's that type
		if table, tableOk := event.Data.(regtypes.Table); tableOk {
			return sms.handleTableCreatedFromTable(ctx, table)
		}
		return fmt.Errorf("invalid event data format for table creation: expected map[string]interface{} or regtypes.Table, got %T", event.Data)
	}

	database, ok := tableData["database"].(string)
	if !ok {
		// Try database_id and look up database name
		if databaseID, idOk := tableData["database_id"].(int64); idOk {
			// For now, we'll use a placeholder. In a real implementation,
			// we'd need to look up the database name from the database_id
			database = fmt.Sprintf("db_%d", databaseID)
		} else {
			return fmt.Errorf("missing or invalid database field in event data")
		}
	}

	tableName, ok := tableData["name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid table name field in event data")
	}

	// Extract table ID if available
	var tableID int64
	if id, idOk := tableData["id"].(int64); idOk {
		tableID = id
	}

	return sms.proactivelyCacheSchemaWithID(ctx, database, tableName, tableID, "table_created")
}

// handleTableCreatedFromTable processes table creation from regtypes.Table
func (sms *SchemaManagerSubscriber) handleTableCreatedFromTable(ctx context.Context, table regtypes.Table) error {
	// For now, use a placeholder database name since we only have database_id
	database := fmt.Sprintf("db_%d", table.DatabaseID)

	return sms.proactivelyCacheSchemaWithID(ctx, database, table.Name, table.ID, "table_created")
}

// handleColumnAdded processes column addition events
func (sms *SchemaManagerSubscriber) handleColumnAdded(ctx context.Context, event astha.Event[any]) error {
	// Extract column information
	columnData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.TableColumn if it's that type
		if column, columnOk := event.Data.(regtypes.TableColumn); columnOk {
			return sms.handleColumnAddedFromColumn(ctx, column)
		}
		return fmt.Errorf("invalid event data format for column addition: expected map[string]interface{} or regtypes.TableColumn, got %T", event.Data)
	}

	tableID, ok := columnData["table_id"].(int64)
	if !ok {
		return fmt.Errorf("missing or invalid table_id field in event data")
	}

	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", tableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "column_added")
}

// handleColumnAddedFromColumn processes column addition from regtypes.TableColumn
func (sms *SchemaManagerSubscriber) handleColumnAddedFromColumn(ctx context.Context, column regtypes.TableColumn) error {
	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, column.TableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", column.TableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "column_added")
}

// handleSchemaChanged processes schema modification events
func (sms *SchemaManagerSubscriber) handleSchemaChanged(ctx context.Context, event astha.Event[any]) error {
	// Extract column change information
	columnData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.TableColumn if it's that type
		if column, columnOk := event.Data.(regtypes.TableColumn); columnOk {
			return sms.handleSchemaChangedFromColumn(ctx, column)
		}
		return fmt.Errorf("invalid event data format for column change: expected map[string]interface{} or regtypes.TableColumn, got %T", event.Data)
	}

	tableID, ok := columnData["table_id"].(int64)
	if !ok {
		return fmt.Errorf("missing or invalid table_id field in event data")
	}

	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", tableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "schema_changed")
}

// handleSchemaChangedFromColumn processes schema changes from regtypes.TableColumn
func (sms *SchemaManagerSubscriber) handleSchemaChangedFromColumn(ctx context.Context, column regtypes.TableColumn) error {
	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, column.TableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", column.TableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "schema_changed")
}

// handleMetadataChanged processes table metadata modification events
func (sms *SchemaManagerSubscriber) handleMetadataChanged(ctx context.Context, event astha.Event[any]) error {
	// Extract metadata change information
	metadataData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.TableMetadata if it's that type
		if metadata, metadataOk := event.Data.(regtypes.TableMetadata); metadataOk {
			return sms.handleMetadataChangedFromMetadata(ctx, metadata)
		}
		return fmt.Errorf("invalid event data format for metadata change: expected map[string]interface{} or regtypes.TableMetadata, got %T", event.Data)
	}

	tableID, ok := metadataData["table_id"].(int64)
	if !ok {
		return fmt.Errorf("missing or invalid table_id field in event data")
	}

	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", tableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "metadata_changed")
}

// handleMetadataChangedFromMetadata processes metadata changes from regtypes.TableMetadata
func (sms *SchemaManagerSubscriber) handleMetadataChangedFromMetadata(ctx context.Context, metadata regtypes.TableMetadata) error {
	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, metadata.TableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", metadata.TableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "metadata_changed")
}

// handleTableDeleted processes table deletion events
func (sms *SchemaManagerSubscriber) handleTableDeleted(ctx context.Context, event astha.Event[any]) error {
	tableData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.Table if it's that type
		if table, tableOk := event.Data.(regtypes.Table); tableOk {
			return sms.handleTableDeletedFromTable(ctx, table)
		}
		return fmt.Errorf("invalid event data format for table deletion: expected map[string]interface{} or regtypes.Table, got %T", event.Data)
	}

	database, ok := tableData["database"].(string)
	if !ok {
		// Try database_id and look up database name
		if databaseID, idOk := tableData["database_id"].(int64); idOk {
			database = fmt.Sprintf("db_%d", databaseID)
		} else {
			return fmt.Errorf("missing or invalid database field in event data")
		}
	}

	tableName, ok := tableData["name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid table name field in event data")
	}

	// Remove from cache using enhanced cleanup method
	// Requirement 6.3: WHEN tables are dropped THEN their schemas SHALL be removed from cache
	sms.schemaManager.CleanupDeletedTable(database, tableName)

	sms.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Msg("Cleaned up cache for deleted table")

	return nil
}

// handleTableDeletedFromTable processes table deletion from regtypes.Table
func (sms *SchemaManagerSubscriber) handleTableDeletedFromTable(ctx context.Context, table regtypes.Table) error {
	database := fmt.Sprintf("db_%d", table.DatabaseID)

	// Remove from cache using enhanced cleanup method
	// Requirement 6.3: WHEN tables are dropped THEN their schemas SHALL be removed from cache
	sms.schemaManager.CleanupDeletedTable(database, table.Name)

	sms.logger.Info().
		Str("database", database).
		Str("table", table.Name).
		Msg("Cleaned up cache for deleted table")

	return nil
}

// handleColumnDeleted processes column deletion events
func (sms *SchemaManagerSubscriber) handleColumnDeleted(ctx context.Context, event astha.Event[any]) error {
	// Extract column information
	columnData, ok := event.Data.(map[string]interface{})
	if !ok {
		// Try to extract from regtypes.TableColumn if it's that type
		if column, columnOk := event.Data.(regtypes.TableColumn); columnOk {
			return sms.handleColumnDeletedFromColumn(ctx, column)
		}
		return fmt.Errorf("invalid event data format for column deletion: expected map[string]interface{} or regtypes.TableColumn, got %T", event.Data)
	}

	tableID, ok := columnData["table_id"].(int64)
	if !ok {
		return fmt.Errorf("missing or invalid table_id field in event data")
	}

	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", tableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "column_deleted")
}

// handleColumnDeletedFromColumn processes column deletion from regtypes.TableColumn
func (sms *SchemaManagerSubscriber) handleColumnDeletedFromColumn(ctx context.Context, column regtypes.TableColumn) error {
	// Get table information to invalidate cache
	database, tableName, err := sms.getTableInfo(ctx, column.TableID)
	if err != nil {
		return fmt.Errorf("failed to get table info for ID %d: %w", column.TableID, err)
	}

	return sms.invalidateAndRefreshCache(ctx, database, tableName, "column_deleted")
}

// proactivelyCacheSchema proactively caches schema for new tables with enhanced metadata
// Requirement 6.1: WHEN new tables are created THEN their schemas SHALL be automatically cached
func (sms *SchemaManagerSubscriber) proactivelyCacheSchema(ctx context.Context, database, tableName, reason string) error {
	return sms.proactivelyCacheSchemaWithID(ctx, database, tableName, 0, reason)
}

// proactivelyCacheSchemaWithID proactively caches schema for new tables with table ID
func (sms *SchemaManagerSubscriber) proactivelyCacheSchemaWithID(ctx context.Context, database, tableName string, tableID int64, reason string) error {
	// Use enhanced cache method with retry logic
	err := sms.retryOperation(ctx, func() error {
		if tableID > 0 {
			// Use enhanced caching method for new tables
			return sms.schemaManager.CacheNewTableSchema(ctx, database, tableName, tableID)
		} else {
			// Fallback to regular schema retrieval
			_, err := sms.schemaManager.GetSchema(ctx, database, tableName)
			return err
		}
	})

	if err != nil {
		sms.healthChecker.failureCount++
		sms.logger.Error().Err(err).
			Str("database", database).
			Str("table", tableName).
			Int64("table_id", tableID).
			Str("reason", reason).
			Int("failure_count", sms.healthChecker.failureCount).
			Msg("Failed to cache schema for new table")

		// Don't return error to prevent blocking other subscribers
		// Requirement 5.4: WHEN schema caching fails THEN the error SHALL be logged but not block other subscribers
		return nil
	}

	sms.healthChecker.failureCount = 0
	sms.healthChecker.isHealthy = true

	sms.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int64("table_id", tableID).
		Str("reason", reason).
		Msg("Successfully cached schema for table")

	return nil
}

// invalidateAndRefreshCache invalidates and refreshes cached schema with enhanced retry logic
// Requirement 6.2: WHEN table schemas are modified THEN cached schemas SHALL be invalidated and refreshed
func (sms *SchemaManagerSubscriber) invalidateAndRefreshCache(ctx context.Context, database, tableName, reason string) error {
	// Use enhanced invalidate and refresh method with retry logic
	// Requirement 6.4: WHEN cache refresh fails THEN the system SHALL retry with exponential backoff
	err := sms.retryOperation(ctx, func() error {
		return sms.schemaManager.InvalidateAndRefreshSchema(ctx, database, tableName)
	})

	if err != nil {
		sms.healthChecker.failureCount++
		sms.logger.Error().Err(err).
			Str("database", database).
			Str("table", tableName).
			Str("reason", reason).
			Int("failure_count", sms.healthChecker.failureCount).
			Msg("Failed to refresh schema cache after multiple retries")

		// Don't return error to prevent blocking other subscribers
		// Requirement 5.4: WHEN schema caching fails THEN the error SHALL be logged but not block other subscribers
		return nil
	}

	sms.healthChecker.failureCount = 0
	sms.healthChecker.isHealthy = true

	sms.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("reason", reason).
		Msg("Successfully refreshed cached schema")

	return nil
}

// retryOperation implements retry logic with exponential backoff
// Requirement 5.5: Implement proper error handling and health checks with retry logic
func (sms *SchemaManagerSubscriber) retryOperation(ctx context.Context, operation func() error) error {
	delay := sms.retryConfig.InitialDelay

	for attempt := 0; attempt <= sms.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				delay = time.Duration(float64(delay) * sms.retryConfig.BackoffFactor)
				if delay > sms.retryConfig.MaxDelay {
					delay = sms.retryConfig.MaxDelay
				}
			}
		}

		if err := operation(); err == nil {
			return nil
		} else if attempt == sms.retryConfig.MaxRetries {
			return err
		}
	}

	return fmt.Errorf("operation failed after %d retries", sms.retryConfig.MaxRetries)
}

// getTableInfo retrieves database and table name from table ID
// This is a placeholder implementation - in a real system, this would query the registry
func (sms *SchemaManagerSubscriber) getTableInfo(ctx context.Context, tableID int64) (database, tableName string, err error) {
	// TODO: Implement actual table lookup from registry
	// For now, return placeholder values
	return fmt.Sprintf("db_%d", tableID), fmt.Sprintf("table_%d", tableID), nil
}

// OnHealth performs health check for the component
// Requirement 5.6: WHEN component health checks fail THEN Astha SHALL mark Schema Manager as unhealthy
func (sms *SchemaManagerSubscriber) OnHealth(ctx context.Context) error {
	sms.healthChecker.lastHealthCheck = time.Now()

	// Check if we've had too many failures
	if sms.healthChecker.failureCount >= sms.healthChecker.maxFailures {
		sms.healthChecker.isHealthy = false
		return errors.New(SchemaManagerUnhealthy, "schema manager has exceeded maximum failure count", nil).
			AddContext("failure_count", sms.healthChecker.failureCount).
			AddContext("max_failures", sms.healthChecker.maxFailures)
	}

	// Check if we haven't seen any activity recently (stale health check)
	if time.Since(sms.healthChecker.lastHealthCheck) > 5*time.Minute {
		sms.healthChecker.isHealthy = false
		return errors.New(SchemaManagerStale, "schema manager health check is stale", nil).
			AddContext("last_health_check", sms.healthChecker.lastHealthCheck)
	}

	// Try to get cache stats to verify schema manager is responsive
	stats := sms.schemaManager.GetCacheStats()
	if stats.LastUpdated.IsZero() {
		sms.healthChecker.isHealthy = false
		return errors.New(SchemaManagerUnresponsive, "schema manager cache stats are not available", nil)
	}

	sms.healthChecker.isHealthy = true
	return nil
}

// OnRefresh tells component to refresh from Registry
// Requirement 5.7: WHEN Schema Manager is unavailable THEN Astha SHALL queue events for later processing
func (sms *SchemaManagerSubscriber) OnRefresh(ctx context.Context) error {
	sms.logger.Info().Msg("Refreshing schema manager cache from registry")

	// Clear the entire cache to force refresh from registry
	sms.schemaManager.ClearCache()

	sms.logger.Info().Msg("Schema manager cache cleared and ready for refresh")
	return nil
}

// IsHealthy returns the current health status
func (sms *SchemaManagerSubscriber) IsHealthy() bool {
	if sms.healthChecker.failureCount >= sms.healthChecker.maxFailures {
		return false
	}

	// Consider unhealthy if no recent activity
	if time.Since(sms.healthChecker.lastHealthCheck) > 5*time.Minute {
		return false
	}

	return sms.healthChecker.isHealthy
}

// GetComponentInfo returns component information for registration
func (sms *SchemaManagerSubscriber) GetComponentInfo() astha.ComponentInfo {
	return astha.ComponentInfo{
		Name:          "schema_manager",
		Version:       "1.0.0",
		Status:        sms.getStatusString(),
		LastSeen:      time.Now(),
		Subscriptions: []string{"tables", "table_columns", "table_metadata"},
	}
}

// getStatusString returns the current status as a string
func (sms *SchemaManagerSubscriber) getStatusString() string {
	if sms.IsHealthy() {
		return "active"
	}
	return "error"
}
