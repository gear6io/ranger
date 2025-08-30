package schema_manager

import (
	"context"
	"database/sql"

	"github.com/gear6io/ranger/server/astha"
	"github.com/rs/zerolog"
)

// IntegrationExample demonstrates how to integrate Schema Manager with Astha
// This is an example implementation showing the complete integration flow
func IntegrationExample() error {
	// 1. Create logger
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// 2. Create database connection (placeholder)
	var db *sql.DB // This would be your actual database connection

	// 3. Create Astha instance
	asthaConfig := &astha.Config{
		Database:     db,
		Logger:       logger,
		BatchSize:    100,
		PollInterval: 1000, // 1 second
	}

	asthaInstance, err := astha.NewAstha(asthaConfig)
	if err != nil {
		return err
	}

	// 4. Create Schema Manager
	schemaManagerConfig := DefaultSchemaManagerConfig()

	// Create a metadata manager interface (placeholder)
	var metadataManager MetadataManagerInterface // This would be your actual metadata manager

	schemaManager := NewManager(metadataManager, schemaManagerConfig, logger)

	// 5. Register Schema Manager with Astha
	// This is the key integration step - it registers the schema manager as a subscriber
	// for table events (tables, table_columns, table_metadata)
	err = schemaManager.RegisterWithAstha(asthaInstance)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to register schema manager with Astha")
		return err
	}

	// 6. Start Astha (this would typically be done in your main application)
	err = asthaInstance.Start()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to start Astha")
		return err
	}

	logger.Info().Msg("Schema Manager successfully integrated with Astha")

	// 7. The integration is now complete. The Schema Manager will:
	//    - Receive events when tables are created, modified, or deleted
	//    - Automatically cache schemas for new tables
	//    - Invalidate and refresh cached schemas when changes occur
	//    - Handle health checks and provide component status to Astha
	//    - Support retry logic with exponential backoff for failed operations

	// Example of what happens automatically:
	// - When a CREATE TABLE statement is executed and stored in the registry
	// - Astha detects the change and creates an event
	// - The Schema Manager subscriber receives the event
	// - The subscriber proactively caches the new table's schema
	// - Future INSERT operations can validate against the cached schema

	return nil
}

// Example of how to use the Schema Manager after integration
func ExampleUsage(ctx context.Context, schemaManager SchemaManager) error {
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Get schema for a table (will use cache if available, otherwise fetch from registry)
	schema, err := schemaManager.GetSchema(ctx, "my_database", "users")
	if err != nil {
		logger.Error().Err(err).
			Str("database", "my_database").
			Str("table", "users").
			Msg("Failed to get schema")
		return err
	}

	logger.Info().
		Str("database", "my_database").
		Str("table", "users").
		Int("field_count", len(schema.Fields())).
		Msg("Successfully retrieved schema")

	// Get cache statistics
	stats := schemaManager.GetCacheStats()
	logger.Info().
		Int64("hit_count", stats.HitCount).
		Int64("miss_count", stats.MissCount).
		Float64("hit_ratio", stats.HitRatio).
		Int("cache_size", stats.CacheSize).
		Msg("Cache statistics")

	return nil
}

// Example of manual cache management (if needed)
func ExampleCacheManagement(schemaManager SchemaManager) {
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Manually invalidate a schema (usually not needed as Astha handles this automatically)
	schemaManager.InvalidateSchema("my_database", "users")
	logger.Info().
		Str("database", "my_database").
		Str("table", "users").
		Msg("Manually invalidated schema cache")

	// Clear entire cache (useful for maintenance or testing)
	schemaManager.ClearCache()
	logger.Info().Msg("Cleared entire schema cache")
}

// Example of graceful shutdown
func ExampleShutdown(asthaInstance *astha.Astha, schemaManager SchemaManager) error {
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// 1. Stop Astha first to prevent new events
	err := asthaInstance.Stop()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to stop Astha")
		return err
	}

	// 2. Shutdown Schema Manager
	schemaManager.Shutdown()

	logger.Info().Msg("Successfully shut down Schema Manager and Astha integration")
	return nil
}
