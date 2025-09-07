package schema

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/storage/parquet"
	"github.com/rs/zerolog"
)

// SchemaLoaderFunc is a function type for loading schemas from registry
type SchemaLoaderFunc = registry.SchemaLoaderFunc

// Manager implements the SchemaManager interface with caching and registry-based retrieval
type Schema struct {
	cache        *SchemaCache
	schemaLoader SchemaLoaderFunc
	config       *config.SchemaManagerConfig
	logger       zerolog.Logger
	initialData  map[string]*registry.SchemaData

	// Background cleanup
	stopCleanup chan struct{}
}

// NewManager creates a new schema manager with registry data and schema converter
func NewSchema(ctx context.Context, initialData map[string]*registry.SchemaData, config *config.SchemaManagerConfig, logger zerolog.Logger, schemaLoader SchemaLoaderFunc) (*Schema, error) {
	if config == nil {
		return nil, errors.New(ErrSchemaManagerInitializationFailed, "config is nil", nil)
	}

	// Create cache
	cache := NewSchemaCache(config)

	// Load all schema data at initialization
	schema := &Schema{
		cache:        cache,
		schemaLoader: schemaLoader,
		config:       config,
		logger:       logger,
		initialData:  initialData,
		stopCleanup:  make(chan struct{}),
	}

	// Pre-populate cache with initial data
	schema.prePopulateCache(ctx)

	// Start background cleanup if metrics are enabled
	if config.EnableMetrics {
		go schema.backgroundCleanup()
	}

	return schema, nil
}

// prePopulateCache loads all initial schema data into the cache
func (s *Schema) prePopulateCache(ctx context.Context) error {
	for cacheKey, schemaData := range s.initialData {
		// Convert to Iceberg schema using utility function
		schema, err := parquet.ConvertRegistryDataToIcebergSchema(schemaData)
		if err != nil {
			return err
		}

		// Cache the schema
		s.cache.PutWithMetadata(
			cacheKey,
			schema,
			"registry",
			"initial_load",
			schemaData.TableID,
			false, // Not a new table during initialization
		)
	}

	return nil
}

// GetSchema retrieves schema from cache or database
// Requirement 4.7: Proper error logging with appropriate severity levels
func (s *Schema) GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	cacheKey := generateCacheKey(database, tableName)

	// Try to get from cache first
	if schema, found := s.cache.Get(cacheKey); found {
		return schema, nil
	}

	// Get schema data from registry
	schemaData, err := s.schemaLoader(ctx, database, tableName)
	if err != nil {
		// Track error in cache metrics
		metrics := s.cache.GetMetrics()
		metrics.ErrorCount++

		// Return error
		return nil, err
	}

	// Convert to Iceberg schema using utility function
	schema, err := parquet.ConvertRegistryDataToIcebergSchema(schemaData)
	if err != nil {
		// Track error in cache metrics
		metrics := s.cache.GetMetrics()
		metrics.ErrorCount++

		// Return error
		return nil, err
	}

	// Store in cache
	s.cache.Put(cacheKey, schema)
	return schema, nil
}

// InvalidateSchema removes schema from cache (for Astha integration)
func (s *Schema) InvalidateSchema(database, tableName string) {
	cacheKey := generateCacheKey(database, tableName)
	s.cache.Delete(cacheKey)

	s.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Msg("Schema invalidated from cache")
}

// InvalidateCache is an alias for InvalidateSchema for backward compatibility
func (s *Schema) InvalidateCache(database, tableName string) {
	s.InvalidateSchema(database, tableName)
}

// CacheNewTableSchema automatically caches schema for new tables with high priority
// Requirement 6.1: WHEN new tables are created THEN their schemas SHALL be automatically cached
func (s *Schema) CacheNewTableSchema(ctx context.Context, database, tableName string, tableID int64) error {
	cacheKey := generateCacheKey(database, tableName)

	// Check if already cached
	if schema, found := s.cache.Get(cacheKey); found {
		// Update existing entry to mark as new table with high priority
		s.cache.PutWithMetadata(cacheKey, schema, "astha_event", "proactive_cache", tableID, true)
		return nil
	}

	// Get schema data from registry
	schemaData, err := s.schemaLoader(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Convert to Iceberg schema using utility function
	schema, err := parquet.ConvertRegistryDataToIcebergSchema(schemaData)
	if err != nil {
		return err
	}

	// Store in cache with high priority metadata
	s.cache.PutWithMetadata(cacheKey, schema, "astha_event", "proactive_cache", tableID, true)
	return nil
}

// InvalidateAndRefreshSchema invalidates and refreshes cached schema
// Requirement 6.2: WHEN table schemas are modified THEN cached schemas SHALL be invalidated and refreshed
func (s *Schema) InvalidateAndRefreshSchema(ctx context.Context, database, tableName string) error {
	cacheKey := generateCacheKey(database, tableName)

	// Invalidate existing cache entry
	s.cache.InvalidateAndRefresh(cacheKey)

	// Get updated schema data from registry
	schemaData, err := s.schemaLoader(ctx, database, tableName)
	if err != nil {
		return err
	}

	// Convert to Iceberg schema using utility function
	schema, err := parquet.ConvertRegistryDataToIcebergSchema(schemaData)
	if err != nil {
		return err
	}

	// Store refreshed schema in cache
	s.cache.PutWithMetadata(cacheKey, schema, "registry", "refresh", 0, false)

	return nil
}

// CleanupDeletedTable removes schema from cache for deleted tables
// Requirement 6.3: WHEN tables are dropped THEN their schemas SHALL be removed from cache
func (s *Schema) CleanupDeletedTable(database, tableName string) {
	cacheKey := generateCacheKey(database, tableName)

	s.cache.Delete(cacheKey)

}

// GetCacheMetrics returns detailed cache metrics
func (s *Schema) GetCacheMetrics() *CacheMetrics {
	return s.cache.GetMetrics()
}

// ClearCache removes all cached schemas
func (s *Schema) ClearCache() {
	s.cache.Clear()
	s.logger.Info().Msg("Schema cache cleared")
}

// GetCacheStats returns cache performance metrics
func (s *Schema) GetCacheStats() CacheStats {
	return s.cache.GetStats()
}

// RegisterWithAstha registers the schema manager as an Astha subscriber
// Requirement 5.1: WHEN Schema Manager starts THEN it SHALL register as an Astha subscriber for table creation events
// Requirement 5.2: WHEN new tables are created THEN Astha SHALL notify all subscribed components
func (s *Schema) RegisterWithAstha(asthaInstance AsthaInterface) error {
	subscriber := NewSchemaManagerSubscriber(s, s.logger)

	componentInfo := subscriber.GetComponentInfo()

	err := asthaInstance.RegisterComponentWithInstance(componentInfo, subscriber)
	if err != nil {
		return err
	}

	s.logger.Info().
		Str("component", componentInfo.Name).
		Strs("subscriptions", componentInfo.Subscriptions).
		Msg("Successfully registered schema manager with Astha")

	return nil
}

// PerformMaintenanceCleanup performs comprehensive cache maintenance
// Requirement 6.4: WHEN cache refresh fails THEN the system SHALL retry with exponential backoff
func (s *Schema) PerformMaintenanceCleanup() error {
	// Perform cleanup with retry logic
	err := s.cache.CleanupWithRetry(5, 200*time.Millisecond)
	if err != nil {

		return err
	}

	return nil
}

// GetParquetConfigForTable returns the resolved parquet configuration for a table
// This is the main function that storage managers will use to get parquet config
func (s *Schema) GetParquetConfigForTable(ctx context.Context, database, tableName string) (*parquet.ParquetConfig, error) {
	// Get schema data to extract table metadata
	schemaData, err := s.schemaLoader(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// Start with default parquet config
	config := parquet.DefaultParquetConfig()

	// Override with table metadata settings
	if err := s.applyTableMetadataToConfig(config, schemaData.Metadata); err != nil {
		return nil, err
	}

	return config, nil
}

// applyTableMetadataToConfig applies table metadata settings to parquet config
func (s *Schema) applyTableMetadataToConfig(config *parquet.ParquetConfig, metadata *regtypes.TableMetadata) error {
	// Apply compression setting
	if metadata.Compression != "" {
		config.Compression = metadata.Compression
	}

	// Apply batch size setting
	if metadata.BatchSize > 0 {
		config.BatchSize = metadata.BatchSize
	}

	// Parse Settings JSON for additional parquet-specific settings
	if metadata.Settings != "" {
		var settings map[string]interface{}
		if err := json.Unmarshal([]byte(metadata.Settings), &settings); err != nil {
			return errors.New(ErrSchemaManagerRetrievalError, "failed to parse table settings JSON", err).
				AddContext("settings", metadata.Settings)
		}

		// Apply parquet-specific settings from JSON
		if maxMemory, ok := settings["max_memory_usage"].(float64); ok {
			config.MaxMemoryUsage = int64(maxMemory)
		}
		if streaming, ok := settings["streaming_enabled"].(bool); ok {
			config.StreamingEnabled = streaming
		}
		if chunkSize, ok := settings["chunk_size"].(float64); ok {
			config.ChunkSize = int(chunkSize)
		}
		if compressionLevel, ok := settings["compression_level"].(float64); ok {
			config.CompressionLevel = int(compressionLevel)
		}
		if enableStats, ok := settings["enable_stats"].(bool); ok {
			config.EnableStats = enableStats
		}
		if memoryPoolSize, ok := settings["memory_pool_size"].(float64); ok {
			config.MemoryPoolSize = int64(memoryPoolSize)
		}
		if maxFileSize, ok := settings["max_file_size"].(float64); ok {
			config.MaxFileSize = int64(maxFileSize)
		}
		if rotationTimeout, ok := settings["rotation_timeout"].(float64); ok {
			config.RotationTimeout = int64(rotationTimeout)
		}

		// Handle column-specific compression
		if columnCompression, ok := settings["column_compression"].(map[string]interface{}); ok {
			config.ColumnCompression = make(map[string]string)
			for col, comp := range columnCompression {
				if compStr, ok := comp.(string); ok {
					config.ColumnCompression[col] = compStr
				}
			}
		}
	}

	return nil
}

// Shutdown gracefully shuts down the schema manager
func (s *Schema) Shutdown() {
	close(s.stopCleanup)
	s.logger.Info().Msg("Schema manager shutdown")
}

// backgroundCleanup runs periodic cache cleanup and statistics logging
func (s *Schema) backgroundCleanup() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Enhanced cleanup with retry logic
			// Requirement 6.5: Add retry logic with exponential backoff for cache operations
			err := s.cache.CleanupWithRetry(3, 100*time.Millisecond)
			if err != nil {
				s.logger.Warn().Err(err).
					Msg("Cache cleanup failed after retries")
			}

			// Clean up expired entries (this is included in CleanupWithRetry but we'll track it separately)
			expiredCount := s.cache.CleanupExpired()
			if expiredCount > 0 {
				s.logger.Debug().
					Int("expired_count", expiredCount).
					Msg("Cleaned up expired schema cache entries")
			}

			// Log cache statistics if enabled
			if s.config.EnableMetrics {
				stats := s.cache.GetStats()
				metrics := s.cache.GetMetrics()

				// Enhanced logging with lifecycle metrics
				// Requirement 6.1: Update cache entries to track schema source and metadata
				s.logger.Info().
					Int64("hit_count", stats.HitCount).
					Int64("miss_count", stats.MissCount).
					Float64("hit_ratio", stats.HitRatio).
					Int("cache_size", stats.CacheSize).
					Int64("evict_count", stats.EvictCount).
					Int64("memory_usage_bytes", stats.MemoryUsage).
					Float64("memory_usage_percent", stats.MemoryPercent).
					Int64("proactive_caches", metrics.ProactiveCaches).
					Int64("refresh_operations", metrics.RefreshOperations).
					Int64("invalidation_count", metrics.InvalidationCount).
					Int64("error_count", metrics.ErrorCount).
					Msg("Schema cache statistics with enhanced lifecycle metrics")
			}

		case <-s.stopCleanup:
			return
		}
	}
}
