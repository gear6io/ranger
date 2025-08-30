package schema_manager

import (
	"context"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/rs/zerolog"
)

// Manager implements the SchemaManager interface with caching and retrieval capabilities
type Manager struct {
	cache     *SchemaCache
	retriever *SchemaRetriever
	config    *SchemaManagerConfig
	logger    zerolog.Logger

	// Background cleanup
	stopCleanup chan struct{}
}

// NewManager creates a new schema manager with the given configuration
func NewManager(metadataManager MetadataManagerInterface, config *SchemaManagerConfig, logger zerolog.Logger) *Manager {
	if config == nil {
		config = DefaultSchemaManagerConfig()
	}

	cache := NewSchemaCache(config)
	retriever := NewSchemaRetriever(metadataManager)

	manager := &Manager{
		cache:       cache,
		retriever:   retriever,
		config:      config,
		logger:      logger,
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup if metrics are enabled
	if config.EnableMetrics {
		go manager.backgroundCleanup()
	}

	return manager
}

// GetSchema retrieves schema from cache or database
// Requirement 4.7: Proper error logging with appropriate severity levels
func (m *Manager) GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	cacheKey := generateCacheKey(database, tableName)

	// Try to get from cache first
	if schema, found := m.cache.Get(cacheKey); found {
		m.logger.Debug().
			Str("database", database).
			Str("table", tableName).
			Str("cache_key", cacheKey).
			Msg("Schema retrieved from cache")
		return schema, nil
	}

	// Cache miss - retrieve from database
	m.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Str("cache_key", cacheKey).
		Msg("Schema cache miss, retrieving from database")

	schema, err := m.retriever.RetrieveSchema(ctx, database, tableName)
	if err != nil {
		// Track error in cache metrics
		metrics := m.cache.GetMetrics()
		metrics.ErrorCount++

		// Log error with appropriate severity level (Requirement 4.7)
		m.logger.Error().
			Err(err).
			Str("database", database).
			Str("table", tableName).
			Str("cache_key", cacheKey).
			Msg("Failed to retrieve schema from database")

		// Return enhanced error with context
		return nil, errors.New(SchemaManagerRetrievalError, "failed to retrieve schema", err).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("cache_key", cacheKey).
			AddContext("error_severity", "error")
	}

	// Store in cache
	m.cache.Put(cacheKey, schema)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("cache_key", cacheKey).
		Int("field_count", len(schema.Fields())).
		Msg("Schema retrieved from database and cached successfully")

	return schema, nil
}

// InvalidateSchema removes schema from cache (for Astha integration)
func (m *Manager) InvalidateSchema(database, tableName string) {
	cacheKey := generateCacheKey(database, tableName)
	m.cache.Delete(cacheKey)

	m.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Msg("Schema invalidated from cache")
}

// InvalidateCache is an alias for InvalidateSchema for backward compatibility
func (m *Manager) InvalidateCache(database, tableName string) {
	m.InvalidateSchema(database, tableName)
}

// CacheNewTableSchema automatically caches schema for new tables with high priority
// Requirement 6.1: WHEN new tables are created THEN their schemas SHALL be automatically cached
func (m *Manager) CacheNewTableSchema(ctx context.Context, database, tableName string, tableID int64) error {
	cacheKey := generateCacheKey(database, tableName)

	m.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Int64("table_id", tableID).
		Str("cache_key", cacheKey).
		Msg("Proactively caching schema for new table")

	// Check if already cached
	if schema, found := m.cache.Get(cacheKey); found {
		// Update existing entry to mark as new table with high priority
		m.cache.PutWithMetadata(cacheKey, schema, "astha_event", "proactive_cache", tableID, true)

		m.logger.Debug().
			Str("database", database).
			Str("table", tableName).
			Int64("table_id", tableID).
			Msg("Updated existing cache entry for new table with high priority")
		return nil
	}

	// Fetch schema from registry with retry logic
	schema, err := m.retriever.RetrieveSchema(ctx, database, tableName)
	if err != nil {
		m.logger.Error().Err(err).
			Str("database", database).
			Str("table", tableName).
			Int64("table_id", tableID).
			Msg("Failed to fetch schema for new table")
		return errors.New(SchemaManagerRetrievalError, "failed to fetch schema for new table", err).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("table_id", tableID)
	}

	// Store in cache with high priority metadata
	m.cache.PutWithMetadata(cacheKey, schema, "astha_event", "proactive_cache", tableID, true)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int64("table_id", tableID).
		Int("field_count", len(schema.Fields())).
		Msg("Successfully cached schema for new table")

	return nil
}

// InvalidateAndRefreshSchema invalidates and refreshes cached schema
// Requirement 6.2: WHEN table schemas are modified THEN cached schemas SHALL be invalidated and refreshed
func (m *Manager) InvalidateAndRefreshSchema(ctx context.Context, database, tableName string) error {
	cacheKey := generateCacheKey(database, tableName)

	m.logger.Debug().
		Str("database", database).
		Str("table", tableName).
		Str("cache_key", cacheKey).
		Msg("Invalidating and refreshing cached schema")

	// Invalidate existing cache entry
	m.cache.InvalidateAndRefresh(cacheKey)

	// Fetch updated schema from registry
	schema, err := m.retriever.RetrieveSchema(ctx, database, tableName)
	if err != nil {
		m.logger.Error().Err(err).
			Str("database", database).
			Str("table", tableName).
			Msg("Failed to refresh schema after invalidation")
		return errors.New(SchemaManagerRetrievalError, "failed to refresh schema", err).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("operation", "refresh")
	}

	// Store refreshed schema in cache
	m.cache.PutWithMetadata(cacheKey, schema, "registry", "refresh", 0, false)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Int("field_count", len(schema.Fields())).
		Msg("Successfully refreshed cached schema")

	return nil
}

// CleanupDeletedTable removes schema from cache for deleted tables
// Requirement 6.3: WHEN tables are dropped THEN their schemas SHALL be removed from cache
func (m *Manager) CleanupDeletedTable(database, tableName string) {
	cacheKey := generateCacheKey(database, tableName)

	m.cache.Delete(cacheKey)

	m.logger.Info().
		Str("database", database).
		Str("table", tableName).
		Str("cache_key", cacheKey).
		Msg("Cleaned up cache for deleted table")
}

// GetCacheMetrics returns detailed cache metrics
func (m *Manager) GetCacheMetrics() *CacheMetrics {
	return m.cache.GetMetrics()
}

// ClearCache removes all cached schemas
func (m *Manager) ClearCache() {
	m.cache.Clear()
	m.logger.Info().Msg("Schema cache cleared")
}

// GetCacheStats returns cache performance metrics
func (m *Manager) GetCacheStats() CacheStats {
	return m.cache.GetStats()
}

// RegisterWithAstha registers the schema manager as an Astha subscriber
// Requirement 5.1: WHEN Schema Manager starts THEN it SHALL register as an Astha subscriber for table creation events
// Requirement 5.2: WHEN new tables are created THEN Astha SHALL notify all subscribed components
func (m *Manager) RegisterWithAstha(asthaInstance AsthaInterface) error {
	subscriber := NewSchemaManagerSubscriber(m, m.logger)

	componentInfo := subscriber.GetComponentInfo()

	err := asthaInstance.RegisterComponentWithInstance(componentInfo, subscriber)
	if err != nil {
		m.logger.Error().Err(err).
			Str("component", componentInfo.Name).
			Strs("subscriptions", componentInfo.Subscriptions).
			Msg("Failed to register schema manager with Astha")
		return err
	}

	m.logger.Info().
		Str("component", componentInfo.Name).
		Strs("subscriptions", componentInfo.Subscriptions).
		Msg("Successfully registered schema manager with Astha")

	return nil
}

// GetCacheEntryDetails returns detailed information about a specific cache entry
// Requirement 6.1: Update cache entries to track schema source and metadata
func (m *Manager) GetCacheEntryDetails(database, tableName string) (*CacheEntry, bool) {
	cacheKey := generateCacheKey(database, tableName)
	return m.cache.GetCacheEntryMetadata(cacheKey)
}

// GetCacheOverview returns an overview of cache contents with metadata
func (m *Manager) GetCacheOverview() map[string]CacheEntry {
	m.cache.mutex.RLock()
	defer m.cache.mutex.RUnlock()

	overview := make(map[string]CacheEntry)
	for key, entry := range m.cache.cache {
		// Create a copy to prevent external modification
		overview[key] = *entry
	}

	return overview
}

// PerformMaintenanceCleanup performs comprehensive cache maintenance
// Requirement 6.4: WHEN cache refresh fails THEN the system SHALL retry with exponential backoff
func (m *Manager) PerformMaintenanceCleanup() error {
	m.logger.Info().Msg("Starting comprehensive cache maintenance")

	// Perform cleanup with retry logic
	err := m.cache.CleanupWithRetry(5, 200*time.Millisecond)
	if err != nil {
		m.logger.Error().Err(err).
			Msg("Cache maintenance failed")
		return err
	}

	// Log maintenance results
	stats := m.cache.GetStats()
	metrics := m.cache.GetMetrics()

	m.logger.Info().
		Int("cache_size", stats.CacheSize).
		Int64("memory_usage_bytes", stats.MemoryUsage).
		Float64("memory_usage_percent", stats.MemoryPercent).
		Int64("total_evictions", metrics.Evictions).
		Msg("Cache maintenance completed successfully")

	return nil
}

// Shutdown gracefully shuts down the schema manager
func (m *Manager) Shutdown() {
	close(m.stopCleanup)
	m.logger.Info().Msg("Schema manager shutdown")
}

// backgroundCleanup runs periodic cache cleanup and statistics logging
func (m *Manager) backgroundCleanup() {
	ticker := time.NewTicker(m.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Enhanced cleanup with retry logic
			// Requirement 6.5: Add retry logic with exponential backoff for cache operations
			err := m.cache.CleanupWithRetry(3, 100*time.Millisecond)
			if err != nil {
				m.logger.Warn().Err(err).
					Msg("Cache cleanup failed after retries")
			}

			// Clean up expired entries (this is included in CleanupWithRetry but we'll track it separately)
			expiredCount := m.cache.CleanupExpired()
			if expiredCount > 0 {
				m.logger.Debug().
					Int("expired_count", expiredCount).
					Msg("Cleaned up expired schema cache entries")
			}

			// Log cache statistics if enabled
			if m.config.EnableMetrics {
				stats := m.cache.GetStats()
				metrics := m.cache.GetMetrics()

				// Enhanced logging with lifecycle metrics
				// Requirement 6.1: Update cache entries to track schema source and metadata
				m.logger.Info().
					Int64("hit_count", stats.HitCount).
					Int64("miss_count", stats.MissCount).
					Float64("hit_ratio", stats.HitRatio).
					Int("cache_size", stats.CacheSize).
					Int64("evict_count", stats.EvictCount).
					Int64("memory_usage_bytes", stats.MemoryUsage).
					Float64("memory_usage_percent", stats.MemoryPercent).
					Int64("max_memory_bytes", m.config.MaxMemoryBytes).
					Int64("proactive_caches", metrics.ProactiveCaches).
					Int64("refresh_operations", metrics.RefreshOperations).
					Int64("invalidation_count", metrics.InvalidationCount).
					Int64("error_count", metrics.ErrorCount).
					Msg("Schema cache statistics with enhanced lifecycle metrics")
			}

		case <-m.stopCleanup:
			return
		}
	}
}
