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

// ClearCache removes all cached schemas
func (m *Manager) ClearCache() {
	m.cache.Clear()
	m.logger.Info().Msg("Schema cache cleared")
}

// GetCacheStats returns cache performance metrics
func (m *Manager) GetCacheStats() CacheStats {
	return m.cache.GetStats()
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
			// Clean up expired entries
			expiredCount := m.cache.CleanupExpired()
			if expiredCount > 0 {
				m.logger.Debug().
					Int("expired_count", expiredCount).
					Msg("Cleaned up expired schema cache entries")
			}

			// Log cache statistics if enabled
			if m.config.EnableMetrics {
				stats := m.cache.GetStats()
				m.logger.Info().
					Int64("hit_count", stats.HitCount).
					Int64("miss_count", stats.MissCount).
					Float64("hit_ratio", stats.HitRatio).
					Int("cache_size", stats.CacheSize).
					Int64("evict_count", stats.EvictCount).
					Int64("memory_usage_bytes", stats.MemoryUsage).
					Float64("memory_usage_percent", stats.MemoryPercent).
					Int64("max_memory_bytes", m.config.MaxMemoryBytes).
					Msg("Schema cache statistics")
			}

		case <-m.stopCleanup:
			return
		}
	}
}
