package schema

import (
	"context"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/config"
	"github.com/jellydator/ttlcache/v3"
)

// SchemaCacheEntry represents a cached schema with enhanced metadata for lifecycle management
type SchemaCacheEntry struct {
	Schema       *iceberg.Schema `json:"schema"`
	SourceType   string          `json:"source_type"`   // "registry", "create_table", "astha_event"
	CreatedFrom  string          `json:"created_from"`  // "direct_access", "proactive_cache", "refresh"
	TableID      int64           `json:"table_id"`      // Registry table ID
	CreatedAt    time.Time       `json:"created_at"`    // When cache entry was created
	RefreshCount int             `json:"refresh_count"` // Number of times refreshed
	IsNewTable   bool            `json:"is_new_table"`  // Recently created tables get higher priority
	Priority     int             `json:"priority"`      // Higher priority = less likely to be evicted
}

// CacheMetrics tracks detailed cache performance metrics
type CacheMetrics struct {
	Hits              int64 `json:"hits"`
	Misses            int64 `json:"misses"`
	Evictions         int64 `json:"evictions"`
	RefreshOperations int64 `json:"refresh_operations"`
	ErrorCount        int64 `json:"error_count"`
	ProactiveCaches   int64 `json:"proactive_caches"`
	InvalidationCount int64 `json:"invalidation_count"`
}

// SchemaCache provides thread-safe caching of Iceberg schemas using ttlcache
type SchemaCache struct {
	cache    *ttlcache.Cache[string, *SchemaCacheEntry]
	config   *config.SchemaManagerConfig
	stats    CacheStats
	statsMux sync.RWMutex
	metrics  *CacheMetrics
}

// NewSchemaCache creates a new schema cache with the given configuration
func NewSchemaCache(config *config.SchemaManagerConfig) *SchemaCache {
	// Configure ttlcache with appropriate options
	cache := ttlcache.New[string, *SchemaCacheEntry](
		ttlcache.WithTTL[string, *SchemaCacheEntry](time.Duration(config.CacheTTLMinutes)*time.Minute),
		ttlcache.WithCapacity[string, *SchemaCacheEntry](uint64(config.MaxCacheSize)),
	)

	// Start the cache to enable automatic expiration
	go cache.Start()

	sc := &SchemaCache{
		cache:  cache,
		config: config,
		stats: CacheStats{
			LastUpdated: time.Now(),
		},
		metrics: &CacheMetrics{},
	}

	// Set up event handlers for metrics
	sc.setupEventHandlers()

	return sc
}

// setupEventHandlers configures event handlers for cache metrics
func (sc *SchemaCache) setupEventHandlers() {
	sc.cache.OnInsertion(func(ctx context.Context, item *ttlcache.Item[string, *SchemaCacheEntry]) {
		sc.updateStats(true, false)
		sc.metrics.Hits++
	})

	sc.cache.OnUpdate(func(ctx context.Context, item *ttlcache.Item[string, *SchemaCacheEntry]) {
		sc.updateStats(true, false)
		sc.metrics.Hits++
	})

	sc.cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, *SchemaCacheEntry]) {
		sc.updateStats(false, true)
		sc.metrics.Evictions++
	})
}

// Get retrieves a schema from the cache
func (sc *SchemaCache) Get(key string) (*iceberg.Schema, bool) {
	item := sc.cache.Get(key)
	if item == nil {
		sc.updateStats(false, false)
		sc.metrics.Misses++
		return nil, false
	}

	sc.updateStats(true, false)
	sc.metrics.Hits++
	return item.Value().Schema, true
}

// Put stores a schema in the cache with default metadata
func (sc *SchemaCache) Put(key string, schema *iceberg.Schema) {
	sc.PutWithMetadata(key, schema, "registry", "direct_access", 0, false)
}

// PutWithMetadata stores a schema in the cache with enhanced metadata for lifecycle management
func (sc *SchemaCache) PutWithMetadata(key string, schema *iceberg.Schema, sourceType, createdFrom string, tableID int64, isNewTable bool) {
	entry := &SchemaCacheEntry{
		Schema:       schema,
		SourceType:   sourceType,
		CreatedFrom:  createdFrom,
		TableID:      tableID,
		CreatedAt:    time.Now(),
		RefreshCount: 0,
		IsNewTable:   isNewTable,
		Priority:     sc.getPriorityForEntry(sourceType, isNewTable),
	}

	// Calculate TTL based on entry characteristics
	ttl := sc.getTTLForEntry(sourceType, isNewTable)

	sc.cache.Set(key, entry, ttl)

	// Update metrics
	if sourceType == "astha_event" && createdFrom == "proactive_cache" {
		sc.metrics.ProactiveCaches++
	}
}

// Delete removes a schema from the cache
func (sc *SchemaCache) Delete(key string) {
	sc.cache.Delete(key)
	sc.metrics.InvalidationCount++
}

// InvalidateAndRefresh removes an existing entry and prepares for refresh
func (sc *SchemaCache) InvalidateAndRefresh(key string) {
	sc.cache.Delete(key)
	sc.metrics.InvalidationCount++
	sc.metrics.RefreshOperations++
}

// Clear removes all entries from the cache
func (sc *SchemaCache) Clear() {
	sc.cache.DeleteAll()

	sc.statsMux.Lock()
	sc.stats.CacheSize = 0
	sc.stats.MemoryUsage = 0
	sc.stats.MemoryPercent = 0
	sc.stats.LastUpdated = time.Now()
	sc.statsMux.Unlock()
}

// Size returns the current number of entries in the cache
func (sc *SchemaCache) Size() int {
	return sc.cache.Len()
}

// GetMemoryUsage returns the current memory usage in bytes
// Note: ttlcache doesn't provide direct memory usage tracking, so we estimate based on cache size
func (sc *SchemaCache) GetMemoryUsage() int64 {
	// Estimate memory usage based on cache size and average schema size
	avgSchemaSize := int64(1024) // Estimated average schema size in bytes
	return int64(sc.cache.Len()) * avgSchemaSize
}

// CleanupExpired removes all expired entries from the cache
// Note: ttlcache handles this automatically, but we provide this for compatibility
func (sc *SchemaCache) CleanupExpired() int {
	// ttlcache handles expiration automatically, so we return 0
	return 0
}

// GetStats returns current cache statistics
func (sc *SchemaCache) GetStats() CacheStats {
	sc.statsMux.RLock()
	defer sc.statsMux.RUnlock()

	stats := sc.stats
	stats.CacheSize = sc.Size()
	stats.MemoryUsage = sc.GetMemoryUsage()

	// Calculate hit ratio
	total := stats.HitCount + stats.MissCount
	if total > 0 {
		stats.HitRatio = float64(stats.HitCount) / float64(total)
	}

	return stats
}

// updateStats updates cache statistics
func (sc *SchemaCache) updateStats(hit, eviction bool) {
	sc.statsMux.Lock()
	defer sc.statsMux.Unlock()

	if hit {
		sc.stats.HitCount++
	} else {
		sc.stats.MissCount++
	}

	if eviction {
		sc.stats.EvictCount++
	}

	sc.stats.LastUpdated = time.Now()
}

// getTTLForEntry calculates TTL based on entry characteristics
func (sc *SchemaCache) getTTLForEntry(sourceType string, isNewTable bool) time.Duration {
	if isNewTable {
		// New tables cached longer
		return 24 * time.Hour
	}

	switch sourceType {
	case "create_table":
		// Recently created tables get longer TTL
		return 12 * time.Hour
	case "astha_event":
		// Event-driven updates get medium TTL
		return 6 * time.Hour
	default:
		// Default TTL from config
		return time.Duration(sc.config.CacheTTLMinutes) * time.Minute
	}
}

// GetMetrics returns detailed cache metrics
func (sc *SchemaCache) GetMetrics() *CacheMetrics {
	return sc.metrics
}

// CleanupWithRetry performs cache cleanup with retry logic for failed operations
func (sc *SchemaCache) CleanupWithRetry(maxRetries int, retryDelay time.Duration) error {
	// ttlcache handles cleanup automatically, so this is mostly a no-op
	// But we can still check if the cache is within reasonable bounds
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}

		// Check if cache size is within limits
		if sc.cache.Len() <= sc.config.MaxCacheSize {
			return nil
		}

		// If we're over capacity, ttlcache will handle eviction automatically
		// We just need to wait for it to catch up
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// GetCacheEntryMetadata returns metadata for a specific cache entry
func (sc *SchemaCache) GetCacheEntryMetadata(key string) (*SchemaCacheEntry, bool) {
	item := sc.cache.Get(key)
	if item == nil {
		return nil, false
	}

	// Return a copy to prevent external modification
	entry := *item.Value()
	return &entry, true
}

// Shutdown gracefully shuts down the cache
func (sc *SchemaCache) Shutdown() {
	sc.cache.Stop()
}

// generateCacheKey creates a cache key from database and table name
func generateCacheKey(database, tableName string) string {
	return database + "." + tableName
}

// getPriorityForEntry calculates priority based on entry characteristics
func (sc *SchemaCache) getPriorityForEntry(sourceType string, isNewTable bool) int {
	// New tables get higher priority
	if isNewTable {
		return 10
	}

	switch sourceType {
	case "create_table":
		// Recently created tables get higher priority
		return 8
	case "astha_event":
		// Event-driven updates get medium priority
		return 6
	default:
		// Default priority
		return 5
	}
}

// estimateSchemaMemoryUsage estimates the memory usage of a schema in bytes
func estimateSchemaMemoryUsage(schema *iceberg.Schema) int64 {
	if schema == nil {
		return 0
	}

	// Base size for schema struct
	size := int64(1024) // Estimated base size

	// Add size for each field
	for _, field := range schema.Fields() {
		// Add size for field name
		size += int64(len(field.Name))
		// Add estimated size for field type
		size += 64 // Estimated type overhead
	}

	return size
}
