package schema_manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnhancedCacheLifecycleManagement(t *testing.T) {
	config := DefaultSchemaManagerConfig()
	config.CacheTTL = 1 * time.Hour // Longer TTL for testing
	config.MaxCacheSize = 5
	config.MaxMemoryBytes = 1024 * 1024 // 1MB

	t.Run("AutomaticSchemaCachingForNewTables", func(t *testing.T) {
		// Requirement 6.1: WHEN new tables are created THEN their schemas SHALL be automatically cached
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		database := "test_db"
		tableName := "new_table"
		tableID := int64(123)
		cacheKey := generateCacheKey(database, tableName)

		// Cache with enhanced metadata for new table
		cache.PutWithMetadata(cacheKey, testSchema, "astha_event", "proactive_cache", tableID, true)

		// Verify schema is cached
		cachedSchema, found := cache.Get(cacheKey)
		require.True(t, found)
		assert.Equal(t, testSchema, cachedSchema)

		// Verify enhanced metadata
		entry, exists := cache.GetCacheEntryMetadata(cacheKey)
		require.True(t, exists)
		assert.Equal(t, "astha_event", entry.SourceType)
		assert.Equal(t, "proactive_cache", entry.CreatedFrom)
		assert.Equal(t, tableID, entry.TableID)
		assert.True(t, entry.IsNewTable)
		assert.Equal(t, 10, entry.Priority) // New tables get priority 10

		// Verify metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.ProactiveCaches)
	})

	t.Run("CacheInvalidationAndRefreshOnSchemaChanges", func(t *testing.T) {
		// Requirement 6.2: WHEN table schemas are modified THEN cached schemas SHALL be invalidated and refreshed
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		database := "test_db"
		tableName := "existing_table"
		cacheKey := generateCacheKey(database, tableName)

		// First, put a schema in cache
		cache.Put(cacheKey, testSchema)
		_, found := cache.Get(cacheKey)
		assert.True(t, found)

		// Invalidate and prepare for refresh
		cache.InvalidateAndRefresh(cacheKey)

		// Verify schema is removed from cache
		_, found = cache.Get(cacheKey)
		assert.False(t, found)

		// Verify metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.RefreshOperations)
		assert.Equal(t, int64(1), metrics.InvalidationCount)
	})

	t.Run("CacheCleanupForDeletedTables", func(t *testing.T) {
		// Requirement 6.3: WHEN tables are dropped THEN their schemas SHALL be removed from cache
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		database := "test_db"
		tableName := "deleted_table"
		cacheKey := generateCacheKey(database, tableName)

		// First, put a schema in cache
		cache.Put(cacheKey, testSchema)
		_, found := cache.Get(cacheKey)
		assert.True(t, found)

		// Delete the entry (simulating table deletion)
		cache.Delete(cacheKey)

		// Verify schema is removed from cache
		_, found = cache.Get(cacheKey)
		assert.False(t, found)

		// Verify metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.InvalidationCount)
	})

	t.Run("RetryLogicWithExponentialBackoff", func(t *testing.T) {
		// Requirement 6.5: Add retry logic with exponential backoff for cache operations
		cache := NewSchemaCache(config)

		// Test cleanup with retry logic
		err := cache.CleanupWithRetry(3, 10*time.Millisecond)
		assert.NoError(t, err)

		// Add some entries to test memory-based cleanup
		testSchema := createTestSchema()
		for i := 0; i < 3; i++ {
			key := generateCacheKey("db", fmt.Sprintf("table_%d", i))
			cache.Put(key, testSchema)
		}

		// Test cleanup with retry when memory is within limits
		err = cache.CleanupWithRetry(2, 5*time.Millisecond)
		assert.NoError(t, err)
	})

	t.Run("LRUEvictionWithPriorityConsideration", func(t *testing.T) {
		// Requirement 6.6: WHEN cache memory limits are reached THEN LRU eviction SHALL prioritize recently created tables
		smallConfig := DefaultSchemaManagerConfig()
		smallConfig.MaxCacheSize = 3
		smallConfig.CacheTTL = 1 * time.Hour

		cache := NewSchemaCache(smallConfig)
		testSchema := createTestSchema()

		// Add low priority entry
		cache.PutWithMetadata(
			"db.low_priority",
			testSchema,
			"registry",
			"direct_access",
			0,
			false, // not new table
		)

		// Add high priority entry (new table)
		cache.PutWithMetadata(
			"db.high_priority",
			testSchema,
			"astha_event",
			"proactive_cache",
			123,
			true, // new table
		)

		// Add medium priority entry
		cache.PutWithMetadata(
			"db.medium_priority",
			testSchema,
			"registry",
			"refresh",
			0,
			false,
		)

		// Add fourth entry to trigger eviction
		cache.Put("db.fourth_entry", testSchema)

		// Verify all entries are present (should evict one)
		assert.Equal(t, 3, cache.Size())

		// High priority entry should still be cached
		_, found := cache.Get("db.high_priority")
		assert.True(t, found, "High priority entry should not be evicted")
	})

	t.Run("CacheSystemRestartBehavior", func(t *testing.T) {
		// Requirement 6.7: WHEN system restarts THEN schema cache SHALL be empty and populate on demand
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		// Add some entries
		cache.Put("db.table1", testSchema)
		cache.Put("db.table2", testSchema)
		assert.Equal(t, 2, cache.Size())

		// Simulate system restart by clearing cache
		cache.Clear()

		// Verify cache is empty
		assert.Equal(t, 0, cache.Size())
		_, found := cache.Get("db.table1")
		assert.False(t, found)
		_, found = cache.Get("db.table2")
		assert.False(t, found)

		// Verify stats are reset
		stats := cache.GetStats()
		assert.Equal(t, 0, stats.CacheSize)
		assert.Equal(t, int64(0), stats.MemoryUsage)
		assert.Equal(t, 0.0, stats.MemoryPercent)
	})

	t.Run("EnhancedMetadataTracking", func(t *testing.T) {
		// Requirement 6.1: Update cache entries to track schema source and metadata
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		// Test different source types and metadata
		testCases := []struct {
			key         string
			sourceType  string
			createdFrom string
			tableID     int64
			isNewTable  bool
		}{
			{"db.registry_table", "registry", "direct_access", 0, false},
			{"db.create_table", "create_table", "sql_statement", 456, false},
			{"db.astha_table", "astha_event", "proactive_cache", 789, true},
			{"db.refresh_table", "registry", "refresh", 101, false},
		}

		for _, tc := range testCases {
			cache.PutWithMetadata(tc.key, testSchema, tc.sourceType, tc.createdFrom, tc.tableID, tc.isNewTable)

			entry, exists := cache.GetCacheEntryMetadata(tc.key)
			require.True(t, exists, "Entry should exist for key: %s", tc.key)
			assert.Equal(t, tc.sourceType, entry.SourceType)
			assert.Equal(t, tc.createdFrom, entry.CreatedFrom)
			assert.Equal(t, tc.tableID, entry.TableID)
			assert.Equal(t, tc.isNewTable, entry.IsNewTable)
			assert.True(t, entry.CreatedAt.After(time.Time{}))
			assert.Equal(t, 0, entry.RefreshCount) // Initial refresh count
		}
	})

	t.Run("TTLBasedOnEntryCharacteristics", func(t *testing.T) {
		cache := NewSchemaCache(config)

		// Test different TTLs based on source type and characteristics
		newTableTTL := cache.getTTLForEntry("astha_event", true)
		createTableTTL := cache.getTTLForEntry("create_table", false)
		asthaEventTTL := cache.getTTLForEntry("astha_event", false)
		defaultTTL := cache.getTTLForEntry("registry", false)

		assert.Equal(t, 24*time.Hour, newTableTTL, "New tables should have 24h TTL")
		assert.Equal(t, 12*time.Hour, createTableTTL, "Create table should have 12h TTL")
		assert.Equal(t, 6*time.Hour, asthaEventTTL, "Astha events should have 6h TTL")
		assert.Equal(t, config.CacheTTL, defaultTTL, "Default should use config TTL")
	})

	t.Run("ComprehensiveMetricsTracking", func(t *testing.T) {
		cache := NewSchemaCache(config)
		testSchema := createTestSchema()

		// Test all metric types

		// Test miss
		_, found := cache.Get("nonexistent")
		assert.False(t, found)
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.Misses)

		// Test hit
		cache.Put("test_key", testSchema)
		_, found = cache.Get("test_key")
		assert.True(t, found)
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.Hits)

		// Test invalidation
		cache.Delete("test_key")
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.InvalidationCount)

		// Test proactive cache
		cache.PutWithMetadata("proactive_key", testSchema, "astha_event", "proactive_cache", 123, true)
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.ProactiveCaches)

		// Test refresh operation - first add the key
		cache.Put("refresh_key", testSchema)
		cache.InvalidateAndRefresh("refresh_key")
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.RefreshOperations)
		assert.Equal(t, int64(2), metrics.InvalidationCount) // Should be 2 now
	})
}
