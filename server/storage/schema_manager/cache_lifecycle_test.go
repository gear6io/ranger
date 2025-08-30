package schema_manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheLifecycleManagement(t *testing.T) {
	config := DefaultSchemaManagerConfig()
	config.CacheTTL = 1 * time.Hour // Longer TTL for testing

	// Create a test schema
	testSchema := createTestSchema()

	t.Run("CacheWithEnhancedMetadata", func(t *testing.T) {
		cache := NewSchemaCache(config)
		database := "test_db"
		tableName := "new_table"
		tableID := int64(123)
		cacheKey := generateCacheKey(database, tableName)

		// Cache with enhanced metadata
		cache.PutWithMetadata(cacheKey, testSchema, "astha_event", "proactive_cache", tableID, true)

		// Verify schema is cached
		cachedSchema, found := cache.Get(cacheKey)
		require.True(t, found)
		assert.Equal(t, testSchema, cachedSchema)

		// Verify enhanced metadata
		cache.mutex.RLock()
		entry, exists := cache.cache[cacheKey]
		cache.mutex.RUnlock()

		require.True(t, exists)
		assert.Equal(t, "astha_event", entry.SourceType)
		assert.Equal(t, "proactive_cache", entry.CreatedFrom)
		assert.Equal(t, tableID, entry.TableID)
		assert.True(t, entry.IsNewTable)
		assert.Equal(t, 10, entry.Priority) // New tables get priority 10

		// Verify cache metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.ProactiveCaches)
	})

	t.Run("InvalidateAndRefresh", func(t *testing.T) {
		cache := NewSchemaCache(config)
		database := "test_db"
		tableName := "existing_table"
		cacheKey := generateCacheKey(database, tableName)

		// First, put a schema in cache
		cache.Put(cacheKey, testSchema)

		// Verify schema is cached
		_, found := cache.Get(cacheKey)
		assert.True(t, found)

		// Invalidate and prepare for refresh
		cache.InvalidateAndRefresh(cacheKey)

		// Verify schema is removed from cache
		_, found = cache.Get(cacheKey)
		assert.False(t, found)

		// Verify cache metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.RefreshOperations)
		assert.Equal(t, int64(1), metrics.InvalidationCount)
	})

	t.Run("CleanupDeletedTable", func(t *testing.T) {
		cache := NewSchemaCache(config)
		database := "test_db"
		tableName := "deleted_table"
		cacheKey := generateCacheKey(database, tableName)

		// First, put a schema in cache
		cache.Put(cacheKey, testSchema)

		// Verify schema is cached
		_, found := cache.Get(cacheKey)
		assert.True(t, found)

		// Delete the entry
		cache.Delete(cacheKey)

		// Verify schema is removed from cache
		_, found = cache.Get(cacheKey)
		assert.False(t, found)

		// Verify cache metrics
		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.InvalidationCount)
	})

	t.Run("EnhancedCacheEntry", func(t *testing.T) {
		cache := NewSchemaCache(config)
		database := "test_db"
		tableName := "enhanced_table"
		tableID := int64(456)
		cacheKey := generateCacheKey(database, tableName)

		// Put schema with enhanced metadata
		cache.PutWithMetadata(
			cacheKey,
			testSchema,
			"astha_event",
			"proactive_cache",
			tableID,
			true, // isNewTable
		)

		// Get the cache entry directly to verify metadata
		cache.mutex.RLock()
		entry, exists := cache.cache[cacheKey]
		cache.mutex.RUnlock()

		require.True(t, exists)
		assert.Equal(t, "astha_event", entry.SourceType)
		assert.Equal(t, "proactive_cache", entry.CreatedFrom)
		assert.Equal(t, tableID, entry.TableID)
		assert.True(t, entry.IsNewTable)
		assert.Equal(t, 10, entry.Priority) // New tables get priority 10
	})

	t.Run("PriorityBasedEviction", func(t *testing.T) {
		// Create a small cache for testing eviction
		smallConfig := DefaultSchemaManagerConfig()
		smallConfig.MaxCacheSize = 2
		smallConfig.CacheTTL = 1 * time.Hour

		smallCache := NewSchemaCache(smallConfig)

		// Add two entries with different priorities
		lowPrioritySchema := createTestSchema()
		highPrioritySchema := createTestSchema() // Use same schema for simplicity

		// Add low priority entry
		smallCache.PutWithMetadata(
			"db.low_priority",
			lowPrioritySchema,
			"registry",
			"direct_access",
			0,
			false, // not new table
		)

		// Add high priority entry (new table)
		smallCache.PutWithMetadata(
			"db.high_priority",
			highPrioritySchema,
			"astha_event",
			"proactive_cache",
			123,
			true, // new table
		)

		// Add third entry to trigger eviction
		thirdSchema := createTestSchema()
		smallCache.PutWithMetadata(
			"db.third_entry",
			thirdSchema,
			"registry",
			"direct_access",
			0,
			false,
		)

		// Verify high priority entry is still cached
		_, found := smallCache.Get("db.high_priority")
		assert.True(t, found, "High priority entry should not be evicted")

		// Verify low priority entry was evicted
		_, found = smallCache.Get("db.low_priority")
		assert.False(t, found, "Low priority entry should be evicted")

		// Verify third entry is cached
		_, found = smallCache.Get("db.third_entry")
		assert.True(t, found, "Third entry should be cached")
	})

	t.Run("TTLBasedOnSourceType", func(t *testing.T) {
		cache := NewSchemaCache(config)

		// Test different TTLs based on source type
		newTableTTL := cache.getTTLForEntry("astha_event", true)
		createTableTTL := cache.getTTLForEntry("create_table", false)
		asthaEventTTL := cache.getTTLForEntry("astha_event", false)
		defaultTTL := cache.getTTLForEntry("registry", false)

		assert.Equal(t, 24*time.Hour, newTableTTL, "New tables should have 24h TTL")
		assert.Equal(t, 12*time.Hour, createTableTTL, "Create table should have 12h TTL")
		assert.Equal(t, 6*time.Hour, asthaEventTTL, "Astha events should have 6h TTL")
		assert.Equal(t, config.CacheTTL, defaultTTL, "Default should use config TTL")
	})

	t.Run("EvictionScoreCalculation", func(t *testing.T) {
		cache := NewSchemaCache(config)
		now := time.Now()

		// Create entries with different characteristics
		newTableEntry := &CacheEntry{
			LastUsed:     now.Add(-1 * time.Minute),
			HitCount:     5,
			Priority:     10,
			IsNewTable:   true,
			RefreshCount: 0,
			MemoryBytes:  1024,
		}

		oldEntry := &CacheEntry{
			LastUsed:     now.Add(-60 * time.Minute),
			HitCount:     1,
			Priority:     5,
			IsNewTable:   false,
			RefreshCount: 0,
			MemoryBytes:  1024,
		}

		frequentEntry := &CacheEntry{
			LastUsed:     now.Add(-5 * time.Minute),
			HitCount:     100,
			Priority:     8,
			IsNewTable:   false,
			RefreshCount: 5,
			MemoryBytes:  1024,
		}

		newTableScore := cache.calculateEvictionScore(newTableEntry)
		oldScore := cache.calculateEvictionScore(oldEntry)
		frequentScore := cache.calculateEvictionScore(frequentEntry)

		// New table should have highest score (least likely to evict)
		assert.Greater(t, newTableScore, oldScore, "New table should have higher score than old entry")
		assert.Greater(t, newTableScore, frequentScore, "New table should have higher score than frequent entry")

		// Frequent entry should have higher score than old entry
		assert.Greater(t, frequentScore, oldScore, "Frequent entry should have higher score than old entry")
	})
}

func TestCacheMetrics(t *testing.T) {
	config := DefaultSchemaManagerConfig()
	cache := NewSchemaCache(config)
	testSchema := createTestSchema()

	t.Run("MetricsTracking", func(t *testing.T) {
		// Test hit/miss tracking
		_, found := cache.Get("nonexistent")
		assert.False(t, found)

		metrics := cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.Misses)

		// Add entry and test hit
		cache.Put("test_key", testSchema)
		_, found = cache.Get("test_key")
		assert.True(t, found)

		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.Hits)

		// Test invalidation tracking
		cache.Delete("test_key")
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.InvalidationCount)

		// Test proactive cache tracking
		cache.PutWithMetadata("proactive_key", testSchema, "astha_event", "proactive_cache", 123, true)
		metrics = cache.GetMetrics()
		assert.Equal(t, int64(1), metrics.ProactiveCaches)
	})
}

// Helper functions for creating test schemas (using existing ones from astha_subscriber_test.go)
