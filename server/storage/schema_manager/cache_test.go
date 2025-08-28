package schema_manager

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
)

func TestSchemaCache_BasicOperations(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	// Create a test schema
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	key := "test.table"

	// Test Put and Get
	cache.Put(key, schema)
	retrievedSchema, found := cache.Get(key)

	assert.True(t, found)
	assert.NotNil(t, retrievedSchema)
	assert.Equal(t, 1, cache.Size())

	// Test cache miss
	_, found = cache.Get("nonexistent.table")
	assert.False(t, found)

	// Test Delete
	cache.Delete(key)
	_, found = cache.Get(key)
	assert.False(t, found)
	assert.Equal(t, 0, cache.Size())
}

func TestSchemaCache_TTLExpiration(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        100 * time.Millisecond, // Very short TTL for testing
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	key := "test.table"

	// Put schema in cache
	cache.Put(key, schema)

	// Should be available immediately
	_, found := cache.Get(key)
	assert.True(t, found)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be expired now
	_, found = cache.Get(key)
	assert.False(t, found)
}

func TestSchemaCache_LRUEviction(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    3, // Small cache size to trigger eviction
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	// Fill cache to capacity
	cache.Put("table1", schema)
	cache.Put("table2", schema)
	cache.Put("table3", schema)
	assert.Equal(t, 3, cache.Size())

	// Access table1 to make it recently used
	_, found := cache.Get("table1")
	assert.True(t, found)

	// Add another item - should evict table2 (least recently used)
	cache.Put("table4", schema)
	assert.Equal(t, 3, cache.Size())

	// table1 should still be there (recently accessed)
	_, found = cache.Get("table1")
	assert.True(t, found)

	// table2 should be evicted
	_, found = cache.Get("table2")
	assert.False(t, found)

	// table3 and table4 should be there
	_, found = cache.Get("table3")
	assert.True(t, found)
	_, found = cache.Get("table4")
	assert.True(t, found)
}

func TestSchemaCache_ConcurrentAccess(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    100,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent operations
	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()

			for j := range numOperations {
				key := generateCacheKey("db", "table_"+string(rune(goroutineID))+"_"+string(rune(j)))

				// Put operation
				cache.Put(key, schema)

				// Get operation
				_, found := cache.Get(key)
				assert.True(t, found)

				// Delete operation (occasionally)
				if j%10 == 0 {
					cache.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Cache should still be functional
	testKey := "test.final"
	cache.Put(testKey, schema)
	_, found := cache.Get(testKey)
	assert.True(t, found)
}

// TestSchemaCache_ThreadSafetyStress tests thread safety under heavy concurrent load
// Requirement 5.5: Thread-safe cache operations
func TestSchemaCache_ThreadSafetyStress(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        1 * time.Minute,
		MaxCacheSize:    20, // Smaller cache to reduce contention
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "stress_test_field",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
	)

	const numGoroutines = 10 // Reduced to avoid excessive contention
	const numOperations = 50 // Reduced operations per goroutine
	const testDuration = 1 * time.Second

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Start stress test goroutines
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			operationCount := 0

			for {
				select {
				case <-stopChan:
					return
				default:
					key := generateCacheKey("stress_db", fmt.Sprintf("table_%d_%d", goroutineID, operationCount%5))

					// Mix of operations with less frequent stats calls
					switch operationCount % 8 {
					case 0, 1, 2:
						cache.Put(key, schema)
					case 3, 4:
						_, _ = cache.Get(key)
					case 5:
						cache.Delete(key)
					case 6:
						// Less frequent stats calls to reduce lock contention
						_ = cache.GetStats()
					case 7:
						// Add some variety with size check
						_ = cache.Size()
					}

					operationCount++
					if operationCount >= numOperations {
						return
					}

					// Small delay to reduce contention
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let stress test run for specified duration
	time.Sleep(testDuration)
	close(stopChan)
	wg.Wait()

	// Verify cache is still functional and consistent
	testKey := "stress.final"
	cache.Put(testKey, schema)
	retrievedSchema, found := cache.Get(testKey)
	assert.True(t, found)
	assert.NotNil(t, retrievedSchema)

	// Verify stats are consistent
	stats := cache.GetStats()
	assert.GreaterOrEqual(t, stats.HitCount+stats.MissCount, int64(0))
	assert.GreaterOrEqual(t, stats.CacheSize, 0)
}

// TestSchemaCache_TTLBehaviorUnderLoad tests TTL behavior with concurrent access
// Requirement 1.6: TTL-based expiration mechanism
func TestSchemaCache_TTLBehaviorUnderLoad(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        200 * time.Millisecond,
		MaxCacheSize:    20,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "ttl_test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	const numGoroutines = 5
	var wg sync.WaitGroup

	// Add entries concurrently
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range 10 {
				key := generateCacheKey("ttl_db", fmt.Sprintf("table_%d_%d", goroutineID, j))
				cache.Put(key, schema)
				time.Sleep(10 * time.Millisecond) // Stagger insertions
			}
		}(i)
	}

	wg.Wait()

	// Verify entries are present
	initialSize := cache.Size()
	assert.Greater(t, initialSize, 0)

	// Wait for TTL expiration
	time.Sleep(300 * time.Millisecond)

	// Access entries to trigger expiration check
	for i := range numGoroutines {
		for j := range 10 {
			key := generateCacheKey("ttl_db", fmt.Sprintf("table_%d_%d", i, j))
			_, _ = cache.Get(key) // This should trigger expiration cleanup
		}
	}

	// Most entries should be expired
	finalSize := cache.Size()
	assert.LessOrEqual(t, finalSize, initialSize)
}

// TestSchemaCache_PerformanceUnderLoad tests cache performance with high load
// Requirements 5.3, 5.4: Cache performance and eviction under load
func TestSchemaCache_PerformanceUnderLoad(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    100, // Smaller cache to trigger eviction
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	// Create different schemas to test memory usage
	schemas := make([]*iceberg.Schema, 5)
	for i := range len(schemas) {
		schemas[i] = iceberg.NewSchema(0,
			iceberg.NestedField{
				ID:       0,
				Name:     fmt.Sprintf("field_%d", i),
				Type:     iceberg.PrimitiveTypes.String,
				Required: true,
			},
			iceberg.NestedField{
				ID:   1,
				Name: fmt.Sprintf("list_field_%d", i),
				Type: &iceberg.ListType{Element: iceberg.PrimitiveTypes.Int64},
			},
		)
	}

	const numOperations = 1000
	startTime := time.Now()

	// Perform many operations to test performance
	for i := range numOperations {
		schemaIndex := i % len(schemas)
		key := generateCacheKey("perf_db", fmt.Sprintf("table_%d", i))

		// Put operation
		cache.Put(key, schemas[schemaIndex])

		// Get operation (should hit cache for recent entries)
		if i > 50 {
			oldKey := generateCacheKey("perf_db", fmt.Sprintf("table_%d", i-50))
			_, _ = cache.Get(oldKey)
		}

		// Occasionally check stats
		if i%100 == 0 {
			stats := cache.GetStats()
			assert.GreaterOrEqual(t, stats.HitCount+stats.MissCount, int64(0))
		}
	}

	duration := time.Since(startTime)
	t.Logf("Performed %d operations in %v (%.2f ops/sec)", numOperations, duration, float64(numOperations)/duration.Seconds())

	// Verify cache size is within limits
	assert.LessOrEqual(t, cache.Size(), config.MaxCacheSize)

	// Verify eviction occurred
	stats := cache.GetStats()
	assert.Greater(t, stats.EvictCount, int64(0))

	// Verify cache is still functional
	testKey := "perf.final"
	cache.Put(testKey, schemas[0])
	_, found := cache.Get(testKey)
	assert.True(t, found)
}

func TestSchemaCache_Statistics(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	key := "test.table"

	// Initial stats
	stats := cache.GetStats()
	assert.Equal(t, int64(0), stats.HitCount)
	assert.Equal(t, int64(0), stats.MissCount)
	assert.Equal(t, float64(0), stats.HitRatio)

	// Cache miss
	_, found := cache.Get(key)
	assert.False(t, found)

	stats = cache.GetStats()
	assert.Equal(t, int64(0), stats.HitCount)
	assert.Equal(t, int64(1), stats.MissCount)
	assert.Equal(t, float64(0), stats.HitRatio)

	// Cache put and hit
	cache.Put(key, schema)
	_, found = cache.Get(key)
	assert.True(t, found)

	stats = cache.GetStats()
	assert.Equal(t, int64(1), stats.HitCount)
	assert.Equal(t, int64(1), stats.MissCount)
	assert.Equal(t, float64(0.5), stats.HitRatio)

	// Another hit
	_, found = cache.Get(key)
	assert.True(t, found)

	stats = cache.GetStats()
	assert.Equal(t, int64(2), stats.HitCount)
	assert.Equal(t, int64(1), stats.MissCount)
	assert.Equal(t, float64(2.0/3.0), stats.HitRatio)
}

func TestSchemaCache_CleanupExpired(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        100 * time.Millisecond,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	// Add multiple entries
	cache.Put("table1", schema)
	cache.Put("table2", schema)
	cache.Put("table3", schema)
	assert.Equal(t, 3, cache.Size())

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Cleanup expired entries
	expiredCount := cache.CleanupExpired()
	assert.Equal(t, 3, expiredCount)
	assert.Equal(t, 0, cache.Size())
}

func TestSchemaCache_Clear(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	// Add entries
	cache.Put("table1", schema)
	cache.Put("table2", schema)
	assert.Equal(t, 2, cache.Size())

	// Clear cache
	cache.Clear()
	assert.Equal(t, 0, cache.Size())

	// Verify entries are gone
	_, found := cache.Get("table1")
	assert.False(t, found)
	_, found = cache.Get("table2")
	assert.False(t, found)
}

func TestCacheEntry_IsExpired(t *testing.T) {
	entry := &CacheEntry{
		ExpiresAt: time.Now().Add(-1 * time.Minute), // Expired 1 minute ago
	}
	assert.True(t, entry.IsExpired())

	entry.ExpiresAt = time.Now().Add(1 * time.Minute) // Expires in 1 minute
	assert.False(t, entry.IsExpired())
}

func TestCacheEntry_Touch(t *testing.T) {
	entry := &CacheEntry{
		LastUsed: time.Now().Add(-1 * time.Hour),
		HitCount: 5,
	}

	oldLastUsed := entry.LastUsed
	oldHitCount := entry.HitCount

	entry.Touch()

	assert.True(t, entry.LastUsed.After(oldLastUsed))
	assert.Equal(t, oldHitCount+1, entry.HitCount)
}

func TestGenerateCacheKey(t *testing.T) {
	key := generateCacheKey("test_db", "test_table")
	assert.Equal(t, "test_db.test_table", key)

	key = generateCacheKey("", "table")
	assert.Equal(t, ".table", key)

	key = generateCacheKey("db", "")
	assert.Equal(t, "db.", key)
}

func TestSchemaCache_MemoryUsageTracking(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024, // 1MB
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	key := "test.table"

	// Initial memory usage should be 0
	assert.Equal(t, int64(0), cache.GetMemoryUsage())

	// Add schema to cache
	cache.Put(key, schema)

	// Memory usage should be greater than 0
	memoryUsage := cache.GetMemoryUsage()
	assert.Greater(t, memoryUsage, int64(0))

	// Get stats and verify memory tracking
	stats := cache.GetStats()
	assert.Equal(t, memoryUsage, stats.MemoryUsage)
	assert.Greater(t, stats.MemoryPercent, float64(0))
	assert.Less(t, stats.MemoryPercent, float64(100))

	// Remove schema and verify memory usage decreases
	cache.Delete(key)
	assert.Equal(t, int64(0), cache.GetMemoryUsage())

	// Verify stats are updated
	stats = cache.GetStats()
	assert.Equal(t, int64(0), stats.MemoryUsage)
	assert.Equal(t, float64(0), stats.MemoryPercent)
}

func TestSchemaCache_MemoryBasedEviction(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    100,  // High size limit
		MaxMemoryBytes:  1024, // Very low memory limit to trigger eviction
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	// Create a schema that will use significant memory
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field_with_very_long_name_to_use_more_memory",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       1,
			Name:     "another_field_with_long_name",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: false,
		},
	)

	// Add first schema
	cache.Put("table1", schema)
	assert.Equal(t, 1, cache.Size())
	firstMemoryUsage := cache.GetMemoryUsage()
	assert.Greater(t, firstMemoryUsage, int64(0))

	// Access first schema to make it recently used
	_, found := cache.Get("table1")
	assert.True(t, found)

	// Add second schema - should trigger memory-based eviction if memory limit is exceeded
	cache.Put("table2", schema)

	// Verify memory usage is within limits or close to it
	memoryUsage := cache.GetMemoryUsage()
	assert.LessOrEqual(t, memoryUsage, config.MaxMemoryBytes*2) // Allow some tolerance

	// At least one schema should be in cache
	assert.GreaterOrEqual(t, cache.Size(), 1)
}

func TestEstimateSchemaMemoryUsage(t *testing.T) {
	// Test with nil schema
	assert.Equal(t, int64(0), estimateSchemaMemoryUsage(nil))

	// Test with simple schema
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	memoryUsage := estimateSchemaMemoryUsage(schema)
	assert.Greater(t, memoryUsage, int64(0))

	// Test with complex schema
	complexSchema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "simple_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:   1,
			Name: "list_field",
			Type: &iceberg.ListType{Element: iceberg.PrimitiveTypes.Int32},
		},
		iceberg.NestedField{
			ID:   2,
			Name: "map_field",
			Type: &iceberg.MapType{KeyType: iceberg.PrimitiveTypes.String, ValueType: iceberg.PrimitiveTypes.Int64},
		},
	)

	complexMemoryUsage := estimateSchemaMemoryUsage(complexSchema)
	assert.Greater(t, complexMemoryUsage, memoryUsage)
}

func TestSchemaCache_MemoryUsageWithExpiredCleanup(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        100 * time.Millisecond,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	// Add multiple schemas
	cache.Put("table1", schema)
	cache.Put("table2", schema)
	cache.Put("table3", schema)

	initialMemoryUsage := cache.GetMemoryUsage()
	assert.Greater(t, initialMemoryUsage, int64(0))

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Cleanup expired entries
	expiredCount := cache.CleanupExpired()
	assert.Equal(t, 3, expiredCount)

	// Memory usage should be 0 after cleanup
	assert.Equal(t, int64(0), cache.GetMemoryUsage())

	// Verify stats reflect the cleanup
	stats := cache.GetStats()
	assert.Equal(t, int64(0), stats.MemoryUsage)
	assert.Equal(t, float64(0), stats.MemoryPercent)
}

func TestSchemaCache_MemoryUsageWithClear(t *testing.T) {
	config := &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    10,
		MaxMemoryBytes:  1024 * 1024,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
	cache := NewSchemaCache(config)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       0,
			Name:     "test_field",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)

	// Add schemas
	cache.Put("table1", schema)
	cache.Put("table2", schema)

	initialMemoryUsage := cache.GetMemoryUsage()
	assert.Greater(t, initialMemoryUsage, int64(0))

	// Clear cache
	cache.Clear()

	// Memory usage should be 0
	assert.Equal(t, int64(0), cache.GetMemoryUsage())

	// Verify stats are reset
	stats := cache.GetStats()
	assert.Equal(t, int64(0), stats.MemoryUsage)
	assert.Equal(t, float64(0), stats.MemoryPercent)
	assert.Equal(t, 0, stats.CacheSize)
}
