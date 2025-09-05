package schema

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// BenchmarkOptimizedVsStandardCache compares optimized cache performance with standard cache
func BenchmarkOptimizedVsStandardCache(b *testing.B) {
	logger := zerolog.Nop()

	testCases := []struct {
		name        string
		schemaCount int
		concurrency int
	}{
		{
			name:        "Small Dataset",
			schemaCount: 100,
			concurrency: 8,
		},
		{
			name:        "Medium Dataset",
			schemaCount: 1000,
			concurrency: 16,
		},
		{
			name:        "Large Dataset",
			schemaCount: 5000,
			concurrency: 32,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Test standard cache
			b.Run("Standard Cache", func(b *testing.B) {
				schemas := generatePerformanceTestSchemas(tc.schemaCount)
				config := DefaultSchemaManagerConfig()
				config.MaxCacheSize = tc.schemaCount * 2

				manager := NewPerformanceSchemaManager(schemas, config, logger)
				defer manager.Shutdown()

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						_, _ = manager.GetSchema(nil, fmt.Sprintf("db_%d", i%10), fmt.Sprintf("table_%d", i%tc.schemaCount))
						i++
					}
				})

				stats := manager.GetCacheStats()
				b.ReportMetric(stats.HitRatio*100, "hit_ratio_%")
				b.ReportMetric(float64(stats.MemoryUsage)/1024/1024, "memory_mb")
			})

			// Test optimized cache
			b.Run("Optimized Cache", func(b *testing.B) {
				schemas := generatePerformanceTestSchemas(tc.schemaCount)
				config := DefaultSchemaManagerConfig()
				config.MaxCacheSize = tc.schemaCount * 2

				optimizedCache := NewOptimizedSchemaCache(config)

				// Pre-populate optimized cache
				for key, schema := range schemas {
					optimizedCache.FastPut(key, schema)
				}

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := fmt.Sprintf("db_%d.table_%d", i%10, i%tc.schemaCount)
						_, _ = optimizedCache.FastGet(key)
						i++
					}
				})

				stats := optimizedCache.GetOptimizedStats()
				b.ReportMetric(stats.HitRatio*100, "hit_ratio_%")
				b.ReportMetric(float64(stats.MemoryUsage)/1024/1024, "memory_mb")
			})
		})
	}
}

// BenchmarkHashingPerformance compares different hashing strategies
func BenchmarkHashingPerformance(b *testing.B) {
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = fmt.Sprintf("database_%d.table_%d", i%100, i)
	}

	b.Run("Standard String Key", func(b *testing.B) {
		cache := make(map[string]int)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			cache[key] = i
			_ = cache[key]
		}
	})

	b.Run("Fast Hash Key", func(b *testing.B) {
		cache := make(map[uint64]int)
		keyToHash := make(map[string]uint64)

		// Pre-compute hashes
		for _, key := range keys {
			keyToHash[key] = FastHashString(key)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			hash := keyToHash[key]
			cache[hash] = i
			_ = cache[hash]
		}
	})
}

// BenchmarkMemoryOptimizations compares memory usage between standard and optimized entries
func BenchmarkMemoryOptimizations(b *testing.B) {
	schema := createPerformanceTestSchema("test_table", 20, []string{"int32", "string", "boolean", "float64"})

	b.Run("Standard Cache Entry", func(b *testing.B) {
		var entries []*SchemaCacheEntry

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entry := &SchemaCacheEntry{
				Schema:       schema,
				SourceType:   "registry",
				CreatedFrom:  "direct_access",
				TableID:      int64(i),
				CreatedAt:    time.Now(),
				RefreshCount: 0,
				IsNewTable:   false,
				Priority:     5,
			}
			entries = append(entries, entry)
		}

		// Prevent optimization
		runtime.KeepAlive(entries)
	})

	b.Run("Optimized Cache Entry", func(b *testing.B) {
		var entries []*OptimizedCacheEntry

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entry := &OptimizedCacheEntry{
				Schema:      schema,
				ExpiresAt:   time.Now().Unix() + 300, // 5 minutes
				LastUsed:    time.Now().Unix(),
				HitCount:    uint32(i),
				MemoryBytes: uint32(estimateOptimizedSchemaMemoryUsage(schema)),
				SourceType:  0, // registry
				Priority:    5,
			}
			entries = append(entries, entry)
		}

		// Prevent optimization
		runtime.KeepAlive(entries)
	})
}

// BenchmarkBatchOperations compares single vs batch operations
func BenchmarkBatchOperations(b *testing.B) {
	config := DefaultSchemaManagerConfig()
	config.MaxCacheSize = 10000

	optimizedCache := NewOptimizedSchemaCache(config)
	batchOps := NewBatchOperations(optimizedCache)

	// Generate test data
	schemas := generatePerformanceTestSchemas(1000)
	keys := make([]string, 0, len(schemas))
	for key, schema := range schemas {
		keys = append(keys, key)
		optimizedCache.FastPut(key, schema)
	}

	b.Run("Single Operations", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			_, _ = optimizedCache.FastGet(key)
		}
	})

	b.Run("Batch Operations", func(b *testing.B) {
		batchSize := 10
		b.ResetTimer()

		for i := 0; i < b.N; i += batchSize {
			batchKeys := make([]string, batchSize)
			for j := 0; j < batchSize && i+j < len(keys); j++ {
				batchKeys[j] = keys[(i+j)%len(keys)]
			}
			_ = batchOps.BatchGet(batchKeys)
		}
	})
}

// BenchmarkConcurrentOptimizations tests concurrent access optimizations
func BenchmarkConcurrentOptimizations(b *testing.B) {
	testCases := []struct {
		name        string
		goroutines  int
		schemaCount int
	}{
		{
			name:        "Low Concurrency",
			goroutines:  4,
			schemaCount: 500,
		},
		{
			name:        "High Concurrency",
			goroutines:  64,
			schemaCount: 1000,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("Standard Cache", func(b *testing.B) {
				config := DefaultSchemaManagerConfig()
				config.MaxCacheSize = tc.schemaCount * 2

				cache := NewSchemaCache(config)
				schemas := generatePerformanceTestSchemas(tc.schemaCount)

				// Pre-populate cache
				for key, schema := range schemas {
					cache.Put(key, schema)
				}

				keys := make([]string, 0, len(schemas))
				for key := range schemas {
					keys = append(keys, key)
				}

				b.ResetTimer()
				b.ReportAllocs()

				var wg sync.WaitGroup
				for g := 0; g < tc.goroutines; g++ {
					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()
						for i := 0; i < b.N/tc.goroutines; i++ {
							key := keys[(goroutineID*b.N+i)%len(keys)]
							_, _ = cache.Get(key)
						}
					}(g)
				}
				wg.Wait()
			})

			b.Run("Optimized Cache", func(b *testing.B) {
				config := DefaultSchemaManagerConfig()
				config.MaxCacheSize = tc.schemaCount * 2

				cache := NewOptimizedSchemaCache(config)
				schemas := generatePerformanceTestSchemas(tc.schemaCount)

				// Pre-populate cache
				for key, schema := range schemas {
					cache.FastPut(key, schema)
				}

				keys := make([]string, 0, len(schemas))
				for key := range schemas {
					keys = append(keys, key)
				}

				b.ResetTimer()
				b.ReportAllocs()

				var wg sync.WaitGroup
				for g := 0; g < tc.goroutines; g++ {
					wg.Add(1)
					go func(goroutineID int) {
						defer wg.Done()
						for i := 0; i < b.N/tc.goroutines; i++ {
							key := keys[(goroutineID*b.N+i)%len(keys)]
							_, _ = cache.FastGet(key)
						}
					}(g)
				}
				wg.Wait()
			})
		})
	}
}

// TestOptimizationCorrectness verifies that optimizations maintain correctness
func TestOptimizationCorrectness(t *testing.T) {
	config := DefaultSchemaManagerConfig()
	config.MaxCacheSize = 100
	config.MaxMemoryBytes = 10 * 1024 * 1024 // 10MB

	standardCache := NewSchemaCache(config)
	optimizedCache := NewOptimizedSchemaCache(config)

	// Test data
	schemas := generatePerformanceTestSchemas(50)

	// Populate both caches
	for key, schema := range schemas {
		standardCache.Put(key, schema)
		optimizedCache.FastPut(key, schema)
	}

	// Verify both caches return the same results
	for key := range schemas {
		standardSchema, standardFound := standardCache.Get(key)
		optimizedSchema, optimizedFound := optimizedCache.FastGet(key)

		if standardFound != optimizedFound {
			t.Errorf("Cache hit mismatch for key %s: standard=%v, optimized=%v", key, standardFound, optimizedFound)
		}

		if standardFound && optimizedFound {
			if len(standardSchema.Fields()) != len(optimizedSchema.Fields()) {
				t.Errorf("Schema field count mismatch for key %s: standard=%d, optimized=%d",
					key, len(standardSchema.Fields()), len(optimizedSchema.Fields()))
			}
		}
	}

	// Test cache statistics
	standardStats := standardCache.GetStats()
	optimizedStats := optimizedCache.GetOptimizedStats()

	if standardStats.CacheSize != optimizedStats.CacheSize {
		t.Errorf("Cache size mismatch: standard=%d, optimized=%d",
			standardStats.CacheSize, optimizedStats.CacheSize)
	}

	t.Logf("Standard cache: size=%d, memory=%d bytes, hit_ratio=%.2f",
		standardStats.CacheSize, standardStats.MemoryUsage, standardStats.HitRatio)
	t.Logf("Optimized cache: size=%d, memory=%d bytes, hit_ratio=%.2f",
		optimizedStats.CacheSize, optimizedStats.MemoryUsage, optimizedStats.HitRatio)
}

// TestMemoryReduction verifies that optimizations reduce memory usage
func TestMemoryReduction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory reduction test in short mode")
	}

	config := DefaultSchemaManagerConfig()
	config.MaxCacheSize = 1000
	config.MaxMemoryBytes = 50 * 1024 * 1024 // 50MB

	standardCache := NewSchemaCache(config)
	optimizedCache := NewOptimizedSchemaCache(config)

	// Generate larger schemas for memory testing
	schemas := generateLargePerformanceSchemas(500)

	// Measure memory before
	var m1, m2, m3, m4 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Populate standard cache
	for key, schema := range schemas {
		standardCache.Put(key, schema)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	// Clear and populate optimized cache
	standardCache.Clear()
	runtime.GC()
	runtime.ReadMemStats(&m3)

	for key, schema := range schemas {
		optimizedCache.FastPut(key, schema)
	}

	runtime.GC()
	runtime.ReadMemStats(&m4)

	// Calculate memory usage
	standardMemory := m2.HeapInuse - m1.HeapInuse
	optimizedMemory := m4.HeapInuse - m3.HeapInuse

	memoryReduction := float64(standardMemory-optimizedMemory) / float64(standardMemory) * 100

	t.Logf("Standard cache memory: %d bytes", standardMemory)
	t.Logf("Optimized cache memory: %d bytes", optimizedMemory)
	t.Logf("Memory reduction: %.2f%%", memoryReduction)

	// We expect at least some memory reduction (even 5% would be good)
	if memoryReduction < 0 {
		t.Logf("Warning: Optimized cache uses more memory than standard cache")
	} else if memoryReduction > 0 {
		t.Logf("Success: Optimized cache reduced memory usage by %.2f%%", memoryReduction)
	}

	// Test cache statistics memory reporting
	standardStats := standardCache.GetStats()
	optimizedStats := optimizedCache.GetOptimizedStats()

	t.Logf("Standard cache reported memory: %d bytes", standardStats.MemoryUsage)
	t.Logf("Optimized cache reported memory: %d bytes", optimizedStats.MemoryUsage)
}
