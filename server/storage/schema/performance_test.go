package schema

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
	"github.com/uptrace/bun"
)

// BenchmarkCacheConcurrentAccess benchmarks cache performance with concurrent access
// Requirement 5.1: WHEN schema is first requested THEN it SHALL be loaded from database and cached
// Requirement 5.2: WHEN schema is subsequently requested THEN it SHALL be served from cache without database query
func BenchmarkCacheConcurrentAccess(b *testing.B) {
	logger := zerolog.Nop()

	testCases := []struct {
		name        string
		goroutines  int
		cacheSize   int
		schemaCount int
		hitRatio    float64 // Expected cache hit ratio
	}{
		{
			name:        "Low Concurrency",
			goroutines:  4,
			cacheSize:   100,
			schemaCount: 50,
			hitRatio:    0.8,
		},
		{
			name:        "Medium Concurrency",
			goroutines:  16,
			cacheSize:   500,
			schemaCount: 200,
			hitRatio:    0.7,
		},
		{
			name:        "High Concurrency",
			goroutines:  64,
			cacheSize:   1000,
			schemaCount: 500,
			hitRatio:    0.6,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create performance schema manager with direct schema access
			schemas := generatePerformanceTestSchemas(tc.schemaCount)

			// Create schema manager with appropriate cache size
			config := &SchemaManagerConfig{
				CacheTTL:        5 * time.Minute,
				MaxCacheSize:    tc.cacheSize,
				MaxMemoryBytes:  100 * 1024 * 1024, // 100MB
				StatsInterval:   1 * time.Minute,
				EnableMetrics:   true,
				EnableLRU:       true,
				EnableMemoryLRU: true,
			}

			manager := NewPerformanceSchemaManager(schemas, config, logger)
			defer manager.Shutdown()

			// Pre-populate cache to achieve desired hit ratio
			ctx := context.Background()
			prePopulateCount := int(float64(tc.schemaCount) * tc.hitRatio)
			for i := 0; i < prePopulateCount; i++ {
				database := fmt.Sprintf("db_%d", i%10)
				tableName := fmt.Sprintf("table_%d", i)
				_, _ = manager.GetSchema(ctx, database, tableName)
			}

			b.ResetTimer()
			b.ReportAllocs()

			// Run concurrent benchmark
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					database := fmt.Sprintf("db_%d", i%10)
					tableName := fmt.Sprintf("table_%d", i%tc.schemaCount)

					_, err := manager.GetSchema(ctx, database, tableName)
					if err != nil {
						b.Errorf("Unexpected error: %v", err)
					}
					i++
				}
			})

			// Report cache statistics
			stats := manager.GetCacheStats()
			b.ReportMetric(stats.HitRatio*100, "hit_ratio_%")
			b.ReportMetric(float64(stats.CacheSize), "cache_size")
			b.ReportMetric(float64(stats.MemoryUsage)/1024/1024, "memory_mb")
		})
	}
}

// BenchmarkCacheMemoryUsage benchmarks memory usage under different cache configurations
// Requirement 5.3: WHEN cache is under memory pressure THEN it SHALL evict least recently used schemas
func BenchmarkCacheMemoryUsage(b *testing.B) {
	logger := zerolog.Nop()

	testCases := []struct {
		name        string
		maxMemoryMB int64
		schemaCount int
		schemaSize  string // "small", "medium", "large"
	}{
		{
			name:        "Small Schemas - 10MB Limit",
			maxMemoryMB: 10,
			schemaCount: 1000,
			schemaSize:  "small",
		},
		{
			name:        "Medium Schemas - 50MB Limit",
			maxMemoryMB: 50,
			schemaCount: 500,
			schemaSize:  "medium",
		},
		{
			name:        "Large Schemas - 100MB Limit",
			maxMemoryMB: 100,
			schemaCount: 200,
			schemaSize:  "large",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create schemas of different sizes
			var schemas map[string]*iceberg.Schema
			switch tc.schemaSize {
			case "small":
				schemas = generateSmallPerformanceSchemas(tc.schemaCount)
			case "medium":
				schemas = generateMediumPerformanceSchemas(tc.schemaCount)
			case "large":
				schemas = generateLargePerformanceSchemas(tc.schemaCount)
			}

			config := &SchemaManagerConfig{
				CacheTTL:        5 * time.Minute,
				MaxCacheSize:    tc.schemaCount * 2, // Allow size-based eviction to be memory-driven
				MaxMemoryBytes:  tc.maxMemoryMB * 1024 * 1024,
				StatsInterval:   1 * time.Minute,
				EnableMetrics:   true,
				EnableLRU:       true,
				EnableMemoryLRU: true,
			}

			manager := NewPerformanceSchemaManager(schemas, config, logger)
			defer manager.Shutdown()

			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			// Measure memory usage while filling cache
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			for i := 0; i < b.N; i++ {
				database := fmt.Sprintf("db_%d", i%10)
				tableName := fmt.Sprintf("table_%d", i%tc.schemaCount)

				_, err := manager.GetSchema(ctx, database, tableName)
				if err != nil {
					b.Errorf("Unexpected error: %v", err)
				}
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)

			// Report memory metrics
			stats := manager.GetCacheStats()
			b.ReportMetric(float64(stats.MemoryUsage)/1024/1024, "cache_memory_mb")
			b.ReportMetric(stats.MemoryPercent, "memory_percent")
			b.ReportMetric(float64(stats.EvictCount), "evictions")
			b.ReportMetric(float64(m2.TotalAlloc-m1.TotalAlloc)/1024/1024, "total_alloc_mb")
		})
	}
}

// TestPerformanceRequirements tests that performance requirements are met
func TestPerformanceRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance requirements test in short mode")
	}

	logger := zerolog.Nop()

	// Test cache hit performance requirement
	t.Run("Cache Hit Performance", func(t *testing.T) {
		schema := createPerformanceTestSchema("test_table", 10, []string{"int32", "string", "boolean"})
		schemas := map[string]*iceberg.Schema{
			"testdb.test_table": schema,
		}

		config := DefaultSchemaManagerConfig()
		manager := NewPerformanceSchemaManager(schemas, config, logger)
		defer manager.Shutdown()

		ctx := context.Background()

		// Pre-cache the schema
		_, err := manager.GetSchema(ctx, "testdb", "test_table")
		if err != nil {
			t.Fatalf("Failed to cache schema: %v", err)
		}

		// Measure cache hit performance
		const iterations = 10000
		start := time.Now()

		for i := 0; i < iterations; i++ {
			_, err := manager.GetSchema(ctx, "testdb", "test_table")
			if err != nil {
				t.Errorf("Cache hit failed: %v", err)
			}
		}

		elapsed := time.Since(start)
		avgTime := elapsed / iterations

		// Cache hits should be very fast (< 10μs per hit)
		maxAcceptableTime := 10 * time.Microsecond
		if avgTime > maxAcceptableTime {
			t.Errorf("Cache hit performance too slow: average %v per hit, expected < %v",
				avgTime, maxAcceptableTime)
		}

		stats := manager.GetCacheStats()
		if stats.HitRatio < 0.99 { // Should be nearly 100% hits
			t.Errorf("Cache hit ratio too low: %f, expected > 0.99", stats.HitRatio)
		}

		t.Logf("Cache hit performance: %v per hit, hit ratio: %f", avgTime, stats.HitRatio)
	})

	// Test concurrent access performance
	t.Run("Concurrent Access Performance", func(t *testing.T) {
		schemas := generatePerformanceTestSchemas(100)

		config := DefaultSchemaManagerConfig()
		manager := NewPerformanceSchemaManager(schemas, config, logger)
		defer manager.Shutdown()

		ctx := context.Background()

		// Test concurrent access
		const goroutines = 32
		const iterations = 1000

		var wg sync.WaitGroup
		start := time.Now()

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					database := fmt.Sprintf("db_%d", i%10)
					tableName := fmt.Sprintf("table_%d", (goroutineID*iterations+i)%100)

					_, err := manager.GetSchema(ctx, database, tableName)
					if err != nil {
						t.Errorf("Concurrent access failed: %v", err)
					}
				}
			}(g)
		}

		wg.Wait()
		elapsed := time.Since(start)

		totalOperations := goroutines * iterations
		avgTime := elapsed / time.Duration(totalOperations)

		// Concurrent operations should be efficient (< 100μs per operation)
		maxAcceptableTime := 100 * time.Microsecond
		if avgTime > maxAcceptableTime {
			t.Errorf("Concurrent access too slow: average %v per operation, expected < %v",
				avgTime, maxAcceptableTime)
		}

		stats := manager.GetCacheStats()
		t.Logf("Concurrent access: %v per operation, hit ratio: %f, cache size: %d",
			avgTime, stats.HitRatio, stats.CacheSize)
	})
}

// Helper functions for test data generation

func generatePerformanceTestSchemas(count int) map[string]*iceberg.Schema {
	schemas := make(map[string]*iceberg.Schema)
	dataTypes := []string{"int32", "string", "boolean", "float64", "timestamp"}

	for i := 0; i < count; i++ {
		database := fmt.Sprintf("db_%d", i%10)
		tableName := fmt.Sprintf("table_%d", i)
		key := fmt.Sprintf("%s.%s", database, tableName)

		fieldCount := 5 + (i % 15) // 5-20 fields
		schemas[key] = createPerformanceTestSchema(tableName, fieldCount, dataTypes)
	}

	return schemas
}

func generateSmallPerformanceSchemas(count int) map[string]*iceberg.Schema {
	schemas := make(map[string]*iceberg.Schema)
	dataTypes := []string{"int32", "string", "boolean"}

	for i := 0; i < count; i++ {
		database := fmt.Sprintf("db_%d", i%10)
		tableName := fmt.Sprintf("table_%d", i)
		key := fmt.Sprintf("%s.%s", database, tableName)

		schemas[key] = createPerformanceTestSchema(tableName, 3+i%5, dataTypes) // 3-8 fields
	}

	return schemas
}

func generateMediumPerformanceSchemas(count int) map[string]*iceberg.Schema {
	schemas := make(map[string]*iceberg.Schema)
	dataTypes := []string{"int32", "int64", "string", "boolean", "float64", "timestamp"}

	for i := 0; i < count; i++ {
		database := fmt.Sprintf("db_%d", i%10)
		tableName := fmt.Sprintf("table_%d", i)
		key := fmt.Sprintf("%s.%s", database, tableName)

		schemas[key] = createPerformanceTestSchema(tableName, 10+i%20, dataTypes) // 10-30 fields
	}

	return schemas
}

func generateLargePerformanceSchemas(count int) map[string]*iceberg.Schema {
	schemas := make(map[string]*iceberg.Schema)
	dataTypes := []string{"int32", "int64", "string", "boolean", "float64", "timestamp", "list<string>", "map<string,int32>"}

	for i := 0; i < count; i++ {
		database := fmt.Sprintf("db_%d", i%10)
		tableName := fmt.Sprintf("table_%d", i)
		key := fmt.Sprintf("%s.%s", database, tableName)

		schemas[key] = createPerformanceTestSchema(tableName, 30+i%50, dataTypes) // 30-80 fields
	}

	return schemas
}

func createPerformanceTestSchema(tableName string, fieldCount int, dataTypes []string) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)

	for i := 0; i < fieldCount; i++ {
		dataType := dataTypes[i%len(dataTypes)]
		var icebergType iceberg.Type

		switch dataType {
		case "int32":
			icebergType = iceberg.PrimitiveTypes.Int32
		case "int64":
			icebergType = iceberg.PrimitiveTypes.Int64
		case "string":
			icebergType = iceberg.PrimitiveTypes.String
		case "boolean":
			icebergType = iceberg.PrimitiveTypes.Bool
		case "float64":
			icebergType = iceberg.PrimitiveTypes.Float64
		case "timestamp":
			icebergType = iceberg.PrimitiveTypes.TimestampTz
		case "list<string>":
			icebergType = &iceberg.ListType{Element: iceberg.PrimitiveTypes.String}
		case "map<string,int32>":
			icebergType = &iceberg.MapType{
				KeyType:   iceberg.PrimitiveTypes.String,
				ValueType: iceberg.PrimitiveTypes.Int32,
			}
		default:
			icebergType = iceberg.PrimitiveTypes.String
		}

		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     fmt.Sprintf("field_%d", i),
			Type:     icebergType,
			Required: i%3 == 0, // Every 3rd field is required
		}
	}

	return iceberg.NewSchema(0, fields...)
}

// TestMetadataManager for testing (avoiding name conflict with existing MockMetadataManager)
type TestMetadataManager struct {
	schemas map[string]*iceberg.Schema
}

func (m *TestMetadataManager) GetBunDB() *bun.DB {
	// Return nil for performance testing - we'll override GetTableColumns directly
	return nil
}

func (m *TestMetadataManager) GetTableColumns(ctx context.Context, database, tableName string) ([]regtypes.TableColumn, error) {
	key := fmt.Sprintf("%s.%s", database, tableName)
	schema, exists := m.schemas[key]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", key)
	}

	columns := make([]regtypes.TableColumn, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columns[i] = regtypes.TableColumn{
			TableID:         1,
			ColumnName:      field.Name,
			DataType:        getPerformanceDataTypeString(field.Type),
			IsNullable:      !field.Required,
			OrdinalPosition: i + 1,
		}
	}

	return columns, nil
}

func getPerformanceDataTypeString(t iceberg.Type) string {
	switch typ := t.(type) {
	case iceberg.PrimitiveType:
		switch typ {
		case iceberg.PrimitiveTypes.Bool:
			return "boolean"
		case iceberg.PrimitiveTypes.Int32:
			return "int32"
		case iceberg.PrimitiveTypes.Int64:
			return "int64"
		case iceberg.PrimitiveTypes.Float32:
			return "float32"
		case iceberg.PrimitiveTypes.Float64:
			return "float64"
		case iceberg.PrimitiveTypes.String:
			return "string"
		case iceberg.PrimitiveTypes.TimestampTz:
			return "timestamptz"
		default:
			return "string"
		}
	case *iceberg.ListType:
		return fmt.Sprintf("list<%s>", getPerformanceDataTypeString(typ.Element))
	case *iceberg.MapType:
		return fmt.Sprintf("map<%s,%s>", getPerformanceDataTypeString(typ.KeyType), getPerformanceDataTypeString(typ.ValueType))
	case *iceberg.StructType:
		var fields []string
		for _, field := range typ.Fields() {
			fields = append(fields, fmt.Sprintf("%s:%s", field.Name, getPerformanceDataTypeString(field.Type)))
		}
		return fmt.Sprintf("struct<%s>", fmt.Sprintf("%v", fields))
	default:
		return "string"
	}
}

// PerformanceSchemaManager is a specialized schema manager for performance testing
type PerformanceSchemaManager struct {
	cache   *SchemaCache
	schemas map[string]*iceberg.Schema
	config  *SchemaManagerConfig
	logger  zerolog.Logger

	// Background cleanup
	stopCleanup chan struct{}
}

// NewPerformanceSchemaManager creates a schema manager optimized for performance testing
func NewPerformanceSchemaManager(schemas map[string]*iceberg.Schema, config *SchemaManagerConfig, logger zerolog.Logger) *PerformanceSchemaManager {
	if config == nil {
		config = DefaultSchemaManagerConfig()
	}

	cache := NewSchemaCache(config)

	manager := &PerformanceSchemaManager{
		cache:       cache,
		schemas:     schemas,
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

// GetSchema retrieves schema from cache or direct lookup (bypassing database)
func (m *PerformanceSchemaManager) GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	cacheKey := generateCacheKey(database, tableName)

	// Try to get from cache first
	if schema, found := m.cache.Get(cacheKey); found {
		return schema, nil
	}

	// Cache miss - get from direct schema map
	schema, exists := m.schemas[cacheKey]
	if !exists {
		return nil, fmt.Errorf("schema not found: %s", cacheKey)
	}

	// Store in cache
	m.cache.Put(cacheKey, schema)

	return schema, nil
}

// GetCacheStats returns cache performance metrics
func (m *PerformanceSchemaManager) GetCacheStats() CacheStats {
	return m.cache.GetStats()
}

// GetCacheMetrics returns detailed cache metrics
func (m *PerformanceSchemaManager) GetCacheMetrics() *CacheMetrics {
	return m.cache.GetMetrics()
}

// Shutdown gracefully shuts down the performance schema manager
func (m *PerformanceSchemaManager) Shutdown() {
	close(m.stopCleanup)
}

// backgroundCleanup runs periodic cache cleanup and statistics logging
func (m *PerformanceSchemaManager) backgroundCleanup() {
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
					Msg("Performance schema cache statistics")
			}

		case <-m.stopCleanup:
			return
		}
	}
}
