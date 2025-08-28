package schema_manager

import (
	"sync"
	"time"
	"unsafe"

	"github.com/apache/iceberg-go"
)

// CacheEntry represents a cached schema with metadata
type CacheEntry struct {
	Schema      *iceberg.Schema `json:"schema"`
	ExpiresAt   time.Time       `json:"expires_at"`
	LastUsed    time.Time       `json:"last_used"`
	HitCount    int64           `json:"hit_count"`
	MemoryBytes int64           `json:"memory_bytes"`
}

// IsExpired checks if the cache entry has expired
func (ce *CacheEntry) IsExpired() bool {
	return time.Now().After(ce.ExpiresAt)
}

// Touch updates the last used time and increments hit count
func (ce *CacheEntry) Touch() {
	ce.LastUsed = time.Now()
	ce.HitCount++
}

// SchemaCache provides thread-safe caching of Iceberg schemas with TTL and LRU eviction
type SchemaCache struct {
	cache       map[string]*CacheEntry
	mutex       sync.RWMutex
	config      *SchemaManagerConfig
	stats       CacheStats
	statsMux    sync.RWMutex
	memoryUsage int64 // Current memory usage in bytes
}

// NewSchemaCache creates a new schema cache with the given configuration
func NewSchemaCache(config *SchemaManagerConfig) *SchemaCache {
	return &SchemaCache{
		cache:  make(map[string]*CacheEntry),
		config: config,
		stats: CacheStats{
			LastUpdated: time.Now(),
		},
	}
}

// Get retrieves a schema from the cache
func (sc *SchemaCache) Get(key string) (*iceberg.Schema, bool) {
	sc.mutex.RLock()
	entry, exists := sc.cache[key]
	sc.mutex.RUnlock()

	if !exists {
		sc.updateStats(false, false)
		return nil, false
	}

	// Check if expired
	if entry.IsExpired() {
		sc.mutex.Lock()
		delete(sc.cache, key)
		sc.mutex.Unlock()
		sc.updateStats(false, false)
		return nil, false
	}

	// Update access time and hit count
	sc.mutex.Lock()
	entry.Touch()
	sc.mutex.Unlock()

	sc.updateStats(true, false)
	return entry.Schema, true
}

// Put stores a schema in the cache
func (sc *SchemaCache) Put(key string, schema *iceberg.Schema) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	// Calculate memory usage for the new entry
	memoryBytes := estimateSchemaMemoryUsage(schema)
	memoryBytes += int64(len(key))                    // Add key size
	memoryBytes += int64(unsafe.Sizeof(CacheEntry{})) // Add entry overhead

	// Check if we need to evict entries (size-based)
	if len(sc.cache) >= sc.config.MaxCacheSize {
		sc.evictLRU()
	}

	// Check if we need to evict entries (memory-based)
	if sc.config.EnableMemoryLRU && sc.memoryUsage+memoryBytes > sc.config.MaxMemoryBytes {
		sc.evictMemoryLRU(memoryBytes)
	}

	// Create new cache entry
	entry := &CacheEntry{
		Schema:      schema,
		ExpiresAt:   time.Now().Add(sc.config.CacheTTL),
		LastUsed:    time.Now(),
		HitCount:    0,
		MemoryBytes: memoryBytes,
	}

	sc.cache[key] = entry
	sc.memoryUsage += memoryBytes
}

// Delete removes a schema from the cache
func (sc *SchemaCache) Delete(key string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if entry, exists := sc.cache[key]; exists {
		sc.memoryUsage -= entry.MemoryBytes
		delete(sc.cache, key)
	}
}

// Clear removes all entries from the cache
func (sc *SchemaCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.cache = make(map[string]*CacheEntry)
	sc.memoryUsage = 0

	sc.statsMux.Lock()
	sc.stats.CacheSize = 0
	sc.stats.MemoryUsage = 0
	sc.stats.MemoryPercent = 0
	sc.stats.LastUpdated = time.Now()
	sc.statsMux.Unlock()
}

// Size returns the current number of entries in the cache
func (sc *SchemaCache) Size() int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	return len(sc.cache)
}

// GetMemoryUsage returns the current memory usage in bytes
func (sc *SchemaCache) GetMemoryUsage() int64 {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	return sc.memoryUsage
}

// evictLRU removes the least recently used entry from the cache
func (sc *SchemaCache) evictLRU() {
	if len(sc.cache) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, entry := range sc.cache {
		if first || entry.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastUsed
			first = false
		}
	}

	if oldestKey != "" {
		if entry, exists := sc.cache[oldestKey]; exists {
			sc.memoryUsage -= entry.MemoryBytes
		}
		delete(sc.cache, oldestKey)
		sc.updateStatsEviction()
	}
}

// evictMemoryLRU removes entries until there's enough memory for the new entry
func (sc *SchemaCache) evictMemoryLRU(requiredBytes int64) {
	if len(sc.cache) == 0 {
		return
	}

	// Continue evicting until we have enough memory
	for sc.memoryUsage+requiredBytes > sc.config.MaxMemoryBytes && len(sc.cache) > 0 {
		var oldestKey string
		var oldestTime time.Time
		first := true

		for key, entry := range sc.cache {
			if first || entry.LastUsed.Before(oldestTime) {
				oldestKey = key
				oldestTime = entry.LastUsed
				first = false
			}
		}

		if oldestKey != "" {
			if entry, exists := sc.cache[oldestKey]; exists {
				sc.memoryUsage -= entry.MemoryBytes
			}
			delete(sc.cache, oldestKey)
			sc.updateStatsEviction()
		} else {
			break // Safety break if no oldest key found
		}
	}
}

// CleanupExpired removes all expired entries from the cache
func (sc *SchemaCache) CleanupExpired() int {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, entry := range sc.cache {
		if now.After(entry.ExpiresAt) {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		if entry, exists := sc.cache[key]; exists {
			sc.memoryUsage -= entry.MemoryBytes
		}
		delete(sc.cache, key)
	}

	return len(expiredKeys)
}

// GetStats returns current cache statistics
func (sc *SchemaCache) GetStats() CacheStats {
	sc.statsMux.RLock()
	defer sc.statsMux.RUnlock()

	stats := sc.stats
	stats.CacheSize = sc.Size()

	// Get current memory usage
	sc.mutex.RLock()
	stats.MemoryUsage = sc.memoryUsage
	sc.mutex.RUnlock()

	// Calculate memory percentage
	if sc.config.MaxMemoryBytes > 0 {
		stats.MemoryPercent = float64(stats.MemoryUsage) / float64(sc.config.MaxMemoryBytes) * 100.0
	}

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

	sc.stats.LastUpdated = time.Now()
}

// updateStatsEviction updates eviction statistics
func (sc *SchemaCache) updateStatsEviction() {
	sc.statsMux.Lock()
	defer sc.statsMux.Unlock()

	sc.stats.EvictCount++
	sc.stats.LastUpdated = time.Now()
}

// generateCacheKey creates a cache key from database and table name
func generateCacheKey(database, tableName string) string {
	return database + "." + tableName
}

// estimateSchemaMemoryUsage estimates the memory usage of a schema in bytes
func estimateSchemaMemoryUsage(schema *iceberg.Schema) int64 {
	if schema == nil {
		return 0
	}

	// Base size for schema struct
	size := int64(unsafe.Sizeof(*schema))

	// Add size for each field
	for _, field := range schema.Fields() {
		// Base field size
		size += int64(unsafe.Sizeof(field))

		// Add size for field name
		size += int64(len(field.Name))

		// Add estimated size for field type (simplified estimation)
		size += estimateTypeSize(field.Type)
	}

	return size
}

// estimateTypeSize estimates the memory usage of an Iceberg type
func estimateTypeSize(t iceberg.Type) int64 {
	// Base size for type interface
	size := int64(64) // Estimated interface overhead

	switch typ := t.(type) {
	case iceberg.PrimitiveType:
		// Primitive types have minimal overhead
		size += int64(8)
	case *iceberg.ListType:
		// List types include element type
		size += int64(16) + estimateTypeSize(typ.Element)
	case *iceberg.MapType:
		// Map types include key and value types
		size += int64(24) + estimateTypeSize(typ.KeyType) + estimateTypeSize(typ.ValueType)
	case *iceberg.StructType:
		// Struct types include all field types
		size += int64(32)
		for _, field := range typ.Fields() {
			size += int64(len(field.Name)) + estimateTypeSize(field.Type)
		}
	default:
		// Default estimation for unknown types
		size += int64(32)
	}

	return size
}
