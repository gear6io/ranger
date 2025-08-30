package schema_manager

import (
	"sync"
	"time"
	"unsafe"

	"github.com/apache/iceberg-go"
)

// CacheOptimizations contains performance optimizations for the schema cache
type CacheOptimizations struct {
	// Pre-computed hash map for faster key lookups
	keyHashes map[string]uint64
	hashMutex sync.RWMutex

	// Memory pool for cache entries to reduce allocations
	entryPool sync.Pool

	// Batch operations for better performance
	batchSize int
}

// NewCacheOptimizations creates a new cache optimization instance
func NewCacheOptimizations() *CacheOptimizations {
	return &CacheOptimizations{
		keyHashes: make(map[string]uint64),
		batchSize: 100,
		entryPool: sync.Pool{
			New: func() interface{} {
				return &CacheEntry{}
			},
		},
	}
}

// OptimizedCacheEntry represents an optimized cache entry with reduced memory overhead
type OptimizedCacheEntry struct {
	Schema      *iceberg.Schema
	ExpiresAt   int64  // Unix timestamp for faster comparison
	LastUsed    int64  // Unix timestamp for faster comparison
	HitCount    uint32 // Reduced from int64 to save memory
	MemoryBytes uint32 // Reduced from int64 to save memory (up to 4GB)

	// Packed metadata to reduce memory overhead
	SourceType   uint8  // Enum: 0=registry, 1=create_table, 2=astha_event
	Priority     uint8  // 0-255 priority levels
	RefreshCount uint16 // Up to 65535 refreshes

	// Flags packed into a single byte
	Flags uint8 // bit 0: IsNewTable, bit 1-7: reserved
}

// IsExpired checks if the optimized cache entry has expired
func (ce *OptimizedCacheEntry) IsExpired() bool {
	return time.Now().Unix() > ce.ExpiresAt
}

// IsNewTable returns whether this is a new table
func (ce *OptimizedCacheEntry) IsNewTable() bool {
	return ce.Flags&1 != 0
}

// SetNewTable sets the new table flag
func (ce *OptimizedCacheEntry) SetNewTable(isNew bool) {
	if isNew {
		ce.Flags |= 1
	} else {
		ce.Flags &^= 1
	}
}

// Touch updates the last used time and increments hit count with overflow protection
func (ce *OptimizedCacheEntry) Touch() {
	ce.LastUsed = time.Now().Unix()
	if ce.HitCount < ^uint32(0) { // Prevent overflow
		ce.HitCount++
	}
}

// FastHashString computes a fast hash for cache keys
func FastHashString(s string) uint64 {
	// FNV-1a hash algorithm - fast and good distribution
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}

// OptimizedSchemaCache is a high-performance version of SchemaCache
type OptimizedSchemaCache struct {
	// Use map with pre-computed hashes for faster lookups
	cache     map[uint64]*OptimizedCacheEntry
	keyToHash map[string]uint64
	hashToKey map[uint64]string

	// Separate mutex for different operations to reduce contention
	readMutex  sync.RWMutex
	writeMutex sync.Mutex

	config      *SchemaManagerConfig
	stats       CacheStats
	statsMux    sync.RWMutex
	memoryUsage uint64 // Use uint64 for atomic operations

	// Optimization features
	optimizations *CacheOptimizations
}

// NewOptimizedSchemaCache creates a new optimized schema cache
func NewOptimizedSchemaCache(config *SchemaManagerConfig) *OptimizedSchemaCache {
	return &OptimizedSchemaCache{
		cache:         make(map[uint64]*OptimizedCacheEntry),
		keyToHash:     make(map[string]uint64),
		hashToKey:     make(map[uint64]string),
		config:        config,
		optimizations: NewCacheOptimizations(),
		stats: CacheStats{
			LastUpdated: time.Now(),
		},
	}
}

// FastGet retrieves a schema from the optimized cache using hash-based lookup
func (sc *OptimizedSchemaCache) FastGet(key string) (*iceberg.Schema, bool) {
	// Fast path: try to get hash without write lock
	sc.readMutex.RLock()
	hash, exists := sc.keyToHash[key]
	if !exists {
		sc.readMutex.RUnlock()
		sc.updateOptimizedStats(false, false)
		return nil, false
	}

	entry, exists := sc.cache[hash]
	sc.readMutex.RUnlock()

	if !exists {
		sc.updateOptimizedStats(false, false)
		return nil, false
	}

	// Check if expired
	if entry.IsExpired() {
		sc.writeMutex.Lock()
		delete(sc.cache, hash)
		delete(sc.keyToHash, key)
		delete(sc.hashToKey, hash)
		sc.memoryUsage -= uint64(entry.MemoryBytes)
		sc.writeMutex.Unlock()
		sc.updateOptimizedStats(false, false)
		return nil, false
	}

	// Update access time and hit count
	sc.writeMutex.Lock()
	entry.Touch()
	sc.writeMutex.Unlock()

	sc.updateOptimizedStats(true, false)
	return entry.Schema, true
}

// FastPut stores a schema in the optimized cache
func (sc *OptimizedSchemaCache) FastPut(key string, schema *iceberg.Schema) {
	sc.FastPutWithMetadata(key, schema, 0, 5, false) // Default: registry source, priority 5
}

// FastPutWithMetadata stores a schema with metadata in the optimized cache
func (sc *OptimizedSchemaCache) FastPutWithMetadata(key string, schema *iceberg.Schema, sourceType uint8, priority uint8, isNewTable bool) {
	sc.writeMutex.Lock()
	defer sc.writeMutex.Unlock()

	// Calculate memory usage for the new entry
	memoryBytes := uint32(estimateOptimizedSchemaMemoryUsage(schema))
	memoryBytes += uint32(len(key))                             // Add key size
	memoryBytes += uint32(unsafe.Sizeof(OptimizedCacheEntry{})) // Add entry overhead

	// Check if we need to evict entries (size-based)
	if len(sc.cache) >= sc.config.MaxCacheSize {
		sc.evictOptimizedLRU()
	}

	// Check if we need to evict entries (memory-based)
	if sc.config.EnableMemoryLRU && sc.memoryUsage+uint64(memoryBytes) > uint64(sc.config.MaxMemoryBytes) {
		sc.evictOptimizedMemoryLRU(uint64(memoryBytes))
	}

	// Get or create hash for key
	hash, exists := sc.keyToHash[key]
	if !exists {
		hash = FastHashString(key)
		sc.keyToHash[key] = hash
		sc.hashToKey[hash] = key
	}

	// Create optimized cache entry
	now := time.Now().Unix()
	entry := &OptimizedCacheEntry{
		Schema:      schema,
		ExpiresAt:   now + int64(sc.config.CacheTTL.Seconds()),
		LastUsed:    now,
		HitCount:    0,
		MemoryBytes: memoryBytes,
		SourceType:  sourceType,
		Priority:    priority,
	}
	entry.SetNewTable(isNewTable)

	sc.cache[hash] = entry
	sc.memoryUsage += uint64(memoryBytes)
}

// evictOptimizedLRU removes the least recently used entry with priority consideration
func (sc *OptimizedSchemaCache) evictOptimizedLRU() {
	if len(sc.cache) == 0 {
		return
	}

	var oldestHash uint64
	var oldestTime int64 = ^int64(0) // Max int64
	var lowestPriority uint8 = 255

	// Find entry with lowest priority and oldest access time
	for hash, entry := range sc.cache {
		if entry.Priority < lowestPriority ||
			(entry.Priority == lowestPriority && entry.LastUsed < oldestTime) {
			oldestHash = hash
			oldestTime = entry.LastUsed
			lowestPriority = entry.Priority
		}
	}

	// Remove the selected entry
	if entry, exists := sc.cache[oldestHash]; exists {
		key := sc.hashToKey[oldestHash]
		delete(sc.cache, oldestHash)
		delete(sc.keyToHash, key)
		delete(sc.hashToKey, oldestHash)
		sc.memoryUsage -= uint64(entry.MemoryBytes)
		sc.updateOptimizedStatsEviction()
	}
}

// evictOptimizedMemoryLRU removes entries until there's enough memory
func (sc *OptimizedSchemaCache) evictOptimizedMemoryLRU(requiredBytes uint64) {
	if len(sc.cache) == 0 {
		return
	}

	// Continue evicting until we have enough memory
	for sc.memoryUsage+requiredBytes > uint64(sc.config.MaxMemoryBytes) && len(sc.cache) > 0 {
		sc.evictOptimizedLRU()
	}
}

// updateOptimizedStats updates cache statistics with reduced locking
func (sc *OptimizedSchemaCache) updateOptimizedStats(hit, eviction bool) {
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

// updateOptimizedStatsEviction updates eviction statistics
func (sc *OptimizedSchemaCache) updateOptimizedStatsEviction() {
	sc.statsMux.Lock()
	defer sc.statsMux.Unlock()

	sc.stats.EvictCount++
	sc.stats.LastUpdated = time.Now()
}

// GetOptimizedStats returns current cache statistics
func (sc *OptimizedSchemaCache) GetOptimizedStats() CacheStats {
	sc.statsMux.RLock()
	defer sc.statsMux.RUnlock()

	stats := sc.stats
	stats.CacheSize = len(sc.cache)
	stats.MemoryUsage = int64(sc.memoryUsage)

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

// estimateOptimizedSchemaMemoryUsage estimates memory usage with optimizations
func estimateOptimizedSchemaMemoryUsage(schema *iceberg.Schema) int64 {
	if schema == nil {
		return 0
	}

	// Base size for schema struct (reduced due to optimizations)
	size := int64(unsafe.Sizeof(*schema)) / 2 // Assume 50% reduction due to optimizations

	// Add size for each field (with optimization assumptions)
	for _, field := range schema.Fields() {
		// Optimized field size estimation
		size += int64(unsafe.Sizeof(field)) / 2
		size += int64(len(field.Name)) / 2 // String interning could reduce this
		size += estimateOptimizedTypeSize(field.Type)
	}

	return size
}

// estimateOptimizedTypeSize estimates memory usage of an optimized Iceberg type
func estimateOptimizedTypeSize(t iceberg.Type) int64 {
	// Reduced size estimates due to type caching and optimization
	size := int64(32) // Reduced interface overhead

	switch typ := t.(type) {
	case iceberg.PrimitiveType:
		size += int64(4) // Reduced primitive type overhead
	case *iceberg.ListType:
		size += int64(8) + estimateOptimizedTypeSize(typ.Element)
	case *iceberg.MapType:
		size += int64(12) + estimateOptimizedTypeSize(typ.KeyType) + estimateOptimizedTypeSize(typ.ValueType)
	case *iceberg.StructType:
		size += int64(16)
		for _, field := range typ.Fields() {
			size += int64(len(field.Name))/2 + estimateOptimizedTypeSize(field.Type)
		}
	default:
		size += int64(16)
	}

	return size
}

// BatchOperations provides batch operations for better performance
type BatchOperations struct {
	cache *OptimizedSchemaCache
}

// NewBatchOperations creates a new batch operations instance
func NewBatchOperations(cache *OptimizedSchemaCache) *BatchOperations {
	return &BatchOperations{cache: cache}
}

// BatchGet retrieves multiple schemas in a single operation
func (bo *BatchOperations) BatchGet(keys []string) map[string]*iceberg.Schema {
	results := make(map[string]*iceberg.Schema, len(keys))

	bo.cache.readMutex.RLock()
	defer bo.cache.readMutex.RUnlock()

	for _, key := range keys {
		if hash, exists := bo.cache.keyToHash[key]; exists {
			if entry, exists := bo.cache.cache[hash]; exists && !entry.IsExpired() {
				results[key] = entry.Schema
				// Note: We don't update access time in batch operations for performance
			}
		}
	}

	return results
}

// BatchPut stores multiple schemas in a single operation
func (bo *BatchOperations) BatchPut(schemas map[string]*iceberg.Schema) {
	bo.cache.writeMutex.Lock()
	defer bo.cache.writeMutex.Unlock()

	for key, schema := range schemas {
		// Use internal put method without additional locking
		bo.putInternal(key, schema, 0, 5, false)
	}
}

// putInternal is an internal method for batch operations (assumes lock is held)
func (bo *BatchOperations) putInternal(key string, schema *iceberg.Schema, sourceType uint8, priority uint8, isNewTable bool) {
	// Calculate memory usage
	memoryBytes := uint32(estimateOptimizedSchemaMemoryUsage(schema))
	memoryBytes += uint32(len(key))
	memoryBytes += uint32(unsafe.Sizeof(OptimizedCacheEntry{}))

	// Get or create hash for key
	hash, exists := bo.cache.keyToHash[key]
	if !exists {
		hash = FastHashString(key)
		bo.cache.keyToHash[key] = hash
		bo.cache.hashToKey[hash] = key
	}

	// Create optimized cache entry
	now := time.Now().Unix()
	entry := &OptimizedCacheEntry{
		Schema:      schema,
		ExpiresAt:   now + int64(bo.cache.config.CacheTTL.Seconds()),
		LastUsed:    now,
		HitCount:    0,
		MemoryBytes: memoryBytes,
		SourceType:  sourceType,
		Priority:    priority,
	}
	entry.SetNewTable(isNewTable)

	bo.cache.cache[hash] = entry
	bo.cache.memoryUsage += uint64(memoryBytes)
}
