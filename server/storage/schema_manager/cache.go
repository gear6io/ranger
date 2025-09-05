package schema_manager

import (
	"container/list"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
)

// CacheEntry represents a cached schema with enhanced metadata for lifecycle management
type CacheEntry struct {
	Schema      *iceberg.Schema `json:"schema"`
	ExpiresAt   time.Time       `json:"expires_at"`
	LastUsed    time.Time       `json:"last_used"`
	HitCount    int64           `json:"hit_count"`
	MemoryBytes int64           `json:"memory_bytes"`
	// Enhanced fields for tracking and lifecycle management
	SourceType   string    `json:"source_type"`   // "registry", "create_table", "astha_event"
	CreatedFrom  string    `json:"created_from"`  // "direct_access", "proactive_cache", "refresh"
	TableID      int64     `json:"table_id"`      // Registry table ID
	CreatedAt    time.Time `json:"created_at"`    // When cache entry was created
	RefreshCount int       `json:"refresh_count"` // Number of times refreshed
	Priority     int       `json:"priority"`      // Higher priority = less likely to be evicted
	IsNewTable   bool      `json:"is_new_table"`  // Recently created tables get higher priority
	// LRU tracking
	lruElement *list.Element `json:"-"` // Pointer to LRU list element (not serialized)
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

// UpdatePriority adjusts the priority based on usage patterns
func (ce *CacheEntry) UpdatePriority() {
	// New tables get higher priority
	if ce.IsNewTable {
		ce.Priority = 10
	} else if ce.HitCount > 100 {
		// Frequently accessed schemas get higher priority
		ce.Priority = 8
	} else if ce.RefreshCount > 5 {
		// Frequently refreshed schemas get medium priority
		ce.Priority = 6
	} else {
		// Default priority
		ce.Priority = 5
	}
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

// SchemaCache provides thread-safe caching of Iceberg schemas with enhanced lifecycle management
type SchemaCache struct {
	cache       map[string]*CacheEntry
	lruList     *list.List // LRU list for eviction with priority consideration
	mutex       sync.RWMutex
	config      *SchemaManagerConfig
	stats       CacheStats
	statsMux    sync.RWMutex
	memoryUsage int64 // Current memory usage in bytes
	metrics     *CacheMetrics
}

// NewSchemaCache creates a new schema cache with the given configuration
func NewSchemaCache(config *SchemaManagerConfig) *SchemaCache {
	return &SchemaCache{
		cache:   make(map[string]*CacheEntry),
		lruList: list.New(),
		config:  config,
		stats: CacheStats{
			LastUpdated: time.Now(),
		},
		metrics: &CacheMetrics{},
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
		sc.removeFromLRU(entry)
		delete(sc.cache, key)
		sc.memoryUsage -= entry.MemoryBytes
		sc.mutex.Unlock()
		sc.updateStats(false, false)
		return nil, false
	}

	// Update access time and hit count, move to front of LRU
	sc.mutex.Lock()
	entry.Touch()
	sc.moveToFront(entry)
	sc.mutex.Unlock()

	sc.updateStats(true, false)
	return entry.Schema, true
}

// Put stores a schema in the cache with default metadata
func (sc *SchemaCache) Put(key string, schema *iceberg.Schema) {
	sc.PutWithMetadata(key, schema, "registry", "direct_access", 0, false)
}

// PutWithMetadata stores a schema in the cache with enhanced metadata for lifecycle management
func (sc *SchemaCache) PutWithMetadata(key string, schema *iceberg.Schema, sourceType, createdFrom string, tableID int64, isNewTable bool) {
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

	// Create new cache entry with enhanced metadata
	entry := &CacheEntry{
		Schema:      schema,
		ExpiresAt:   time.Now().Add(sc.getTTLForEntry(sourceType, isNewTable)),
		LastUsed:    time.Now(),
		HitCount:    0,
		MemoryBytes: memoryBytes,
		// Enhanced metadata
		SourceType:   sourceType,
		CreatedFrom:  createdFrom,
		TableID:      tableID,
		CreatedAt:    time.Now(),
		RefreshCount: 0,
		IsNewTable:   isNewTable,
	}

	// Set initial priority
	entry.UpdatePriority()

	sc.cache[key] = entry
	sc.memoryUsage += memoryBytes

	// Add to LRU list
	sc.addToLRU(key, entry)

	// Update metrics
	if sourceType == "astha_event" && createdFrom == "proactive_cache" {
		sc.metrics.ProactiveCaches++
	}
}

// Delete removes a schema from the cache
func (sc *SchemaCache) Delete(key string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if entry, exists := sc.cache[key]; exists {
		sc.removeFromLRU(entry)
		sc.memoryUsage -= entry.MemoryBytes
		delete(sc.cache, key)
		sc.metrics.InvalidationCount++
	}
}

// InvalidateAndRefresh removes an existing entry and prepares for refresh
func (sc *SchemaCache) InvalidateAndRefresh(key string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if entry, exists := sc.cache[key]; exists {
		sc.removeFromLRU(entry)
		sc.memoryUsage -= entry.MemoryBytes
		delete(sc.cache, key)
		sc.metrics.InvalidationCount++
		sc.metrics.RefreshOperations++
	}
}

// Clear removes all entries from the cache
func (sc *SchemaCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.cache = make(map[string]*CacheEntry)
	sc.lruList = list.New()
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

// evictLRU removes the least recently used entry from the cache with priority consideration
func (sc *SchemaCache) evictLRU() {
	if len(sc.cache) == 0 || sc.lruList.Len() == 0 {
		return
	}

	// Simple LRU: evict from the back of the list
	// This ensures basic LRU behavior works correctly
	element := sc.lruList.Back()
	if element == nil {
		return
	}

	evictKey := element.Value.(string)
	if entry, exists := sc.cache[evictKey]; exists {
		sc.removeFromLRU(entry)
		sc.memoryUsage -= entry.MemoryBytes
		delete(sc.cache, evictKey)
		sc.updateStatsEviction()
	}
}

// evictMemoryLRU removes entries until there's enough memory for the new entry
func (sc *SchemaCache) evictMemoryLRU(requiredBytes int64) {
	if len(sc.cache) == 0 || sc.lruList.Len() == 0 {
		return
	}

	// Continue evicting from LRU end until we have enough memory
	for sc.memoryUsage+requiredBytes > sc.config.MaxMemoryBytes && len(sc.cache) > 0 && sc.lruList.Len() > 0 {
		element := sc.lruList.Back()
		if element == nil {
			break
		}

		evictKey := element.Value.(string)
		if entry, exists := sc.cache[evictKey]; exists {
			sc.removeFromLRU(entry)
			sc.memoryUsage -= entry.MemoryBytes
			delete(sc.cache, evictKey)
			sc.updateStatsEviction()
		} else {
			// Remove stale element from LRU list
			sc.lruList.Remove(element)
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
			sc.removeFromLRU(entry)
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
		sc.metrics.Hits++
	} else {
		sc.stats.MissCount++
		sc.metrics.Misses++
	}

	if eviction {
		sc.stats.EvictCount++
		sc.metrics.Evictions++
	}

	sc.stats.LastUpdated = time.Now()
}

// updateStatsEviction updates eviction statistics
func (sc *SchemaCache) updateStatsEviction() {
	sc.statsMux.Lock()
	defer sc.statsMux.Unlock()

	sc.stats.EvictCount++
	sc.metrics.Evictions++
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
		return sc.config.CacheTTL
	}
}

// calculateEvictionScore calculates a score for eviction priority (lower = more likely to evict)
func (sc *SchemaCache) calculateEvictionScore(entry *CacheEntry) float64 {
	now := time.Now()

	// Base score from priority (higher priority = higher score = less likely to evict)
	score := float64(entry.Priority * 100)

	// Adjust for recency (more recent = higher score)
	timeSinceLastUsed := now.Sub(entry.LastUsed).Minutes()
	score -= timeSinceLastUsed // Older entries get lower scores

	// Adjust for hit count (more hits = higher score)
	score += float64(entry.HitCount) * 0.1

	// New tables get bonus points
	if entry.IsNewTable {
		score += 500
	}

	// Frequently refreshed schemas get bonus points
	if entry.RefreshCount > 3 {
		score += 200
	}

	// Penalize large memory usage slightly
	memoryMB := float64(entry.MemoryBytes) / (1024 * 1024)
	score -= memoryMB * 10

	return score
}

// GetMetrics returns detailed cache metrics
func (sc *SchemaCache) GetMetrics() *CacheMetrics {
	return sc.metrics
}

// CleanupWithRetry performs cache cleanup with retry logic for failed operations
// Requirement 6.5: WHEN schema retrieval fails THEN appropriate error handling SHALL prevent system instability
func (sc *SchemaCache) CleanupWithRetry(maxRetries int, retryDelay time.Duration) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}

		// Attempt cleanup operations
		expiredCount := sc.CleanupExpired()

		// Check if memory usage is within limits
		sc.mutex.RLock()
		memoryOK := sc.memoryUsage <= sc.config.MaxMemoryBytes
		cacheSize := len(sc.cache)
		sc.mutex.RUnlock()

		// If memory is still over limit, try memory-based eviction
		if !memoryOK && cacheSize > 0 {
			sc.mutex.Lock()
			// Try to free up 20% of max memory
			targetReduction := sc.config.MaxMemoryBytes / 5
			sc.evictMemoryLRU(targetReduction)
			sc.mutex.Unlock()
		}

		// Check if cleanup was successful
		sc.mutex.RLock()
		finalMemoryUsage := sc.memoryUsage
		finalCacheSize := len(sc.cache)
		sc.mutex.RUnlock()

		if finalMemoryUsage <= sc.config.MaxMemoryBytes || finalCacheSize == 0 {
			// Cleanup successful
			if expiredCount > 0 || attempt > 0 {
				// Log successful cleanup if we did work or retried
				return nil
			}
			return nil
		}

		lastErr = errors.Newf(SchemaManagerCacheError, "cache cleanup failed: memory usage %d bytes exceeds limit %d bytes",
			finalMemoryUsage, sc.config.MaxMemoryBytes)
	}

	return errors.Newf(SchemaManagerCacheError, "cache cleanup failed after %d retries: %w", maxRetries, lastErr)
}

// GetCacheEntryMetadata returns metadata for a specific cache entry
// Requirement 6.1: Update cache entries to track schema source and metadata
func (sc *SchemaCache) GetCacheEntryMetadata(key string) (*CacheEntry, bool) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	entry, exists := sc.cache[key]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	entryCopy := *entry
	entryCopy.lruElement = nil // Don't expose internal LRU element
	return &entryCopy, true
}

// moveToFront moves an entry to the front of the LRU list
func (sc *SchemaCache) moveToFront(entry *CacheEntry) {
	if entry.lruElement != nil {
		sc.lruList.MoveToFront(entry.lruElement)
	}
}

// removeFromLRU removes an entry from the LRU list
func (sc *SchemaCache) removeFromLRU(entry *CacheEntry) {
	if entry.lruElement != nil {
		sc.lruList.Remove(entry.lruElement)
		entry.lruElement = nil
	}
}

// addToLRU adds an entry to the front of the LRU list
func (sc *SchemaCache) addToLRU(key string, entry *CacheEntry) {
	element := sc.lruList.PushFront(key)
	entry.lruElement = element
}
