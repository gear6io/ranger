package schema

import (
	"context"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/astha"
)

// SchemaManager defines the interface for managing table schemas with enhanced caching lifecycle
type SchemaManager interface {
	// GetSchema retrieves schema from cache or database
	GetSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error)

	// InvalidateSchema removes schema from cache (for Astha integration)
	InvalidateSchema(database, tableName string)

	// ClearCache removes all cached schemas
	ClearCache()

	// GetCacheStats returns cache performance metrics
	GetCacheStats() CacheStats

	// Enhanced lifecycle management methods

	// CacheNewTableSchema automatically caches schema for new tables with high priority
	CacheNewTableSchema(ctx context.Context, database, tableName string, tableID int64) error

	// InvalidateAndRefreshSchema invalidates and refreshes cached schema
	InvalidateAndRefreshSchema(ctx context.Context, database, tableName string) error

	// CleanupDeletedTable removes schema from cache for deleted tables
	CleanupDeletedTable(database, tableName string)

	// GetCacheMetrics returns detailed cache metrics
	GetCacheMetrics() *CacheMetrics

	// Shutdown gracefully shuts down the schema manager
	Shutdown()
}

// CacheStats contains cache performance metrics
type CacheStats struct {
	HitCount      int64     `json:"hit_count"`
	MissCount     int64     `json:"miss_count"`
	HitRatio      float64   `json:"hit_ratio"`
	CacheSize     int       `json:"cache_size"`
	EvictCount    int64     `json:"evict_count"`
	MemoryUsage   int64     `json:"memory_usage"`   // Memory usage in bytes
	MemoryPercent float64   `json:"memory_percent"` // Memory usage as percentage of max
	LastUpdated   time.Time `json:"last_updated"`
}

// AsthaInterface defines the interface for Astha integration
type AsthaInterface interface {
	RegisterComponentWithInstance(info astha.ComponentInfo, instance astha.Subscriber[any]) error
}
