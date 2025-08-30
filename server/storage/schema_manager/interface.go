package schema_manager

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

// SchemaManagerConfig holds configuration for the schema manager
type SchemaManagerConfig struct {
	CacheTTL        time.Duration `json:"cache_ttl"`         // Default: 5 minutes
	MaxCacheSize    int           `json:"max_cache_size"`    // Default: 1000 schemas
	MaxMemoryBytes  int64         `json:"max_memory_bytes"`  // Default: 100MB
	StatsInterval   time.Duration `json:"stats_interval"`    // Default: 1 minute
	EnableMetrics   bool          `json:"enable_metrics"`    // Default: true
	EnableLRU       bool          `json:"enable_lru"`        // Default: true
	EnableMemoryLRU bool          `json:"enable_memory_lru"` // Default: true
}

// DefaultSchemaManagerConfig returns default configuration
func DefaultSchemaManagerConfig() *SchemaManagerConfig {
	return &SchemaManagerConfig{
		CacheTTL:        5 * time.Minute,
		MaxCacheSize:    1000,
		MaxMemoryBytes:  100 * 1024 * 1024, // 100MB
		StatsInterval:   1 * time.Minute,
		EnableMetrics:   true,
		EnableLRU:       true,
		EnableMemoryLRU: true,
	}
}

// AsthaInterface defines the interface for Astha integration
type AsthaInterface interface {
	RegisterComponentWithInstance(info astha.ComponentInfo, instance astha.Subscriber[any]) error
}
