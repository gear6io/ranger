package context

import (
	"sync"
	"time"

	"github.com/gear6io/ranger/server/schema/config"
	"github.com/gear6io/ranger/server/storage"
)

// TableContext represents the complete context for a table operation
type TableContext struct {
	// Table identification
	TableID   string
	TableName string
	Database  string
	
	// Configuration
	Config *config.TableConfig
	
	// Schema information
	Schema []byte // Raw Iceberg schema bytes
	SchemaVersion int
	
	// Storage context
	StorageEngine storage.FileSystem
	StoragePath   string
	
	// Metadata context
	MetadataManager interface{} // Will be properly typed when we integrate
	
	// Query context
	QueryContext *QueryContext
	
	// Internal state
	mu        sync.RWMutex
	createdAt time.Time
	updatedAt time.Time
}

// QueryContext represents the query-specific context within a table
type QueryContext struct {
	// Schema context
	CurrentSchema []byte // Raw schema bytes
	
	// Type resolution cache
	TypeCache map[string]interface{} // Will be properly typed when we integrate
	
	// Validation context
	ValidationMode string // "strict", "permissive", etc.
	
	// Internal state
	mu sync.RWMutex
}

// NewTableContext creates a new table context
func NewTableContext(tableID, tableName, database string, tableConfig *config.TableConfig) *TableContext {
	now := time.Now()
	
	return &TableContext{
		TableID:        tableID,
		TableName:      tableName,
		Database:       database,
		Config:         tableConfig,
		SchemaVersion:  1,
		QueryContext:   NewQueryContext(),
		createdAt:      now,
		updatedAt:      now,
	}
}

// NewQueryContext creates a new query context
func NewQueryContext() *QueryContext {
	return &QueryContext{
		TypeCache:      make(map[string]interface{}),
		ValidationMode: "strict",
	}
}

// GetTableID returns the table ID
func (tc *TableContext) GetTableID() string {
	return tc.TableID
}

// GetTableName returns the table name
func (tc *TableContext) GetTableName() string {
	return tc.TableName
}

// GetDatabase returns the database name
func (tc *TableContext) GetDatabase() string {
	return tc.Database
}

// GetConfig returns the table configuration
func (tc *TableContext) GetConfig() *config.TableConfig {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.Config
}

// GetSchema returns the current schema
func (tc *TableContext) GetSchema() []byte {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.Schema
}

// SetSchema sets the schema and increments version
func (tc *TableContext) SetSchema(schema []byte) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Schema = schema
	tc.SchemaVersion++
	tc.updatedAt = time.Now()
}

// GetSchemaVersion returns the current schema version
func (tc *TableContext) GetSchemaVersion() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.SchemaVersion
}

// GetStorageEngine returns the storage engine
func (tc *TableContext) GetStorageEngine() storage.FileSystem {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.StorageEngine
}

// SetStorageEngine sets the storage engine
func (tc *TableContext) SetStorageEngine(engine storage.FileSystem) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.StorageEngine = engine
	tc.updatedAt = time.Now()
}

// GetStoragePath returns the storage path
func (tc *TableContext) GetStoragePath() string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.StoragePath
}

// SetStoragePath sets the storage path
func (tc *TableContext) SetStoragePath(path string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.StoragePath = path
	tc.updatedAt = time.Now()
}

// GetQueryContext returns the query context
func (tc *TableContext) GetQueryContext() *QueryContext {
	return tc.QueryContext
}

// GetCreatedAt returns when the context was created
func (tc *TableContext) GetCreatedAt() time.Time {
	return tc.createdAt
}

// GetUpdatedAt returns when the context was last updated
func (tc *TableContext) GetUpdatedAt() time.Time {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.updatedAt
}

// UpdateConfig updates the table configuration
func (tc *TableContext) UpdateConfig(newConfig *config.TableConfig) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	// Validate the new configuration
	if err := newConfig.Validate(); err != nil {
		return err
	}
	
	tc.Config = newConfig
	tc.updatedAt = time.Now()
	return nil
}

// UpdateSettings updates specific configuration settings
func (tc *TableContext) UpdateSettings(updates map[string]interface{}) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	if err := tc.Config.UpdateSettings(updates); err != nil {
		return err
	}
	
	tc.updatedAt = time.Now()
	return nil
}

// Clone creates a deep copy of the table context
func (tc *TableContext) Clone() *TableContext {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	cloned := &TableContext{
		TableID:        tc.TableID,
		TableName:      tc.TableName,
		Database:       tc.Database,
		Config:         tc.Config.Clone(),
		Schema:         make([]byte, len(tc.Schema)),
		SchemaVersion:  tc.SchemaVersion,
		StoragePath:    tc.StoragePath,
		QueryContext:   tc.QueryContext.Clone(),
		createdAt:      tc.createdAt,
		updatedAt:      tc.updatedAt,
	}
	
	// Deep copy schema
	copy(cloned.Schema, tc.Schema)
	
	// Note: StorageEngine and MetadataManager are not cloned as they are references
	
	return cloned
}

// IsValid checks if the context is valid
func (tc *TableContext) IsValid() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	return tc.TableID != "" && 
		   tc.TableName != "" && 
		   tc.Database != "" && 
		   tc.Config != nil
}

// String returns a string representation of the context
func (tc *TableContext) String() string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	return "TableContext{" +
		"TableID: " + tc.TableID + ", " +
		"TableName: " + tc.TableName + ", " +
		"Database: " + tc.Database + ", " +
		"SchemaVersion: " + string(rune(tc.SchemaVersion)) + ", " +
		"StorageEngine: " + tc.Config.StorageEngine + "}"
}

// Clone creates a deep copy of the query context
func (qc *QueryContext) Clone() *QueryContext {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	
	cloned := &QueryContext{
		ValidationMode: qc.ValidationMode,
		TypeCache:      make(map[string]interface{}),
	}
	
	// Deep copy type cache
	for k, v := range qc.TypeCache {
		cloned.TypeCache[k] = v
	}
	
	return cloned
}

// GetTypeFromCache retrieves a type from the cache
func (qc *QueryContext) GetTypeFromCache(key string) (interface{}, bool) {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	
	value, exists := qc.TypeCache[key]
	return value, exists
}

// SetTypeInCache stores a type in the cache
func (qc *QueryContext) SetTypeInCache(key string, value interface{}) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	qc.TypeCache[key] = value
}

// ClearTypeCache clears the type cache
func (qc *QueryContext) ClearTypeCache() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	qc.TypeCache = make(map[string]interface{})
}

// GetValidationMode returns the current validation mode
func (qc *QueryContext) GetValidationMode() string {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	
	return qc.ValidationMode
}

// SetValidationMode sets the validation mode
func (qc *QueryContext) SetValidationMode(mode string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	qc.ValidationMode = mode
}
