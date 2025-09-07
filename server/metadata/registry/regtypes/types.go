package regtypes

import (
	"time"

	"github.com/uptrace/bun"
)

// TimeAuditable provides common timestamp fields for all auditable entities
type TimeAuditable struct {
	CreatedAt time.Time `bun:"created_at" json:"createdAt"`
	UpdatedAt time.Time `bun:"updated_at" json:"updatedAt"`
}

// =============================================================================
// CORE DATABASE ENTITIES
// =============================================================================

// User represents the users table for authentication and access control
type User struct {
	bun.BaseModel `bun:"table:users"`

	ID          int64      `bun:"id,pk,autoincrement" json:"id"`
	Username    string     `bun:"username,notnull,unique" json:"username"`
	Email       string     `bun:"email,notnull,unique" json:"email"`
	DisplayName string     `bun:"display_name" json:"display_name"`
	IsActive    bool       `bun:"is_active,notnull,default:true" json:"is_active"`
	IsAdmin     bool       `bun:"is_admin,notnull,default:false" json:"is_admin"`
	LastLoginAt *time.Time `bun:"last_login_at" json:"last_login_at,omitempty"`

	TimeAuditable

	// Relations
	// User can have many databases, tables, and access logs
}

// Database represents the databases table for organizing tables
type Database struct {
	bun.BaseModel `bun:"table:databases"`

	ID          int64      `bun:"id,pk,autoincrement" json:"id"`
	Name        string     `bun:"name,notnull,unique" json:"name"`
	DisplayName string     `bun:"display_name" json:"display_name"`
	Description string     `bun:"description" json:"description"`
	IsSystem    bool       `bun:"is_system,notnull,default:false" json:"is_system"`
	IsReadOnly  bool       `bun:"is_read_only,notnull,default:false" json:"is_read_only"`
	TableCount  int        `bun:"table_count,notnull,default:0" json:"table_count"`
	TotalSize   int64      `bun:"total_size,notnull,default:0" json:"total_size"`
	DeletedAt   *time.Time `bun:"deleted_at" json:"deleted_at,omitempty"`

	TimeAuditable

	// Relations
	// Database belongs to a User (Owner)
	// Database has many Tables
}

// Table represents the tables table for storing table metadata
type Table struct {
	bun.BaseModel `bun:"table:tables"`

	ID          int64      `bun:"id,pk,autoincrement" json:"id"`
	DatabaseID  int64      `bun:"database_id,notnull" json:"database_id"`
	Name        string     `bun:"name,notnull" json:"name"`
	DisplayName string     `bun:"display_name" json:"display_name"`
	Description string     `bun:"description" json:"description"`
	TableType   string     `bun:"table_type,notnull,default:'user'" json:"table_type"`
	IsTemporary bool       `bun:"is_temporary,notnull,default:false" json:"is_temporary"`
	IsExternal  bool       `bun:"is_external,notnull,default:false" json:"is_external"`
	RowCount    int64      `bun:"row_count,notnull,default:0" json:"row_count"`
	FileCount   int        `bun:"file_count,notnull,default:0" json:"file_count"`
	TotalSize   int64      `bun:"total_size,notnull,default:0" json:"total_size"`
	DeletedAt   *time.Time `bun:"deleted_at" json:"deleted_at,omitempty"`

	TimeAuditable

	// Relations
	Database *Database `bun:"rel:belongs-to,join:database_id=id"`
	// Table has many TableMetadata, TableFile, TableColumn, etc.
}

// =============================================================================
// TABLE METADATA AND SCHEMA
// =============================================================================

// TableMetadata represents the table_metadata table for schema and engine info
type TableMetadata struct {
	bun.BaseModel `bun:"table:table_metadata"`

	ID            int64  `bun:"id,pk,autoincrement" json:"id"`
	TableID       int64  `bun:"table_id,notnull" json:"table_id"`
	SchemaVersion int    `bun:"schema_version,notnull,default:1" json:"schema_version"`
	StorageEngine string `bun:"storage_engine,notnull" json:"storage_engine"`
	EngineConfig  string `bun:"engine_config,default:'{}'" json:"engine_config"`
	Format        string `bun:"format" json:"format"`
	Compression   string `bun:"compression" json:"compression"`
	PartitionBy   string `bun:"partition_by" json:"partition_by"`
	SortBy        string `bun:"sort_by" json:"sort_by"`
	Settings      string `bun:"settings,default:'{}'" json:"settings"` // Renamed from Properties

	// Table configuration (JSON)
	TableConfig string `bun:"table_config,default:'{}'" json:"table_config"`

	// Enhanced partitioning (JSON arrays instead of strings)
	PartitionOrder    string `bun:"partition_order,default:'[]'" json:"partition_order"` // JSON array of partition columns
	PartitionStrategy string `bun:"partition_strategy,default:'column'" json:"partition_strategy"`

	// Enhanced sorting (JSON arrays instead of strings)
	SortOrder    string `bun:"sort_order,default:'[]'" json:"sort_order"` // JSON array of sort columns
	SortStrategy string `bun:"sort_strategy,default:'asc'" json:"sort_strategy"`

	// Performance configuration
	CacheEnabled       bool `bun:"cache_enabled,default:true" json:"cache_enabled"`
	CacheSize          int  `bun:"cache_size,default:1000" json:"cache_size"`
	BatchSize          int  `bun:"batch_size,default:10000" json:"batch_size"`
	ParallelProcessing bool `bun:"parallel_processing,default:true" json:"parallel_processing"`

	// Schema evolution configuration
	StrictValidation    bool `bun:"strict_validation,default:true" json:"strict_validation"`
	StrictCompliance    bool `bun:"strict_compliance,default:true" json:"strict_compliance"`
	AllowTypePromotions bool `bun:"allow_type_promotions,default:false" json:"allow_type_promotions"`
	MaxSchemaVersions   int  `bun:"max_schema_versions,default:1" json:"max_schema_versions"`

	// Validation configuration
	BatchValidationSize int `bun:"batch_validation_size,default:10000" json:"batch_validation_size"`
	MaxValidationErrors int `bun:"max_validation_errors,default:100" json:"max_validation_errors"`

	// Table metadata
	TableUUID       string `bun:"table_uuid" json:"table_uuid"`
	FormatVersion   int    `bun:"format_version,default:2" json:"format_version"`
	LastColumnID    int    `bun:"last_column_id,default:0" json:"last_column_id"`
	LastPartitionID int    `bun:"last_partition_id,default:999" json:"last_partition_id"`

	LastModified time.Time `bun:"last_modified,notnull,default:current_timestamp" json:"last_modified"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableColumn represents the table_columns table for column definitions
type TableColumn struct {
	bun.BaseModel `bun:"table:table_columns"`

	ID              int    `bun:"id,pk,autoincrement" json:"id"`
	TableID         int64  `bun:"table_id,notnull" json:"table_id"`
	ColumnName      string `bun:"column_name,notnull" json:"column_name"`
	DisplayName     string `bun:"display_name" json:"display_name"`
	DataType        string `bun:"data_type,notnull" json:"data_type"`
	IsNullable      bool   `bun:"is_nullable,notnull,default:true" json:"is_nullable"`
	IsPrimary       bool   `bun:"is_primary,notnull,default:false" json:"is_primary"`
	IsUnique        bool   `bun:"is_unique,notnull,default:false" json:"is_unique"`
	DefaultValue    string `bun:"default_value" json:"default_value"`
	Description     string `bun:"description" json:"description"`
	OrdinalPosition int    `bun:"ordinal_position,notnull" json:"ordinal_position"`
	MaxLength       int    `bun:"max_length" json:"max_length"`
	Precision       int    `bun:"precision" json:"precision"`
	Scale           int    `bun:"scale" json:"scale"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// =============================================================================
// TABLE FILES AND STORAGE
// =============================================================================

// TableFile represents the table_files table for file tracking
type TableFile struct {
	bun.BaseModel `bun:"table:table_files"`

	ID                   int64  `bun:"id,pk,autoincrement" json:"id"`
	TableID              int64  `bun:"table_id,notnull" json:"table_id"`
	FileName             string `bun:"file_name,notnull" json:"file_name"`
	FilePath             string `bun:"file_path,notnull" json:"file_path"`
	FileSize             int64  `bun:"file_size,notnull" json:"file_size"`
	FileType             string `bun:"file_type,notnull" json:"file_type"`
	PartitionPath        string `bun:"partition_path" json:"partition_path"`
	RowCount             int64  `bun:"row_count,notnull,default:0" json:"row_count"`
	Checksum             string `bun:"checksum" json:"checksum"`
	IsCompressed         bool   `bun:"is_compressed,notnull,default:false" json:"is_compressed"`
	IcebergMetadataState string `bun:"iceberg_metadata_state,notnull,default:'pending'" json:"iceberg_metadata_state"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// =============================================================================
// TABLE OPTIMIZATION AND INDEXING
// =============================================================================

// TablePartition represents the table_partitions table for partitioning info
type TablePartition struct {
	bun.BaseModel `bun:"table:table_partitions"`

	ID            int64  `bun:"id,pk,autoincrement" json:"id"`
	TableID       int64  `bun:"table_id,notnull" json:"table_id"`
	PartitionKey  string `bun:"partition_key,notnull" json:"partition_key"`
	PartitionPath string `bun:"partition_path,notnull" json:"partition_path"`
	RowCount      int64  `bun:"row_count,notnull,default:0" json:"row_count"`
	FileCount     int    `bun:"file_count,notnull,default:0" json:"file_count"`
	TotalSize     int64  `bun:"total_size,notnull,default:0" json:"total_size"`
	MinValues     string `bun:"min_values" json:"min_values"`
	MaxValues     string `bun:"max_values" json:"max_values"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableIndex represents the table_indexes table for index definitions
type TableIndex struct {
	bun.BaseModel `bun:"table:table_indexes"`

	ID        int64  `bun:"id,pk,autoincrement" json:"id"`
	TableID   int64  `bun:"table_id,notnull" json:"table_id"`
	IndexName string `bun:"index_name,notnull" json:"index_name"`
	IndexType string `bun:"index_type,notnull" json:"index_type"`
	Columns   string `bun:"columns,notnull" json:"columns"`
	IsUnique  bool   `bun:"is_unique,notnull,default:false" json:"is_unique"`
	IsPrimary bool   `bun:"is_primary,notnull,default:false" json:"is_primary"`
	IsActive  bool   `bun:"is_active,notnull,default:true" json:"is_active"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableConstraint represents the table_constraints table for constraint definitions
type TableConstraint struct {
	bun.BaseModel `bun:"table:table_constraints"`

	ID               int64  `bun:"id,pk,autoincrement" json:"id"`
	TableID          int64  `bun:"table_id,notnull" json:"table_id"`
	ConstraintName   string `bun:"constraint_name,notnull" json:"constraint_name"`
	ConstraintType   string `bun:"constraint_type,notnull" json:"constraint_type"`
	Columns          string `bun:"columns,notnull" json:"columns"`
	ReferenceTable   string `bun:"reference_table" json:"reference_table"`
	ReferenceColumns string `bun:"reference_columns" json:"reference_columns"`
	OnDelete         string `bun:"on_delete" json:"on_delete"`
	OnUpdate         string `bun:"on_update" json:"on_update"`
	IsDeferrable     bool   `bun:"is_deferrable,notnull,default:false" json:"is_deferrable"`
	IsDeferred       bool   `bun:"is_deferred,notnull,default:false" json:"is_deferred"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// =============================================================================
// TABLE STATISTICS AND MONITORING
// =============================================================================

// TableStatistic represents the table_statistics table for performance metrics
type TableStatistic struct {
	bun.BaseModel `bun:"table:table_statistics"`

	ID          int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID     int64     `bun:"table_id,notnull" json:"table_id"`
	ColumnName  string    `bun:"column_name" json:"column_name"`
	StatType    string    `bun:"stat_type,notnull" json:"stat_type"`
	StatValue   string    `bun:"stat_value" json:"stat_value"`
	StatData    string    `bun:"stat_data" json:"stat_data"`
	LastUpdated time.Time `bun:"last_updated,notnull,default:current_timestamp" json:"last_updated"`

	TimeAuditable

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// =============================================================================
// SYSTEM AND AUDIT TABLES
// =============================================================================

// AccessLog represents the access_log table for audit trail
type AccessLog struct {
	bun.BaseModel `bun:"table:access_log"`

	ID        int64  `bun:"id,pk,autoincrement" json:"id"`
	UserID    *int64 `bun:"user_id" json:"user_id,omitempty"`
	Action    string `bun:"action,notnull" json:"action"`
	Resource  string `bun:"resource,notnull" json:"resource"`
	IPAddress string `bun:"ip_address" json:"ip_address"`
	UserAgent string `bun:"user_agent" json:"user_agent"`
	Status    int    `bun:"status,notnull" json:"status"`
	Duration  int64  `bun:"duration_ms" json:"duration_ms"`

	TimeAuditable

	// Relations
	User *User `bun:"rel:belongs-to,join:user_id=id"`
}

// SchemaVersion represents the schema_versions table for migration tracking
type SchemaVersion struct {
	bun.BaseModel `bun:"table:schema_versions"`

	ID          int64     `bun:"id,pk,autoincrement" json:"id"`
	Version     int       `bun:"version,notnull" json:"version"`
	Name        string    `bun:"name,notnull" json:"name"`
	Description string    `bun:"description" json:"description"`
	AppliedAt   time.Time `bun:"applied_at,notnull,default:current_timestamp" json:"applied_at"`
	Checksum    string    `bun:"checksum" json:"checksum"`

	TimeAuditable
}

// =============================================================================
// CDC AND EVENT TYPES
// =============================================================================

// ChangeLog represents a raw change from the CDC log table
type ChangeLog struct {
	bun.BaseModel `bun:"table:__cdc_log"`

	ID        int64  `bun:"id,pk,autoincrement" json:"id"`
	Timestamp string `bun:"timestamp,notnull" json:"timestamp"`
	TableName string `bun:"tablename,notnull" json:"tablename"`
	Operation string `bun:"operation,notnull" json:"operation"`
	Before    string `bun:"before" json:"before"` // JSON string of OLD values
	After     string `bun:"after" json:"after"`   // JSON string of NEW values
	CreatedAt string `bun:"created_at,notnull" json:"created_at"`
}
