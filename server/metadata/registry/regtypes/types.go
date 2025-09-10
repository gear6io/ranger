package regtypes

import (
	"time"

	"github.com/uptrace/bun"
)

// TimeAuditable provides common timestamp fields for all auditable entities
type TimeAuditable struct {
	CreatedAt time.Time `bun:"created_at,default:datetime('now')" json:"createdAt"`
	UpdatedAt time.Time `bun:"updated_at,default:datetime('now')" json:"updatedAt"`
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
	TimeAuditable `bun:",inherit"`

	ID         int64  `bun:"id,pk,autoincrement" json:"id"`
	Name       string `bun:"name,notnull,unique" json:"name"`
	IsSystem   bool   `bun:"is_system,notnull,default:false" json:"is_system"`
	TableCount int    `bun:"table_count,notnull,default:0" json:"table_count"`
	TotalSize  int64  `bun:"total_size,notnull,default:0" json:"total_size"`
}

// Table represents the tables table for storing table metadata
type Table struct {
	bun.BaseModel `bun:"table:tables"`
	TimeAuditable `bun:",inherit"`

	ID            int64  `bun:"id,pk,autoincrement" json:"id"`
	DatabaseID    int64  `bun:"database_id,notnull" json:"database_id"`
	Name          string `bun:"name,notnull" json:"name"`
	RowCount      int64  `bun:"row_count,notnull,default:0" json:"row_count"`
	FileCount     int    `bun:"file_count,notnull,default:0" json:"file_count"`
	TotalSize     int64  `bun:"total_size,notnull,default:0" json:"total_size"`
	StorageEngine string `bun:"storage_engine,notnull" json:"storage_engine"`
	Settings      string `bun:"settings,default:'{}'" json:"settings"`
	PartitionBy   string `bun:"partition_by" json:"partition_by"`
	OrderBy       string `bun:"order_by" json:"order_by"`
	Compression   string `bun:"compression,notnull" json:"compression"`

	// Relations
	Database *Database `bun:"rel:belongs-to,join:database_id=id"`
	// Table has many TableMetadata, TableFile, TableColumn, etc.
}

// TableColumn represents the table_columns table for column definitions
type TableColumn struct {
	bun.BaseModel `bun:"table:table_columns"`

	ID              int    `bun:"id,pk,autoincrement" json:"id"`
	TableID         int64  `bun:"table_id,notnull" json:"table_id"`
	ColumnName      string `bun:"column_name,notnull" json:"column_name"`
	DataType        string `bun:"data_type,notnull" json:"data_type"`
	IsNullable      bool   `bun:"is_nullable,notnull,default:true" json:"is_nullable"`
	IsPrimary       bool   `bun:"is_primary,notnull,default:false" json:"is_primary"`
	IsUnique        bool   `bun:"is_unique,notnull,default:false" json:"is_unique"`
	DefaultValue    string `bun:"default_value" json:"default_value"`
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

	ID        int64   `bun:"id,pk,autoincrement" json:"id"`
	Timestamp string  `bun:"timestamp,notnull" json:"timestamp"`
	TableName string  `bun:"tablename,notnull" json:"tablename"`
	Operation string  `bun:"operation,notnull" json:"operation"`
	Before    *string `bun:"before" json:"before,omitempty"` // JSON string of OLD values (nullable for INSERT)
	After     *string `bun:"after" json:"after,omitempty"`   // JSON string of NEW values (nullable for DELETE)
	CreatedAt string  `bun:"created_at,notnull" json:"created_at"`
}
