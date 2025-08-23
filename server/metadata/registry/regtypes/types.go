package regtypes

import (
	"time"

	"github.com/uptrace/bun"
)

// =============================================================================
// CORE BUN MODELS (Database Tables)
// =============================================================================

// User represents the users table
type User struct {
	bun.BaseModel `bun:"table:users"`

	ID          int64      `bun:"id,pk,autoincrement" json:"id"`
	Username    string     `bun:"username,notnull,unique" json:"username"`
	Email       string     `bun:"email,notnull,unique" json:"email"`
	DisplayName string     `bun:"display_name" json:"display_name"`
	IsActive    bool       `bun:"is_active,notnull,default:true" json:"is_active"`
	IsAdmin     bool       `bun:"is_admin,notnull,default:false" json:"is_admin"`
	CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`
	LastLoginAt *time.Time `bun:"last_login_at" json:"last_login_at,omitempty"`
}

// Database represents the databases table
type Database struct {
	bun.BaseModel `bun:"table:databases"`

	ID          int64     `bun:"id,pk,autoincrement" json:"id"`
	Name        string    `bun:"name,notnull,unique" json:"name"`
	DisplayName string    `bun:"display_name" json:"display_name"`
	Description string    `bun:"description" json:"description"`
	OwnerID     int64     `bun:"owner_id,notnull" json:"owner_id"`
	TableCount  int       `bun:"table_count,notnull,default:0" json:"table_count"`
	CreatedAt   time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt   time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`
}

// Table represents the tables table
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
	CreatedAt   time.Time  `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`
	DeletedAt   *time.Time `bun:"deleted_at" json:"deleted_at,omitempty"`

	// Relations
	Database *Database `bun:"rel:belongs-to,join:database_id=id"`
}

// TableMetadata represents the table_metadata table
type TableMetadata struct {
	bun.BaseModel `bun:"table:table_metadata"`

	ID            int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID       int64     `bun:"table_id,notnull" json:"table_id"`
	SchemaVersion int       `bun:"schema_version,notnull,default:1" json:"schema_version"`
	Schema        []byte    `bun:"schema,type:blob" json:"schema"`
	StorageEngine string    `bun:"storage_engine,notnull" json:"storage_engine"`
	EngineConfig  string    `bun:"engine_config,default:'{}'" json:"engine_config"`
	Format        string    `bun:"format" json:"format"`
	Compression   string    `bun:"compression" json:"compression"`
	PartitionBy   string    `bun:"partition_by" json:"partition_by"`
	SortBy        string    `bun:"sort_by" json:"sort_by"`
	Properties    string    `bun:"properties,default:'{}'" json:"properties"`
	LastModified  time.Time `bun:"last_modified,notnull,default:current_timestamp" json:"last_modified"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt     time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableFile represents the table_files table
type TableFile struct {
	bun.BaseModel `bun:"table:table_files"`

	ID                   int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID              int64     `bun:"table_id,notnull" json:"table_id"`
	FileName             string    `bun:"file_name,notnull" json:"file_name"`
	FilePath             string    `bun:"file_path,notnull" json:"file_path"`
	FileSize             int64     `bun:"file_size,notnull" json:"file_size"`
	FileType             string    `bun:"file_type,notnull" json:"file_type"`
	PartitionPath        string    `bun:"partition_path" json:"partition_path"`
	RowCount             int64     `bun:"row_count,notnull,default:0" json:"row_count"`
	Checksum             string    `bun:"checksum" json:"checksum"`
	IsCompressed         bool      `bun:"is_compressed,notnull,default:false" json:"is_compressed"`
	CreatedAt            time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	ModifiedAt           time.Time `bun:"modified_at,notnull,default:current_timestamp" json:"modified_at"`
	IcebergMetadataState string    `bun:"iceberg_metadata_state,notnull,default:'pending'" json:"iceberg_metadata_state"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableColumn represents the table_columns table
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
	CreatedAt       string `bun:"created_at,notnull" json:"created_at"`
	UpdatedAt       string `bun:"updated_at,notnull" json:"updated_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TablePartition represents the table_partitions table
type TablePartition struct {
	bun.BaseModel `bun:"table:table_partitions"`

	ID            int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID       int64     `bun:"table_id,notnull" json:"table_id"`
	PartitionKey  string    `bun:"partition_key,notnull" json:"partition_key"`
	PartitionPath string    `bun:"partition_path,notnull" json:"partition_path"`
	RowCount      int64     `bun:"row_count,notnull,default:0" json:"row_count"`
	FileCount     int       `bun:"file_count,notnull,default:0" json:"file_count"`
	TotalSize     int64     `bun:"total_size,notnull,default:0" json:"total_size"`
	MinValues     string    `bun:"min_values" json:"min_values"`
	MaxValues     string    `bun:"max_values" json:"max_values"`
	CreatedAt     time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt     time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableIndex represents the table_indexes table
type TableIndex struct {
	bun.BaseModel `bun:"table:table_indexes"`

	ID        int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID   int64     `bun:"table_id,notnull" json:"table_id"`
	IndexName string    `bun:"index_name,notnull" json:"index_name"`
	IndexType string    `bun:"index_type,notnull" json:"index_type"`
	Columns   string    `bun:"columns,notnull" json:"columns"`
	IsUnique  bool      `bun:"is_unique,notnull,default:false" json:"is_unique"`
	IsPrimary bool      `bun:"is_primary,notnull,default:false" json:"is_primary"`
	IsActive  bool      `bun:"is_active,notnull,default:true" json:"is_active"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableConstraint represents the table_constraints table
type TableConstraint struct {
	bun.BaseModel `bun:"table:table_constraints"`

	ID               int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID          int64     `bun:"table_id,notnull" json:"table_id"`
	ConstraintName   string    `bun:"constraint_name,notnull" json:"constraint_name"`
	ConstraintType   string    `bun:"constraint_type,notnull" json:"constraint_type"`
	Columns          string    `bun:"columns,notnull" json:"columns"`
	ReferenceTable   string    `bun:"reference_table" json:"reference_table"`
	ReferenceColumns string    `bun:"reference_columns" json:"reference_columns"`
	OnDelete         string    `bun:"on_delete" json:"on_delete"`
	OnUpdate         string    `bun:"on_update" json:"on_update"`
	IsDeferrable     bool      `bun:"is_deferrable,notnull,default:false" json:"is_deferrable"`
	IsDeferred       bool      `bun:"is_deferred,notnull,default:false" json:"is_deferred"`
	CreatedAt        time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt        time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// TableStatistic represents the table_statistics table
type TableStatistic struct {
	bun.BaseModel `bun:"table:table_statistics"`

	ID          int64     `bun:"id,pk,autoincrement" json:"id"`
	TableID     int64     `bun:"table_id,notnull" json:"table_id"`
	ColumnName  string    `bun:"column_name" json:"column_name"`
	StatType    string    `bun:"stat_type,notnull" json:"stat_type"`
	StatValue   string    `bun:"stat_value" json:"stat_value"`
	StatData    string    `bun:"stat_data" json:"stat_data"`
	LastUpdated time.Time `bun:"last_updated,notnull,default:current_timestamp" json:"last_updated"`
	CreatedAt   time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`

	// Relations
	Table *Table `bun:"rel:belongs-to,join:table_id=id"`
}

// AccessLog represents the access_log table
type AccessLog struct {
	bun.BaseModel `bun:"table:access_log"`

	ID        int64     `bun:"id,pk,autoincrement" json:"id"`
	UserID    *int64    `bun:"user_id" json:"user_id,omitempty"`
	Action    string    `bun:"action,notnull" json:"action"`
	Resource  string    `bun:"resource,notnull" json:"resource"`
	IPAddress string    `bun:"ip_address" json:"ip_address"`
	UserAgent string    `bun:"user_agent" json:"user_agent"`
	Status    int       `bun:"status,notnull" json:"status"`
	Duration  int64     `bun:"duration_ms" json:"duration_ms"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`

	// Relations
	User *User `bun:"rel:belongs-to,join:user_id=id"`
}

// SchemaVersion represents the schema_versions table
type SchemaVersion struct {
	bun.BaseModel `bun:"table:schema_versions"`

	ID          int64     `bun:"id,pk,autoincrement" json:"id"`
	Version     int       `bun:"version,notnull" json:"version"`
	Name        string    `bun:"name,notnull" json:"name"`
	Description string    `bun:"description" json:"description"`
	AppliedAt   time.Time `bun:"applied_at,notnull,default:current_timestamp" json:"applied_at"`
	Checksum    string    `bun:"checksum" json:"checksum"`
	CreatedAt   time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

// TableReference represents a simple table reference
type TableReference struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// FileReference represents a simple file reference
type FileReference struct {
	TableID int64  `json:"table_id"`
	FileID  int64  `json:"file_id"`
	Path    string `json:"path"`
}

// =============================================================================
// CDC AND EVENT TYPES
// =============================================================================

// CDCLogEntry represents a change data capture log entry
type CDCLogEntry struct {
	ID        int64     `json:"id"`
	TableName string    `json:"table_name"`
	Operation string    `json:"operation"` // INSERT, UPDATE, DELETE
	RecordID  int64     `json:"record_id"`
	OldData   string    `json:"old_data,omitempty"` // JSON string of old values
	NewData   string    `json:"new_data,omitempty"` // JSON string of new values
	Timestamp time.Time `json:"timestamp"`
	Processed bool      `json:"processed"`
}

// CDCSetup represents CDC configuration and setup
type CDCSetup struct {
	Enabled      bool     `json:"enabled"`
	Tables       []string `json:"tables"`
	BatchSize    int      `json:"batch_size"`
	PollInterval int      `json:"poll_interval_ms"`
}

// =============================================================================
// CONFIGURATION TYPES
// =============================================================================

// ManagerConfig holds configuration for the metadata manager
type ManagerConfig struct {
	SQLitePath string `json:"sqlite_path"`
	BasePath   string `json:"base_path"`
}
