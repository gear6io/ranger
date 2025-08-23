package registry

import "github.com/TFMV/icebox/server/metadata/registry/regtypes"

// =============================================================================
// CORE REGISTRY TYPES
// =============================================================================

// UserInfo represents user information from the users table
type UserInfo struct {
	ID          int64  `json:"id"`
	Username    string `json:"username"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	IsActive    bool   `json:"is_active"`
	IsAdmin     bool   `json:"is_admin"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	LastLoginAt string `json:"last_login_at,omitempty"`
}

// AccessLog represents access logging information
type AccessLog struct {
	ID        int64  `json:"id"`
	UserID    *int64 `json:"user_id,omitempty"`
	Action    string `json:"action"`
	Resource  string `json:"resource"`
	IPAddress string `json:"ip_address"`
	UserAgent string `json:"user_agent"`
	Status    int    `json:"status"`
	Duration  int64  `json:"duration_ms"`
	CreatedAt string `json:"created_at"`
}

// SchemaVersion represents schema version tracking
type SchemaVersion struct {
	ID          int64  `json:"id"`
	Version     int    `json:"version"`
	Name        string `json:"name"`
	Description string `json:"description"`
	AppliedAt   string `json:"applied_at"`
	Checksum    string `json:"checksum"`
	CreatedAt   string `json:"created_at"`
}

// FileInfo represents a file tracked in the table_files table
type FileInfo struct {
	ID            int64  `json:"id"`
	TableID       int64  `json:"table_id"`
	FileName      string `json:"file_name"`
	FilePath      string `json:"file_path"`
	FileSize      int64  `json:"file_size"`
	FileType      string `json:"file_type"`
	PartitionPath string `json:"partition_path"`
	RowCount      int64  `json:"row_count"`
	Checksum      string `json:"checksum"`
	IsCompressed  bool   `json:"is_compressed"`
	CreatedAt     string `json:"created_at"`
	ModifiedAt    string `json:"modified_at"`
	State         string `json:"iceberg_metadata_state"` // Added for tracking Iceberg metadata generation
}

// TableInfo represents basic table information from the tables table
type TableInfo struct {
	ID          int64  `json:"id"`
	DatabaseID  int64  `json:"database_id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	TableType   string `json:"table_type"`
	IsTemporary bool   `json:"is_temporary"`
	IsExternal  bool   `json:"is_external"`
	RowCount    int64  `json:"row_count"`
	FileCount   int    `json:"file_count"`
	TotalSize   int64  `json:"total_size"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	DeletedAt   string `json:"deleted_at"`
}

// DatabaseInfo represents database information from the databases table
type DatabaseInfo struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	OwnerID     int64  `json:"owner_id"`
	TableCount  int    `json:"table_count"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// =============================================================================
// SCHEMA AND COLUMN TYPES
// =============================================================================

// ColumnInfo represents a table column from the table_columns table
type ColumnInfo struct {
	ID              int64  `json:"id"`
	TableID         int64  `json:"table_id"`
	ColumnName      string `json:"column_name"`
	DisplayName     string `json:"display_name"`
	DataType        string `json:"data_type"`
	IsNullable      bool   `json:"is_nullable"`
	IsPrimary       bool   `json:"is_primary"`
	IsUnique        bool   `json:"is_unique"`
	DefaultValue    string `json:"default_value"`
	Description     string `json:"description"`
	OrdinalPosition int    `json:"ordinal_position"`
	MaxLength       int    `json:"max_length"`
	Precision       int    `json:"precision"`
	Scale           int    `json:"scale"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

// TableSchema represents the complete schema of a table
type TableSchema struct {
	TableID   int64        `json:"table_id"`
	TableName string       `json:"table_name"`
	Database  string       `json:"database"`
	Columns   []ColumnInfo `json:"columns"`
	Version   int          `json:"schema_version"`
}

// =============================================================================
// COMPLETE TABLE INFO (combines basic info + schema)
// =============================================================================

// CompleteTableInfo represents a complete table with all its metadata
type CompleteTableInfo struct {
	*TableInfo
	Database    string                      `json:"database"`
	Schema      []ColumnInfo                `json:"schema"`
	StorageInfo *TableMetadata              `json:"storage_info,omitempty"`
	Partitions  []*regtypes.TablePartition  `json:"partitions,omitempty"`
	Indexes     []*regtypes.TableIndex      `json:"indexes,omitempty"`
	Constraints []*regtypes.TableConstraint `json:"constraints,omitempty"`
	Statistics  []*regtypes.TableStatistic  `json:"statistics,omitempty"`
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
