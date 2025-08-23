package registry

import (
	"time"

	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
)

// =============================================================================
// COMPOSITE AND UTILITY TYPES (Not duplicated in regtypes)
// =============================================================================

// TableMetadata represents a composite view of table metadata for storage operations
// This combines information from multiple tables for convenience
type TableMetadata struct {
	Database      string                `json:"database"`
	Name          string                `json:"name"`
	Schema        []byte                `json:"schema"`
	StorageEngine string                `json:"storage_engine"`
	EngineConfig  string                `json:"engine_config"`
	FileCount     int                   `json:"file_count"`
	TotalSize     int64                 `json:"total_size"`
	LastModified  time.Time             `json:"last_modified"`
	Created       time.Time             `json:"created"`
	Files         []*regtypes.TableFile `json:"files"`
}

// TableSchema represents the complete schema of a table
type TableSchema struct {
	TableID   int64                   `json:"table_id"`
	TableName string                  `json:"table_name"`
	Database  string                  `json:"database"`
	Columns   []*regtypes.TableColumn `json:"columns"`
	Version   int                     `json:"schema_version"`
}

// CompleteTableInfo represents a complete table with all its metadata
type CompleteTableInfo struct {
	*regtypes.Table
	Database    string                      `json:"database"`
	Schema      []*regtypes.TableColumn     `json:"schema"`
	StorageInfo *regtypes.TableMetadata     `json:"storage_info,omitempty"`
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

// FileInfo represents file information for metadata operations
// This is used by the Iceberg manager and other components
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
	State         string `json:"state"`
}

// TableInfo represents table information for Iceberg metadata operations
type TableInfo struct {
	ID            int64  `json:"id"`
	Database      string `json:"database"`
	Name          string `json:"name"`
	Schema        []byte `json:"schema"`
	StorageEngine string `json:"storage_engine"`
	EngineConfig  string `json:"engine_config"`
	Format        string `json:"format"`
	Compression   string `json:"compression"`
	PartitionBy   string `json:"partition_by"`
	SortBy        string `json:"sort_by"`
	Properties    string `json:"properties"`
}
