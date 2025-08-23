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
