package types

// TableMetadata represents detailed table metadata for storage operations
type TableMetadata struct {
	Database      string                 `json:"database"`
	Name          string                 `json:"name"`
	Schema        []byte                 `json:"schema"`
	StorageEngine string                 `json:"storage_engine"`
	EngineConfig  map[string]interface{} `json:"engine_config,omitempty"`
	FileCount     int                    `json:"file_count"`
	TotalSize     int64                  `json:"total_size"`
	LastModified  string                 `json:"last_modified"`
	Created       string                 `json:"created"`
	Files         []FileInfo             `json:"files"`
}

// FileInfo represents information about a data file
type FileInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Created  string `json:"created"`
	Modified string `json:"modified"`
	Date     string `json:"date"`
}
