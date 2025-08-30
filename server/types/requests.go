package types

import (
	"time"
)

// UserContext represents user context for operations
type UserContext struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
	Database string `json:"database"`
	IsAdmin  bool   `json:"is_admin"`
}

// CreateTableRequest represents a request to create a table
type CreateTableRequest struct {
	Statement     interface{}            `json:"statement"` // Use interface{} to avoid import cycle
	Database      string                 `json:"database"`
	RequestID     string                 `json:"request_id"`
	UserContext   *UserContext           `json:"user_context"`
	StorageEngine string                 `json:"storage_engine"`
	EngineConfig  map[string]interface{} `json:"engine_config"`
}

// CreateTableResponse represents the response from creating a table
type CreateTableResponse struct {
	TableID  int64                  `json:"table_id"`
	Success  bool                   `json:"success"`
	Metadata *TableCreationMetadata `json:"metadata"`
}

// TableCreationMetadata contains metadata about the created table
type TableCreationMetadata struct {
	CreatedAt     time.Time `json:"created_at"`
	ColumnCount   int       `json:"column_count"`
	StorageEngine string    `json:"storage_engine"`
	SchemaVersion int       `json:"schema_version"`
}
