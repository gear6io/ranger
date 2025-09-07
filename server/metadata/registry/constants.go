package registry

import "time"

const (
	CDCLogTableName = "__cdc_log"
)

// IcebergMetadataGenerationState constants for tracking file processing status
const (
	IcebergMetadataGenerationStatePending    = "pending"
	IcebergMetadataGenerationStateProcessing = "processing"
	IcebergMetadataGenerationStateCompleted  = "completed"
	IcebergMetadataGenerationStateFailed     = "failed"
)

// Registry table names for CDC monitoring
const (
	TableFilesTableName    = "table_files"
	TableStatsTableName    = "table_statistics"
	TableMetadataTableName = "table_metadata"
)

// Batch processing constants for optimal metadata generation
const (
	// Primary batching - file count
	MaxFilesPerBatch = 10

	// Secondary batching - size thresholds
	MinBatchSizeBytes = 100 * 1024 * 1024      // 100MB
	MaxBatchSizeBytes = 1 * 1024 * 1024 * 1024 // 1GB
)

// Retry configuration for failed operations
const (
	MaxRetryAttempts = 3
	BaseRetryDelay   = 1 * time.Second
)
