package regtypes

import "time"

// =============================================================================
// ICEBERG METADATA GENERATION CONSTANTS
// =============================================================================

// IcebergMetadataGenerationState constants for tracking file processing status
const (
	IcebergMetadataGenerationStatePending    = "pending"
	IcebergMetadataGenerationStateProcessing = "processing"
	IcebergMetadataGenerationStateCompleted  = "completed"
	IcebergMetadataGenerationStateFailed     = "failed"
)

// =============================================================================
// BATCH PROCESSING CONSTANTS
// =============================================================================

// Batch processing constants for optimal metadata generation
const (
	// Primary batching - file count
	MaxFilesPerBatch = 10

	// Secondary batching - size thresholds
	MinBatchSizeBytes = 100 * 1024 * 1024      // 100MB
	MaxBatchSizeBytes = 1 * 1024 * 1024 * 1024 // 1GB
)

// =============================================================================
// RETRY CONFIGURATION
// =============================================================================

// Retry configuration for failed operations
const (
	MaxRetryAttempts = 3
	BaseRetryDelay   = 1 * time.Second
)

// =============================================================================
// TABLE TYPE CONSTANTS
// =============================================================================

// Table type constants
const (
	TableTypeUser      = "user"
	TableTypeSystem    = "system"
	TableTypeTemporary = "temporary"
	TableTypeExternal  = "external"
)

// =============================================================================
// STORAGE ENGINE CONSTANTS
// =============================================================================

// Storage engine constants
const (
	StorageEngineFilesystem = "filesystem"
	StorageEngineMemory     = "memory"
	StorageEngineS3         = "s3"
)

// =============================================================================
// FILE TYPE CONSTANTS
// =============================================================================

// File type constants
const (
	FileTypeParquet = "parquet"
	FileTypeJSON    = "json"
	FileTypeCSV     = "csv"
	FileTypeAvro    = "avro"
)

// =============================================================================
// COMPRESSION CONSTANTS
// =============================================================================

// Compression constants
const (
	CompressionNone   = "none"
	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLZ4    = "lz4"
)

// =============================================================================
// CONSTRAINT TYPE CONSTANTS
// =============================================================================

// Constraint type constants
const (
	ConstraintTypePrimaryKey = "PRIMARY KEY"
	ConstraintTypeForeignKey = "FOREIGN KEY"
	ConstraintTypeUnique     = "UNIQUE"
	ConstraintTypeCheck      = "CHECK"
	ConstraintTypeNotNull    = "NOT NULL"
)

// =============================================================================
// USER ROLE CONSTANTS
// =============================================================================

// User role constants
const (
	UserRoleAdmin = "admin"
	UserRoleUser  = "user"
	UserRoleGuest = "guest"
)

// =============================================================================
// INDEX TYPE CONSTANTS
// =============================================================================

// Index type constants
const (
	IndexTypeBTree = "BTREE"
	IndexTypeHash  = "HASH"
	IndexTypeGIN   = "GIN"
	IndexTypeGiST  = "GIST"
)

// =============================================================================
// STATISTICS TYPE CONSTANTS
// =============================================================================

// Statistics type constants
const (
	StatTypeRowCount    = "row_count"
	StatTypeColumnStats = "column_stats"
	StatTypeTableSize   = "table_size"
	StatTypePartition   = "partition_stats"
)

// =============================================================================
// CDC OPERATION CONSTANTS
// =============================================================================

// CDC operation constants
const (
	CDCOperationInsert = "INSERT"
	CDCOperationUpdate = "UPDATE"
	CDCOperationDelete = "DELETE"
)

// =============================================================================
// DEFAULT VALUES
// =============================================================================

// Default values for various fields
const (
	DefaultSchemaVersion = 1
	DefaultTableType     = TableTypeUser
	DefaultStorageEngine = StorageEngineFilesystem
	DefaultFileType      = FileTypeParquet
	DefaultCompression   = CompressionNone
)
