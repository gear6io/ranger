package regtypes

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
// TABLE TYPE CONSTANTS
// =============================================================================

// Table type constants for categorizing tables
const (
	TableTypeUser      = "user"
	TableTypeSystem    = "system"
	TableTypeTemporary = "temporary"
	TableTypeExternal  = "external"
)

// =============================================================================
// STORAGE ENGINE CONSTANTS
// =============================================================================

// Storage engine constants for different storage backends
const (
	StorageEngineFilesystem = "FILESYSTEM"
	StorageEngineMemory     = "MEMORY"
	StorageEngineS3         = "S3"
)

// =============================================================================
// FILE TYPE CONSTANTS
// =============================================================================

// File type constants for supported data formats
const (
	FileTypeParquet = "PARQUET"
)

// =============================================================================
// COMPRESSION CONSTANTS
// =============================================================================

// Compression constants for data compression algorithms
const (
	CompressionNone   = "none"
	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLZ4    = "lz4"
)

// =============================================================================
// CONSTRAINT TYPE CONSTANTS
// =============================================================================

// Constraint type constants for database constraints
const (
	ConstraintTypePrimaryKey = "PRIMARY KEY"
	ConstraintTypeForeignKey = "FOREIGN KEY"
	ConstraintTypeUnique     = "UNIQUE"
	ConstraintTypeCheck      = "CHECK"
	ConstraintTypeNotNull    = "NOT NULL"
)

// =============================================================================
// INDEX TYPE CONSTANTS
// =============================================================================

// Index type constants for database indexes
const (
	IndexTypeBTree = "BTREE"
	IndexTypeHash  = "HASH"
	IndexTypeGIN   = "GIN"
	IndexTypeGiST  = "GIST"
)

// =============================================================================
// STATISTICS TYPE CONSTANTS
// =============================================================================

// Statistics type constants for table statistics
const (
	StatTypeRowCount    = "row_count"
	StatTypeColumnStats = "column_stats"
	StatTypeTableSize   = "table_size"
	StatTypePartition   = "partition_stats"
)

// =============================================================================
// CDC OPERATION CONSTANTS
// =============================================================================

// CDC operation constants for change data capture
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
