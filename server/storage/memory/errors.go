package memory

import "github.com/TFMV/icebox/pkg/errors"

// Error codes for memory storage package
var (
	// File operation errors
	ErrFileNotFound = errors.MustNewCode("memory.file_not_found")

	// Table operation errors
	ErrTableNotFound      = errors.MustNewCode("memory.table_not_found")
	ErrTableAlreadyExists = errors.MustNewCode("memory.table_already_exists")
	ErrTableDataNotFound  = errors.MustNewCode("memory.table_data_not_found")

	// Schema and data conversion errors
	ErrSchemaConversionFailed  = errors.MustNewCode("memory.schema_conversion_failed")
	ErrSchemaInferenceFailed   = errors.MustNewCode("memory.schema_inference_failed")
	ErrEmptyDataForSchema      = errors.MustNewCode("memory.empty_data_for_schema")
	ErrDataUnmarshalFailed     = errors.MustNewCode("memory.data_unmarshal_failed")
	ErrDataSerializationFailed = errors.MustNewCode("memory.data_serialization_failed")
	ErrDataMarshalFailed       = errors.MustNewCode("memory.data_marshal_failed")

	// Writer and reader errors
	ErrWriterClosed          = errors.MustNewCode("memory.writer_closed")
	ErrWriteNotSupported     = errors.MustNewCode("memory.write_not_supported")
	ErrDataRetrievalFailed   = errors.MustNewCode("memory.data_retrieval_failed")
	ErrDataStorageFailed     = errors.MustNewCode("memory.data_storage_failed")
	ErrDefaultSchemaCreation = errors.MustNewCode("memory.default_schema_creation_failed")

	// Parquet manager errors
	ErrDataManagerClosed      = errors.MustNewCode("memory.data_manager_closed")
	ErrDataValidationFailed   = errors.MustNewCode("memory.data_validation_failed")
	ErrMemoryLimitExceeded    = errors.MustNewCode("memory.memory_limit_exceeded")
	ErrRecordConversionFailed = errors.MustNewCode("memory.record_conversion_failed")
	ErrSchemaIsNil            = errors.MustNewCode("memory.schema_is_nil")
	ErrColumnConversionFailed = errors.MustNewCode("memory.column_conversion_failed")
	ErrInsufficientColumns    = errors.MustNewCode("memory.insufficient_columns")
	ErrValueAppendFailed      = errors.MustNewCode("memory.value_append_failed")
	ErrTypeMismatch           = errors.MustNewCode("memory.type_mismatch")
	ErrUnsupportedDataType    = errors.MustNewCode("memory.unsupported_data_type")
	ErrValueExtractionFailed  = errors.MustNewCode("memory.value_extraction_failed")
)
