package duckdb

import (
	"github.com/gear6io/ranger/pkg/errors"
)

// DuckDB engine error codes
var (
	ErrDuckDBConnectionFailed       = errors.MustNewCode("duckdb.connection_failed")
	ErrDuckDBPingFailed             = errors.MustNewCode("duckdb.ping_failed")
	ErrDuckDBConfigurationFailed    = errors.MustNewCode("duckdb.configuration_failed")
	ErrDuckDBQueryFailed            = errors.MustNewCode("duckdb.query_failed")
	ErrDuckDBPrepareStatementFailed = errors.MustNewCode("duckdb.prepare_statement_failed")
	ErrDuckDBExecutionFailed        = errors.MustNewCode("duckdb.execution_failed")
	ErrDuckDBScanFailed             = errors.MustNewCode("duckdb.scan_failed")
	ErrDuckDBCloseFailed            = errors.MustNewCode("duckdb.close_failed")
	ErrDuckDBTransactionFailed      = errors.MustNewCode("duckdb.transaction_failed")
	ErrDuckDBSchemaFailed           = errors.MustNewCode("duckdb.schema_failed")
	ErrDuckDBDataTypeFailed         = errors.MustNewCode("duckdb.data_type_failed")
	ErrDuckDBResultProcessingFailed = errors.MustNewCode("duckdb.result_processing_failed")
	ErrDuckDBQueryTimeout           = errors.MustNewCode("duckdb.query_timeout")
	ErrDuckDBTableNotFound          = errors.MustNewCode("duckdb.table_not_found")
	ErrDuckDBColumnRetrievalFailed  = errors.MustNewCode("duckdb.column_retrieval_failed")
	ErrDuckDBEngineCreationFailed   = errors.MustNewCode("duckdb.engine_creation_failed")
	ErrDuckDBExtensionFailed        = errors.MustNewCode("duckdb.extension_failed")
	ErrDuckDBCatalogAttachFailed    = errors.MustNewCode("duckdb.catalog_attach_failed")
)
