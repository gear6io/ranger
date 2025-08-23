package sqlite

import "github.com/TFMV/icebox/pkg/errors"

// SQLite catalog-specific error codes
var (
	// Database operations
	ErrCatalogDirectoryCreateFailed = errors.MustNewCode("catalog.sqlite.directory_create_failed")
	ErrDatabaseOpenFailed           = errors.MustNewCode("catalog.sqlite.database_open_failed")
	ErrDatabaseInitFailed           = errors.MustNewCode("catalog.sqlite.database_init_failed")
	ErrDatabaseCloseFailed          = errors.MustNewCode("catalog.sqlite.database_close_failed")

	// Table operations
	ErrTableCreateFailed            = errors.MustNewCode("catalog.sqlite.table_create_failed")
	ErrTableMetadataWriteFailed     = errors.MustNewCode("catalog.sqlite.table_metadata_write_failed")
	ErrTableInsertFailed            = errors.MustNewCode("catalog.sqlite.table_insert_failed")
	ErrTableQueryFailed             = errors.MustNewCode("catalog.sqlite.table_query_failed")
	ErrTableLoadFailed              = errors.MustNewCode("catalog.sqlite.table_load_failed")
	ErrTableCheckFailed             = errors.MustNewCode("catalog.sqlite.table_check_failed")
	ErrTableDeleteFailed            = errors.MustNewCode("catalog.sqlite.table_delete_failed")
	ErrTableRenameFailed            = errors.MustNewCode("catalog.sqlite.table_rename_failed")
	ErrTableListFailed              = errors.MustNewCode("catalog.sqlite.table_list_failed")
	ErrTableScanFailed              = errors.MustNewCode("catalog.sqlite.table_scan_failed")
	ErrTableIterationFailed         = errors.MustNewCode("catalog.sqlite.table_iteration_failed")

	// Namespace operations
	ErrNamespaceCheckFailed         = errors.MustNewCode("catalog.sqlite.namespace_check_failed")
	ErrNamespaceCreateFailed        = errors.MustNewCode("catalog.sqlite.namespace_create_failed")
	ErrNamespaceInsertFailed        = errors.MustNewCode("catalog.sqlite.namespace_insert_failed")
	ErrNamespaceCountFailed         = errors.MustNewCode("catalog.sqlite.namespace_count_failed")
	ErrNamespaceDeleteFailed        = errors.MustNewCode("catalog.sqlite.namespace_delete_failed")
	ErrNamespaceListFailed          = errors.MustNewCode("catalog.sqlite.namespace_list_failed")
	ErrNamespaceScanFailed          = errors.MustNewCode("catalog.sqlite.namespace_scan_failed")
	ErrNamespaceIterationFailed     = errors.MustNewCode("catalog.sqlite.namespace_iteration_failed")

	// Properties operations
	ErrPropertiesLoadFailed         = errors.MustNewCode("catalog.sqlite.properties_load_failed")
	ErrPropertiesScanFailed         = errors.MustNewCode("catalog.sqlite.properties_scan_failed")
	ErrPropertiesIterationFailed    = errors.MustNewCode("catalog.sqlite.properties_iteration_failed")
	ErrPropertiesUpdateFailed       = errors.MustNewCode("catalog.sqlite.properties_update_failed")
	ErrPropertiesTransactionFailed  = errors.MustNewCode("catalog.sqlite.properties_transaction_failed")
	ErrPropertiesRemoveFailed       = errors.MustNewCode("catalog.sqlite.properties_remove_failed")
	ErrPropertiesCheckFailed        = errors.MustNewCode("catalog.sqlite.properties_check_failed")
	ErrPropertiesCommitFailed       = errors.MustNewCode("catalog.sqlite.properties_commit_failed")

	// Metadata operations
	ErrMetadataSerializeFailed      = errors.MustNewCode("catalog.sqlite.metadata_serialize_failed")
	ErrMetadataFileCreateFailed     = errors.MustNewCode("catalog.sqlite.metadata_file_create_failed")
	ErrMetadataFileWriteFailed      = errors.MustNewCode("catalog.sqlite.metadata_file_write_failed")
	ErrMetadataQueryFailed          = errors.MustNewCode("catalog.sqlite.metadata_query_failed")
	ErrMetadataValidationFailed     = errors.MustNewCode("catalog.sqlite.metadata_validation_failed")
	ErrMetadataBuilderFailed        = errors.MustNewCode("catalog.sqlite.metadata_builder_failed")
	ErrMetadataUpdateFailed         = errors.MustNewCode("catalog.sqlite.metadata_update_failed")
	ErrMetadataBuildFailed          = errors.MustNewCode("catalog.sqlite.metadata_build_failed")
	ErrMetadataWriteFailed          = errors.MustNewCode("catalog.sqlite.metadata_write_failed")
	ErrMetadataLocationUpdateFailed = errors.MustNewCode("catalog.sqlite.metadata_location_update_failed")
)
