package json

import "github.com/TFMV/icebox/pkg/errors"

// JSON catalog-specific error codes
var (
	// File operations
	ErrIndexReadFailed              = errors.MustNewCode("catalog.json.index_read_failed")
	ErrIndexParseFailed             = errors.MustNewCode("catalog.json.index_parse_failed")
	ErrCatalogReadFailed            = errors.MustNewCode("catalog.json.catalog_read_failed")
	ErrCatalogWriteFailed           = errors.MustNewCode("catalog.json.catalog_write_failed")
	ErrCatalogOpenFailed            = errors.MustNewCode("catalog.json.catalog_open_failed")
	ErrCatalogDecodeFailed          = errors.MustNewCode("catalog.json.catalog_decode_failed")
	ErrCatalogValidationFailed      = errors.MustNewCode("catalog.json.catalog_validation_failed")
	ErrCatalogDirectoryCreateFailed = errors.MustNewCode("catalog.json.catalog_directory_create_failed")
	ErrCatalogFileCreateFailed      = errors.MustNewCode("catalog.json.catalog_file_create_failed")
	ErrCatalogFileEncodeFailed      = errors.MustNewCode("catalog.json.catalog_file_encode_failed")
	ErrCatalogFileSyncFailed        = errors.MustNewCode("catalog.json.catalog_file_sync_failed")
	ErrCatalogFileCloseFailed       = errors.MustNewCode("catalog.json.catalog_file_close_failed")
	ErrCatalogFileReplaceFailed     = errors.MustNewCode("catalog.json.catalog_file_replace_failed")

	// Metadata operations
	ErrViewMetadataWriteFailed       = errors.MustNewCode("catalog.json.view_metadata_write_failed")
	ErrViewMetadataReadFailed        = errors.MustNewCode("catalog.json.view_metadata_read_failed")
	ErrViewMetadataLoadFailed        = errors.MustNewCode("catalog.json.view_metadata_load_failed")
	ErrTableMetadataWriteFailed      = errors.MustNewCode("catalog.json.table_metadata_write_failed")
	ErrTableMetadataReadFailed       = errors.MustNewCode("catalog.json.table_metadata_read_failed")
	ErrTableMetadataLoadFailed       = errors.MustNewCode("catalog.json.table_metadata_load_failed")
	ErrMetadataDirectoryCreateFailed = errors.MustNewCode("catalog.json.metadata_directory_create_failed")
	ErrMetadataFileCreateFailed      = errors.MustNewCode("catalog.json.metadata_file_create_failed")
	ErrMetadataFileEncodeFailed      = errors.MustNewCode("catalog.json.metadata_file_encode_failed")
	ErrMetadataFileSyncFailed        = errors.MustNewCode("catalog.json.metadata_file_sync_failed")
	ErrMetadataFileCloseFailed       = errors.MustNewCode("catalog.json.metadata_file_close_failed")
	ErrMetadataFileWriteFailed       = errors.MustNewCode("catalog.json.metadata_file_write_failed")
	ErrMetadataFileRenameFailed      = errors.MustNewCode("catalog.json.metadata_file_rename_failed")
	ErrMetadataFileMarshalFailed     = errors.MustNewCode("catalog.json.metadata_file_marshal_failed")

	// Validation errors
	ErrInvalidSourceIdentifier      = errors.MustNewCode("catalog.json.invalid_source_identifier")
	ErrInvalidDestinationIdentifier = errors.MustNewCode("catalog.json.invalid_destination_identifier")
	ErrInvalidProperty              = errors.MustNewCode("catalog.json.invalid_property")
	ErrRequirementValidationFailed  = errors.MustNewCode("catalog.json.requirement_validation_failed")

	// Namespace operations
	ErrNamespaceCheckFailed          = errors.MustNewCode("catalog.json.namespace_check_failed")
	ErrNamespaceExistenceCheckFailed = errors.MustNewCode("catalog.json.namespace_existence_check_failed")

	// Table operations
	ErrTableLoadFailed        = errors.MustNewCode("catalog.json.table_load_failed")
	ErrTableUpdateFailed      = errors.MustNewCode("catalog.json.table_update_failed")
	ErrTableStageUpdateFailed = errors.MustNewCode("catalog.json.table_stage_update_failed")
	ErrTableLoadUpdatedFailed = errors.MustNewCode("catalog.json.table_load_updated_failed")

	// Metadata version operations
	ErrNextMetadataVersionFailed  = errors.MustNewCode("catalog.json.next_metadata_version_failed")
	ErrCurrentMetadataReadFailed  = errors.MustNewCode("catalog.json.current_metadata_read_failed")
	ErrCurrentMetadataParseFailed = errors.MustNewCode("catalog.json.current_metadata_parse_failed")
	ErrMetadataUpdateApplyFailed  = errors.MustNewCode("catalog.json.metadata_update_apply_failed")

	// Catalog operations
	ErrCatalogEnsureFailed = errors.MustNewCode("catalog.json.catalog_ensure_failed")
	ErrCatalogUpdateFailed = errors.MustNewCode("catalog.json.catalog_update_failed")
	ErrCatalogCloseFailed  = errors.MustNewCode("catalog.json.catalog_close_failed")
)
