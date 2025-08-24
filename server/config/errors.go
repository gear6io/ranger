package config

import "github.com/gear6io/ranger/pkg/errors"

// Config-specific error codes
var (
	ErrConfigFileReadFailed        = errors.MustNewCode("config.file_read_failed")
	ErrConfigFileParseFailed       = errors.MustNewCode("config.file_parse_failed")
	ErrConfigValidationFailed      = errors.MustNewCode("config.validation_failed")
	ErrConfigFileMarshalFailed     = errors.MustNewCode("config.file_marshal_failed")
	ErrConfigFileWriteFailed       = errors.MustNewCode("config.file_write_failed")
	ErrStorageValidationFailed     = errors.MustNewCode("config.storage_validation_failed")
	ErrCatalogValidationFailed     = errors.MustNewCode("config.catalog_validation_failed")
	ErrDataStorageValidationFailed = errors.MustNewCode("config.data_storage_validation_failed")
	ErrDataPathRequired            = errors.MustNewCode("config.data_path_required")
	ErrCatalogTypeRequired         = errors.MustNewCode("config.catalog_type_required")

	// Logging-specific error codes
	ErrLogDirectoryCreationFailed = errors.MustNewCode("config.log_directory_creation_failed")
	ErrLogFileOpenFailed          = errors.MustNewCode("config.log_file_open_failed")
	ErrLogFilePathRequired        = errors.MustNewCode("config.log_file_path_required")
	ErrLogRotationCheckFailed     = errors.MustNewCode("config.log_rotation_check_failed")
	ErrLogFileStatFailed          = errors.MustNewCode("config.log_file_stat_failed")
	ErrLogRotationFailed          = errors.MustNewCode("config.log_rotation_failed")
	ErrLogBackupReadFailed        = errors.MustNewCode("config.log_backup_read_failed")
	ErrLogBackupRemoveFailed      = errors.MustNewCode("config.log_backup_remove_failed")
	ErrLogCleanupFailed           = errors.MustNewCode("config.log_cleanup_failed")
	ErrLogFileWriterSetupFailed   = errors.MustNewCode("config.log_file_writer_setup_failed")
)
