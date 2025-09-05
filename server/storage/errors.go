package storage

import "github.com/gear6io/ranger/pkg/errors"

// Storage-specific error codes
var (
	ErrStorageManagerNoEnginesAvailable = errors.MustNewCode("storage.no_engines_available")
	ErrStorageManagerUnsupportedEngine  = errors.MustNewCode("storage.unsupported_engine")
	ErrStorageManagerMetadataFailed     = errors.MustNewCode("storage.metadata_failed")
	ErrStorageManagerWriteFailed        = errors.MustNewCode("storage.write_failed")
	ErrStorageManagerDirectoryFailed    = errors.MustNewCode("storage.directory_failed")
	ErrStorageManagerUnsupportedType    = errors.MustNewCode("storage.unsupported_type")
	ErrStorageManagerPanicRecovery      = errors.MustNewCode("storage.panic_recovery")
)
