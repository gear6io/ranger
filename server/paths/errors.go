package paths

import "github.com/gear6io/ranger/pkg/errors"

// Path-specific error codes
var (
	ErrDirectoryCreationFailed = errors.MustNewCode("paths.directory_creation_failed")
)
