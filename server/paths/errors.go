package paths

import "github.com/TFMV/icebox/pkg/errors"

// Path-specific error codes
var (
	ErrDirectoryCreationFailed = errors.MustNewCode("paths.directory_creation_failed")
)
