package query

import "github.com/TFMV/icebox/pkg/errors"

// Error codes for query package
var (
	// Query management errors
	ErrQueryNotFound   = errors.MustNewCode("query.not_found")
	ErrQueryNotRunning = errors.MustNewCode("query.not_running")
)

