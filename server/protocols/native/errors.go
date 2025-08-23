package native

import "github.com/TFMV/icebox/pkg/errors"

// Native protocol-specific error codes
var (
	ErrServerListenFailed = errors.MustNewCode("native.server_listen_failed")
)
