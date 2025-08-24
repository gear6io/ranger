package native

import "github.com/gear6io/ranger/pkg/errors"

// Native protocol-specific error codes
var (
	ErrServerListenFailed = errors.MustNewCode("native.server_listen_failed")
)
