package loader

import "github.com/gear6io/ranger/pkg/errors"

// Loader-specific error codes
var (
	ErrComponentInitializationFailed = errors.MustNewCode("loader.component_initialization_failed")
	ErrComponentInitFailed           = errors.MustNewCode("loader.component_init_failed")
	ErrGatewayStartFailed            = errors.MustNewCode("loader.gateway_start_failed")
)
