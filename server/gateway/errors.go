package gateway

import "github.com/gear6io/ranger/pkg/errors"

// Error codes for gateway package
var (
	// Server creation errors
	ErrHTTPServerCreationFailed   = errors.MustNewCode("gateway.http_server_creation_failed")
	ErrJDBCServerCreationFailed   = errors.MustNewCode("gateway.jdbc_server_creation_failed")
	ErrNativeServerCreationFailed = errors.MustNewCode("gateway.native_server_creation_failed")

	// Gateway lifecycle errors
	ErrGatewayAlreadyStarted   = errors.MustNewCode("gateway.already_started")
	ErrAllServersFailedToStart = errors.MustNewCode("gateway.all_servers_failed_to_start")
	ErrGatewayStopFailed       = errors.MustNewCode("gateway.stop_failed")
)

