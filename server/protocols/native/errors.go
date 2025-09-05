package native

import "github.com/gear6io/ranger/pkg/errors"

// Native protocol-specific error codes
var (
	ErrServerListenFailed      = errors.MustNewCode("native.server_listen_failed")
	ErrUnknownMessageType      = errors.MustNewCode("native.unknown_message_type")
	ErrQueryCancellationFailed = errors.MustNewCode("native.query_cancellation_failed")
	ErrConnectionClosed        = errors.MustNewCode("native.connection_closed")
	ErrInvalidMessageFormat    = errors.MustNewCode("native.invalid_message_format")
	ErrAuthenticationFailed    = errors.MustNewCode("native.authentication_failed")
	ErrQueryExecutionFailed    = errors.MustNewCode("native.query_execution_failed")
	ErrDataTransferFailed      = errors.MustNewCode("native.data_transfer_failed")
	ErrProtocolViolation       = errors.MustNewCode("native.protocol_violation")
	ErrServerShutdown          = errors.MustNewCode("native.server_shutdown")
)
