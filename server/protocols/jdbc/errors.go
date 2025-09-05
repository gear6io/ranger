package jdbc

import "github.com/gear6io/ranger/pkg/errors"

// JDBC-specific error codes
var (
	ErrStartupMessageParseFailed  = errors.MustNewCode("jdbc.startup_message_parse_failed")
	ErrStartupResponseWriteFailed = errors.MustNewCode("jdbc.startup_response_write_failed")
	ErrRowDescriptionWriteFailed  = errors.MustNewCode("jdbc.row_description_write_failed")
	ErrDataRowWriteFailed         = errors.MustNewCode("jdbc.data_row_write_failed")
	ErrCommandCompleteWriteFailed = errors.MustNewCode("jdbc.command_complete_write_failed")
	ErrQueryExecutionFailed       = errors.MustNewCode("jdbc.query_execution_failed")

	// Protocol-specific error codes
	ErrMessageTypeReadFailed          = errors.MustNewCode("jdbc.message_type_read_failed")
	ErrMessageLengthReadFailed        = errors.MustNewCode("jdbc.message_length_read_failed")
	ErrMessageDataReadFailed          = errors.MustNewCode("jdbc.message_data_read_failed")
	ErrMessageTypeWriteFailed         = errors.MustNewCode("jdbc.message_type_write_failed")
	ErrMessageLengthWriteFailed       = errors.MustNewCode("jdbc.message_length_write_failed")
	ErrMessageDataWriteFailed         = errors.MustNewCode("jdbc.message_data_write_failed")
	ErrServerListenFailed             = errors.MustNewCode("jdbc.server_listen_failed")
	ErrStartupMessageLengthReadFailed = errors.MustNewCode("jdbc.startup_message_length_read_failed")
	ErrStartupMessageDataReadFailed   = errors.MustNewCode("jdbc.startup_message_data_read_failed")
	ErrQueryParseFailed               = errors.MustNewCode("jdbc.query_parse_failed")
	ErrStatementTypeNotAllowed        = errors.MustNewCode("jdbc.statement_type_not_allowed")
	ErrQueryValidationFailed          = errors.MustNewCode("jdbc.query_validation_failed")
)
