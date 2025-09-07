package jdbc

import "github.com/gear6io/ranger/pkg/errors"

// JDBC-specific error codes for query package
var (
	ErrStartupMessageParseFailed  = errors.MustNewCode("jdbc.query.startup_message_parse_failed")
	ErrStartupResponseWriteFailed = errors.MustNewCode("jdbc.query.startup_response_write_failed")
	ErrRowDescriptionWriteFailed  = errors.MustNewCode("jdbc.query.row_description_write_failed")
	ErrDataRowWriteFailed         = errors.MustNewCode("jdbc.query.data_row_write_failed")
	ErrCommandCompleteWriteFailed = errors.MustNewCode("jdbc.query.command_complete_write_failed")
	ErrQueryExecutionFailed       = errors.MustNewCode("jdbc.query.query_execution_failed")

	// Protocol-specific error codes
	ErrMessageTypeReadFailed          = errors.MustNewCode("jdbc.query.message_type_read_failed")
	ErrMessageLengthReadFailed        = errors.MustNewCode("jdbc.query.message_length_read_failed")
	ErrMessageDataReadFailed          = errors.MustNewCode("jdbc.query.message_data_read_failed")
	ErrMessageTypeWriteFailed         = errors.MustNewCode("jdbc.query.message_type_write_failed")
	ErrMessageLengthWriteFailed       = errors.MustNewCode("jdbc.query.message_length_write_failed")
	ErrMessageDataWriteFailed         = errors.MustNewCode("jdbc.query.message_data_write_failed")
	ErrServerListenFailed             = errors.MustNewCode("jdbc.query.server_listen_failed")
	ErrStartupMessageLengthReadFailed = errors.MustNewCode("jdbc.query.startup_message_length_read_failed")
	ErrStartupMessageDataReadFailed   = errors.MustNewCode("jdbc.query.startup_message_data_read_failed")
	ErrQueryParseFailed               = errors.MustNewCode("jdbc.query.query_parse_failed")
	ErrStatementTypeNotAllowed        = errors.MustNewCode("jdbc.query.statement_type_not_allowed")
	ErrQueryValidationFailed          = errors.MustNewCode("jdbc.query.query_validation_failed")
)
