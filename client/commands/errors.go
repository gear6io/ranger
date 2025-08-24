package commands

import "github.com/TFMV/icebox/pkg/errors"

// Error codes for client commands package
var (
	// Query validation errors
	ErrQueryEmpty = errors.MustNewCode("client_commands.query_empty")

	// Query execution errors
	ErrQueryExecutionFailed = errors.MustNewCode("client_commands.query_execution_failed")
	ErrQueryParseFailed = errors.MustNewCode("client_commands.query_parse_failed")
	ErrQueryFormatFailed = errors.MustNewCode("client_commands.query_format_failed")
	ErrQueryExplainFailed = errors.MustNewCode("client_commands.query_explain_failed")
)

