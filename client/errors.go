package client

import "github.com/gear6io/ranger/pkg/errors"

// Error codes for client package
var (
	// Connection errors
	ErrClientNotConnected = errors.MustNewCode("client.not_connected")
	ErrConnectionFailed   = errors.MustNewCode("client.connection_failed")

	// SDK client errors
	ErrSDKClientCreationFailed = errors.MustNewCode("client.sdk_client_creation_failed")

	// Query execution errors
	ErrQueryExecutionFailed = errors.MustNewCode("client.query_execution_failed")
	ErrQueryExecutionError  = errors.MustNewCode("client.query_execution_error")

	// File operation errors
	ErrFileImportFailed = errors.MustNewCode("client.file_import_failed")

	// Table operation errors
	ErrTableListFailed     = errors.MustNewCode("client.table_list_failed")
	ErrTableDescribeFailed = errors.MustNewCode("client.table_describe_failed")
	ErrTableDropFailed     = errors.MustNewCode("client.table_drop_failed")

	// Namespace operation errors
	ErrNamespaceCreationFailed = errors.MustNewCode("client.namespace_creation_failed")
	ErrNamespaceDropFailed     = errors.MustNewCode("client.namespace_drop_failed")
)

