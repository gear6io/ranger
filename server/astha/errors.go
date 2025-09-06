package astha

import "github.com/gear6io/ranger/pkg/errors"

// Error codes for astha package
var (
	// Configuration errors
	ErrDatabaseConnectionRequired = errors.MustNewCode("astha.database_connection_required")

	// CDC infrastructure errors

	// Scheduler-specific error codes
	ErrComponentNameEmpty       = errors.MustNewCode("astha.component_name_empty")
	ErrComponentNoSubscriptions = errors.MustNewCode("astha.component_no_subscriptions")
	ErrComponentInstanceNil     = errors.MustNewCode("astha.component_instance_nil")
	ErrComponentNotFound        = errors.MustNewCode("astha.component_not_found")
	ErrUnsupportedEventType     = errors.MustNewCode("astha.unsupported_event_type")

	// CDC consumer errors
	ErrCDCGetChangesFailed     = errors.MustNewCode("astha.cdc_get_changes_failed")
	ErrCDCProcessBatchFailed   = errors.MustNewCode("astha.cdc_process_batch_failed")
	ErrCDCDeleteLogsFailed     = errors.MustNewCode("astha.cdc_delete_logs_failed")
	ErrCDCQueryLogFailed       = errors.MustNewCode("astha.cdc_query_log_failed")
	ErrCDCScanLogFailed        = errors.MustNewCode("astha.cdc_scan_log_failed")
	ErrCDCTransactionFailed    = errors.MustNewCode("astha.cdc_transaction_failed")
	ErrCDCParseDataFailed      = errors.MustNewCode("astha.cdc_parse_data_failed")
	ErrCDCUnknownTableType     = errors.MustNewCode("astha.cdc_unknown_table_type")
	ErrCDCTimestampParseFailed = errors.MustNewCode("astha.cdc_timestamp_parse_failed")
	ErrCDCCreatedAtParseFailed = errors.MustNewCode("astha.cdc_created_at_parse_failed")
	ErrCDCUnknownOperation     = errors.MustNewCode("astha.cdc_unknown_operation")
	ErrCDCNoDataAvailable      = errors.MustNewCode("astha.cdc_no_data_available")
)
