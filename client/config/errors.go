package config

import "github.com/TFMV/icebox/pkg/errors"

// Error codes for client config package
var (
	// File operation errors
	ErrConfigFileReadFailed = errors.MustNewCode("client_config.file_read_failed")
	ErrConfigFileParseFailed = errors.MustNewCode("client_config.file_parse_failed")
	ErrConfigFileWriteFailed = errors.MustNewCode("client_config.file_write_failed")
	ErrConfigFileMarshalFailed = errors.MustNewCode("client_config.file_marshal_failed")

	// Validation errors
	ErrServerAddressEmpty = errors.MustNewCode("client_config.server_address_empty")
	ErrServerPortInvalid = errors.MustNewCode("client_config.server_port_invalid")
)

