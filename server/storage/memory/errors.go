package memory

import "github.com/TFMV/icebox/pkg/errors"

// Error codes for memory storage package
var (
	// File operation errors
	ErrFileNotFound = errors.MustNewCode("memory.file_not_found")

	// Table operation errors
	ErrTableNotFound      = errors.MustNewCode("memory.table_not_found")
	ErrTableAlreadyExists = errors.MustNewCode("memory.table_already_exists")
	ErrTableDataNotFound  = errors.MustNewCode("memory.table_data_not_found")
)
