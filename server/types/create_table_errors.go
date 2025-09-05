package types

import (
	"github.com/gear6io/ranger/pkg/errors"
)

// CreateTableError - minimal stub to fix build issues
// TODO: Remove this and simplify error handling
type CreateTableError struct {
	Err         *errors.Error
	Diagnostics *RegistryDiagnostics
}

// Error implements the error interface
func (e *CreateTableError) Error() string {
	return e.Err.Error()
}

// Stub functions to fix build issues
func NewCreateTableRegistryError(message, tableName, database, transactionID, requestID string, cause error) *CreateTableError {
	return &CreateTableError{
		Err:         errors.New(errors.CommonInternal, message, cause),
		Diagnostics: &RegistryDiagnostics{},
	}
}

func NewCreateTableTypeValidationError(message, tableName string, invalidTypes []string, cause error) *CreateTableError {
	return &CreateTableError{
		Err:         errors.New(errors.CommonValidation, message, cause),
		Diagnostics: &RegistryDiagnostics{},
	}
}

func (e *CreateTableError) AddSuggestion(suggestion string) *CreateTableError {
	e.Err = e.Err.AddSuggestion(suggestion)
	return e
}

func (e *CreateTableError) AddRecoveryHint(hint interface{}) *CreateTableError {
	// Stub - just return self
	return e
}

func (e *CreateTableError) AddContext(key string, value interface{}) *CreateTableError {
	e.Err = e.Err.AddContext(key, value)
	return e
}

// Stub for diagnostics
type RegistryDiagnostics struct {
	TransactionID        string
	TablesCreated        []string
	ColumnsCreated       int
	RollbackPerformed    bool
	DatabaseState        map[string]string
	ConstraintViolations []string
	ValidationDetails    interface{}          // Stub field
	RegistryState        *RegistryDiagnostics // Stub field
}

// RecoveryHint - stub type
type RecoveryHint struct {
	Type        string
	Description string
	Action      string
	Automatic   bool
}
