package types

import (
	"fmt"
	"strings"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
)

// CREATE TABLE specific error codes
var (
	// Parser errors
	CreateTableParseError            = errors.MustNewCode("create_table.parse.syntax_error")
	CreateTableTypeValidationError   = errors.MustNewCode("create_table.parse.type_validation")
	CreateTableColumnValidationError = errors.MustNewCode("create_table.parse.column_validation")
	CreateTableStructureError        = errors.MustNewCode("create_table.parse.structure_error")

	// Storage Manager errors
	CreateTableStorageValidationError = errors.MustNewCode("create_table.storage.validation_error")
	CreateTableStorageEngineError     = errors.MustNewCode("create_table.storage.engine_error")
	CreateTableStorageConfigError     = errors.MustNewCode("create_table.storage.config_error")
	CreateTableStorageOperationError  = errors.MustNewCode("create_table.storage.operation_error")

	// Metadata Manager errors
	CreateTableRegistryError      = errors.MustNewCode("create_table.registry.operation_error")
	CreateTableTransactionError   = errors.MustNewCode("create_table.registry.transaction_error")
	CreateTableDuplicateError     = errors.MustNewCode("create_table.registry.duplicate_table")
	CreateTableDatabaseError      = errors.MustNewCode("create_table.registry.database_error")
	CreateTableSchemaStorageError = errors.MustNewCode("create_table.registry.schema_storage")

	// Astha integration errors
	CreateTableEventProcessingError = errors.MustNewCode("create_table.astha.event_processing")
	CreateTableEventPublishError    = errors.MustNewCode("create_table.astha.event_publish")
	CreateTableSubscriberError      = errors.MustNewCode("create_table.astha.subscriber_error")

	// Schema Manager errors
	CreateTableCacheError           = errors.MustNewCode("create_table.schema_cache.operation_error")
	CreateTableCacheRefreshError    = errors.MustNewCode("create_table.schema_cache.refresh_error")
	CreateTableCacheInvalidateError = errors.MustNewCode("create_table.schema_cache.invalidate_error")

	// Integration errors
	CreateTableIntegrationError = errors.MustNewCode("create_table.integration.flow_error")
	CreateTableRecoveryError    = errors.MustNewCode("create_table.recovery.operation_error")
)

// CreateTableError represents a comprehensive error for CREATE TABLE operations
type CreateTableError struct {
	BaseError     *errors.Error           `json:"base_error"`
	Code          errors.Code             `json:"code"`
	Component     string                  `json:"component"`
	Operation     string                  `json:"operation"`
	TableName     string                  `json:"table_name,omitempty"`
	Database      string                  `json:"database,omitempty"`
	RequestID     string                  `json:"request_id,omitempty"`
	Suggestions   []string                `json:"suggestions,omitempty"`
	RecoveryHints []RecoveryHint          `json:"recovery_hints,omitempty"`
	Diagnostics   *CreateTableDiagnostics `json:"diagnostics,omitempty"`
}

// RecoveryHint provides actionable recovery suggestions
type RecoveryHint struct {
	Type        string `json:"type"`        // "retry", "fix_syntax", "check_config", etc.
	Description string `json:"description"` // Human-readable description
	Action      string `json:"action"`      // Specific action to take
	Automatic   bool   `json:"automatic"`   // Whether this can be automatically recovered
}

// CreateTableDiagnostics contains detailed diagnostic information
type CreateTableDiagnostics struct {
	Timestamp         time.Time              `json:"timestamp"`
	Component         string                 `json:"component"`
	Operation         string                 `json:"operation"`
	Duration          time.Duration          `json:"duration,omitempty"`
	StackTrace        []errors.Frame         `json:"stack_trace,omitempty"`
	SystemState       map[string]interface{} `json:"system_state,omitempty"`
	ValidationDetails *ValidationDiagnostics `json:"validation_details,omitempty"`
	RegistryState     *RegistryDiagnostics   `json:"registry_state,omitempty"`
	CacheState        *CacheDiagnostics      `json:"cache_state,omitempty"`
}

// ValidationDiagnostics contains validation-specific diagnostic information
type ValidationDiagnostics struct {
	FailedColumns    []string          `json:"failed_columns,omitempty"`
	InvalidTypes     []string          `json:"invalid_types,omitempty"`
	SuggestedTypes   map[string]string `json:"suggested_types,omitempty"`
	ValidationSteps  []string          `json:"validation_steps,omitempty"`
	SupportedTypes   []string          `json:"supported_types,omitempty"`
	ComplexTypeHints []string          `json:"complex_type_hints,omitempty"`
}

// RegistryDiagnostics contains registry-specific diagnostic information
type RegistryDiagnostics struct {
	TransactionID        string            `json:"transaction_id,omitempty"`
	TablesCreated        []string          `json:"tables_created,omitempty"`
	ColumnsCreated       int               `json:"columns_created,omitempty"`
	RollbackPerformed    bool              `json:"rollback_performed"`
	DatabaseState        map[string]string `json:"database_state,omitempty"`
	ConstraintViolations []string          `json:"constraint_violations,omitempty"`
}

// CacheDiagnostics contains cache-specific diagnostic information
type CacheDiagnostics struct {
	CacheHit        bool      `json:"cache_hit"`
	CacheSize       int       `json:"cache_size"`
	EvictionsCount  int       `json:"evictions_count"`
	RefreshAttempts int       `json:"refresh_attempts"`
	LastRefresh     time.Time `json:"last_refresh,omitempty"`
	CacheKeys       []string  `json:"cache_keys,omitempty"`
	MemoryUsage     int64     `json:"memory_usage,omitempty"`
}

// Error creation functions with comprehensive context

// NewCreateTableParseError creates a parsing error with suggestions
func NewCreateTableParseError(message string, tableName string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableParseError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableParseError,
		Component: "parser",
		Operation: "parse_create_table",
		TableName: tableName,
		Suggestions: []string{
			"Check SQL syntax for CREATE TABLE statement",
			"Ensure all required keywords are present (CREATE, TABLE, column definitions)",
			"Verify column definitions follow the format: column_name data_type [constraints]",
			"Check for missing commas between column definitions",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "fix_syntax",
				Description: "Fix SQL syntax errors in the CREATE TABLE statement",
				Action:      "Review and correct the SQL statement syntax",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "parser",
			Operation: "parse_create_table",
		},
	}
}

// NewCreateTableTypeValidationError creates a type validation error with detailed suggestions
func NewCreateTableTypeValidationError(message string, tableName string, invalidTypes []string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableTypeValidationError, message, cause)

	validator := NewIcebergTypeValidator()
	supportedTypes := validator.GetSupportedTypes()

	suggestions := []string{
		fmt.Sprintf("Use only supported Iceberg types: %s", strings.Join(supportedTypes, ", ")),
		"Complex types: list<type>, map<keyType,valueType>, struct<field:type,...>",
		"Decimal types: decimal(precision,scale) where precision > 0 and scale >= 0",
	}

	// Add specific migration suggestions for invalid types
	suggestedTypes := make(map[string]string)
	for _, invalidType := range invalidTypes {
		if suggestion, err := validator.GetMigrationSuggestion(invalidType); err == nil {
			suggestedTypes[invalidType] = suggestion
			suggestions = append(suggestions, fmt.Sprintf("Replace '%s' with '%s'", invalidType, suggestion))
		}
	}

	return &CreateTableError{
		BaseError:   baseErr,
		Code:        CreateTableTypeValidationError,
		Component:   "parser",
		Operation:   "validate_types",
		TableName:   tableName,
		Suggestions: suggestions,
		RecoveryHints: []RecoveryHint{
			{
				Type:        "fix_types",
				Description: "Replace invalid data types with supported Iceberg types",
				Action:      "Update column data types to use Iceberg type system",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "parser",
			Operation: "validate_types",
			ValidationDetails: &ValidationDiagnostics{
				InvalidTypes:   invalidTypes,
				SuggestedTypes: suggestedTypes,
				SupportedTypes: supportedTypes,
				ComplexTypeHints: []string{
					"list<string> for arrays of strings",
					"map<string,int32> for key-value mappings",
					"struct<name:string,age:int32> for nested structures",
					"decimal(10,2) for precise decimal numbers",
				},
			},
		},
	}
}

// NewCreateTableColumnValidationError creates a column validation error
func NewCreateTableColumnValidationError(message string, tableName string, failedColumns []string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableColumnValidationError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableColumnValidationError,
		Component: "parser",
		Operation: "validate_columns",
		TableName: tableName,
		Suggestions: []string{
			"Ensure all column names are unique within the table",
			"Column names must not be empty",
			"Check for reserved keywords used as column names",
			"Verify column constraints are valid",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "fix_columns",
				Description: "Fix column definition issues",
				Action:      "Review and correct column names and constraints",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "parser",
			Operation: "validate_columns",
			ValidationDetails: &ValidationDiagnostics{
				FailedColumns: failedColumns,
				ValidationSteps: []string{
					"Check column name uniqueness",
					"Validate column name format",
					"Verify data type validity",
					"Check constraint syntax",
				},
			},
		},
	}
}

// NewCreateTableStorageError creates a storage-related error
func NewCreateTableStorageError(message string, tableName, database string, requestID string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableStorageOperationError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableStorageOperationError,
		Component: "storage_manager",
		Operation: "create_table",
		TableName: tableName,
		Database:  database,
		RequestID: requestID,
		Suggestions: []string{
			"Check storage engine configuration",
			"Verify database permissions",
			"Ensure storage backend is accessible",
			"Check disk space availability",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "retry",
				Description: "Retry the operation after checking storage configuration",
				Action:      "Verify storage backend connectivity and retry",
				Automatic:   true,
			},
			{
				Type:        "check_config",
				Description: "Verify storage engine configuration",
				Action:      "Check storage engine settings and permissions",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "storage_manager",
			Operation: "create_table",
		},
	}
}

// NewCreateTableRegistryError creates a registry operation error
func NewCreateTableRegistryError(message string, tableName, database string, requestID string, transactionID string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableRegistryError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableRegistryError,
		Component: "metadata_manager",
		Operation: "registry_operation",
		TableName: tableName,
		Database:  database,
		RequestID: requestID,
		Suggestions: []string{
			"Check database connectivity",
			"Verify transaction state",
			"Ensure registry schema is up to date",
			"Check for database locks or conflicts",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "retry",
				Description: "Retry the operation with a new transaction",
				Action:      "Start a new transaction and retry the registry operation",
				Automatic:   true,
			},
			{
				Type:        "check_database",
				Description: "Verify database connectivity and state",
				Action:      "Check database connection and schema integrity",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "metadata_manager",
			Operation: "registry_operation",
			RegistryState: &RegistryDiagnostics{
				TransactionID: transactionID,
			},
		},
	}
}

// NewCreateTableDuplicateError creates a duplicate table error
func NewCreateTableDuplicateError(tableName, database string, requestID string) *CreateTableError {
	message := fmt.Sprintf("table '%s' already exists in database '%s'", tableName, database)
	baseErr := errors.New(CreateTableDuplicateError, message, nil)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableDuplicateError,
		Component: "metadata_manager",
		Operation: "check_table_existence",
		TableName: tableName,
		Database:  database,
		RequestID: requestID,
		Suggestions: []string{
			"Use a different table name",
			"Drop the existing table first (if intended)",
			"Use CREATE TABLE IF NOT EXISTS (if supported)",
			"Check if the table was created in a previous operation",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "rename_table",
				Description: "Use a different table name",
				Action:      "Choose a unique table name that doesn't conflict with existing tables",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "metadata_manager",
			Operation: "check_table_existence",
			RegistryState: &RegistryDiagnostics{
				ConstraintViolations: []string{
					fmt.Sprintf("UNIQUE constraint violation: table name '%s' already exists", tableName),
				},
			},
		},
	}
}

// NewCreateTableEventProcessingError creates an Astha event processing error
func NewCreateTableEventProcessingError(message string, tableName, database string, eventType string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableEventProcessingError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableEventProcessingError,
		Component: "astha",
		Operation: "event_processing",
		TableName: tableName,
		Database:  database,
		Suggestions: []string{
			"Check Astha service health",
			"Verify event subscriber registration",
			"Check event queue status",
			"Ensure all subscribers are responsive",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "retry",
				Description: "Retry event processing",
				Action:      "Re-queue the event for processing",
				Automatic:   true,
			},
			{
				Type:        "check_subscribers",
				Description: "Verify subscriber health",
				Action:      "Check all event subscribers are healthy and responsive",
				Automatic:   false,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "astha",
			Operation: "event_processing",
			SystemState: map[string]interface{}{
				"event_type": eventType,
				"table_name": tableName,
				"database":   database,
			},
		},
	}
}

// NewCreateTableCacheError creates a schema cache error
func NewCreateTableCacheError(message string, tableName, database string, operation string, cause error) *CreateTableError {
	baseErr := errors.New(CreateTableCacheError, message, cause)

	return &CreateTableError{
		BaseError: baseErr,
		Code:      CreateTableCacheError,
		Component: "schema_manager",
		Operation: operation,
		TableName: tableName,
		Database:  database,
		Suggestions: []string{
			"Check schema cache health",
			"Verify cache memory limits",
			"Check cache eviction policies",
			"Ensure schema retrieval is working",
		},
		RecoveryHints: []RecoveryHint{
			{
				Type:        "retry",
				Description: "Retry cache operation with exponential backoff",
				Action:      "Retry the cache operation after a brief delay",
				Automatic:   true,
			},
			{
				Type:        "invalidate_cache",
				Description: "Clear and rebuild cache entry",
				Action:      "Invalidate the cache entry and fetch fresh data",
				Automatic:   true,
			},
		},
		Diagnostics: &CreateTableDiagnostics{
			Timestamp: time.Now(),
			Component: "schema_manager",
			Operation: operation,
		},
	}
}

// Error enhancement methods

// AddDiagnostics adds diagnostic information to the error
func (e *CreateTableError) AddDiagnostics(diagnostics *CreateTableDiagnostics) *CreateTableError {
	e.Diagnostics = diagnostics
	return e
}

// AddRecoveryHint adds a recovery hint to the error
func (e *CreateTableError) AddRecoveryHint(hint RecoveryHint) *CreateTableError {
	e.RecoveryHints = append(e.RecoveryHints, hint)
	return e
}

// AddSuggestion adds a suggestion to the error
func (e *CreateTableError) AddSuggestion(suggestion string) *CreateTableError {
	e.Suggestions = append(e.Suggestions, suggestion)
	return e
}

// WithRequestID adds request ID context to the error
func (e *CreateTableError) WithRequestID(requestID string) *CreateTableError {
	e.RequestID = requestID
	e.AddContext("request_id", requestID)
	return e
}

// WithDuration adds operation duration to diagnostics
func (e *CreateTableError) WithDuration(duration time.Duration) *CreateTableError {
	if e.Diagnostics != nil {
		e.Diagnostics.Duration = duration
	}
	return e
}

// IsRecoverable returns true if the error has automatic recovery hints
func (e *CreateTableError) IsRecoverable() bool {
	for _, hint := range e.RecoveryHints {
		if hint.Automatic {
			return true
		}
	}
	return false
}

// GetRecoveryActions returns all available recovery actions
func (e *CreateTableError) GetRecoveryActions() []string {
	var actions []string
	for _, hint := range e.RecoveryHints {
		actions = append(actions, hint.Action)
	}
	return actions
}

// GetAutomaticRecoveryActions returns only automatic recovery actions
func (e *CreateTableError) GetAutomaticRecoveryActions() []string {
	var actions []string
	for _, hint := range e.RecoveryHints {
		if hint.Automatic {
			actions = append(actions, hint.Action)
		}
	}
	return actions
}

// Error implements the error interface
func (e *CreateTableError) Error() string {
	if e.BaseError != nil {
		return e.BaseError.Error()
	}
	return "unknown CREATE TABLE error"
}

// String returns a comprehensive error description with context
func (e *CreateTableError) String() string {
	var parts []string

	// Base error message
	if e.BaseError != nil {
		parts = append(parts, e.BaseError.Error())
	}

	// Component and operation context
	if e.Component != "" && e.Operation != "" {
		parts = append(parts, fmt.Sprintf("[%s:%s]", e.Component, e.Operation))
	}

	// Table context
	if e.TableName != "" {
		if e.Database != "" {
			parts = append(parts, fmt.Sprintf("table=%s.%s", e.Database, e.TableName))
		} else {
			parts = append(parts, fmt.Sprintf("table=%s", e.TableName))
		}
	}

	// Request context
	if e.RequestID != "" {
		parts = append(parts, fmt.Sprintf("request_id=%s", e.RequestID))
	}

	return strings.Join(parts, " ")
}

// DetailedError returns a comprehensive error description with suggestions
func (e *CreateTableError) DetailedError() string {
	var parts []string

	// Base error
	parts = append(parts, e.String())

	// Suggestions
	if len(e.Suggestions) > 0 {
		parts = append(parts, "\nSuggestions:")
		for i, suggestion := range e.Suggestions {
			parts = append(parts, fmt.Sprintf("  %d. %s", i+1, suggestion))
		}
	}

	// Recovery hints
	if len(e.RecoveryHints) > 0 {
		parts = append(parts, "\nRecovery Actions:")
		for i, hint := range e.RecoveryHints {
			autoFlag := ""
			if hint.Automatic {
				autoFlag = " (automatic)"
			}
			parts = append(parts, fmt.Sprintf("  %d. %s: %s%s", i+1, hint.Type, hint.Description, autoFlag))
		}
	}

	return strings.Join(parts, "")
}

// AddContext adds context to the base error
func (e *CreateTableError) AddContext(key string, value interface{}) *CreateTableError {
	if e.BaseError != nil {
		e.BaseError.AddContext(key, value)
	}
	return e
}

// Transform implements the InternalError interface
func (e *CreateTableError) Transform() *errors.Error {
	return e.BaseError
}
