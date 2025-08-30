package types

import (
	"testing"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
)

func TestCreateTableParseError(t *testing.T) {
	tableName := "test_table"
	message := "invalid syntax in CREATE TABLE statement"
	cause := errors.New(errors.CommonValidation, "missing column definition", nil)

	err := NewCreateTableParseError(message, tableName, cause)

	// Test basic properties
	if err.Component != "parser" {
		t.Errorf("Expected component 'parser', got '%s'", err.Component)
	}
	if err.Operation != "parse_create_table" {
		t.Errorf("Expected operation 'parse_create_table', got '%s'", err.Operation)
	}
	if err.TableName != tableName {
		t.Errorf("Expected table name '%s', got '%s'", tableName, err.TableName)
	}

	// Test suggestions
	if len(err.Suggestions) == 0 {
		t.Error("Expected suggestions to be provided")
	}

	// Test recovery hints
	if len(err.RecoveryHints) == 0 {
		t.Error("Expected recovery hints to be provided")
	}

	// Test diagnostics
	if err.Diagnostics == nil {
		t.Error("Expected diagnostics to be provided")
	}
	if err.Diagnostics.Component != "parser" {
		t.Errorf("Expected diagnostics component 'parser', got '%s'", err.Diagnostics.Component)
	}
}

func TestCreateTableTypeValidationError(t *testing.T) {
	tableName := "test_table"
	message := "invalid data types detected"
	invalidTypes := []string{"VARCHAR", "INT", "BOOLEAN"}
	cause := errors.New(errors.CommonValidation, "unsupported types", nil)

	err := NewCreateTableTypeValidationError(message, tableName, invalidTypes, cause)

	// Test basic properties
	if err.Component != "parser" {
		t.Errorf("Expected component 'parser', got '%s'", err.Component)
	}
	if err.Operation != "validate_types" {
		t.Errorf("Expected operation 'validate_types', got '%s'", err.Operation)
	}

	// Test validation diagnostics
	if err.Diagnostics == nil || err.Diagnostics.ValidationDetails == nil {
		t.Error("Expected validation diagnostics to be provided")
	}

	validationDetails := err.Diagnostics.ValidationDetails
	if len(validationDetails.InvalidTypes) != len(invalidTypes) {
		t.Errorf("Expected %d invalid types, got %d", len(invalidTypes), len(validationDetails.InvalidTypes))
	}

	// Test migration suggestions
	if len(validationDetails.SuggestedTypes) == 0 {
		t.Error("Expected migration suggestions to be provided")
	}

	// Test supported types
	if len(validationDetails.SupportedTypes) == 0 {
		t.Error("Expected supported types to be provided")
	}

	// Test complex type hints
	if len(validationDetails.ComplexTypeHints) == 0 {
		t.Error("Expected complex type hints to be provided")
	}
}

func TestCreateTableDuplicateError(t *testing.T) {
	tableName := "existing_table"
	database := "test_db"
	requestID := "req_123"

	err := NewCreateTableDuplicateError(tableName, database, requestID)

	// Test basic properties
	if err.Component != "metadata_manager" {
		t.Errorf("Expected component 'metadata_manager', got '%s'", err.Component)
	}
	if err.Operation != "check_table_existence" {
		t.Errorf("Expected operation 'check_table_existence', got '%s'", err.Operation)
	}
	if err.TableName != tableName {
		t.Errorf("Expected table name '%s', got '%s'", tableName, err.TableName)
	}
	if err.Database != database {
		t.Errorf("Expected database '%s', got '%s'", database, err.Database)
	}
	if err.RequestID != requestID {
		t.Errorf("Expected request ID '%s', got '%s'", requestID, err.RequestID)
	}

	// Test error message
	expectedMessage := "table 'existing_table' already exists in database 'test_db'"
	if err.BaseError.Message != expectedMessage {
		t.Errorf("Expected message '%s', got '%s'", expectedMessage, err.BaseError.Message)
	}

	// Test registry diagnostics
	if err.Diagnostics == nil || err.Diagnostics.RegistryState == nil {
		t.Error("Expected registry diagnostics to be provided")
	}

	registryState := err.Diagnostics.RegistryState
	if len(registryState.ConstraintViolations) == 0 {
		t.Error("Expected constraint violations to be recorded")
	}
}

func TestCreateTableErrorRecoverability(t *testing.T) {
	// Test recoverable error
	recoverableErr := NewCreateTableStorageError(
		"storage operation failed",
		"test_table",
		"test_db",
		"req_123",
		errors.New(errors.CommonTimeout, "connection timeout", nil),
	)

	if !recoverableErr.IsRecoverable() {
		t.Error("Expected storage error to be recoverable")
	}

	recoveryActions := recoverableErr.GetAutomaticRecoveryActions()
	if len(recoveryActions) == 0 {
		t.Error("Expected automatic recovery actions to be available")
	}

	// Test non-recoverable error (parse error)
	nonRecoverableErr := NewCreateTableParseError(
		"syntax error",
		"test_table",
		errors.New(errors.CommonValidation, "invalid syntax", nil),
	)

	// Parse errors typically don't have automatic recovery
	automaticActions := nonRecoverableErr.GetAutomaticRecoveryActions()
	if len(automaticActions) > 0 {
		t.Error("Expected parse error to not have automatic recovery actions")
	}
}

func TestCreateTableErrorContextEnhancement(t *testing.T) {
	err := NewCreateTableParseError(
		"test error",
		"test_table",
		nil,
	)

	// Test adding context
	err.AddContext("custom_field", "custom_value")
	if err.BaseError.Context["custom_field"] != "custom_value" {
		t.Error("Expected custom context to be added")
	}

	// Test adding suggestions
	originalSuggestionCount := len(err.Suggestions)
	err.AddSuggestion("Custom suggestion")
	if len(err.Suggestions) != originalSuggestionCount+1 {
		t.Error("Expected suggestion to be added")
	}

	// Test adding recovery hints
	originalHintCount := len(err.RecoveryHints)
	err.AddRecoveryHint(RecoveryHint{
		Type:        "custom",
		Description: "Custom recovery hint",
		Action:      "Take custom action",
		Automatic:   false,
	})
	if len(err.RecoveryHints) != originalHintCount+1 {
		t.Error("Expected recovery hint to be added")
	}

	// Test request ID enhancement
	requestID := "test_request_123"
	err.WithRequestID(requestID)
	if err.RequestID != requestID {
		t.Errorf("Expected request ID '%s', got '%s'", requestID, err.RequestID)
	}

	// Test duration enhancement
	duration := 100 * time.Millisecond
	err.WithDuration(duration)
	if err.Diagnostics != nil && err.Diagnostics.Duration != duration {
		t.Errorf("Expected duration '%v', got '%v'", duration, err.Diagnostics.Duration)
	}
}

func TestCreateTableErrorFormatting(t *testing.T) {
	err := NewCreateTableStorageError(
		"storage operation failed",
		"test_table",
		"test_db",
		"req_123",
		errors.New(errors.CommonTimeout, "connection timeout", nil),
	)

	// Test basic error string
	errorStr := err.String()
	if errorStr == "" {
		t.Error("Expected non-empty error string")
	}

	// Test detailed error
	detailedError := err.DetailedError()
	if detailedError == "" {
		t.Error("Expected non-empty detailed error")
	}

	// Detailed error should contain suggestions
	if len(err.Suggestions) > 0 {
		found := false
		for _, suggestion := range err.Suggestions {
			if len(suggestion) > 0 {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected detailed error to contain suggestions")
		}
	}

	// Detailed error should contain recovery hints
	if len(err.RecoveryHints) > 0 {
		found := false
		for _, hint := range err.RecoveryHints {
			if len(hint.Description) > 0 {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected detailed error to contain recovery hints")
		}
	}
}

func TestCreateTableErrorTransform(t *testing.T) {
	originalErr := errors.New(errors.CommonValidation, "test error", nil)
	createTableErr := NewCreateTableParseError(
		"parse error",
		"test_table",
		originalErr,
	)

	// Test Transform method
	transformedErr := createTableErr.Transform()
	if transformedErr == nil {
		t.Error("Expected transformed error to not be nil")
	}

	if transformedErr != createTableErr.BaseError {
		t.Error("Expected transformed error to be the same as the embedded BaseError")
	}
}

func TestDiagnosticsStructures(t *testing.T) {
	// Test ValidationDiagnostics
	validationDiag := &ValidationDiagnostics{
		FailedColumns:    []string{"col1", "col2"},
		InvalidTypes:     []string{"VARCHAR", "INT"},
		SuggestedTypes:   map[string]string{"VARCHAR": "string", "INT": "int32"},
		ValidationSteps:  []string{"step1", "step2"},
		SupportedTypes:   []string{"string", "int32", "int64"},
		ComplexTypeHints: []string{"list<string>", "map<string,int32>"},
	}

	if len(validationDiag.FailedColumns) != 2 {
		t.Error("Expected 2 failed columns")
	}
	if len(validationDiag.InvalidTypes) != 2 {
		t.Error("Expected 2 invalid types")
	}
	if len(validationDiag.SuggestedTypes) != 2 {
		t.Error("Expected 2 suggested types")
	}

	// Test RegistryDiagnostics
	registryDiag := &RegistryDiagnostics{
		TransactionID:        "tx_123",
		TablesCreated:        []string{"table1"},
		ColumnsCreated:       5,
		RollbackPerformed:    false,
		DatabaseState:        map[string]string{"status": "ok"},
		ConstraintViolations: []string{"unique constraint"},
	}

	if registryDiag.TransactionID != "tx_123" {
		t.Error("Expected transaction ID to be set")
	}
	if registryDiag.ColumnsCreated != 5 {
		t.Error("Expected 5 columns created")
	}

	// Test CacheDiagnostics
	cacheDiag := &CacheDiagnostics{
		CacheHit:        true,
		CacheSize:       100,
		EvictionsCount:  5,
		RefreshAttempts: 3,
		LastRefresh:     time.Now(),
		CacheKeys:       []string{"key1", "key2"},
		MemoryUsage:     1024,
	}

	if !cacheDiag.CacheHit {
		t.Error("Expected cache hit to be true")
	}
	if cacheDiag.CacheSize != 100 {
		t.Error("Expected cache size to be 100")
	}
	if cacheDiag.MemoryUsage != 1024 {
		t.Error("Expected memory usage to be 1024")
	}
}

func TestRecoveryHint(t *testing.T) {
	hint := RecoveryHint{
		Type:        "retry",
		Description: "Retry the operation",
		Action:      "Wait and retry",
		Automatic:   true,
	}

	if hint.Type != "retry" {
		t.Error("Expected hint type to be 'retry'")
	}
	if !hint.Automatic {
		t.Error("Expected hint to be automatic")
	}
	if hint.Description == "" {
		t.Error("Expected hint description to not be empty")
	}
	if hint.Action == "" {
		t.Error("Expected hint action to not be empty")
	}
}
