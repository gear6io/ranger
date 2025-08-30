package types

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestNewDiagnosticLogger(t *testing.T) {
	logger := zerolog.Nop()
	component := "test_component"
	requestID := "req_123"

	diagLogger := NewDiagnosticLogger(logger, component, requestID)

	if diagLogger.component != component {
		t.Errorf("Expected component '%s', got '%s'", component, diagLogger.component)
	}
	if diagLogger.requestID != requestID {
		t.Errorf("Expected request ID '%s', got '%s'", requestID, diagLogger.requestID)
	}
	if diagLogger.operations == nil {
		t.Error("Expected operations slice to be initialized")
	}
	if diagLogger.startTime.IsZero() {
		t.Error("Expected start time to be set")
	}
}

func TestDiagnosticLoggerOperationLogging(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	// Test operation start logging
	context := map[string]interface{}{
		"param1": "value1",
		"param2": 42,
	}
	diagLogger.LogOperationStart("test_operation", context)

	// Test operation end logging - success
	metadata := map[string]interface{}{
		"result": "success",
		"count":  5,
	}
	diagLogger.LogOperationEnd("test_operation", true, nil, metadata)

	// Verify operation was recorded
	if len(diagLogger.operations) != 1 {
		t.Errorf("Expected 1 operation to be recorded, got %d", len(diagLogger.operations))
	}

	operation := diagLogger.operations[0]
	if operation.Operation != "test_operation" {
		t.Errorf("Expected operation name 'test_operation', got '%s'", operation.Operation)
	}
	if operation.Component != "test_component" {
		t.Errorf("Expected component 'test_component', got '%s'", operation.Component)
	}
	if !operation.Success {
		t.Error("Expected operation to be marked as successful")
	}
	if operation.Error != "" {
		t.Errorf("Expected no error, got '%s'", operation.Error)
	}
	if operation.Metadata == nil {
		t.Error("Expected metadata to be recorded")
	}
}

func TestDiagnosticLoggerOperationLoggingWithError(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	// Test operation end logging - failure
	testError := NewCreateTableParseError("test error", "test_table", nil)
	diagLogger.LogOperationEnd("test_operation", false, testError, nil)

	// Verify operation was recorded with error
	if len(diagLogger.operations) != 1 {
		t.Errorf("Expected 1 operation to be recorded, got %d", len(diagLogger.operations))
	}

	operation := diagLogger.operations[0]
	if operation.Success {
		t.Error("Expected operation to be marked as failed")
	}
	if operation.Error == "" {
		t.Error("Expected error to be recorded")
	}
	if len(operation.StackTrace) == 0 {
		t.Error("Expected stack trace to be captured for failed operation")
	}
}

func TestDiagnosticLoggerValidationError(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	validationDetails := &ValidationDiagnostics{
		FailedColumns:    []string{"col1", "col2"},
		InvalidTypes:     []string{"VARCHAR", "INT"},
		SuggestedTypes:   map[string]string{"VARCHAR": "string", "INT": "int32"},
		ValidationSteps:  []string{"step1", "step2"},
		SupportedTypes:   []string{"string", "int32"},
		ComplexTypeHints: []string{"list<string>"},
	}

	// This should not panic and should log the validation details
	diagLogger.LogValidationError("validate_types", validationDetails)

	// No direct way to verify logging output, but we can ensure it doesn't crash
}

func TestDiagnosticLoggerRegistryOperation(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	registryDetails := &RegistryDiagnostics{
		TransactionID:        "tx_123",
		TablesCreated:        []string{"table1", "table2"},
		ColumnsCreated:       10,
		RollbackPerformed:    false,
		DatabaseState:        map[string]string{"status": "ok"},
		ConstraintViolations: []string{},
	}

	// This should not panic and should log the registry details
	diagLogger.LogRegistryOperation("create_table", registryDetails)

	// No direct way to verify logging output, but we can ensure it doesn't crash
}

func TestDiagnosticLoggerCacheOperation(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	cacheDetails := &CacheDiagnostics{
		CacheHit:        true,
		CacheSize:       100,
		EvictionsCount:  5,
		RefreshAttempts: 3,
		LastRefresh:     time.Now(),
		CacheKeys:       []string{"key1", "key2"},
		MemoryUsage:     1024,
	}

	// This should not panic and should log the cache details
	diagLogger.LogCacheOperation("cache_refresh", cacheDetails)

	// No direct way to verify logging output, but we can ensure it doesn't crash
}

func TestDiagnosticLoggerErrorRecovery(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	err := NewCreateTableStorageError(
		"storage failed",
		"test_table",
		"test_db",
		"req_123",
		nil,
	)

	// Test successful recovery
	diagLogger.LogErrorRecovery(err, 1, 3, true)

	// Test failed recovery
	diagLogger.LogErrorRecovery(err, 3, 3, false)

	// No direct way to verify logging output, but we can ensure it doesn't crash
}

func TestDiagnosticLoggerPerformanceMetrics(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	duration := 500 * time.Millisecond
	metadata := map[string]interface{}{
		"rows_processed": 1000,
		"memory_used":    1024,
	}

	// This should not panic and should log the performance metrics
	diagLogger.LogPerformanceMetrics("process_data", duration, metadata)

	// No direct way to verify logging output, but we can ensure it doesn't crash
}

func TestGenerateOperationSummary(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	// Add some operations
	diagLogger.LogOperationEnd("operation1", true, nil, nil)
	diagLogger.LogOperationEnd("operation2", false, NewCreateTableParseError("error", "table", nil), nil)
	diagLogger.LogOperationEnd("operation3", true, nil, nil)

	summary := diagLogger.GenerateOperationSummary()

	if summary.RequestID != "req_123" {
		t.Errorf("Expected request ID 'req_123', got '%s'", summary.RequestID)
	}
	if summary.Component != "test_component" {
		t.Errorf("Expected component 'test_component', got '%s'", summary.Component)
	}
	if summary.TotalOperations != 3 {
		t.Errorf("Expected 3 total operations, got %d", summary.TotalOperations)
	}
	if summary.SuccessCount != 2 {
		t.Errorf("Expected 2 successful operations, got %d", summary.SuccessCount)
	}
	if summary.ErrorCount != 1 {
		t.Errorf("Expected 1 failed operation, got %d", summary.ErrorCount)
	}

	expectedSuccessRate := 2.0 / 3.0
	if summary.SuccessRate != expectedSuccessRate {
		t.Errorf("Expected success rate %f, got %f", expectedSuccessRate, summary.SuccessRate)
	}

	if len(summary.Operations) != 3 {
		t.Errorf("Expected 3 operations in summary, got %d", len(summary.Operations))
	}
}

func TestExportDiagnostics(t *testing.T) {
	logger := zerolog.Nop()
	diagLogger := NewDiagnosticLogger(logger, "test_component", "req_123")

	// Add an operation
	diagLogger.LogOperationEnd("test_operation", true, nil, map[string]interface{}{"result": "success"})

	jsonStr, err := diagLogger.ExportDiagnostics()
	if err != nil {
		t.Errorf("Expected no error exporting diagnostics, got: %v", err)
	}
	if jsonStr == "" {
		t.Error("Expected non-empty JSON string")
	}

	// Basic check that it's valid JSON (should contain expected fields)
	if !contains(jsonStr, "req_123") {
		t.Error("Expected JSON to contain request ID")
	}
	if !contains(jsonStr, "test_component") {
		t.Error("Expected JSON to contain component name")
	}
}

func TestCreateTableDiagnosticContext(t *testing.T) {
	requestID := "req_123"
	tableName := "test_table"
	database := "test_db"
	userCtx := &UserContext{
		UserID:   1,
		Username: "testuser",
		Database: database,
		IsAdmin:  false,
	}

	diagCtx := NewCreateTableDiagnosticContext(requestID, tableName, database, userCtx)

	if diagCtx.RequestID != requestID {
		t.Errorf("Expected request ID '%s', got '%s'", requestID, diagCtx.RequestID)
	}
	if diagCtx.TableName != tableName {
		t.Errorf("Expected table name '%s', got '%s'", tableName, diagCtx.TableName)
	}
	if diagCtx.Database != database {
		t.Errorf("Expected database '%s', got '%s'", database, diagCtx.Database)
	}
	if diagCtx.UserContext != userCtx {
		t.Error("Expected user context to be set")
	}
	if diagCtx.SystemState == nil {
		t.Error("Expected system state to be initialized")
	}
	if diagCtx.ComponentLogs == nil {
		t.Error("Expected component logs to be initialized")
	}
}

func TestCreateTableDiagnosticContextGetComponentLogger(t *testing.T) {
	diagCtx := NewCreateTableDiagnosticContext("req_123", "test_table", "test_db", nil)
	logger := zerolog.Nop()

	// Get logger for first time
	componentLogger1 := diagCtx.GetComponentLogger("parser", logger)
	if componentLogger1 == nil {
		t.Error("Expected component logger to be created")
	}

	// Get logger for same component again
	componentLogger2 := diagCtx.GetComponentLogger("parser", logger)
	if componentLogger1 != componentLogger2 {
		t.Error("Expected same component logger instance to be returned")
	}

	// Get logger for different component
	componentLogger3 := diagCtx.GetComponentLogger("storage", logger)
	if componentLogger1 == componentLogger3 {
		t.Error("Expected different component logger instances for different components")
	}

	// Verify component loggers are stored
	if len(diagCtx.ComponentLogs) != 2 {
		t.Errorf("Expected 2 component loggers to be stored, got %d", len(diagCtx.ComponentLogs))
	}
}

func TestCreateTableDiagnosticContextAddSystemState(t *testing.T) {
	diagCtx := NewCreateTableDiagnosticContext("req_123", "test_table", "test_db", nil)

	diagCtx.AddSystemState("memory_usage", 1024)
	diagCtx.AddSystemState("cpu_usage", 75.5)
	diagCtx.AddSystemState("status", "running")

	if len(diagCtx.SystemState) != 3 {
		t.Errorf("Expected 3 system state entries, got %d", len(diagCtx.SystemState))
	}

	if diagCtx.SystemState["memory_usage"] != 1024 {
		t.Error("Expected memory_usage to be set correctly")
	}
	if diagCtx.SystemState["cpu_usage"] != 75.5 {
		t.Error("Expected cpu_usage to be set correctly")
	}
	if diagCtx.SystemState["status"] != "running" {
		t.Error("Expected status to be set correctly")
	}
}

func TestGenerateComprehensiveReport(t *testing.T) {
	userCtx := &UserContext{
		UserID:   1,
		Username: "testuser",
		Database: "test_db",
		IsAdmin:  false,
	}

	diagCtx := NewCreateTableDiagnosticContext("req_123", "test_table", "test_db", userCtx)
	logger := zerolog.Nop()

	// Add some component loggers and operations
	parserLogger := diagCtx.GetComponentLogger("parser", logger)
	parserLogger.LogOperationEnd("parse", true, nil, nil)

	storageLogger := diagCtx.GetComponentLogger("storage", logger)
	storageLogger.LogOperationEnd("create", false, NewCreateTableStorageError("failed", "test_table", "test_db", "req_123", nil), nil)

	diagCtx.AddSystemState("memory_usage", 1024)

	report := diagCtx.GenerateComprehensiveReport()

	if report.RequestID != "req_123" {
		t.Errorf("Expected request ID 'req_123', got '%s'", report.RequestID)
	}
	if report.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", report.TableName)
	}
	if report.Database != "test_db" {
		t.Errorf("Expected database 'test_db', got '%s'", report.Database)
	}
	if report.UserContext != userCtx {
		t.Error("Expected user context to be included")
	}
	if len(report.SystemState) != 1 {
		t.Errorf("Expected 1 system state entry, got %d", len(report.SystemState))
	}
	if len(report.ComponentReports) != 2 {
		t.Errorf("Expected 2 component reports, got %d", len(report.ComponentReports))
	}

	// Check component reports
	if _, exists := report.ComponentReports["parser"]; !exists {
		t.Error("Expected parser component report to exist")
	}
	if _, exists := report.ComponentReports["storage"]; !exists {
		t.Error("Expected storage component report to exist")
	}
}

func TestComprehensiveReportExport(t *testing.T) {
	diagCtx := NewCreateTableDiagnosticContext("req_123", "test_table", "test_db", nil)
	report := diagCtx.GenerateComprehensiveReport()

	jsonStr, err := report.ExportReport()
	if err != nil {
		t.Errorf("Expected no error exporting report, got: %v", err)
	}
	if jsonStr == "" {
		t.Error("Expected non-empty JSON string")
	}

	// Basic check that it's valid JSON
	if !contains(jsonStr, "req_123") {
		t.Error("Expected JSON to contain request ID")
	}
	if !contains(jsonStr, "test_table") {
		t.Error("Expected JSON to contain table name")
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
