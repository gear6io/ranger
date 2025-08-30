package types

import (
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/rs/zerolog"
)

// DiagnosticLogger provides comprehensive logging for CREATE TABLE operations
type DiagnosticLogger struct {
	logger     zerolog.Logger
	component  string
	requestID  string
	startTime  time.Time
	operations []OperationLog
}

// OperationLog represents a single operation in the CREATE TABLE flow
type OperationLog struct {
	Operation  string                 `json:"operation"`
	Component  string                 `json:"component"`
	StartTime  time.Time              `json:"start_time"`
	EndTime    time.Time              `json:"end_time"`
	Duration   time.Duration          `json:"duration"`
	Success    bool                   `json:"success"`
	Error      string                 `json:"error,omitempty"`
	Context    map[string]interface{} `json:"context,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	StackTrace []string               `json:"stack_trace,omitempty"`
}

// NewDiagnosticLogger creates a new diagnostic logger for CREATE TABLE operations
func NewDiagnosticLogger(logger zerolog.Logger, component string, requestID string) *DiagnosticLogger {
	return &DiagnosticLogger{
		logger:     logger.With().Str("component", component).Str("request_id", requestID).Logger(),
		component:  component,
		requestID:  requestID,
		startTime:  time.Now(),
		operations: make([]OperationLog, 0),
	}
}

// LogOperationStart logs the start of an operation
func (dl *DiagnosticLogger) LogOperationStart(operation string, context map[string]interface{}) {
	dl.logger.Info().
		Str("operation", operation).
		Interface("context", context).
		Msg("Operation started")
}

// LogOperationEnd logs the completion of an operation
func (dl *DiagnosticLogger) LogOperationEnd(operation string, success bool, err error, metadata map[string]interface{}) {
	logEvent := dl.logger.Info()
	if !success && err != nil {
		logEvent = dl.logger.Error().Err(err)
	}

	logEvent.
		Str("operation", operation).
		Bool("success", success).
		Interface("metadata", metadata).
		Msg("Operation completed")

	// Record operation for summary
	opLog := OperationLog{
		Operation: operation,
		Component: dl.component,
		EndTime:   time.Now(),
		Success:   success,
		Metadata:  metadata,
	}

	if err != nil {
		opLog.Error = err.Error()
		if !success {
			opLog.StackTrace = captureStackTrace()
		}
	}

	dl.operations = append(dl.operations, opLog)
}

// LogValidationError logs detailed validation errors
func (dl *DiagnosticLogger) LogValidationError(operation string, validationDetails *ValidationDiagnostics) {
	dl.logger.Error().
		Str("operation", operation).
		Interface("failed_columns", validationDetails.FailedColumns).
		Interface("invalid_types", validationDetails.InvalidTypes).
		Interface("suggested_types", validationDetails.SuggestedTypes).
		Interface("supported_types", validationDetails.SupportedTypes).
		Msg("Validation error occurred")
}

// LogRegistryOperation logs registry-specific operations
func (dl *DiagnosticLogger) LogRegistryOperation(operation string, registryDetails *RegistryDiagnostics) {
	dl.logger.Info().
		Str("operation", operation).
		Str("transaction_id", registryDetails.TransactionID).
		Interface("tables_created", registryDetails.TablesCreated).
		Int("columns_created", registryDetails.ColumnsCreated).
		Bool("rollback_performed", registryDetails.RollbackPerformed).
		Interface("database_state", registryDetails.DatabaseState).
		Msg("Registry operation logged")
}

// LogCacheOperation logs cache-specific operations
func (dl *DiagnosticLogger) LogCacheOperation(operation string, cacheDetails *CacheDiagnostics) {
	dl.logger.Info().
		Str("operation", operation).
		Bool("cache_hit", cacheDetails.CacheHit).
		Int("cache_size", cacheDetails.CacheSize).
		Int("evictions_count", cacheDetails.EvictionsCount).
		Int("refresh_attempts", cacheDetails.RefreshAttempts).
		Time("last_refresh", cacheDetails.LastRefresh).
		Int64("memory_usage", cacheDetails.MemoryUsage).
		Msg("Cache operation logged")
}

// LogErrorRecovery logs error recovery attempts
func (dl *DiagnosticLogger) LogErrorRecovery(err *CreateTableError, attempt int, maxAttempts int, success bool) {
	logEvent := dl.logger.Info()
	if !success {
		logEvent = dl.logger.Warn()
	}

	logEvent.
		Str("error_code", err.Code.String()).
		Str("error_component", err.Component).
		Str("error_operation", err.Operation).
		Int("attempt", attempt).
		Int("max_attempts", maxAttempts).
		Bool("recovery_success", success).
		Interface("recovery_hints", err.RecoveryHints).
		Msg("Error recovery attempt")
}

// LogSystemState logs current system state for diagnostics
func (dl *DiagnosticLogger) LogSystemState(state map[string]interface{}) {
	dl.logger.Debug().
		Interface("system_state", state).
		Msg("System state snapshot")
}

// LogPerformanceMetrics logs performance-related metrics
func (dl *DiagnosticLogger) LogPerformanceMetrics(operation string, duration time.Duration, metadata map[string]interface{}) {
	dl.logger.Info().
		Str("operation", operation).
		Dur("duration", duration).
		Interface("metrics", metadata).
		Msg("Performance metrics")
}

// LogIntegrationFlow logs the complete CREATE TABLE integration flow
func (dl *DiagnosticLogger) LogIntegrationFlow(tableName, database string, steps []string, success bool, totalDuration time.Duration) {
	logEvent := dl.logger.Info()
	if !success {
		logEvent = dl.logger.Error()
	}

	logEvent.
		Str("table_name", tableName).
		Str("database", database).
		Interface("flow_steps", steps).
		Bool("flow_success", success).
		Dur("total_duration", totalDuration).
		Int("total_operations", len(dl.operations)).
		Msg("CREATE TABLE integration flow completed")
}

// GenerateOperationSummary generates a comprehensive summary of all operations
func (dl *DiagnosticLogger) GenerateOperationSummary() *OperationSummary {
	totalDuration := time.Since(dl.startTime)
	successCount := 0
	errorCount := 0

	for _, op := range dl.operations {
		if op.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	return &OperationSummary{
		RequestID:       dl.requestID,
		Component:       dl.component,
		StartTime:       dl.startTime,
		EndTime:         time.Now(),
		TotalDuration:   totalDuration,
		TotalOperations: len(dl.operations),
		SuccessCount:    successCount,
		ErrorCount:      errorCount,
		SuccessRate:     float64(successCount) / float64(len(dl.operations)),
		Operations:      dl.operations,
	}
}

// OperationSummary provides a comprehensive summary of CREATE TABLE operations
type OperationSummary struct {
	RequestID       string         `json:"request_id"`
	Component       string         `json:"component"`
	StartTime       time.Time      `json:"start_time"`
	EndTime         time.Time      `json:"end_time"`
	TotalDuration   time.Duration  `json:"total_duration"`
	TotalOperations int            `json:"total_operations"`
	SuccessCount    int            `json:"success_count"`
	ErrorCount      int            `json:"error_count"`
	SuccessRate     float64        `json:"success_rate"`
	Operations      []OperationLog `json:"operations"`
}

// LogSummary logs the operation summary
func (dl *DiagnosticLogger) LogSummary() {
	summary := dl.GenerateOperationSummary()

	dl.logger.Info().
		Str("request_id", summary.RequestID).
		Dur("total_duration", summary.TotalDuration).
		Int("total_operations", summary.TotalOperations).
		Int("success_count", summary.SuccessCount).
		Int("error_count", summary.ErrorCount).
		Float64("success_rate", summary.SuccessRate).
		Msg("CREATE TABLE operation summary")
}

// ExportDiagnostics exports diagnostic information as JSON
func (dl *DiagnosticLogger) ExportDiagnostics() (string, error) {
	summary := dl.GenerateOperationSummary()

	diagnosticData := map[string]interface{}{
		"summary":    summary,
		"timestamp":  time.Now(),
		"component":  dl.component,
		"request_id": dl.requestID,
	}

	jsonData, err := json.MarshalIndent(diagnosticData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to export diagnostics: %w", err)
	}

	return string(jsonData), nil
}

// Helper function to capture stack trace
func captureStackTrace() []string {
	var stackTrace []string

	for i := 1; i < 10; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		fn := runtime.FuncForPC(pc)
		stackTrace = append(stackTrace, fmt.Sprintf("%s:%d %s", file, line, fn.Name()))
	}

	return stackTrace
}

// CreateTableDiagnosticContext provides diagnostic context for CREATE TABLE operations
type CreateTableDiagnosticContext struct {
	RequestID     string                       `json:"request_id"`
	TableName     string                       `json:"table_name"`
	Database      string                       `json:"database"`
	StartTime     time.Time                    `json:"start_time"`
	UserContext   *UserContext                 `json:"user_context,omitempty"`
	SystemState   map[string]interface{}       `json:"system_state,omitempty"`
	ComponentLogs map[string]*DiagnosticLogger `json:"-"`
}

// NewCreateTableDiagnosticContext creates a new diagnostic context
func NewCreateTableDiagnosticContext(requestID, tableName, database string, userCtx *UserContext) *CreateTableDiagnosticContext {
	return &CreateTableDiagnosticContext{
		RequestID:     requestID,
		TableName:     tableName,
		Database:      database,
		StartTime:     time.Now(),
		UserContext:   userCtx,
		SystemState:   make(map[string]interface{}),
		ComponentLogs: make(map[string]*DiagnosticLogger),
	}
}

// GetComponentLogger returns a diagnostic logger for a specific component
func (ctx *CreateTableDiagnosticContext) GetComponentLogger(component string, logger zerolog.Logger) *DiagnosticLogger {
	if componentLogger, exists := ctx.ComponentLogs[component]; exists {
		return componentLogger
	}

	componentLogger := NewDiagnosticLogger(logger, component, ctx.RequestID)
	ctx.ComponentLogs[component] = componentLogger

	return componentLogger
}

// AddSystemState adds system state information
func (ctx *CreateTableDiagnosticContext) AddSystemState(key string, value interface{}) {
	ctx.SystemState[key] = value
}

// GenerateComprehensiveReport generates a comprehensive diagnostic report
func (ctx *CreateTableDiagnosticContext) GenerateComprehensiveReport() *ComprehensiveDiagnosticReport {
	report := &ComprehensiveDiagnosticReport{
		RequestID:        ctx.RequestID,
		TableName:        ctx.TableName,
		Database:         ctx.Database,
		StartTime:        ctx.StartTime,
		EndTime:          time.Now(),
		TotalDuration:    time.Since(ctx.StartTime),
		UserContext:      ctx.UserContext,
		SystemState:      ctx.SystemState,
		ComponentReports: make(map[string]*OperationSummary),
	}

	// Generate reports for each component
	for component, logger := range ctx.ComponentLogs {
		report.ComponentReports[component] = logger.GenerateOperationSummary()
	}

	return report
}

// ComprehensiveDiagnosticReport provides a complete diagnostic report
type ComprehensiveDiagnosticReport struct {
	RequestID        string                       `json:"request_id"`
	TableName        string                       `json:"table_name"`
	Database         string                       `json:"database"`
	StartTime        time.Time                    `json:"start_time"`
	EndTime          time.Time                    `json:"end_time"`
	TotalDuration    time.Duration                `json:"total_duration"`
	UserContext      *UserContext                 `json:"user_context,omitempty"`
	SystemState      map[string]interface{}       `json:"system_state,omitempty"`
	ComponentReports map[string]*OperationSummary `json:"component_reports"`
}

// ExportReport exports the comprehensive report as JSON
func (report *ComprehensiveDiagnosticReport) ExportReport() (string, error) {
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to export comprehensive report: %w", err)
	}

	return string(jsonData), nil
}

// LogReport logs the comprehensive report
func (report *ComprehensiveDiagnosticReport) LogReport(logger zerolog.Logger) {
	logger.Info().
		Str("request_id", report.RequestID).
		Str("table_name", report.TableName).
		Str("database", report.Database).
		Dur("total_duration", report.TotalDuration).
		Int("component_count", len(report.ComponentReports)).
		Interface("system_state", report.SystemState).
		Msg("Comprehensive CREATE TABLE diagnostic report")
}

// Diagnostic utilities for specific error scenarios

// LogParserDiagnostics logs parser-specific diagnostic information
func LogParserDiagnostics(logger zerolog.Logger, requestID string, tableName string, parseError error, sqlStatement string) {
	logger.Error().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Err(parseError).
		Str("sql_statement", sqlStatement).
		Msg("Parser diagnostic information")
}

// LogStorageDiagnostics logs storage-specific diagnostic information
func LogStorageDiagnostics(logger zerolog.Logger, requestID string, tableName string, storageEngine string, config map[string]interface{}, err error) {
	logger.Error().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Str("storage_engine", storageEngine).
		Interface("storage_config", config).
		Err(err).
		Msg("Storage diagnostic information")
}

// LogRegistryDiagnostics logs registry-specific diagnostic information
func LogRegistryDiagnostics(logger zerolog.Logger, requestID string, tableName string, transactionID string, registryState map[string]interface{}, err error) {
	logger.Error().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Str("transaction_id", transactionID).
		Interface("registry_state", registryState).
		Err(err).
		Msg("Registry diagnostic information")
}

// LogAsthaDiagnostics logs Astha-specific diagnostic information
func LogAsthaDiagnostics(logger zerolog.Logger, requestID string, tableName string, eventType string, subscriberCount int, err error) {
	logger.Error().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Str("event_type", eventType).
		Int("subscriber_count", subscriberCount).
		Err(err).
		Msg("Astha diagnostic information")
}

// LogCacheDiagnostics logs cache-specific diagnostic information
func LogCacheDiagnostics(logger zerolog.Logger, requestID string, tableName string, cacheState *CacheDiagnostics, err error) {
	logger.Error().
		Str("request_id", requestID).
		Str("table_name", tableName).
		Interface("cache_state", cacheState).
		Err(err).
		Msg("Cache diagnostic information")
}
