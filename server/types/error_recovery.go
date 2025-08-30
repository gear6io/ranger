package types

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/rs/zerolog"
)

// ErrorRecoveryStrategy defines how to handle and recover from errors
type ErrorRecoveryStrategy interface {
	CanRecover(err error) bool
	Recover(ctx context.Context, err error, attempt int) error
	GetMaxAttempts() int
	GetBackoffDelay(attempt int) time.Duration
}

// RetryConfig defines retry behavior for error recovery
type RetryConfig struct {
	MaxAttempts     int           `json:"max_attempts"`
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffFactor   float64       `json:"backoff_factor"`
	JitterEnabled   bool          `json:"jitter_enabled"`
	RetryableErrors []string      `json:"retryable_errors"`
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		JitterEnabled: true,
		RetryableErrors: []string{
			"create_table.storage.operation_error",
			"create_table.registry.transaction_error",
			"create_table.astha.event_processing",
			"create_table.schema_cache.operation_error",
			"create_table.schema_cache.refresh_error",
		},
	}
}

// CreateTableRecoveryManager manages error recovery for CREATE TABLE operations
type CreateTableRecoveryManager struct {
	config   *RetryConfig
	logger   zerolog.Logger
	metrics  *RecoveryMetrics
	handlers map[string]ErrorRecoveryStrategy
}

// RecoveryMetrics tracks recovery operation statistics
type RecoveryMetrics struct {
	TotalAttempts     int64   `json:"total_attempts"`
	SuccessfulRetries int64   `json:"successful_retries"`
	FailedRetries     int64   `json:"failed_retries"`
	AverageAttempts   float64 `json:"average_attempts"`
	RecoveryRate      float64 `json:"recovery_rate"`
}

// NewCreateTableRecoveryManager creates a new recovery manager
func NewCreateTableRecoveryManager(config *RetryConfig, logger zerolog.Logger) *CreateTableRecoveryManager {
	if config == nil {
		config = DefaultRetryConfig()
	}

	manager := &CreateTableRecoveryManager{
		config:   config,
		logger:   logger,
		metrics:  &RecoveryMetrics{},
		handlers: make(map[string]ErrorRecoveryStrategy),
	}

	// Register default recovery strategies
	manager.registerDefaultStrategies()

	return manager
}

// registerDefaultStrategies registers built-in recovery strategies
func (rm *CreateTableRecoveryManager) registerDefaultStrategies() {
	// Storage operation recovery
	rm.handlers["storage"] = &StorageRecoveryStrategy{
		config: rm.config,
		logger: rm.logger,
	}

	// Registry operation recovery
	rm.handlers["registry"] = &RegistryRecoveryStrategy{
		config: rm.config,
		logger: rm.logger,
	}

	// Astha event processing recovery
	rm.handlers["astha"] = &AsthaRecoveryStrategy{
		config: rm.config,
		logger: rm.logger,
	}

	// Schema cache recovery
	rm.handlers["cache"] = &CacheRecoveryStrategy{
		config: rm.config,
		logger: rm.logger,
	}
}

// RecoverFromError attempts to recover from a CREATE TABLE error
func (rm *CreateTableRecoveryManager) RecoverFromError(ctx context.Context, err error) error {
	rm.metrics.TotalAttempts++

	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		rm.logger.Debug().Err(err).Msg("Error is not a CreateTableError, cannot recover")
		return err
	}

	// Check if error is recoverable
	if !createTableErr.IsRecoverable() {
		rm.logger.Debug().
			Str("component", createTableErr.Component).
			Str("operation", createTableErr.Operation).
			Msg("Error is not recoverable")
		return err
	}

	// Find appropriate recovery strategy
	strategy, exists := rm.handlers[createTableErr.Component]
	if !exists {
		rm.logger.Debug().
			Str("component", createTableErr.Component).
			Msg("No recovery strategy found for component")
		return err
	}

	// Attempt recovery
	return rm.attemptRecovery(ctx, createTableErr, strategy)
}

// attemptRecovery performs the actual recovery attempts
func (rm *CreateTableRecoveryManager) attemptRecovery(ctx context.Context, err *CreateTableError, strategy ErrorRecoveryStrategy) error {
	maxAttempts := strategy.GetMaxAttempts()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Add delay before retry (except for first attempt)
		if attempt > 1 {
			delay := strategy.GetBackoffDelay(attempt)
			rm.logger.Debug().
				Dur("delay", delay).
				Int("attempt", attempt).
				Int("max_attempts", maxAttempts).
				Msg("Waiting before retry attempt")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Continue with retry
			}
		}

		rm.logger.Info().
			Str("component", err.Component).
			Str("operation", err.Operation).
			Int("attempt", attempt).
			Int("max_attempts", maxAttempts).
			Msg("Attempting error recovery")

		// Attempt recovery
		if recoveryErr := strategy.Recover(ctx, err, attempt); recoveryErr == nil {
			rm.metrics.SuccessfulRetries++
			rm.updateMetrics()

			rm.logger.Info().
				Str("component", err.Component).
				Str("operation", err.Operation).
				Int("attempt", attempt).
				Msg("Error recovery successful")

			return nil
		} else {
			rm.logger.Warn().
				Err(recoveryErr).
				Str("component", err.Component).
				Str("operation", err.Operation).
				Int("attempt", attempt).
				Msg("Recovery attempt failed")
		}
	}

	// All recovery attempts failed
	rm.metrics.FailedRetries++
	rm.updateMetrics()

	rm.logger.Error().
		Str("component", err.Component).
		Str("operation", err.Operation).
		Int("max_attempts", maxAttempts).
		Msg("All recovery attempts failed")

	return err
}

// updateMetrics updates recovery statistics
func (rm *CreateTableRecoveryManager) updateMetrics() {
	total := rm.metrics.SuccessfulRetries + rm.metrics.FailedRetries
	if total > 0 {
		rm.metrics.RecoveryRate = float64(rm.metrics.SuccessfulRetries) / float64(total)
		rm.metrics.AverageAttempts = float64(rm.metrics.TotalAttempts) / float64(total)
	}
}

// GetMetrics returns current recovery metrics
func (rm *CreateTableRecoveryManager) GetMetrics() *RecoveryMetrics {
	return rm.metrics
}

// RegisterStrategy registers a custom recovery strategy
func (rm *CreateTableRecoveryManager) RegisterStrategy(component string, strategy ErrorRecoveryStrategy) {
	rm.handlers[component] = strategy
}

// Storage Recovery Strategy
type StorageRecoveryStrategy struct {
	config *RetryConfig
	logger zerolog.Logger
}

func (s *StorageRecoveryStrategy) CanRecover(err error) bool {
	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		return false
	}

	// Check if it's a storage-related error that can be retried
	retryableErrors := []string{
		"create_table.storage.operation_error",
		"create_table.storage.engine_error",
	}

	for _, retryable := range retryableErrors {
		if createTableErr.Code.String() == retryable {
			return true
		}
	}

	return false
}

func (s *StorageRecoveryStrategy) Recover(ctx context.Context, err error, attempt int) error {
	createTableErr := err.(*CreateTableError)

	s.logger.Info().
		Str("table_name", createTableErr.TableName).
		Str("database", createTableErr.Database).
		Int("attempt", attempt).
		Msg("Attempting storage recovery")

	// For storage errors, we typically need to:
	// 1. Check storage backend connectivity
	// 2. Verify configuration
	// 3. Retry the operation

	// This would integrate with the actual storage manager
	// For now, we simulate recovery logic

	return fmt.Errorf("storage recovery not yet implemented")
}

func (s *StorageRecoveryStrategy) GetMaxAttempts() int {
	return s.config.MaxAttempts
}

func (s *StorageRecoveryStrategy) GetBackoffDelay(attempt int) time.Duration {
	delay := time.Duration(float64(s.config.InitialDelay) * math.Pow(s.config.BackoffFactor, float64(attempt-1)))

	if delay > s.config.MaxDelay {
		delay = s.config.MaxDelay
	}

	// Add jitter if enabled
	if s.config.JitterEnabled {
		jitter := time.Duration(float64(delay) * 0.1 * float64(2*time.Now().UnixNano()%2-1))
		delay += jitter
	}

	return delay
}

// Registry Recovery Strategy
type RegistryRecoveryStrategy struct {
	config *RetryConfig
	logger zerolog.Logger
}

func (r *RegistryRecoveryStrategy) CanRecover(err error) bool {
	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		return false
	}

	// Registry errors that can be retried
	retryableErrors := []string{
		"create_table.registry.transaction_error",
		"create_table.registry.operation_error",
	}

	for _, retryable := range retryableErrors {
		if createTableErr.Code.String() == retryable {
			return true
		}
	}

	return false
}

func (r *RegistryRecoveryStrategy) Recover(ctx context.Context, err error, attempt int) error {
	createTableErr := err.(*CreateTableError)

	r.logger.Info().
		Str("table_name", createTableErr.TableName).
		Str("database", createTableErr.Database).
		Int("attempt", attempt).
		Msg("Attempting registry recovery")

	// For registry errors, we typically need to:
	// 1. Start a new transaction
	// 2. Check database connectivity
	// 3. Retry the registry operation

	return fmt.Errorf("registry recovery not yet implemented")
}

func (r *RegistryRecoveryStrategy) GetMaxAttempts() int {
	return r.config.MaxAttempts
}

func (r *RegistryRecoveryStrategy) GetBackoffDelay(attempt int) time.Duration {
	delay := time.Duration(float64(r.config.InitialDelay) * math.Pow(r.config.BackoffFactor, float64(attempt-1)))

	if delay > r.config.MaxDelay {
		delay = r.config.MaxDelay
	}

	if r.config.JitterEnabled {
		jitter := time.Duration(float64(delay) * 0.1 * float64(2*time.Now().UnixNano()%2-1))
		delay += jitter
	}

	return delay
}

// Astha Recovery Strategy
type AsthaRecoveryStrategy struct {
	config *RetryConfig
	logger zerolog.Logger
}

func (a *AsthaRecoveryStrategy) CanRecover(err error) bool {
	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		return false
	}

	return createTableErr.Code.String() == "create_table.astha.event_processing"
}

func (a *AsthaRecoveryStrategy) Recover(ctx context.Context, err error, attempt int) error {
	createTableErr := err.(*CreateTableError)

	a.logger.Info().
		Str("table_name", createTableErr.TableName).
		Str("database", createTableErr.Database).
		Int("attempt", attempt).
		Msg("Attempting Astha event recovery")

	// For Astha errors, we typically need to:
	// 1. Check subscriber health
	// 2. Re-queue the event
	// 3. Verify event processing pipeline

	return fmt.Errorf("astha recovery not yet implemented")
}

func (a *AsthaRecoveryStrategy) GetMaxAttempts() int {
	return a.config.MaxAttempts
}

func (a *AsthaRecoveryStrategy) GetBackoffDelay(attempt int) time.Duration {
	delay := time.Duration(float64(a.config.InitialDelay) * math.Pow(a.config.BackoffFactor, float64(attempt-1)))

	if delay > a.config.MaxDelay {
		delay = a.config.MaxDelay
	}

	if a.config.JitterEnabled {
		jitter := time.Duration(float64(delay) * 0.1 * float64(2*time.Now().UnixNano()%2-1))
		delay += jitter
	}

	return delay
}

// Cache Recovery Strategy
type CacheRecoveryStrategy struct {
	config *RetryConfig
	logger zerolog.Logger
}

func (c *CacheRecoveryStrategy) CanRecover(err error) bool {
	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		return false
	}

	retryableErrors := []string{
		"create_table.schema_cache.operation_error",
		"create_table.schema_cache.refresh_error",
	}

	for _, retryable := range retryableErrors {
		if createTableErr.Code.String() == retryable {
			return true
		}
	}

	return false
}

func (c *CacheRecoveryStrategy) Recover(ctx context.Context, err error, attempt int) error {
	createTableErr := err.(*CreateTableError)

	c.logger.Info().
		Str("table_name", createTableErr.TableName).
		Str("database", createTableErr.Database).
		Int("attempt", attempt).
		Msg("Attempting cache recovery")

	// For cache errors, we typically need to:
	// 1. Invalidate the cache entry
	// 2. Clear any corrupted state
	// 3. Retry the cache operation

	return fmt.Errorf("cache recovery not yet implemented")
}

func (c *CacheRecoveryStrategy) GetMaxAttempts() int {
	return c.config.MaxAttempts
}

func (c *CacheRecoveryStrategy) GetBackoffDelay(attempt int) time.Duration {
	delay := time.Duration(float64(c.config.InitialDelay) * math.Pow(c.config.BackoffFactor, float64(attempt-1)))

	if delay > c.config.MaxDelay {
		delay = c.config.MaxDelay
	}

	if c.config.JitterEnabled {
		jitter := time.Duration(float64(delay) * 0.1 * float64(2*time.Now().UnixNano()%2-1))
		delay += jitter
	}

	return delay
}

// Utility functions for error recovery

// IsRetryableError checks if an error can be retried
func IsRetryableError(err error) bool {
	createTableErr, ok := err.(*CreateTableError)
	if !ok {
		return false
	}

	return createTableErr.IsRecoverable()
}

// GetRecoveryDelay calculates the delay before next retry attempt
func GetRecoveryDelay(attempt int, config *RetryConfig) time.Duration {
	if config == nil {
		config = DefaultRetryConfig()
	}

	delay := time.Duration(float64(config.InitialDelay) * math.Pow(config.BackoffFactor, float64(attempt-1)))

	if delay > config.MaxDelay {
		delay = config.MaxDelay
	}

	if config.JitterEnabled {
		jitter := time.Duration(float64(delay) * 0.1 * float64(2*time.Now().UnixNano()%2-1))
		delay += jitter
	}

	return delay
}

// CreateRecoveryContext creates a context with recovery metadata
func CreateRecoveryContext(ctx context.Context, requestID string, attempt int) context.Context {
	return context.WithValue(ctx, "recovery_metadata", map[string]interface{}{
		"request_id": requestID,
		"attempt":    attempt,
		"timestamp":  time.Now(),
	})
}
