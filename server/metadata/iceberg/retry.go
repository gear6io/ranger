package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/rs/zerolog"
)

// Package-specific error codes for retry operations
var (
	RetryOperationFailed = errors.MustNewCode("iceberg.retry.operation_failed")
)

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxAttempts   int           `json:"max_attempts"`
	BaseDelay     time.Duration `json:"base_delay"`
	MaxDelay      time.Duration `json:"max_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		BaseDelay:     1 * time.Second,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
	}
}

// RetryableOperation represents an operation that can be retried
type RetryableOperation func(ctx context.Context) error

// RetryWithBackoff executes an operation with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, config *RetryConfig, operation RetryableOperation, logger zerolog.Logger) error {
	var lastErr error
	delay := config.BaseDelay

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute the operation
		err := operation(ctx)
		if err == nil {
			// Operation succeeded
			if attempt > 1 {
				logger.Info().
					Int("attempt", attempt).
					Msg("Operation succeeded after retry")
			}
			return nil
		}

		lastErr = err

		// If this is the last attempt, don't wait
		if attempt == config.MaxAttempts {
			break
		}

		// Log the failure
		logger.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_attempts", config.MaxAttempts).
			Dur("delay", delay).
			Msg("Operation failed, retrying")

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Calculate next delay with exponential backoff
		delay = time.Duration(float64(delay) * config.BackoffFactor)
		if delay > config.MaxDelay {
			delay = config.MaxDelay
		}
	}

	// All attempts failed
	return errors.New(RetryOperationFailed, "operation failed after retry attempts").AddContext("max_attempts", fmt.Sprintf("%d", config.MaxAttempts)).WithCause(lastErr)
}

// RetryableFileOperation represents a file operation that can be retried
type RetryableFileOperation func(ctx context.Context, fileInfo registry.FileInfo) error

// RetryFileOperation executes a file operation with retry logic
func RetryFileOperation(ctx context.Context, config *RetryConfig, fileInfo registry.FileInfo, operation RetryableFileOperation, logger zerolog.Logger) error {
	return RetryWithBackoff(ctx, config, func(ctx context.Context) error {
		return operation(ctx, fileInfo)
	}, logger)
}

// RetryableBatchOperation represents a batch operation that can be retried
type RetryableBatchOperation func(ctx context.Context, batch BatchInfo) error

// RetryBatchOperation executes a batch operation with retry logic
func RetryBatchOperation(ctx context.Context, config *RetryConfig, batch BatchInfo, operation RetryableBatchOperation, logger zerolog.Logger) error {
	return RetryWithBackoff(ctx, config, func(ctx context.Context) error {
		return operation(ctx, batch)
	}, logger)
}
