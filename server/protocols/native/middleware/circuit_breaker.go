package middleware

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/rs/zerolog"
)

// CircuitState represents the current state of the circuit breaker
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // Normal operation
	CircuitOpen                         // Circuit is open, reject requests
	CircuitHalfOpen                     // Testing if circuit can close
)

// ResourceThresholds defines limits for resource consumption
type ResourceThresholds struct {
	MaxMemoryBytes       int64         // Maximum memory usage per query
	MaxCPUTime           time.Duration // Maximum CPU time per query
	MaxQueryDuration     time.Duration // Maximum query execution time
	MaxRowsProcessed     int64         // Maximum rows processed per query
	MaxConcurrentQueries int           // Maximum concurrent queries per connection
}

// CircuitBreaker protects Icebox from resource-consuming queries
type CircuitBreaker struct {
	state  CircuitState
	mu     sync.RWMutex
	logger zerolog.Logger

	// Thresholds
	thresholds ResourceThresholds

	// Circuit breaker settings
	failureThreshold int           // Number of failures before opening circuit
	recoveryTimeout  time.Duration // Time to wait before attempting recovery
	lastFailureTime  time.Time

	// Failure tracking
	failureCount int64
	successCount int64

	// Resource monitoring
	currentMemory int64
	currentCPU    int64
	activeQueries int64

	// Query tracking per connection
	connectionQueries map[string]int64
	connectionMu      sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(thresholds ResourceThresholds, failureThreshold int, recoveryTimeout time.Duration, logger zerolog.Logger) *CircuitBreaker {
	cb := &CircuitBreaker{
		state:             CircuitClosed,
		thresholds:        thresholds,
		failureThreshold:  failureThreshold,
		recoveryTimeout:   recoveryTimeout,
		logger:            logger,
		connectionQueries: make(map[string]int64),
	}

	// Start monitoring goroutine
	go cb.monitor()

	return cb
}

// OnEvent handles circuit breaker events
func (cb *CircuitBreaker) OnEvent(ctx context.Context, connCtx *ConnectionContext, event ConnectionEvent, err error) error {
	switch event {
	case EventQueryStarted:
		return cb.onQueryStart(connCtx)
	case EventQueryCompleted:
		return cb.onQueryComplete(connCtx, nil)
	case EventQueryFailed:
		return cb.onQueryComplete(connCtx, err)
	case EventResourceExceeded:
		return cb.onResourceExceeded(connCtx, err)
	}
	return nil
}

// OnRead allows reads if circuit is not open
func (cb *CircuitBreaker) OnRead(ctx context.Context, connCtx *ConnectionContext) error {
	if cb.isCircuitOpen() {
		return errors.New(ErrCircuitBreakerOpen, "circuit breaker is open - service temporarily unavailable", nil)
	}
	return nil
}

// OnWrite allows writes if circuit is not open
func (cb *CircuitBreaker) OnWrite(ctx context.Context, connCtx *ConnectionContext) error {
	if cb.isCircuitOpen() {
		return errors.New(ErrCircuitBreakerOpen, "circuit breaker is open - service temporarily unavailable", nil)
	}
	return nil
}

// OnError tracks errors for circuit breaker logic
func (cb *CircuitBreaker) OnError(ctx context.Context, connCtx *ConnectionContext, err error) error {
	atomic.AddInt64(&cb.failureCount, 1)

	// Check if we should open the circuit
	if atomic.LoadInt64(&cb.failureCount) >= int64(cb.failureThreshold) {
		cb.openCircuit()
	}

	return err
}

// OnQuery checks resource limits before allowing query execution
func (cb *CircuitBreaker) OnQuery(ctx context.Context, connCtx *ConnectionContext, query string) error {
	// Check circuit state
	if cb.isCircuitOpen() {
		return errors.New(ErrCircuitBreakerOpen, "circuit breaker is open - service temporarily unavailable", nil)
	}

	// Check concurrent query limit per connection
	if err := cb.checkConcurrentQueries(connCtx.ConnectionID); err != nil {
		return err
	}

	// Check resource thresholds
	if err := cb.checkResourceThresholds(connCtx); err != nil {
		return err
	}

	// Increment active queries
	atomic.AddInt64(&cb.activeQueries, 1)

	return nil
}

// onQueryStart handles query start events
func (cb *CircuitBreaker) onQueryStart(connCtx *ConnectionContext) error {
	// Track query start time
	connCtx.ResourceUsage.QueryDuration = 0

	// Increment connection query count
	cb.connectionMu.Lock()
	cb.connectionQueries[connCtx.ConnectionID]++
	cb.connectionMu.Unlock()

	return nil
}

// onQueryComplete handles query completion events
func (cb *CircuitBreaker) onQueryComplete(connCtx *ConnectionContext, err error) error {
	// Decrement active queries
	atomic.AddInt64(&cb.activeQueries, -1)

	// Decrement connection query count
	cb.connectionMu.Lock()
	if count := cb.connectionQueries[connCtx.ConnectionID]; count > 0 {
		cb.connectionQueries[connCtx.ConnectionID]--
	}
	cb.connectionMu.Unlock()

	if err != nil {
		// Query failed
		atomic.AddInt64(&cb.failureCount, 1)

		// Check if we should open the circuit
		if atomic.LoadInt64(&cb.failureCount) >= int64(cb.failureThreshold) {
			cb.openCircuit()
		}
	} else {
		// Query succeeded
		atomic.AddInt64(&cb.successCount, 1)

		// Check if we can close the circuit
		if cb.state == CircuitHalfOpen {
			cb.closeCircuit()
		}
	}

	return nil
}

// onResourceExceeded handles resource limit violations
func (cb *CircuitBreaker) onResourceExceeded(connCtx *ConnectionContext, err error) error {
	cb.logger.Warn().
		Err(err).
		Str("client", connCtx.ClientAddr).
		Str("connection_id", connCtx.ConnectionID).
		Int64("memory_bytes", connCtx.ResourceUsage.MemoryBytes).
		Dur("cpu_time", connCtx.ResourceUsage.CPUTime).
		Dur("query_duration", connCtx.ResourceUsage.QueryDuration).
		Int64("rows_processed", connCtx.ResourceUsage.RowsProcessed).
		Msg("Resource limits exceeded")

	// Open circuit immediately for resource violations
	cb.openCircuit()

	return errors.New(ErrResourceLimitsExceeded, "resource limits exceeded", err)
}

// checkConcurrentQueries checks if connection has exceeded query limit
func (cb *CircuitBreaker) checkConcurrentQueries(connectionID string) error {
	cb.connectionMu.RLock()
	currentQueries := cb.connectionQueries[connectionID]
	cb.connectionMu.RUnlock()

	if currentQueries >= int64(cb.thresholds.MaxConcurrentQueries) {
		return errors.Newf(ErrConcurrentQueryLimit, "concurrent query limit exceeded (%d)", cb.thresholds.MaxConcurrentQueries)
	}

	return nil
}

// checkResourceThresholds checks if current resource usage exceeds thresholds
func (cb *CircuitBreaker) checkResourceThresholds(connCtx *ConnectionContext) error {
	// Check memory usage
	if atomic.LoadInt64(&cb.currentMemory) > cb.thresholds.MaxMemoryBytes {
		return errors.Newf(ErrMemoryUsageExceeded, "memory usage exceeds threshold (%d bytes)", cb.thresholds.MaxMemoryBytes)
	}

	// Check CPU usage
	if atomic.LoadInt64(&cb.currentCPU) > int64(cb.thresholds.MaxCPUTime) {
		return errors.Newf(ErrCPUUsageExceeded, "CPU usage exceeds threshold (%v)", cb.thresholds.MaxCPUTime)
	}

	// Check active queries
	if atomic.LoadInt64(&cb.activeQueries) > int64(cb.thresholds.MaxConcurrentQueries) {
		return errors.Newf(ErrConcurrentQueryLimit, "concurrent query limit exceeded (%d)", cb.thresholds.MaxConcurrentQueries)
	}

	return nil
}

// isCircuitOpen checks if the circuit breaker is open
func (cb *CircuitBreaker) isCircuitOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.state == CircuitOpen
}

// openCircuit opens the circuit breaker
func (cb *CircuitBreaker) openCircuit() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != CircuitOpen {
		cb.state = CircuitOpen
		cb.lastFailureTime = time.Now()

		cb.logger.Warn().
			Int64("failure_count", atomic.LoadInt64(&cb.failureCount)).
			Int64("success_count", atomic.LoadInt64(&cb.successCount)).
			Msg("Circuit breaker opened")

		// Schedule recovery attempt
		go cb.scheduleRecovery()
	}
}

// closeCircuit closes the circuit breaker
func (cb *CircuitBreaker) closeCircuit() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != CircuitClosed {
		cb.state = CircuitClosed

		// Reset failure count
		atomic.StoreInt64(&cb.failureCount, 0)

		cb.logger.Info().
			Msg("Circuit breaker closed - service restored")
	}
}

// scheduleRecovery schedules a recovery attempt
func (cb *CircuitBreaker) scheduleRecovery() {
	time.Sleep(cb.recoveryTimeout)

	cb.mu.Lock()
	if cb.state == CircuitOpen {
		cb.state = CircuitHalfOpen
		cb.logger.Info().
			Msg("Circuit breaker half-open - testing recovery")
	}
	cb.mu.Unlock()
}

// monitor periodically checks circuit breaker state
func (cb *CircuitBreaker) monitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		cb.mu.RLock()
		state := cb.state
		lastFailure := cb.lastFailureTime
		cb.mu.RUnlock()

		// Log current state
		cb.logger.Debug().
			Str("state", cb.stateToString(state)).
			Int64("failure_count", atomic.LoadInt64(&cb.failureCount)).
			Int64("success_count", atomic.LoadInt64(&cb.successCount)).
			Int64("active_queries", atomic.LoadInt64(&cb.activeQueries)).
			Time("last_failure", lastFailure).
			Msg("Circuit breaker status")
	}
}

// stateToString converts circuit state to string
func (cb *CircuitBreaker) stateToString(state CircuitState) string {
	switch state {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]interface{}{
		"state":             cb.stateToString(cb.state),
		"failure_count":     atomic.LoadInt64(&cb.failureCount),
		"success_count":     atomic.LoadInt64(&cb.successCount),
		"active_queries":    atomic.LoadInt64(&cb.activeQueries),
		"last_failure_time": cb.lastFailureTime,
		"thresholds":        cb.thresholds,
		"failure_threshold": cb.failureThreshold,
		"recovery_timeout":  cb.recoveryTimeout,
	}
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = CircuitClosed
	atomic.StoreInt64(&cb.failureCount, 0)
	atomic.StoreInt64(&cb.successCount, 0)

	cb.logger.Info().Msg("Circuit breaker reset")
}
