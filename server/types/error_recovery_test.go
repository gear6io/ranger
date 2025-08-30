package types

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	if config.MaxAttempts != 3 {
		t.Errorf("Expected max attempts 3, got %d", config.MaxAttempts)
	}
	if config.InitialDelay != 100*time.Millisecond {
		t.Errorf("Expected initial delay 100ms, got %v", config.InitialDelay)
	}
	if config.MaxDelay != 5*time.Second {
		t.Errorf("Expected max delay 5s, got %v", config.MaxDelay)
	}
	if config.BackoffFactor != 2.0 {
		t.Errorf("Expected backoff factor 2.0, got %f", config.BackoffFactor)
	}
	if !config.JitterEnabled {
		t.Error("Expected jitter to be enabled")
	}
	if len(config.RetryableErrors) == 0 {
		t.Error("Expected retryable errors to be configured")
	}
}

func TestCreateTableRecoveryManager(t *testing.T) {
	logger := zerolog.Nop()
	config := DefaultRetryConfig()

	manager := NewCreateTableRecoveryManager(config, logger)

	if manager.config != config {
		t.Error("Expected config to be set")
	}
	if manager.metrics == nil {
		t.Error("Expected metrics to be initialized")
	}
	if len(manager.handlers) == 0 {
		t.Error("Expected default handlers to be registered")
	}

	// Test that default strategies are registered
	expectedComponents := []string{"storage", "registry", "astha", "cache"}
	for _, component := range expectedComponents {
		if _, exists := manager.handlers[component]; !exists {
			t.Errorf("Expected handler for component '%s' to be registered", component)
		}
	}
}

func TestCreateTableRecoveryManagerWithNilConfig(t *testing.T) {
	logger := zerolog.Nop()

	manager := NewCreateTableRecoveryManager(nil, logger)

	// Should use default config when nil is passed
	if manager.config == nil {
		t.Error("Expected default config to be used when nil is passed")
	}
	if manager.config.MaxAttempts != 3 {
		t.Error("Expected default config values to be used")
	}
}

func TestRecoveryManagerRegisterStrategy(t *testing.T) {
	logger := zerolog.Nop()
	manager := NewCreateTableRecoveryManager(nil, logger)

	// Create a mock strategy
	mockStrategy := &MockRecoveryStrategy{
		canRecover:   true,
		maxAttempts:  5,
		backoffDelay: 200 * time.Millisecond,
	}

	// Register custom strategy
	manager.RegisterStrategy("custom", mockStrategy)

	// Verify it was registered
	if strategy, exists := manager.handlers["custom"]; !exists {
		t.Error("Expected custom strategy to be registered")
	} else if strategy != mockStrategy {
		t.Error("Expected registered strategy to be the same instance")
	}
}

func TestRecoveryManagerRecoverFromNonCreateTableError(t *testing.T) {
	logger := zerolog.Nop()
	manager := NewCreateTableRecoveryManager(nil, logger)

	// Test with non-CreateTableError
	regularErr := NewCreateTableParseError("test error", "test_table", nil)
	// Remove recovery hints to make it non-recoverable
	regularErr.RecoveryHints = []RecoveryHint{}

	ctx := context.Background()
	result := manager.RecoverFromError(ctx, regularErr)

	// Should return the original error since it's not recoverable
	if result != regularErr {
		t.Error("Expected original error to be returned for non-recoverable error")
	}
}

func TestStorageRecoveryStrategy(t *testing.T) {
	config := DefaultRetryConfig()
	logger := zerolog.Nop()
	strategy := &StorageRecoveryStrategy{
		config: config,
		logger: logger,
	}

	// Test CanRecover with storage error
	storageErr := NewCreateTableStorageError(
		"storage failed",
		"test_table",
		"test_db",
		"req_123",
		nil,
	)

	if !strategy.CanRecover(storageErr) {
		t.Error("Expected storage strategy to handle storage errors")
	}

	// Test CanRecover with non-storage error
	parseErr := NewCreateTableParseError("parse failed", "test_table", nil)
	if strategy.CanRecover(parseErr) {
		t.Error("Expected storage strategy to not handle parse errors")
	}

	// Test GetMaxAttempts
	if strategy.GetMaxAttempts() != config.MaxAttempts {
		t.Errorf("Expected max attempts %d, got %d", config.MaxAttempts, strategy.GetMaxAttempts())
	}

	// Test GetBackoffDelay
	delay1 := strategy.GetBackoffDelay(1)
	delay2 := strategy.GetBackoffDelay(2)

	if delay1 >= delay2 {
		t.Error("Expected backoff delay to increase with attempt number")
	}

	// Test max delay cap
	delay10 := strategy.GetBackoffDelay(10)
	if delay10 > config.MaxDelay {
		t.Errorf("Expected delay to be capped at max delay %v, got %v", config.MaxDelay, delay10)
	}
}

func TestRegistryRecoveryStrategy(t *testing.T) {
	config := DefaultRetryConfig()
	logger := zerolog.Nop()
	strategy := &RegistryRecoveryStrategy{
		config: config,
		logger: logger,
	}

	// Test CanRecover with registry error
	registryErr := NewCreateTableRegistryError(
		"registry failed",
		"test_table",
		"test_db",
		"req_123",
		"tx_123",
		nil,
	)

	if !strategy.CanRecover(registryErr) {
		t.Error("Expected registry strategy to handle registry errors")
	}

	// Test CanRecover with non-registry error
	storageErr := NewCreateTableStorageError("storage failed", "test_table", "test_db", "req_123", nil)
	if strategy.CanRecover(storageErr) {
		t.Error("Expected registry strategy to not handle storage errors")
	}
}

func TestAsthaRecoveryStrategy(t *testing.T) {
	config := DefaultRetryConfig()
	logger := zerolog.Nop()
	strategy := &AsthaRecoveryStrategy{
		config: config,
		logger: logger,
	}

	// Test CanRecover with Astha error
	asthaErr := NewCreateTableEventProcessingError(
		"event processing failed",
		"test_table",
		"test_db",
		"INSERT_tables",
		nil,
	)

	if !strategy.CanRecover(asthaErr) {
		t.Error("Expected Astha strategy to handle event processing errors")
	}

	// Test CanRecover with non-Astha error
	parseErr := NewCreateTableParseError("parse failed", "test_table", nil)
	if strategy.CanRecover(parseErr) {
		t.Error("Expected Astha strategy to not handle parse errors")
	}
}

func TestCacheRecoveryStrategy(t *testing.T) {
	config := DefaultRetryConfig()
	logger := zerolog.Nop()
	strategy := &CacheRecoveryStrategy{
		config: config,
		logger: logger,
	}

	// Test CanRecover with cache error
	cacheErr := NewCreateTableCacheError(
		"cache operation failed",
		"test_table",
		"test_db",
		"cache_refresh",
		nil,
	)

	if !strategy.CanRecover(cacheErr) {
		t.Error("Expected cache strategy to handle cache errors")
	}

	// Test CanRecover with non-cache error
	parseErr := NewCreateTableParseError("parse failed", "test_table", nil)
	if strategy.CanRecover(parseErr) {
		t.Error("Expected cache strategy to not handle parse errors")
	}
}

func TestIsRetryableError(t *testing.T) {
	// Test with recoverable error
	recoverableErr := NewCreateTableStorageError(
		"storage failed",
		"test_table",
		"test_db",
		"req_123",
		nil,
	)

	if !IsRetryableError(recoverableErr) {
		t.Error("Expected storage error to be retryable")
	}

	// Test with non-recoverable error
	parseErr := NewCreateTableParseError("parse failed", "test_table", nil)
	// Remove recovery hints to make it non-recoverable
	parseErr.RecoveryHints = []RecoveryHint{}

	if IsRetryableError(parseErr) {
		t.Error("Expected parse error without recovery hints to not be retryable")
	}

	// Test with CreateTableError that has non-automatic recovery hints
	regularErr := NewCreateTableParseError("regular error", "test_table", nil)
	if IsRetryableError(regularErr) {
		t.Error("Expected CreateTableError with non-automatic recovery hints to not be retryable")
	}
}

func TestGetRecoveryDelay(t *testing.T) {
	config := DefaultRetryConfig()

	// Test with provided config
	delay1 := GetRecoveryDelay(1, config)
	delay2 := GetRecoveryDelay(2, config)

	if delay1 >= delay2 {
		t.Error("Expected delay to increase with attempt number")
	}

	// Test with nil config (should use default)
	delayDefault := GetRecoveryDelay(1, nil)
	if delayDefault <= 0 {
		t.Error("Expected positive delay with default config")
	}

	// Test max delay cap
	delay10 := GetRecoveryDelay(10, config)
	if delay10 > config.MaxDelay {
		t.Errorf("Expected delay to be capped at max delay %v, got %v", config.MaxDelay, delay10)
	}
}

func TestCreateRecoveryContext(t *testing.T) {
	ctx := context.Background()
	requestID := "req_123"
	attempt := 2

	recoveryCtx := CreateRecoveryContext(ctx, requestID, attempt)

	// Verify context contains recovery metadata
	metadata := recoveryCtx.Value("recovery_metadata")
	if metadata == nil {
		t.Error("Expected recovery metadata to be present in context")
	}

	metadataMap, ok := metadata.(map[string]interface{})
	if !ok {
		t.Error("Expected recovery metadata to be a map")
	}

	if metadataMap["request_id"] != requestID {
		t.Errorf("Expected request ID '%s', got '%v'", requestID, metadataMap["request_id"])
	}
	if metadataMap["attempt"] != attempt {
		t.Errorf("Expected attempt %d, got %v", attempt, metadataMap["attempt"])
	}
	if metadataMap["timestamp"] == nil {
		t.Error("Expected timestamp to be present")
	}
}

func TestRecoveryMetrics(t *testing.T) {
	logger := zerolog.Nop()
	manager := NewCreateTableRecoveryManager(nil, logger)

	// Initial metrics should be zero
	metrics := manager.GetMetrics()
	if metrics.TotalAttempts != 0 {
		t.Error("Expected initial total attempts to be 0")
	}
	if metrics.SuccessfulRetries != 0 {
		t.Error("Expected initial successful retries to be 0")
	}
	if metrics.FailedRetries != 0 {
		t.Error("Expected initial failed retries to be 0")
	}
	if metrics.RecoveryRate != 0 {
		t.Error("Expected initial recovery rate to be 0")
	}
}

// Mock recovery strategy for testing
type MockRecoveryStrategy struct {
	canRecover   bool
	maxAttempts  int
	backoffDelay time.Duration
	recoverError error
}

func (m *MockRecoveryStrategy) CanRecover(err error) bool {
	return m.canRecover
}

func (m *MockRecoveryStrategy) Recover(ctx context.Context, err error, attempt int) error {
	return m.recoverError
}

func (m *MockRecoveryStrategy) GetMaxAttempts() int {
	return m.maxAttempts
}

func (m *MockRecoveryStrategy) GetBackoffDelay(attempt int) time.Duration {
	return m.backoffDelay
}

func TestMockRecoveryStrategy(t *testing.T) {
	mock := &MockRecoveryStrategy{
		canRecover:   true,
		maxAttempts:  3,
		backoffDelay: 100 * time.Millisecond,
		recoverError: nil,
	}

	// Test CanRecover
	if !mock.CanRecover(nil) {
		t.Error("Expected mock to indicate it can recover")
	}

	// Test GetMaxAttempts
	if mock.GetMaxAttempts() != 3 {
		t.Error("Expected max attempts to be 3")
	}

	// Test GetBackoffDelay
	if mock.GetBackoffDelay(1) != 100*time.Millisecond {
		t.Error("Expected backoff delay to be 100ms")
	}

	// Test Recover
	ctx := context.Background()
	err := mock.Recover(ctx, nil, 1)
	if err != nil {
		t.Error("Expected recovery to succeed")
	}
}
