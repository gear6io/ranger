package middleware

import "github.com/gear6io/ranger/pkg/errors"

// Native middleware-specific error codes
var (
	ErrConnectionPoolAtCapacity = errors.MustNewCode("native.middleware.connection_pool_at_capacity")
	ErrCircuitBreakerOpen      = errors.MustNewCode("native.middleware.circuit_breaker_open")
	ErrResourceLimitsExceeded  = errors.MustNewCode("native.middleware.resource_limits_exceeded")
	ErrConcurrentQueryLimit    = errors.MustNewCode("native.middleware.concurrent_query_limit_exceeded")
	ErrMemoryUsageExceeded     = errors.MustNewCode("native.middleware.memory_usage_exceeded")
	ErrCPUUsageExceeded        = errors.MustNewCode("native.middleware.cpu_usage_exceeded")
	
	// Authentication errors
	ErrUserNotFound           = errors.MustNewCode("native.middleware.user_not_found")
	ErrInvalidPassword        = errors.MustNewCode("native.middleware.invalid_password")
	ErrDatabaseAccessDenied   = errors.MustNewCode("native.middleware.database_access_denied")
	ErrTokenGenerationFailed  = errors.MustNewCode("native.middleware.token_generation_failed")
	ErrInvalidTokenFormat     = errors.MustNewCode("native.middleware.invalid_token_format")
	ErrInvalidToken           = errors.MustNewCode("native.middleware.invalid_token")
	ErrUserAlreadyExists      = errors.MustNewCode("native.middleware.user_already_exists")
	
	// General authentication errors
	ErrAuthenticationRequired = errors.MustNewCode("native.middleware.authentication_required")
	ErrInsufficientPermissions = errors.MustNewCode("native.middleware.insufficient_permissions")
	ErrAuthenticationFailed   = errors.MustNewCode("native.middleware.authentication_failed")
	ErrInvalidCredentials     = errors.MustNewCode("native.middleware.invalid_credentials")
	ErrTokenValidationFailed  = errors.MustNewCode("native.middleware.token_validation_failed")
)
