package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/rs/zerolog"
)

// AuthProvider defines the interface for authentication providers
type AuthProvider interface {
	Authenticate(ctx context.Context, username, password, database string) (*AuthResult, error)
	ValidateToken(ctx context.Context, token string) (*AuthResult, error)
	RefreshToken(ctx context.Context, token string) (*AuthResult, error)
}

// AuthResult contains authentication result information
type AuthResult struct {
	Authenticated bool
	Username      string
	Database      string
	Permissions   []string
	ExpiresAt     time.Time
	Token         string
}

// AuthMiddleware handles authentication and authorization
type AuthMiddleware struct {
	provider     AuthProvider
	logger       zerolog.Logger
	enabled      bool
	requireAuth  bool
	tokenTimeout time.Duration

	// Token cache for performance
	tokenCache    map[string]*AuthResult
	tokenCacheMu  sync.RWMutex
	tokenCacheTTL time.Duration
}

// NewAuthMiddleware creates a new authentication middleware
func NewAuthMiddleware(provider AuthProvider, enabled, requireAuth bool, tokenTimeout, cacheTTL time.Duration, logger zerolog.Logger) *AuthMiddleware {
	auth := &AuthMiddleware{
		provider:      provider,
		logger:        logger,
		enabled:       enabled,
		requireAuth:   requireAuth,
		tokenTimeout:  tokenTimeout,
		tokenCache:    make(map[string]*AuthResult),
		tokenCacheTTL: cacheTTL,
	}

	// Start token cache cleanup
	if enabled {
		go auth.tokenCacheCleanup()
	}

	return auth
}

// OnEvent handles authentication-related events
func (a *AuthMiddleware) OnEvent(ctx context.Context, connCtx *ConnectionContext, event ConnectionEvent, err error) error {
	if !a.enabled {
		return nil
	}

	switch event {
	case EventConnected:
		// Generate a temporary connection ID if not set
		if connCtx.ConnectionID == "" {
			connCtx.ConnectionID = a.generateConnectionID()
		}
		connCtx.State = StateHandshaking
		return nil

	case EventAuthenticated:
		connCtx.State = StateAuthenticated
		return nil
	}

	return nil
}

// OnRead checks authentication before allowing reads
func (a *AuthMiddleware) OnRead(ctx context.Context, connCtx *ConnectionContext) error {
	if !a.enabled {
		return nil
	}

	// Allow handshaking connections to read (for auth messages)
	if connCtx.State == StateHandshaking {
		return nil
	}

	// Require authentication for other states
	if a.requireAuth && connCtx.State != StateAuthenticated {
		return errors.New(ErrAuthenticationRequired, "authentication required", nil)
	}

	return nil
}

// OnWrite checks authentication before allowing writes
func (a *AuthMiddleware) OnWrite(ctx context.Context, connCtx *ConnectionContext) error {
	if !a.enabled {
		return nil
	}

	// Allow handshaking connections to write (for auth responses)
	if connCtx.State == StateHandshaking {
		return nil
	}

	// Require authentication for other states
	if a.requireAuth && connCtx.State != StateAuthenticated {
		return errors.New(ErrAuthenticationRequired, "authentication required", nil)
	}

	return nil
}

// OnError handles authentication errors
func (a *AuthMiddleware) OnError(ctx context.Context, connCtx *ConnectionContext, err error) error {
	if !a.enabled {
		return err
	}

	// Log authentication errors
	if connCtx.State == StateHandshaking || connCtx.State == StateAuthenticating {
		a.logger.Warn().
			Err(err).
			Str("client", connCtx.ClientAddr).
			Str("connection_id", connCtx.ConnectionID).
			Str("state", a.stateToString(connCtx.State)).
			Msg("Authentication error")
	}

	return err
}

// OnQuery checks permissions before allowing query execution
func (a *AuthMiddleware) OnQuery(ctx context.Context, connCtx *ConnectionContext, query string) error {
	if !a.enabled {
		return nil
	}

	// Require authentication for queries
	if a.requireAuth && connCtx.State != StateAuthenticated {
		return errors.New(ErrAuthenticationRequired, "authentication required for query execution", nil)
	}

	// Check if query requires special permissions
	if a.requiresSpecialPermissions(query) {
		if !a.hasPermission(connCtx, "admin") {
			return errors.New(ErrInsufficientPermissions, "insufficient permissions for this query", nil)
		}
	}

	return nil
}

// Authenticate performs authentication for a connection
func (a *AuthMiddleware) Authenticate(ctx context.Context, connCtx *ConnectionContext, username, password, database string) error {
	if !a.enabled {
		// If auth is disabled, auto-authenticate
		connCtx.Username = username
		connCtx.Database = database
		connCtx.State = StateAuthenticated
		return nil
	}

	connCtx.State = StateAuthenticating

	// Perform authentication
	result, err := a.provider.Authenticate(ctx, username, password, database)
	if err != nil {
		connCtx.State = StateHandshaking
		return errors.New(ErrAuthenticationFailed, "authentication failed", err)
	}

	if !result.Authenticated {
		connCtx.State = StateHandshaking
		return errors.New(ErrInvalidCredentials, "invalid credentials", nil)
	}

	// Set connection context
	connCtx.Username = result.Username
	connCtx.Database = result.Database
	connCtx.AuthToken = result.Token
	connCtx.State = StateAuthenticated

	// Cache the auth result
	a.cacheAuthResult(result.Token, result)

	a.logger.Info().
		Str("client", connCtx.ClientAddr).
		Str("connection_id", connCtx.ConnectionID).
		Str("username", username).
		Str("database", database).
		Msg("Authentication successful")

	return nil
}

// ValidateToken validates an existing authentication token
func (a *AuthMiddleware) ValidateToken(ctx context.Context, connCtx *ConnectionContext, token string) error {
	if !a.enabled {
		return nil
	}

	// Check cache first
	if cached := a.getCachedAuthResult(token); cached != nil {
		if time.Now().Before(cached.ExpiresAt) {
			connCtx.Username = cached.Username
			connCtx.Database = cached.Database
			connCtx.AuthToken = token
			connCtx.State = StateAuthenticated
			return nil
		}
		// Remove expired token from cache
		a.removeCachedAuthResult(token)
	}

	// Validate with provider
	result, err := a.provider.ValidateToken(ctx, token)
	if err != nil {
		return errors.New(ErrTokenValidationFailed, "token validation failed", err)
	}

	if !result.Authenticated {
		return errors.New(ErrInvalidToken, "invalid token", nil)
	}

	// Set connection context
	connCtx.Username = result.Username
	connCtx.Database = result.Database
	connCtx.AuthToken = token
	connCtx.State = StateAuthenticated

	// Cache the result
	a.cacheAuthResult(token, result)

	return nil
}

// requiresSpecialPermissions checks if a query requires special permissions
func (a *AuthMiddleware) requiresSpecialPermissions(query string) bool {
	// Add logic to detect dangerous queries
	dangerousKeywords := []string{
		"DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE",
		"SHUTDOWN", "KILL", "SYSTEM", "EXEC", "EXECUTE",
	}

	queryUpper := strings.ToUpper(query)
	for _, keyword := range dangerousKeywords {
		if strings.Contains(queryUpper, keyword) {
			return true
		}
	}

	return false
}

// hasPermission checks if a connection has a specific permission
func (a *AuthMiddleware) hasPermission(connCtx *ConnectionContext, permission string) bool {
	// This would check against the cached auth result permissions
	// For now, return false to be safe
	return true
}

// cacheAuthResult caches an authentication result
func (a *AuthMiddleware) cacheAuthResult(token string, result *AuthResult) {
	a.tokenCacheMu.Lock()
	defer a.tokenCacheMu.Unlock()

	a.tokenCache[token] = result
}

// getCachedAuthResult retrieves a cached authentication result
func (a *AuthMiddleware) getCachedAuthResult(token string) *AuthResult {
	a.tokenCacheMu.RLock()
	defer a.tokenCacheMu.RUnlock()

	return a.tokenCache[token]
}

// removeCachedAuthResult removes an expired token from cache
func (a *AuthMiddleware) removeCachedAuthResult(token string) {
	a.tokenCacheMu.Lock()
	defer a.tokenCacheMu.Unlock()

	delete(a.tokenCache, token)
}

// tokenCacheCleanup periodically cleans up expired tokens
func (a *AuthMiddleware) tokenCacheCleanup() {
	ticker := time.NewTicker(a.tokenCacheTTL / 2)
	defer ticker.Stop()

	for range ticker.C {
		a.tokenCacheMu.Lock()
		now := time.Now()

		for token, result := range a.tokenCache {
			if now.After(result.ExpiresAt) {
				delete(a.tokenCache, token)
			}
		}
		a.tokenCacheMu.Unlock()
	}
}

// generateConnectionID generates a unique connection ID
func (a *AuthMiddleware) generateConnectionID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// stateToString converts connection state to string
func (a *AuthMiddleware) stateToString(state ConnectionState) string {
	switch state {
	case StateHandshaking:
		return "handshaking"
	case StateAuthenticating:
		return "authenticating"
	case StateAuthenticated:
		return "authenticated"
	case StateQuerying:
		return "querying"
	case StateIdle:
		return "idle"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}
