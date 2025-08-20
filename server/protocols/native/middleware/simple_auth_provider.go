package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// SimpleAuthProvider provides basic authentication for development/testing
type SimpleAuthProvider struct {
	users    map[string]*User
	mu       sync.RWMutex
	logger   zerolog.Logger
	tokenTTL time.Duration
}

// User represents a user in the system
type User struct {
	Username    string
	Password    string
	Database    string
	Permissions []string
	CreatedAt   time.Time
}

// NewSimpleAuthProvider creates a new simple authentication provider
func NewSimpleAuthProvider(tokenTTL time.Duration, logger zerolog.Logger) *SimpleAuthProvider {
	provider := &SimpleAuthProvider{
		users:    make(map[string]*User),
		logger:   logger,
		tokenTTL: tokenTTL,
	}

	// Add some default users for development
	provider.addDefaultUsers()

	return provider
}

// addDefaultUsers adds default users for development
func (provider *SimpleAuthProvider) addDefaultUsers() {
	defaultUsers := []*User{
		{
			Username:    "default",
			Password:    "",
			Database:    "default",
			Permissions: []string{"read", "write"},
			CreatedAt:   time.Now(),
		},
		{
			Username:    "admin",
			Password:    "admin123",
			Database:    "default",
			Permissions: []string{"read", "write", "admin"},
			CreatedAt:   time.Now(),
		},
		{
			Username:    "readonly",
			Password:    "readonly123",
			Database:    "default",
			Permissions: []string{"read"},
			CreatedAt:   time.Now(),
		},
	}

	for _, user := range defaultUsers {
		provider.users[user.Username] = user
	}

	provider.logger.Info().
		Int("user_count", len(defaultUsers)).
		Msg("Default users added to authentication provider")
}

// Authenticate authenticates a user with username/password
func (provider *SimpleAuthProvider) Authenticate(ctx context.Context, username, password, database string) (*AuthResult, error) {
	provider.mu.RLock()
	user, exists := provider.users[username]
	provider.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	// Check password (empty password is allowed for default user)
	if user.Password != "" && user.Password != password {
		return nil, fmt.Errorf("invalid password for user: %s", username)
	}

	// Check database access
	if user.Database != database {
		return nil, fmt.Errorf("user %s cannot access database %s", username, database)
	}

	// Generate token
	token, err := provider.generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	result := &AuthResult{
		Authenticated: true,
		Username:      username,
		Database:      database,
		Permissions:   user.Permissions,
		ExpiresAt:     time.Now().Add(provider.tokenTTL),
		Token:         token,
	}

	provider.logger.Debug().
		Str("username", username).
		Str("database", database).
		Str("token", token).
		Msg("User authenticated successfully")

	return result, nil
}

// ValidateToken validates an existing authentication token
func (provider *SimpleAuthProvider) ValidateToken(ctx context.Context, token string) (*AuthResult, error) {
	// For simple provider, we'll just check if the token format is valid
	// In a real implementation, you'd validate against stored tokens

	if len(token) != 32 { // Simple validation
		return nil, fmt.Errorf("invalid token format")
	}

	// For now, return a basic result
	// In production, you'd look up the token in a database/cache
	result := &AuthResult{
		Authenticated: true,
		Username:      "default",
		Database:      "default",
		Permissions:   []string{"read", "write"},
		ExpiresAt:     time.Now().Add(provider.tokenTTL),
		Token:         token,
	}

	return result, nil
}

// RefreshToken refreshes an existing authentication token
func (provider *SimpleAuthProvider) RefreshToken(ctx context.Context, token string) (*AuthResult, error) {
	// Validate the existing token first
	existing, err := provider.ValidateToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("invalid existing token: %w", err)
	}

	// Generate new token
	newToken, err := provider.generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate new token: %w", err)
	}

	// Return refreshed result
	result := &AuthResult{
		Authenticated: true,
		Username:      existing.Username,
		Database:      existing.Database,
		Permissions:   existing.Permissions,
		ExpiresAt:     time.Now().Add(provider.tokenTTL),
		Token:         newToken,
	}

	provider.logger.Debug().
		Str("username", existing.Username).
		Str("old_token", token).
		Str("new_token", newToken).
		Msg("Token refreshed successfully")

	return result, nil
}

// generateToken generates a random authentication token
func (provider *SimpleAuthProvider) generateToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// AddUser adds a new user to the provider
func (provider *SimpleAuthProvider) AddUser(username, password, database string, permissions []string) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	if _, exists := provider.users[username]; exists {
		return fmt.Errorf("user already exists: %s", username)
	}

	user := &User{
		Username:    username,
		Password:    password,
		Database:    database,
		Permissions: permissions,
		CreatedAt:   time.Now(),
	}

	provider.users[username] = user

	provider.logger.Info().
		Str("username", username).
		Str("database", database).
		Interface("permissions", permissions).
		Msg("User added to authentication provider")

	return nil
}

// RemoveUser removes a user from the provider
func (provider *SimpleAuthProvider) RemoveUser(username string) error {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	if _, exists := provider.users[username]; !exists {
		return fmt.Errorf("user not found: %s", username)
	}

	delete(provider.users, username)

	provider.logger.Info().
		Str("username", username).
		Msg("User removed from authentication provider")

	return nil
}

// GetUser returns user information
func (provider *SimpleAuthProvider) GetUser(username string) (*User, error) {
	provider.mu.RLock()
	defer provider.mu.RUnlock()

	user, exists := provider.users[username]
	if !exists {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	return user, nil
}

// ListUsers returns all users
func (provider *SimpleAuthProvider) ListUsers() []*User {
	provider.mu.RLock()
	defer provider.mu.RUnlock()

	users := make([]*User, 0, len(provider.users))
	for _, user := range provider.users {
		users = append(users, user)
	}

	return users
}
