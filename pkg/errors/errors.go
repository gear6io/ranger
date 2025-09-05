package errors

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// InternalError interface for errors that can transform to internal error format
// This interface allows custom error types to integrate with the internal error system
// while maintaining their own structure and behavior.
type InternalError interface {
	error              // Standard Go error interface
	Transform() *Error // Convert to internal error format
}

// Error - simplified structure
type Error struct {
	Code        Code
	Message     string
	Cause       error
	context     map[string]any // private field
	Suggestions []string
	Recovery    []RecoveryAction
	Stack       []Frame
	Timestamp   time.Time
}

// RecoveryAction represents an actionable recovery suggestion
type RecoveryAction struct {
	Type        string `json:"type"`        // "retry", "fix_syntax", "check_config", etc.
	Description string `json:"description"` // Human-readable description
	Action      string `json:"action"`      // Specific action to take
	Automatic   bool   `json:"automatic"`   // Whether this can be automatically recovered
}

// Frame represents a stack frame
type Frame struct {
	Function string
	File     string
	Line     int
}

// Core constructors - code is compulsory first argument

// New creates a new error with the given code and message.
// The cause parameter is optional - pass nil if no underlying error exists.
//
// Use cases:
// - Creating origin-stage errors (validation failures, state errors)
// - Creating errors from external library calls
//
// Examples:
//
//	// Simple error without cause
//	return errors.New(ErrTableNotFound, "table does not exist")
//
//	// Error with underlying cause
//	return errors.New(ErrDataParseFailed, "failed to parse JSON", jsonErr)
//
//	// Error that will be enhanced with context
//	return errors.New(ErrValidationFailed, "validation failed").
//	    AddContext("field", "email").
//	    AddContext("value", userEmail)
func New(code Code, message string, cause error) *Error {
	return &Error{
		Code:      code,
		Message:   message,
		Cause:     cause,
		Timestamp: time.Now(),
		Stack:     captureStackTrace(),
	}
}

// Newf creates a new error with formatted message using fmt.Sprintf.
// The cause parameter is optional - pass nil if no underlying error exists.
//
// Use cases:
// - Creating errors with dynamic message content
// - When you need to include variable values in the error message
//
// Examples:
//
//	// Format message with variables
//	return errors.Newf(ErrTableNotFound, "table %s.%s does not exist", database, tableName)
//
//	// Format message with cause
//	return errors.Newf(ErrDataParseFailed, "failed to parse data from %s", source, parseErr)
func Newf(code Code, format string, args ...interface{}) *Error {
	return New(code, fmt.Sprintf(format, args...), nil)
}

// AddContext is an external function for adding context to internal Error.
// This is the primary way to add context when you don't have an Error instance.
// Add context to errors that the caller didn't had access to.
//
// Use cases:
// - Adding context to errors returned from internal packages only
// - When you need to enhance an existing error with additional information
//
// Examples:
//
//	// Add context to existing error
//	if err := validateUser(user); err != nil {
//	    return errors.AddContext(err, "request_id", requestID)
//	}
func AddContext(err error, key string, value any) *Error {
	// If it's already our Error type, add context to it
	if rangerErr, ok := err.(*Error); ok {
		return rangerErr.AddContext(key, value)
	}

	// For standard errors, create a new error with the context
	newErr := &Error{
		Code:      CommonInternal,
		Message:   err.Error(),
		Cause:     err,
		Timestamp: time.Now(),
		Stack:     captureStackTrace(),
		context:   make(map[string]any),
	}
	newErr.context[key] = value
	return newErr
}

// Methods on *Error for chaining - only essential ones

// AddContext adds a key-value pair to the error's context and returns the error for chaining.
// This method allows you to build up rich context information on an error.
//
// Use cases:
// - Adding multiple context values to an error
// - Building detailed error context for debugging
// - Chaining multiple context additions
//
// Examples:
//
//	// Add multiple context values
//	return errors.New(ErrTableNotFound, "table does not exist").
//	    AddContext("database", database).
//	    AddContext("table", tableName).
//	    AddContext("timestamp", time.Now())
//
//	// Add context with different value types (preserved as-is)
//	return errors.New(ErrValidationFailed, "validation failed").
//	    AddContext("row_index", 42).
//	    AddContext("field_name", "email").
//	    AddContext("is_required", true).
//	    AddContext("failed_columns", []string{"col1", "col2"})
//
//	// Chain with external AddContext
//	baseErr := errors.New(ErrDataParseFailed, "failed to parse data")
//	return errors.AddContext(baseErr, "source", "user_input").
//	    AddContext("format", "json")
func (e *Error) AddContext(key string, value any) *Error {
	if e.context == nil {
		e.context = make(map[string]any)
	}
	e.context[key] = value
	return e
}

// Error methods

// Error returns the error message string with context information.
// If the error has a cause, it includes both the message and cause.
// Context information is appended to provide additional debugging details.
//
// Examples:
//
//	err := errors.New(ErrTableNotFound, "table not found")
//	fmt.Println(err.Error()) // Output: "table not found"
//
//	err = errors.New(ErrDataParseFailed, "parse failed", parseErr).
//	    AddContext("table", "users").AddContext("database", "main")
//	fmt.Println(err.Error()) // Output: "parse failed: invalid json format [table=users database=main]"
func (e *Error) Error() string {
	var parts []string

	// Base message with cause
	if e.Cause != nil {
		parts = append(parts, fmt.Sprintf("%s: %v", e.Message, e.Cause))
	} else {
		parts = append(parts, e.Message)
	}

	// Add context if available
	if len(e.context) > 0 {
		var contextParts []string
		for key, value := range e.context {
			contextParts = append(contextParts, fmt.Sprintf("%s=%v", key, value))
		}
		if len(contextParts) > 0 {
			parts = append(parts, fmt.Sprintf("[%s]", strings.Join(contextParts, " ")))
		}
	}

	return strings.Join(parts, " ")
}

// Unwrap returns the underlying cause error, implementing the errors.Unwrap interface.
// This allows the error to work with Go's error handling utilities.
//
// Examples:
//
//	err := errors.New(ErrDataParseFailed, "parse failed", parseErr)
//	cause := errors.Unwrap(err) // Returns parseErr
func (e *Error) Unwrap() error {
	return e.Cause
}

// Context accessor methods

// GetContext returns the value for the given context key.
// Returns nil if the key doesn't exist.
//
// Examples:
//
//	err := errors.New(ErrTableNotFound, "table not found").
//	    AddContext("table_name", "users")
//	tableName := err.GetContext("table_name") // Returns "users"
func (e *Error) GetContext(key string) any {
	if e.context == nil {
		return nil
	}
	return e.context[key]
}

// HasContext returns true if the given context key exists.
//
// Examples:
//
//	err := errors.New(ErrTableNotFound, "table not found").
//	    AddContext("table_name", "users")
//	hasTable := err.HasContext("table_name") // Returns true
func (e *Error) HasContext(key string) bool {
	if e.context == nil {
		return false
	}
	_, exists := e.context[key]
	return exists
}

// GetContextKeys returns all context keys.
// Useful for debugging or logging all available context.
//
// Examples:
//
//	err := errors.New(ErrTableNotFound, "table not found").
//	    AddContext("table_name", "users").
//	    AddContext("database", "main")
//	keys := err.GetContextKeys() // Returns ["table_name", "database"]
func (e *Error) GetContextKeys() []string {
	if e.context == nil {
		return nil
	}
	keys := make([]string, 0, len(e.context))
	for key := range e.context {
		keys = append(keys, key)
	}
	return keys
}

// Suggestions and Recovery methods

// AddSuggestion adds a suggestion to the error and returns the error for chaining.
//
// Examples:
//
//	return errors.New(ErrTableNotFound, "table not found").
//	    AddSuggestion("Check if the table name is correct").
//	    AddSuggestion("Verify the database connection")
func (e *Error) AddSuggestion(suggestion string) *Error {
	e.Suggestions = append(e.Suggestions, suggestion)
	return e
}

// AddSuggestions adds multiple suggestions to the error and returns the error for chaining.
//
// Examples:
//
//	suggestions := []string{
//	    "Check if the table name is correct",
//	    "Verify the database connection",
//	}
//	return errors.New(ErrTableNotFound, "table not found").
//	    AddSuggestions(suggestions)
func (e *Error) AddSuggestions(suggestions []string) *Error {
	e.Suggestions = append(e.Suggestions, suggestions...)
	return e
}

// AddRecoveryAction adds a recovery action to the error and returns the error for chaining.
//
// Examples:
//
//	return errors.New(ErrConnectionFailed, "connection failed").
//	    AddRecoveryAction(RecoveryAction{
//	        Type: "retry",
//	        Description: "Retry the connection",
//	        Action: "Wait 5 seconds and retry",
//	        Automatic: true,
//	    })
func (e *Error) AddRecoveryAction(action RecoveryAction) *Error {
	e.Recovery = append(e.Recovery, action)
	return e
}

// AddRecoveryActions adds multiple recovery actions to the error and returns the error for chaining.
//
// Examples:
//
//	actions := []RecoveryAction{
//	    {Type: "retry", Description: "Retry operation", Automatic: true},
//	    {Type: "check_config", Description: "Check configuration", Automatic: false},
//	}
//	return errors.New(ErrConnectionFailed, "connection failed").
//	    AddRecoveryActions(actions)
func (e *Error) AddRecoveryActions(actions []RecoveryAction) *Error {
	e.Recovery = append(e.Recovery, actions...)
	return e
}

// IsRecoverable returns true if the error has any automatic recovery actions.
//
// Examples:
//
//	err := errors.New(ErrConnectionFailed, "connection failed").
//	    AddRecoveryAction(RecoveryAction{Type: "retry", Automatic: true})
//	canRecover := err.IsRecoverable() // Returns true
func (e *Error) IsRecoverable() bool {
	for _, action := range e.Recovery {
		if action.Automatic {
			return true
		}
	}
	return false
}

// GetAutomaticRecoveryActions returns only the recovery actions that can be performed automatically.
//
// Examples:
//
//	err := errors.New(ErrConnectionFailed, "connection failed").
//	    AddRecoveryAction(RecoveryAction{Type: "retry", Automatic: true}).
//	    AddRecoveryAction(RecoveryAction{Type: "check_config", Automatic: false})
//	autoActions := err.GetAutomaticRecoveryActions() // Returns only the retry action
func (e *Error) GetAutomaticRecoveryActions() []RecoveryAction {
	var automaticActions []RecoveryAction
	for _, action := range e.Recovery {
		if action.Automatic {
			automaticActions = append(automaticActions, action)
		}
	}
	return automaticActions
}

// Helper functions

// captureStackTrace captures the current call stack for debugging purposes.
// It captures up to 10 stack frames starting from the caller.
func captureStackTrace() []Frame {
	var frames []Frame
	for i := 1; i < 10; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		frames = append(frames, Frame{
			Function: fn.Name(),
			File:     file,
			Line:     line,
		})
	}
	return frames
}
