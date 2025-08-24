package errors

import (
	"fmt"
	"runtime"
	"time"
)

// Error - simplified structure
type Error struct {
	Code      Code
	Message   string
	Cause     error
	Context   map[string]string
	Stack     []Frame
	Timestamp time.Time
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
//
//	// Chain multiple context additions
//	return errors.AddContext(
//	    errors.AddContext(err, "database", dbName),
//	    "table", tableName,
//	)
func AddContext(err error, key string, value interface{}) *Error {
	// If it's already our Error type, add context to it
	if rangerErr, ok := err.(*Error); ok {
		return rangerErr.AddContext(key, fmt.Sprintf("%v", value))
	}

	// For standard errors, create a new error with the context
	newErr := &Error{
		Code:      CommonInternal,
		Message:   err.Error(),
		Cause:     err,
		Timestamp: time.Now(),
		Stack:     captureStackTrace(),
		Context:   make(map[string]string),
	}
	newErr.Context[key] = fmt.Sprintf("%v", value)
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
//	    AddContext("timestamp", time.Now().Format(time.RFC3339))
//
//	// Add context with different value types (all converted to strings)
//	return errors.New(ErrValidationFailed, "validation failed").
//	    AddContext("row_index", 42).
//	    AddContext("field_name", "email").
//	    AddContext("is_required", true)
//
//	// Chain with external AddContext
//	baseErr := errors.New(ErrDataParseFailed, "failed to parse data")
//	return errors.AddContext(baseErr, "source", "user_input").
//	    AddContext("format", "json")
func (e *Error) AddContext(key string, value interface{}) *Error {
	if e.Context == nil {
		e.Context = make(map[string]string)
	}
	e.Context[key] = fmt.Sprintf("%v", value)
	return e
}

// Error methods

// Error returns the error message string.
// If the error has a cause, it includes both the message and cause.
//
// Examples:
//
//	err := errors.New(ErrTableNotFound, "table not found")
//	fmt.Println(err.Error()) // Output: "table not found"
//
//	err = errors.New(ErrDataParseFailed, "parse failed", parseErr)
//	fmt.Println(err.Error()) // Output: "parse failed: invalid json format"
func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
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
