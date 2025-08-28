package errors

import (
	"fmt"
	"strings"
)

// Helper to check if an error is of our Error type
func IsRangerError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

// Helper to extract context from our errors
func GetContext(err error) map[string]string {
	if rangerErr, ok := err.(*Error); ok {
		return rangerErr.Context
	}
	return nil
}

// Helper to get error code
func GetCode(err error) string {
	if rangerErr, ok := err.(*Error); ok {
		return rangerErr.Code.String()
	}
	return ""
}

// Helper to format error for logging
func FormatError(err error) string {
	if rangerErr, ok := err.(*Error); ok {
		var parts []string
		parts = append(parts, fmt.Sprintf("Code: %s", rangerErr.Code))
		parts = append(parts, fmt.Sprintf("Message: %s", rangerErr.Message))

		if len(rangerErr.Context) > 0 {
			parts = append(parts, "Context:")
			for k, v := range rangerErr.Context {
				parts = append(parts, fmt.Sprintf("  %s: %v", k, v))
			}
		}

		if rangerErr.Cause != nil {
			parts = append(parts, fmt.Sprintf("Cause: %v", rangerErr.Cause))
		}

		return strings.Join(parts, "\n")
	}
	return err.Error()
}

// AsError converts any error to internal errors.Error format
// This utility function provides a consistent way to handle different error types:
// - InternalError types are transformed using their Transform() method
// - Existing errors.Error types are returned as-is
// - Standard Go errors are wrapped in a generic internal error
//
// MIGRATION GUIDE:
// This function replaces direct error type assertions throughout the codebase.
// Use this function instead of manual type checking for consistent error handling.
//
// Use cases:
// - Replacing direct type assertions throughout the codebase
// - Ensuring consistent error handling across different error types
// - Providing fallback behavior for unknown error types
// - Converting any error to internal format for logging and context
//
// Examples:
//
//	// OLD PATTERN (deprecated - replace with AsError):
//	if internalErr, ok := err.(errors.Error); ok {
//	    return internalErr
//	}
//	return errors.New(errors.CommonInternal, err.Error(), err)
//
//	// NEW PATTERN (recommended):
//	return AsError(err)
//
//	// Works with any error type:
//	parseErr := &ParseError{...}
//	internalErr := AsError(parseErr) // Uses Transform() method
//
//	standardErr := fmt.Errorf("something went wrong")
//	internalErr := AsError(standardErr) // Creates generic internal error
//
//	// Use in error handling chains:
//	if err := someOperation(); err != nil {
//	    return AsError(err).AddContext("operation", "someOperation")
//	}
func AsError(err error) *Error {
	if err == nil {
		return nil
	}

	// Check if it implements InternalError interface
	if ie, ok := err.(InternalError); ok {
		return ie.Transform()
	}

	// Check if it's already our Error type
	if internalErr, ok := err.(*Error); ok {
		return internalErr
	}

	// Fallback: create generic internal error for unknown error types
	return New(CommonInternal, err.Error(), err)
}
