package errors

import (
	"fmt"
	"strings"
)

// Common error codes for the project (using the new Code type)
// These are now defined in code.go with proper validation

// Migration helpers - make existing code work immediately
func FromFmtErrorf(code Code, format string, args ...interface{}) *Error {
	return Newf(code, format, args...)
}

// Common error constructors for quick use
func Internal(message string) *Error {
	return New(CommonInternal, message)
}

func NotFound(message string) *Error {
	return New(CommonNotFound, message)
}

func Validation(message string) *Error {
	return New(CommonValidation, message)
}

func Timeout(message string) *Error {
	return New(CommonTimeout, message)
}

func Unauthorized(message string) *Error {
	return New(CommonUnauthorized, message)
}

func Forbidden(message string) *Error {
	return New(CommonForbidden, message)
}

func Conflict(message string) *Error {
	return New(CommonConflict, message)
}

func Unsupported(message string) *Error {
	return New(CommonUnsupported, message)
}

func InvalidInput(message string) *Error {
	return New(CommonInvalidInput, message)
}

func AlreadyExists(message string) *Error {
	return New(CommonAlreadyExists, message)
}

// Helper to check if an error is of our Error type
func IsIceboxError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

// Helper to extract context from our errors
func GetContext(err error) map[string]string {
	if iceboxErr, ok := err.(*Error); ok {
		return iceboxErr.Context
	}
	return nil
}

// Helper to get error code
func GetCode(err error) string {
	if iceboxErr, ok := err.(*Error); ok {
		return iceboxErr.Code.String()
	}
	return ""
}

// Helper to format error for logging
func FormatForLog(err error) string {
	if iceboxErr, ok := err.(*Error); ok {
		var parts []string
		parts = append(parts, fmt.Sprintf("Code: %s", iceboxErr.Code))
		parts = append(parts, fmt.Sprintf("Message: %s", iceboxErr.Message))

		if len(iceboxErr.Context) > 0 {
			var contextParts []string
			for k, v := range iceboxErr.Context {
				contextParts = append(contextParts, fmt.Sprintf("%s=%s", k, v))
			}
			parts = append(parts, fmt.Sprintf("Context: %s", strings.Join(contextParts, ", ")))
		}

		if iceboxErr.Cause != nil {
			parts = append(parts, fmt.Sprintf("Cause: %v", iceboxErr.Cause))
		}

		return strings.Join(parts, " | ")
	}
	return err.Error()
}
