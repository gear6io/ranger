package errors

import (
	"fmt"
	"strings"
)

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
