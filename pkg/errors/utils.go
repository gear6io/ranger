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
