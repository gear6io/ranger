package errors

import (
	"fmt"
	"testing"
)

// MockInternalError implements InternalError interface for testing
type MockInternalError struct {
	message string
}

func (m *MockInternalError) Error() string {
	return m.message
}

func (m *MockInternalError) Transform() *Error {
	return New(CommonInternal, m.message, nil).AddContext("mock", "true")
}

// TestAsErrorMigrationPattern tests the AsError utility function with different error types
func TestAsErrorMigrationPattern(t *testing.T) {
	testCases := []struct {
		name     string
		input    error
		expected string
	}{
		{
			name:     "InternalError",
			input:    &MockInternalError{message: "mock internal error"},
			expected: "mock internal error",
		},
		{
			name:     "ExistingError",
			input:    New(CommonInternal, "existing error", nil),
			expected: "existing error",
		},
		{
			name:     "StandardError",
			input:    fmt.Errorf("standard error"),
			expected: "standard error",
		},
		{
			name:     "NilError",
			input:    nil,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := AsError(tc.input)

			if tc.input == nil {
				if result != nil {
					t.Error("AsError should return nil for nil input")
				}
				return
			}

			if result == nil {
				t.Fatal("AsError should not return nil for non-nil input")
			}

			// Verify it's always a Ranger error
			if !IsRangerError(result) {
				t.Error("AsError should always return a Ranger error")
			}

			// Verify message is preserved
			if result.Message != tc.expected {
				t.Errorf("Expected message '%s', got '%s'", tc.expected, result.Message)
			}

			// For MockInternalError, verify Transform() was called
			if tc.name == "InternalError" {
				context := GetContext(result)
				if context == nil || context["mock"] != "true" {
					t.Error("AsError should use Transform() method for InternalError types")
				}
			}
		})
	}
}

// TestAsErrorVsOldPattern demonstrates the difference between old and new patterns
func TestAsErrorVsOldPattern(t *testing.T) {
	// Create a test error
	testErr := fmt.Errorf("test error")

	// NEW PATTERN: Using AsError
	newResult := AsError(testErr)

	// OLD PATTERN: Manual type assertion (for comparison)
	var oldResult *Error
	if internalErr, ok := testErr.(*Error); ok {
		oldResult = internalErr
	} else {
		oldResult = New(CommonInternal, testErr.Error(), testErr)
	}

	// Both should produce equivalent results
	if newResult.Message != oldResult.Message {
		t.Errorf("AsError and old pattern should produce same message. New: %s, Old: %s",
			newResult.Message, oldResult.Message)
	}

	if newResult.Code != oldResult.Code {
		t.Errorf("AsError and old pattern should produce same code. New: %s, Old: %s",
			newResult.Code, oldResult.Code)
	}

	// AsError should be more concise and consistent
	t.Logf("NEW PATTERN: errors.AsError(err) - Simple and consistent")
	t.Logf("OLD PATTERN: Manual type checking - Verbose and error-prone")
}

// TestAsErrorChaining tests error chaining with AsError
func TestAsErrorChaining(t *testing.T) {
	// Simulate a chain of operations that might produce errors
	originalErr := fmt.Errorf("original error")

	// Each step in the chain uses AsError for consistent handling
	step1Err := AsError(originalErr).AddContext("step", "1")
	step2Err := AsError(step1Err).AddContext("step", "2")
	step3Err := AsError(step2Err).AddContext("step", "3")

	// Verify the chain preserves information
	context := GetContext(step3Err)
	if context == nil {
		t.Fatal("Error chain should preserve context")
	}

	// The last context addition should be present
	if context["step"] != "3" {
		t.Errorf("Expected step=3, got step=%s", context["step"])
	}

	// Original error should be preserved in the chain
	if step3Err.Message != "original error" {
		t.Errorf("Original error message should be preserved, got: %s", step3Err.Message)
	}
}
