package errors

import (
	"errors"
	"strings"
	"testing"
)

// Test codes for testing
var (
	testCode          = MustNewCode("test.code")
	testCode2         = MustNewCode("test.code2")
	tableNotFoundCode = MustNewCode("query.table_not_found")
	baseCode          = MustNewCode("test.base")
)

func TestNew(t *testing.T) {
	err := New(CommonInternal, "test error")

	if err.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", err.Message)
	}

	if err.Code.String() != "common.internal" {
		t.Errorf("Expected code 'common.internal', got '%s'", err.Code.String())
	}

	if err.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}

	if len(err.Stack) == 0 {
		t.Error("Expected stack trace to be captured")
	}
}

func TestNewf(t *testing.T) {
	err := Newf(CommonInternal, "test error with %s", "formatting")

	expected := "test error with formatting"
	if err.Message != expected {
		t.Errorf("Expected message '%s', got '%s'", expected, err.Message)
	}

	if err.Code.String() != "common.internal" {
		t.Errorf("Expected code 'common.internal', got '%s'", err.Code.String())
	}
}

func TestWrap(t *testing.T) {
	originalErr := errors.New("original error")
	err := Wrap(testCode, originalErr, "wrapped error")

	if err.Message != "wrapped error" {
		t.Errorf("Expected message 'wrapped error', got '%s'", err.Message)
	}

	if err.Code.String() != "test.code" {
		t.Errorf("Expected code 'test.code', got '%s'", err.Code.String())
	}

	if err.Cause != originalErr {
		t.Error("Expected cause to be set to original error")
	}
}

func TestWrapf(t *testing.T) {
	originalErr := errors.New("original error")
	err := Wrapf(testCode, originalErr, "wrapped error with %s", "formatting")

	expected := "wrapped error with formatting"
	if err.Message != expected {
		t.Errorf("Expected message '%s', got '%s'", expected, err.Message)
	}

	if err.Code.String() != "test.code" {
		t.Errorf("Expected code 'test.code', got '%s'", err.Code.String())
	}

	if err.Cause != originalErr {
		t.Error("Expected cause to be set to original error")
	}
}

func TestWithAdditional(t *testing.T) {
	// Test with our Error type
	originalErr := New(tableNotFoundCode, "table not found").
		AddContext("table_name", "users")

	enhancedErr := WithAdditional(originalErr, "while processing request from user %s", "12345")

	// Check that structure is preserved
	if enhancedErr.Code.String() != "query.table_not_found" {
		t.Errorf("Expected code 'query.table_not_found', got '%s'", enhancedErr.Code.String())
	}

	if enhancedErr.Message != "table not found" {
		t.Errorf("Expected message 'table not found', got '%s'", enhancedErr.Message)
	}

	if enhancedErr.Cause != originalErr.Cause {
		t.Error("Expected cause to be preserved")
	}

	// Check that existing context is preserved
	if enhancedErr.Context["table_name"] != "users" {
		t.Errorf("Expected context table_name='users', got '%s'", enhancedErr.Context["table_name"])
	}

	// Check that new context is added
	additionalKey := "additional_0"
	if enhancedErr.Context[additionalKey] != "while processing request from user 12345" {
		t.Errorf("Expected additional context '%s', got '%s'", "while processing request from user 12345", enhancedErr.Context[additionalKey])
	}

	// Check that stack and timestamp are preserved
	if len(enhancedErr.Stack) != len(originalErr.Stack) {
		t.Error("Expected stack trace to be preserved")
	}

	if !enhancedErr.Timestamp.Equal(originalErr.Timestamp) {
		t.Error("Expected timestamp to be preserved")
	}
}

func TestWithAdditionalMultipleCalls(t *testing.T) {
	// Test multiple WithAdditional calls
	originalErr := New(baseCode, "base error")

	// First additional context
	err1 := WithAdditional(originalErr, "first additional: %s", "context1")

	// Second additional context
	err2 := WithAdditional(err1, "second additional: %s", "context2")

	// Check that all context is preserved
	if err2.Context["additional_0"] != "first additional: context1" {
		t.Errorf("Expected first additional context, got '%s'", err2.Context["additional_0"])
	}

	if err2.Context["additional_1"] != "second additional: context2" {
		t.Errorf("Expected second additional context, got '%s'", err2.Context["additional_1"])
	}
}

func TestWithAdditionalWithStandardError(t *testing.T) {
	// Test with standard error (fallback case)
	standardErr := errors.New("standard error")
	enhancedErr := WithAdditional(standardErr, "additional context: %s", "details")

	// Should create a new Error wrapping the standard error
	if !IsIceboxError(enhancedErr) {
		t.Error("Expected WithAdditional to return our Error type for standard errors")
	}

	if enhancedErr.Cause != standardErr {
		t.Error("Expected cause to be set to standard error")
	}

	// Should have the additional context
	if enhancedErr.Context["additional_0"] != "additional context: details" {
		t.Errorf("Expected additional context, got '%s'", enhancedErr.Context["additional_0"])
	}
}

func TestWithAdditionalWithNilError(t *testing.T) {
	// Test with nil error
	enhancedErr := WithAdditional(nil, "additional context: %s", "details")

	// Should handle nil gracefully
	if enhancedErr == nil {
		t.Error("Expected WithAdditional to handle nil error gracefully")
	}
}

func TestAddContext(t *testing.T) {
	err := New(testCode, "test error").
		AddContext("key1", "value1").
		AddContext("key2", "value2")

	if err.Context["key1"] != "value1" {
		t.Errorf("Expected context key1='value1', got '%s'", err.Context["key1"])
	}

	if err.Context["key2"] != "value2" {
		t.Errorf("Expected context key2='value2', got '%s'", err.Context["key2"])
	}
}

func TestWithCause(t *testing.T) {
	originalErr := errors.New("original error")
	err := New(testCode, "test error").WithCause(originalErr)

	if err.Cause != originalErr {
		t.Error("Expected cause to be set to original error")
	}
}

func TestErrorString(t *testing.T) {
	// Test error without cause
	err := New(testCode, "test error")
	expected := "test error"
	if err.Error() != expected {
		t.Errorf("Expected error string '%s', got '%s'", expected, err.Error())
	}

	// Test error with cause
	originalErr := errors.New("original error")
	err = Wrap(testCode, originalErr, "wrapped error")
	expected = "wrapped error: original error"
	if err.Error() != expected {
		t.Errorf("Expected error string '%s', got '%s'", expected, err.Error())
	}
}

func TestUnwrap(t *testing.T) {
	originalErr := errors.New("original error")
	err := Wrap(testCode, originalErr, "wrapped error")

	unwrapped := err.Unwrap()
	if unwrapped != originalErr {
		t.Error("Expected Unwrap to return original error")
	}
}

func TestCaptureStackTrace(t *testing.T) {
	err := New(testCode, "test error")

	if len(err.Stack) == 0 {
		t.Error("Expected stack trace to be captured")
	}

	// Check that we have function names
	hasValidFunction := false
	for _, frame := range err.Stack {
		if frame.Function != "" && frame.File != "" && frame.Line > 0 {
			hasValidFunction = true
			break
		}
	}

	if !hasValidFunction {
		t.Error("Expected valid stack frame information")
	}
}

func TestMethodChaining(t *testing.T) {
	err := New(testCode, "test error").
		AddContext("key", "value").
		WithCause(errors.New("cause"))

	if err.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", err.Message)
	}

	if err.Code.String() != "test.code" {
		t.Errorf("Expected code 'test.code', got '%s'", err.Code.String())
	}

	if err.Context["key"] != "value" {
		t.Errorf("Expected context key='value', got '%s'", err.Context["key"])
	}

	if err.Cause == nil {
		t.Error("Expected cause to be set")
	}
}

func TestCommonErrorConstructors(t *testing.T) {
	tests := []struct {
		name         string
		constructor  func(string) *Error
		expectedCode string
	}{
		{"Internal", Internal, "common.internal"},
		{"NotFound", NotFound, "common.not_found"},
		{"Validation", Validation, "common.validation"},
		{"Timeout", Timeout, "common.timeout"},
		{"Unauthorized", Unauthorized, "common.unauthorized"},
		{"Forbidden", Forbidden, "common.forbidden"},
		{"Conflict", Conflict, "common.conflict"},
		{"Unsupported", Unsupported, "common.unsupported"},
		{"InvalidInput", InvalidInput, "common.invalid_input"},
		{"AlreadyExists", AlreadyExists, "common.already_exists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.constructor("test message")
			if err.Code.String() != tt.expectedCode {
				t.Errorf("Expected code '%s', got '%s'", tt.expectedCode, err.Code.String())
			}
			if err.Message != "test message" {
				t.Errorf("Expected message 'test message', got '%s'", err.Message)
			}
		})
	}
}

func TestIsIceboxError(t *testing.T) {
	// Test with our error type
	err := New(testCode, "test error")
	if !IsIceboxError(err) {
		t.Error("Expected IsIceboxError to return true for our error type")
	}

	// Test with standard error
	stdErr := errors.New("standard error")
	if IsIceboxError(stdErr) {
		t.Error("Expected IsIceboxError to return false for standard error")
	}
}

func TestGetContext(t *testing.T) {
	// Test with our error type
	err := New(testCode, "test error").AddContext("key", "value")
	context := GetContext(err)

	if context["key"] != "value" {
		t.Errorf("Expected context key='value', got '%s'", context["key"])
	}

	// Test with standard error
	stdErr := errors.New("standard error")
	context = GetContext(stdErr)
	if context != nil {
		t.Error("Expected GetContext to return nil for standard error")
	}
}

func TestGetCode(t *testing.T) {
	// Test with our error type
	err := New(testCode, "test error")
	code := GetCode(err)

	if code != "test.code" {
		t.Errorf("Expected code 'test.code', got '%s'", code)
	}

	// Test with standard error
	stdErr := errors.New("standard error")
	code = GetCode(stdErr)
	if code != "" {
		t.Error("Expected GetCode to return empty string for standard error")
	}
}

func TestFormatForLog(t *testing.T) {
	// Test with our error type
	err := New(testCode, "test error").
		AddContext("key1", "value1").
		AddContext("key2", "value2").
		WithCause(errors.New("cause error"))

	logStr := FormatForLog(err)

	// Check that all components are present
	if !strings.Contains(logStr, "Code: test.code") {
		t.Error("Expected log string to contain code")
	}
	if !strings.Contains(logStr, "Message: test error") {
		t.Error("Expected log string to contain message")
	}
	if !strings.Contains(logStr, "Context: key1=value1, key2=value2") {
		t.Error("Expected log string to contain context")
	}
	if !strings.Contains(logStr, "Cause: cause error") {
		t.Error("Expected log string to contain cause")
	}

	// Test with standard error
	stdErr := errors.New("standard error")
	logStr = FormatForLog(stdErr)
	if logStr != "standard error" {
		t.Errorf("Expected log string 'standard error', got '%s'", logStr)
	}
}

func TestFromFmtErrorf(t *testing.T) {
	err := FromFmtErrorf(testCode, "test error with %s", "formatting")

	expected := "test error with formatting"
	if err.Message != expected {
		t.Errorf("Expected message '%s', got '%s'", expected, err.Message)
	}

	if err.Code.String() != "test.code" {
		t.Errorf("Expected code 'test.code', got '%s'", err.Code.String())
	}
}
