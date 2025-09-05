package errors

import (
	"testing"
)

func TestNewAPI(t *testing.T) {
	// Test basic error creation
	err := New(CommonInternal, "test error", nil)
	if err.Code != CommonInternal {
		t.Errorf("Expected code %v, got %v", CommonInternal, err.Code)
	}
	if err.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", err.Message)
	}
	if err.Cause != nil {
		t.Errorf("Expected nil cause, got %v", err.Cause)
	}

	// Test error creation with cause
	cause := New(CommonNotFound, "cause error", nil)
	err = New(CommonInternal, "test error", cause)
	if err.Cause != cause {
		t.Errorf("Expected cause %v, got %v", cause, err.Cause)
	}
}

func TestAddContext(t *testing.T) {
	// Test external AddContext function
	originalErr := New(CommonInternal, "test error", nil)
	err := AddContext(originalErr, "key1", "value1")

	if err.GetContext("key1") != "value1" {
		t.Errorf("Expected context key1=value1, got %v", err.GetContext("key1"))
	}

	// Test chaining AddContext
	err = err.AddContext("key2", "value2").AddContext("key3", 123)

	if err.GetContext("key2") != "value2" {
		t.Errorf("Expected context key2=value2, got %v", err.GetContext("key2"))
	}
	if err.GetContext("key3") != 123 {
		t.Errorf("Expected context key3=123, got %v", err.GetContext("key3"))
	}
}

func TestNewCodeFormat(t *testing.T) {
	// Test old format (still valid)
	code, err := NewCode("package.name")
	if err != nil {
		t.Errorf("Expected no error for 'package.name', got %v", err)
	}
	if code.String() != "package.name" {
		t.Errorf("Expected 'package.name', got '%s'", code.String())
	}

	// Test new format with sub-component
	code, err = NewCode("package.sub_component.error_code")
	if err != nil {
		t.Errorf("Expected no error for 'package.sub_component.error_code', got %v", err)
	}
	if code.String() != "package.sub_component.error_code" {
		t.Errorf("Expected 'package.sub_component.error_code', got '%s'", code.String())
	}

	// Test component extraction
	if code.Package() != "package" {
		t.Errorf("Expected package 'package', got '%s'", code.Package())
	}
	if code.Component() != "sub_component" {
		t.Errorf("Expected component 'sub_component', got '%s'", code.Component())
	}
	if code.ErrorCode() != "error_code" {
		t.Errorf("Expected error code 'error_code', got '%s'", code.ErrorCode())
	}
}

func TestInvalidCodeFormat(t *testing.T) {
	// Test invalid format
	_, err := NewCode("invalid-format")
	if err == nil {
		t.Error("Expected error for invalid format 'invalid-format'")
	}

	// Test single level (too short)
	_, err = NewCode("package")
	if err == nil {
		t.Error("Expected error for single level 'package'")
	}
}
