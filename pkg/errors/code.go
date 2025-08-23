package errors

import (
	"fmt"
	"regexp"
	"strings"
)

// Code represents a validated error code with package prefix
type Code struct {
	value string
}

// Common error codes that can be used across packages
var (
	CommonInternal      = MustNewCode("common.internal")
	CommonNotFound      = MustNewCode("common.not_found")
	CommonValidation    = MustNewCode("common.validation")
	CommonTimeout       = MustNewCode("common.timeout")
	CommonUnauthorized  = MustNewCode("common.unauthorized")
	CommonForbidden     = MustNewCode("common.forbidden")
	CommonConflict      = MustNewCode("common.conflict")
	CommonUnsupported   = MustNewCode("common.unsupported")
	CommonInvalidInput  = MustNewCode("common.invalid_input")
	CommonAlreadyExists = MustNewCode("common.already_exists")
)

// Validation regex: package.sub_component.error_code format (supports 2-3 levels)
var codeRegex = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)

// NewCode creates a new validated Code
func NewCode(s string) (Code, error) {
	if !codeRegex.MatchString(s) {
		return Code{}, fmt.Errorf("invalid code format '%s': must be 'package.sub_component.error_code' (lowercase, underscores, dots only, supports 2-3 levels)", s)
	}

	return Code{value: s}, nil
}

// MustNewCode creates a new Code or panics if invalid
func MustNewCode(s string) Code {
	code, err := NewCode(s)
	if err != nil {
		panic(err)
	}
	return code
}

// String returns the string representation of the Code
func (c Code) String() string {
	return c.value
}

// Package returns the package prefix from the code
func (c Code) Package() string {
	if idx := strings.Index(c.value, "."); idx != -1 {
		return c.value[:idx]
	}
	return ""
}

// Name returns the name part from the code (everything after the first dot)
func (c Code) Name() string {
	if idx := strings.Index(c.value, "."); idx != -1 {
		return c.value[idx+1:]
	}
	return c.value
}

// Component returns the sub-component part from the code (second level)
func (c Code) Component() string {
	parts := strings.Split(c.value, ".")
	if len(parts) >= 3 {
		return parts[1]
	}
	return ""
}

// ErrorCode returns the actual error code part (last level)
func (c Code) ErrorCode() string {
	parts := strings.Split(c.value, ".")
	if len(parts) >= 3 {
		return parts[2]
	} else if len(parts) == 2 {
		return parts[1]
	}
	return c.value
}

// IsValid returns true if the code is properly formatted
func (c Code) IsValid() bool {
	return codeRegex.MatchString(c.value)
}

// Equals checks if two codes are equal
func (c Code) Equals(other Code) bool {
	return c.value == other.value
}
