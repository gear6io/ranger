package errors

import (
	"testing"
)

func TestNewCode(t *testing.T) {
	// Test valid codes
	validCodes := []string{
		"filesystem.table_not_found",
		"memory.alloc_failed",
		"query.timeout",
		"storage.connection_failed",
		"api.rate_limit_exceeded",
	}

	for _, codeStr := range validCodes {
		code, err := NewCode(codeStr)
		if err != nil {
			t.Errorf("Expected valid code '%s' to succeed, got error: %v", codeStr, err)
		}
		if code.String() != codeStr {
			t.Errorf("Expected code string '%s', got '%s'", codeStr, code.String())
		}
	}

	// Test invalid codes
	invalidCodes := []string{
		"invalid",                     // No dot
		"filesystem.",                 // Ends with dot
		".table_not_found",            // Starts with dot
		"FileSystem.table_not_found",  // Uppercase
		"filesystem.table-not-found",  // Hyphens not allowed
		"filesystem.table_not_found.", // Ends with dot
		"filesystem..table_not_found", // Double dot
		"error.table_not_found",       // Contains "error"
		"err.table_not_found",         // Contains "err"
	}

	for _, codeStr := range invalidCodes {
		_, err := NewCode(codeStr)
		if err == nil {
			t.Errorf("Expected invalid code '%s' to fail, but it succeeded", codeStr)
		}
	}
}

func TestMustNewCode(t *testing.T) {
	// Test valid code
	code := MustNewCode("filesystem.table_not_found")
	if code.String() != "filesystem.table_not_found" {
		t.Errorf("Expected code 'filesystem.table_not_found', got '%s'", code.String())
	}

	// Test that it panics with invalid code
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MustNewCode to panic with invalid code")
		}
	}()
	MustNewCode("invalid")
}

func TestCodePackageAndName(t *testing.T) {
	code := MustNewCode("filesystem.table_not_found")

	if code.Package() != "filesystem" {
		t.Errorf("Expected package 'filesystem', got '%s'", code.Package())
	}

	if code.Name() != "table_not_found" {
		t.Errorf("Expected name 'table_not_found', got '%s'", code.Name())
	}
}

func TestCodeIsValid(t *testing.T) {
	validCode := MustNewCode("filesystem.table_not_found")
	if !validCode.IsValid() {
		t.Error("Expected valid code to return true for IsValid()")
	}

	// Create an invalid code by directly setting the value
	invalidCode := Code{value: "invalid"}
	if invalidCode.IsValid() {
		t.Error("Expected invalid code to return false for IsValid()")
	}
}

func TestCodeEquals(t *testing.T) {
	code1 := MustNewCode("filesystem.table_not_found")
	code2 := MustNewCode("filesystem.table_not_found")
	code3 := MustNewCode("memory.alloc_failed")

	if !code1.Equals(code2) {
		t.Error("Expected identical codes to be equal")
	}

	if code1.Equals(code3) {
		t.Error("Expected different codes to not be equal")
	}
}

func TestPackageSpecificCodeConstructors(t *testing.T) {
	// Test filesystem codes
	fsCode := FilesystemCode("table_not_found")
	if fsCode.String() != "filesystem.table_not_found" {
		t.Errorf("Expected 'filesystem.table_not_found', got '%s'", fsCode.String())
	}

	// Test memory codes
	memCode := MemoryCode("alloc_failed")
	if memCode.String() != "memory.alloc_failed" {
		t.Errorf("Expected 'memory.alloc_failed', got '%s'", memCode.String())
	}

	// Test query codes
	queryCode := QueryCode("timeout")
	if queryCode.String() != "query.timeout" {
		t.Errorf("Expected 'query.timeout', got '%s'", queryCode.String())
	}

	// Test storage codes
	storageCode := StorageCode("connection_failed")
	if storageCode.String() != "storage.connection_failed" {
		t.Errorf("Expected 'storage.connection_failed', got '%s'", storageCode.String())
	}

	// Test network codes
	networkCode := NetworkCode("timeout")
	if networkCode.String() != "network.timeout" {
		t.Errorf("Expected 'network.timeout', got '%s'", networkCode.String())
	}

	// Test auth codes
	authCode := AuthCode("unauthorized")
	if authCode.String() != "auth.unauthorized" {
		t.Errorf("Expected 'auth.unauthorized', got '%s'", authCode.String())
	}

	// Test database codes
	dbCode := DatabaseCode("connection_failed")
	if dbCode.String() != "database.connection_failed" {
		t.Errorf("Expected 'database.connection_failed', got '%s'", dbCode.String())
	}

	// Test API codes
	apiCode := APICode("rate_limit_exceeded")
	if apiCode.String() != "api.rate_limit_exceeded" {
		t.Errorf("Expected 'api.rate_limit_exceeded', got '%s'", apiCode.String())
	}
}

func TestPackageCode(t *testing.T) {
	// Test custom package code
	customCode := PackageCode("custom_package", "specific_failure")
	if customCode.String() != "custom_package.specific_failure" {
		t.Errorf("Expected 'custom_package.specific_failure', got '%s'", customCode.String())
	}

	// Test that it validates the format
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected PackageCode to panic with invalid format")
		}
	}()
	PackageCode("InvalidPackage", "error")
}

func TestCommonCodes(t *testing.T) {
	// Test that common codes are properly formatted
	commonCodes := []Code{
		CommonInternal,
		CommonNotFound,
		CommonValidation,
		CommonTimeout,
		CommonUnauthorized,
		CommonForbidden,
		CommonConflict,
		CommonUnsupported,
		CommonInvalidInput,
		CommonAlreadyExists,
	}

	for _, code := range commonCodes {
		if !code.IsValid() {
			t.Errorf("Common code '%s' is not valid", code.String())
		}

		if code.Package() != "common" {
			t.Errorf("Expected package 'common' for '%s', got '%s'", code.String(), code.Package())
		}
	}
}
