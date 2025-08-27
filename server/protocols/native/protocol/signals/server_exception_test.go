package signals

import (
	"fmt"
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestServerException(t *testing.T) {
	// Test creating a new server exception
	exception := NewServerException("query.unsupported_statement_type", "Database connection failed", "stack trace here")

	// Test Type method
	if exception.Type() != protocol.ServerException {
		t.Errorf("Expected Type() to return ServerException, got %d", exception.Type())
	}

	// Test Size method
	expectedSize := 8 + len("query.unsupported_statement_type") + 8 + len("Database connection failed") + 8 + len("stack trace here")
	if exception.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, exception.Size())
	}

	// Store original values before packing (since Pack modifies the struct)
	originalErrorCode := exception.ErrorCode

	// Test Pack method
	packed, err := exception.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newException := &ServerException{}
	err = newException.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newException.ErrorCode != originalErrorCode {
		t.Errorf("ErrorCode mismatch: expected %s, got %s", originalErrorCode, newException.ErrorCode)
	}
	if newException.ErrorMessage != exception.ErrorMessage {
		t.Errorf("ErrorMessage mismatch: expected %s, got %s", exception.ErrorMessage, newException.ErrorMessage)
	}
	if newException.StackTrace != exception.StackTrace {
		t.Errorf("StackTrace mismatch: expected %s, got %s", exception.StackTrace, newException.StackTrace)
	}
}

func TestServerExceptionUnpackEmpty(t *testing.T) {
	exception := &ServerException{}
	err := exception.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestServerExceptionUnpackInsufficient(t *testing.T) {
	exception := &ServerException{}
	err := exception.Unpack([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}

func TestServerExceptionStringErrorCodes(t *testing.T) {
	// Test with internal error codes
	testCases := []struct {
		errorCode    string
		errorMessage string
		stackTrace   string
	}{
		{
			errorCode:    "query.unsupported_statement_type",
			errorMessage: "DROP TABLE statement not supported",
			stackTrace:   "stack trace here",
		},
		{
			errorCode:    "query.table_not_found",
			errorMessage: "Table 'users' does not exist",
			stackTrace:   "stack trace here",
		},
		{
			errorCode:    "storage.table_drop_failed",
			errorMessage: "Failed to drop table",
			stackTrace:   "stack trace here",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case_%d_%s", i, tc.errorCode), func(t *testing.T) {
			// Create exception with string error code
			exception := NewServerException(tc.errorCode, tc.errorMessage, tc.stackTrace)

			// Pack the exception
			packed, err := exception.Pack()
			if err != nil {
				t.Fatalf("Pack() failed: %v", err)
			}

			// Unpack the exception
			newException := &ServerException{}
			err = newException.Unpack(packed)
			if err != nil {
				t.Fatalf("Unpack() failed: %v", err)
			}

			// Verify all fields match
			if newException.ErrorCode != tc.errorCode {
				t.Errorf("ErrorCode mismatch: expected %s, got %s", tc.errorCode, newException.ErrorCode)
			}
			if newException.ErrorMessage != tc.errorMessage {
				t.Errorf("ErrorMessage mismatch: expected %s, got %s", tc.errorMessage, newException.ErrorMessage)
			}
			if newException.StackTrace != tc.stackTrace {
				t.Errorf("StackTrace mismatch: expected %s, got %s", tc.stackTrace, newException.StackTrace)
			}

			t.Logf("✅ Successfully packed/unpacked error code: %s", tc.errorCode)
		})
	}
}
