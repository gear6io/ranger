package signals

import (
	"testing"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

func TestServerException(t *testing.T) {
	// Test creating a new server exception
	exception := NewServerException(1001, "Database connection failed", "stack trace here")

	// Test Type method
	if exception.Type() != protocol.ServerException {
		t.Errorf("Expected Type() to return ServerException, got %d", exception.Type())
	}

	// Test Size method
	expectedSize := 8 + 8 + len("Database connection failed") + 8 + len("stack trace here")
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
		t.Errorf("ErrorCode mismatch: expected %d, got %d", originalErrorCode, newException.ErrorCode)
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
