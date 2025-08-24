package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestClientCancel(t *testing.T) {
	// Test creating a new client cancel
	cancel := NewClientCancel("query123")

	// Test Type method
	if cancel.Type() != protocol.ClientCancel {
		t.Errorf("Expected Type() to return ClientCancel, got %d", cancel.Type())
	}

	// Test Size method
	expectedSize := 4 + len("query123")
	if cancel.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, cancel.Size())
	}

	// Test Pack method
	packed, err := cancel.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newCancel := &ClientCancel{}
	err = newCancel.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify QueryID was unpacked correctly
	if newCancel.QueryID != cancel.QueryID {
		t.Errorf("QueryID mismatch: expected %s, got %s", cancel.QueryID, newCancel.QueryID)
	}
}

func TestClientCancelUnpackEmpty(t *testing.T) {
	cancel := &ClientCancel{}
	err := cancel.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestClientCancelUnpackInsufficient(t *testing.T) {
	cancel := &ClientCancel{}
	err := cancel.Unpack([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
