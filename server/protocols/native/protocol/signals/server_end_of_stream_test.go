package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestServerEndOfStream(t *testing.T) {
	// Test creating a new server end-of-stream
	eos := NewServerEndOfStream()

	// Test Type method
	if eos.Type() != protocol.ServerEndOfStream {
		t.Errorf("Expected Type() to return ServerEndOfStream, got %d", eos.Type())
	}

	// Test Size method
	expectedSize := 0 // No payload
	if eos.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, eos.Size())
	}

	// Test Pack method
	packed, err := eos.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) != 0 {
		t.Errorf("Expected packed data to be 0 bytes, got %d", len(packed))
	}

	// Test Unpack method
	newEos := &ServerEndOfStream{}
	err = newEos.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// EndOfStream has no fields to verify, just ensure no error occurred
}

func TestServerEndOfStreamUnpackWithPayload(t *testing.T) {
	eos := &ServerEndOfStream{}
	// Test unpacking with non-empty payload (should fail)
	err := eos.Unpack([]byte{1, 2, 3, 4})
	if err == nil {
		t.Error("Expected error when unpacking with payload")
	}
}
