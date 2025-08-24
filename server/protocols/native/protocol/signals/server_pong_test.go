package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestServerPong(t *testing.T) {
	// Test creating a new server pong
	timestamp := int64(1234567890)
	pong := NewServerPong(timestamp)

	// Test Type method
	if pong.Type() != protocol.ServerPong {
		t.Errorf("Expected Type() to return ServerPong, got %d", pong.Type())
	}

	// Test Size method
	expectedSize := 8 // 8 bytes for timestamp
	if pong.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, pong.Size())
	}

	// Test Pack method
	packed, err := pong.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) != 8 {
		t.Errorf("Expected packed data to be 8 bytes, got %d", len(packed))
	}

	// Test Unpack method
	newPong := &ServerPong{}
	err = newPong.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify timestamp was unpacked correctly
	if newPong.Timestamp != pong.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", pong.Timestamp, newPong.Timestamp)
	}
}

func TestServerPongUnpackEmpty(t *testing.T) {
	pong := &ServerPong{}
	err := pong.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestServerPongUnpackInsufficient(t *testing.T) {
	pong := &ServerPong{}
	err := pong.Unpack([]byte{1, 2, 3, 4, 5, 6, 7}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
