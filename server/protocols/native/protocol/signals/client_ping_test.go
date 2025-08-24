package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestClientPing(t *testing.T) {
	// Test creating a new client ping
	timestamp := int64(1234567890)
	ping := NewClientPing(timestamp)

	// Test Type method
	if ping.Type() != protocol.ClientPing {
		t.Errorf("Expected Type() to return ClientPing, got %d", ping.Type())
	}

	// Test Size method
	expectedSize := 8 // 8 bytes for timestamp
	if ping.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, ping.Size())
	}

	// Test Pack method
	packed, err := ping.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) != 8 {
		t.Errorf("Expected packed data to be 8 bytes, got %d", len(packed))
	}

	// Test Unpack method
	newPing := &ClientPing{}
	err = newPing.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify timestamp was unpacked correctly
	if newPing.Timestamp != ping.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", ping.Timestamp, newPing.Timestamp)
	}
}

func TestClientPingUnpackEmpty(t *testing.T) {
	ping := &ClientPing{}
	err := ping.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestClientPingUnpackInsufficient(t *testing.T) {
	ping := &ClientPing{}
	err := ping.Unpack([]byte{1, 2, 3, 4, 5, 6, 7}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
