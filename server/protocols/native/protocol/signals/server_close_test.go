package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestServerClose(t *testing.T) {
	// Test creating a new server close
	reason := "Connection idle timeout"
	close := NewServerClose(reason)

	// Test Type()
	if close.Type() != protocol.ServerClose {
		t.Errorf("Expected Type() to return ServerClose, got %d", close.Type())
	}

	// Test Size()
	expectedSize := 4 + len(reason) // 4 bytes for length + reason string
	if close.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, close.Size())
	}

	// Test Pack()
	packed, err := close.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) != expectedSize {
		t.Errorf("Expected packed size %d, got %d", expectedSize, len(packed))
	}

	// Test Unpack()
	newClose := &ServerClose{}
	err = newClose.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	if newClose.Reason != close.Reason {
		t.Errorf("Reason mismatch: expected %s, got %s", close.Reason, newClose.Reason)
	}
}

func TestServerCloseUnpackEmpty(t *testing.T) {
	close := &ServerClose{}
	err := close.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestServerCloseUnpackInsufficient(t *testing.T) {
	close := &ServerClose{}
	err := close.Unpack([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("Expected error for insufficient data")
	}
}

func TestServerCloseEmptyReason(t *testing.T) {
	// Test with empty reason
	close := NewServerClose("")

	packed, err := close.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	newClose := &ServerClose{}
	err = newClose.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	if newClose.Reason != "" {
		t.Errorf("Expected empty reason, got %s", newClose.Reason)
	}
}
