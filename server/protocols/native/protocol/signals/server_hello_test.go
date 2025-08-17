package signals

import (
	"testing"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

func TestServerHello(t *testing.T) {
	// Test creating a new server hello
	hello := NewServerHello("Icebox Server", "UTC", "Icebox Database Server")

	// Test Type method
	if hello.Type() != protocol.ServerHello {
		t.Errorf("Expected Type() to return ServerHello, got %d", hello.Type())
	}

	// Test Size method
	expectedSize := 4 + len("Icebox Server") + 1 + 1 + 8 + 4 + len("UTC") + 4 + len("Icebox Database Server") + 8
	if hello.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, hello.Size())
	}

	// Store original values before packing (since Pack modifies the struct)
	originalRevision := hello.Revision

	// Test Pack method
	packed, err := hello.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newHello := &ServerHello{}
	err = newHello.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newHello.ServerName != hello.ServerName {
		t.Errorf("ServerName mismatch: expected %s, got %s", hello.ServerName, newHello.ServerName)
	}
	if newHello.MajorVersion != hello.MajorVersion {
		t.Errorf("MajorVersion mismatch: expected %d, got %d", hello.MajorVersion, newHello.MajorVersion)
	}
	if newHello.MinorVersion != hello.MinorVersion {
		t.Errorf("MinorVersion mismatch: expected %d, got %d", hello.MinorVersion, newHello.MinorVersion)
	}
	if newHello.Revision != originalRevision {
		t.Errorf("Revision mismatch: expected %d, got %d", originalRevision, newHello.Revision)
	}
	if newHello.Timezone != hello.Timezone {
		t.Errorf("Timezone mismatch: expected %s, got %s", hello.Timezone, newHello.Timezone)
	}
	if newHello.DisplayName != hello.DisplayName {
		t.Errorf("DisplayName mismatch: expected %s, got %s", hello.DisplayName, newHello.DisplayName)
	}
}

func TestServerHelloUnpackEmpty(t *testing.T) {
	hello := &ServerHello{}
	err := hello.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestServerHelloUnpackInsufficient(t *testing.T) {
	hello := &ServerHello{}
	err := hello.Unpack([]byte{1, 2, 3, 4, 5, 6, 7}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
