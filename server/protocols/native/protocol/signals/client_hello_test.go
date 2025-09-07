package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestClientHello(t *testing.T) {
	// Test creating a new client hello
	hello := NewClientHello("TestClient", "testdb", "testuser", "testpass", 0, 0)

	// Test Type method
	if hello.Type() != protocol.ClientHello {
		t.Errorf("Expected Type() to return ClientHello, got %d", hello.Type())
	}

	// Test Size method
	expectedSize := 1 + len("TestClient") + 1 + len("testdb") + 1 + len("testuser") + 1 + len("testpass") + 20
	if hello.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, hello.Size())
	}

	// Store original values before packing (since Pack modifies the struct)
	originalMajorVersion := hello.MajorVersion
	originalMinorVersion := hello.MinorVersion
	originalProtocolVersion := hello.ProtocolVersion

	// Test Pack method
	packed, err := hello.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newHello := &ClientHello{}
	err = newHello.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newHello.ClientName != hello.ClientName {
		t.Errorf("ClientName mismatch: expected %s, got %s", hello.ClientName, newHello.ClientName)
	}
	if newHello.MajorVersion != originalMajorVersion {
		t.Errorf("MajorVersion mismatch: expected %d, got %d", originalMajorVersion, newHello.MajorVersion)
	}
	if newHello.MinorVersion != originalMinorVersion {
		t.Errorf("MinorVersion mismatch: expected %d, got %d", originalMinorVersion, newHello.MinorVersion)
	}
	if newHello.ProtocolVersion != originalProtocolVersion {
		t.Errorf("ProtocolVersion mismatch: expected %d, got %d", originalProtocolVersion, newHello.ProtocolVersion)
	}
	if newHello.Database != hello.Database {
		t.Errorf("Database mismatch: expected %s, got %s", hello.Database, newHello.Database)
	}
	if newHello.User != hello.User {
		t.Errorf("User mismatch: expected %s, got %s", hello.User, newHello.User)
	}
	if newHello.Password != hello.Password {
		t.Errorf("Password mismatch: expected %s, got %s", hello.Password, newHello.Password)
	}
}

func TestClientHelloUnpackEmpty(t *testing.T) {
	hello := &ClientHello{}
	err := hello.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestClientHelloUnpackInsufficient(t *testing.T) {
	hello := &ClientHello{}
	err := hello.Unpack([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
