package protocol

import (
	"testing"
)

// MockSignal implements the Signal interface for testing
type MockSignal struct {
	signalType SignalType
	data       []byte
}

func (m *MockSignal) Type() SignalType {
	return m.signalType
}

func (m *MockSignal) Pack() ([]byte, error) {
	return m.data, nil
}

func (m *MockSignal) Unpack(data []byte) error {
	m.data = data
	return nil
}

func (m *MockSignal) Size() int {
	return len(m.data)
}

func (m *MockSignal) Register(registry *Registry, info *SignalFactory) error {
	return nil
}

func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	// Test client signal registration
	clientSignal := &MockSignal{signalType: ClientHello, data: []byte("hello")}
	clientInfo := &SignalInfo{
		Type:      ClientHello,
		Direction: ClientToServer,
		Name:      "ClientHello",
		Version:   1,
	}

	err := registry.RegisterClientSignal(clientSignal, clientInfo)
	if err != nil {
		t.Fatalf("Failed to register client signal: %v", err)
	}

	// Test server signal registration
	serverSignal := &MockSignal{signalType: ServerHello, data: []byte("world")}
	serverInfo := &SignalInfo{
		Type:      ServerHello,
		Direction: ServerToClient,
		Name:      "ServerHello",
		Version:   1,
	}

	err = registry.RegisterServerSignal(serverSignal, serverInfo)
	if err != nil {
		t.Fatalf("Failed to register server signal: %v", err)
	}

	// Test retrieval
	retrievedClient, err := registry.GetClientSignal(ClientHello)
	if err != nil {
		t.Fatalf("Failed to get client signal: %v", err)
	}
	if retrievedClient.Type() != ClientHello {
		t.Errorf("Expected ClientHello, got %d", retrievedClient.Type())
	}

	retrievedServer, err := registry.GetServerSignal(ServerHello)
	if err != nil {
		t.Fatalf("Failed to get server signal: %v", err)
	}
	if retrievedServer.Type() != ServerHello {
		t.Errorf("Expected ServerHello, got %d", retrievedServer.Type())
	}

	// Test listing
	clientTypes := registry.ListClientSignals()
	if len(clientTypes) != 1 || clientTypes[0] != ClientHello {
		t.Errorf("Expected [ClientHello], got %v", clientTypes)
	}

	serverTypes := registry.ListServerSignals()
	if len(serverTypes) != 1 || serverTypes[0] != ServerHello {
		t.Errorf("Expected [ServerHello], got %v", serverTypes)
	}

	// Test info retrieval
	info, err := registry.GetSignalInfo(ClientHello)
	if err != nil {
		t.Fatalf("Failed to get signal info: %v", err)
	}
	if info.Name != "ClientHello" {
		t.Errorf("Expected 'ClientHello', got '%s'", info.Name)
	}

	// Test duplicate registration
	err = registry.RegisterClientSignal(clientSignal, clientInfo)
	if err == nil {
		t.Error("Expected error for duplicate registration")
	}

	// Test invalid signal type registration
	invalidSignal := &MockSignal{signalType: ServerHello, data: []byte("invalid")}
	err = registry.RegisterClientSignal(invalidSignal, clientInfo)
	if err == nil {
		t.Error("Expected error for registering server signal as client signal")
	}
}

func TestConstants(t *testing.T) {
	// Test signal type names
	if GetSignalName(ClientHello) != "ClientHello" {
		t.Errorf("Expected 'ClientHello', got '%s'", GetSignalName(ClientHello))
	}

	if GetSignalName(ServerHello) != "ServerHello" {
		t.Errorf("Expected 'ServerHello', got '%s'", GetSignalName(ServerHello))
	}

	// Test direction detection
	if !IsClientSignal(ClientHello) {
		t.Error("ClientHello should be a client signal")
	}

	if !IsServerSignal(ServerHello) {
		t.Error("ServerHello should be a server signal")
	}

	// Test unknown signal type
	if GetSignalName(255) != "Unknown" {
		t.Error("Unknown signal type should return 'Unknown'")
	}
}
