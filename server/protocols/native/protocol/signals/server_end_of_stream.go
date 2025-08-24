package signals

import (
	"fmt"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

// ServerEndOfStream represents a server end-of-stream message
type ServerEndOfStream struct {
	// This signal has no payload, it just indicates the end of a data stream
}

// Type returns the signal type
func (e *ServerEndOfStream) Type() protocol.SignalType {
	return protocol.ServerEndOfStream
}

// Pack serializes the end-of-stream message to bytes
func (e *ServerEndOfStream) Pack() ([]byte, error) {
	// End-of-stream has no payload
	return []byte{}, nil
}

// Unpack deserializes the end-of-stream message from bytes
func (e *ServerEndOfStream) Unpack(data []byte) error {
	// End-of-stream has no payload, so we don't need to unpack anything
	// But we can validate that the data is empty
	if len(data) != 0 {
		return fmt.Errorf("end-of-stream message should have no payload, got %d bytes", len(data))
	}
	return nil
}

// Size returns the estimated size of the packed message
func (e *ServerEndOfStream) Size() int {
	return 0 // No payload
}

// NewServerEndOfStream creates a new server end-of-stream message
func NewServerEndOfStream() *ServerEndOfStream {
	return &ServerEndOfStream{}
}

// Register registers this signal type in both registry and factory
func (e *ServerEndOfStream) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(e, &protocol.SignalInfo{Name: "ServerEndOfStream"}); err != nil {
		return fmt.Errorf("failed to register ServerEndOfStream in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerEndOfStream, func() protocol.Signal {
		return &ServerEndOfStream{}
	})

	return nil
}
