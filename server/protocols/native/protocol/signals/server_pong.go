package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ServerPong represents a server pong response message
type ServerPong struct {
	Timestamp int64
}

// Type returns the signal type
func (p *ServerPong) Type() protocol.SignalType {
	return protocol.ServerPong
}

// Pack serializes the pong message to bytes
func (p *ServerPong) Pack() ([]byte, error) {
	// Calculate total size
	size := p.Size()
	buf := make([]byte, 0, size)

	// Pack timestamp (8 bytes, big endian)
	timestampBytes := make([]byte, 8)
	protocol.WriteUint64BigEndian(timestampBytes, uint64(p.Timestamp))
	buf = append(buf, timestampBytes...)

	return buf, nil
}

// Unpack deserializes the pong message from bytes
func (p *ServerPong) Unpack(data []byte) error {
	if len(data) < 8 { // minimum: 8 bytes for timestamp
		return fmt.Errorf("insufficient data for server pong")
	}

	// Read timestamp (8 bytes, big endian)
	p.Timestamp = int64(protocol.ReadUint64BigEndian(data[:8]))

	return nil
}

// Size returns the estimated size of the packed message
func (p *ServerPong) Size() int {
	return 8 // 8 bytes for timestamp
}

// NewServerPong creates a new server pong message
func NewServerPong(timestamp int64) *ServerPong {
	return &ServerPong{
		Timestamp: timestamp,
	}
}

// Register registers this signal type in both registry and factory
func (p *ServerPong) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(p, &protocol.SignalInfo{Name: "ServerPong"}); err != nil {
		return fmt.Errorf("failed to register ServerPong in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerPong, func() protocol.Signal {
		return &ServerPong{}
	})

	return nil
}
