package signals

import (
	"fmt"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

// ServerClose represents a server close notification message
type ServerClose struct {
	Reason string
}

// Type returns the signal type
func (s *ServerClose) Type() protocol.SignalType {
	return protocol.ServerClose
}

// Pack serializes the close message to bytes
func (s *ServerClose) Pack() ([]byte, error) {
	// Calculate total size
	size := s.Size()
	buf := make([]byte, 0, size)

	// Pack reason length (4 bytes, big endian)
	reasonBytes := []byte(s.Reason)
	reasonLength := uint32(len(reasonBytes))
	lengthBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(lengthBytes, reasonLength)
	buf = append(buf, lengthBytes...)

	// Pack reason string
	buf = append(buf, reasonBytes...)

	return buf, nil
}

// Unpack deserializes the close message from bytes
func (s *ServerClose) Unpack(data []byte) error {
	if len(data) < 4 { // minimum: 4 bytes for reason length
		return fmt.Errorf("insufficient data for server close")
	}

	// Read reason length (4 bytes, big endian)
	reasonLength := protocol.ReadUint32BigEndian(data[:4])

	// Validate data length
	if len(data) < int(4+reasonLength) {
		return fmt.Errorf("insufficient data for server close reason")
	}

	// Read reason string
	s.Reason = string(data[4 : 4+reasonLength])

	return nil
}

// Size returns the estimated size of the packed message
func (s *ServerClose) Size() int {
	return 4 + len(s.Reason) // 4 bytes for length + reason string
}

// NewServerClose creates a new server close message
func NewServerClose(reason string) *ServerClose {
	return &ServerClose{
		Reason: reason,
	}
}

// Register registers this signal type in both registry and factory
func (s *ServerClose) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(s, &protocol.SignalInfo{Name: "ServerClose"}); err != nil {
		return fmt.Errorf("failed to register ServerClose in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerClose, func() protocol.Signal {
		return &ServerClose{}
	})

	return nil
}
