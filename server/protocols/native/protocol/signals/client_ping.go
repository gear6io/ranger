package signals

import (
	"fmt"
	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ClientPing represents a client ping message
type ClientPing struct {
	Timestamp int64
}

// Type returns the signal type
func (p *ClientPing) Type() protocol.SignalType {
	return protocol.ClientPing
}

// Pack serializes the ping message to bytes
func (p *ClientPing) Pack() ([]byte, error) {
	// Calculate total size
	size := p.Size()
	buf := make([]byte, 0, size)

	// Pack timestamp (8 bytes, big endian)
	timestampBytes := make([]byte, 8)
	protocol.WriteUint64BigEndian(timestampBytes, uint64(p.Timestamp))
	buf = append(buf, timestampBytes...)

	return buf, nil
}

// Unpack deserializes the ping message from bytes
func (p *ClientPing) Unpack(data []byte) error {
	if len(data) < 8 { // minimum: 8 bytes for timestamp
		return fmt.Errorf("insufficient data for client ping")
	}

	// Read timestamp (8 bytes, big endian)
	p.Timestamp = int64(protocol.ReadUint64BigEndian(data[:8]))

	return nil
}

// Size returns the estimated size of the packed message
func (p *ClientPing) Size() int {
	return 8 // 8 bytes for timestamp
}

// NewClientPing creates a new client ping message
func NewClientPing(timestamp int64) *ClientPing {
	return &ClientPing{
		Timestamp: timestamp,
	}
}
