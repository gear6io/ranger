package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ClientCancel represents a client cancel message for query cancellation
type ClientCancel struct {
	QueryID string
}

// Type returns the signal type
func (c *ClientCancel) Type() protocol.SignalType {
	return protocol.ClientCancel
}

// Pack serializes the cancel message to bytes
func (c *ClientCancel) Pack() ([]byte, error) {
	// Calculate total size
	size := c.Size()
	buf := make([]byte, 0, size)

	// Pack query ID (4 bytes length + string)
	queryIDBytes := []byte(c.QueryID)
	queryIDLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(queryIDLenBytes, uint32(len(queryIDBytes)))
	buf = append(buf, queryIDLenBytes...)
	buf = append(buf, queryIDBytes...)

	return buf, nil
}

// Unpack deserializes the cancel message from bytes
func (c *ClientCancel) Unpack(data []byte) error {
	if len(data) < 4 { // minimum: 4 bytes for query ID length
		return fmt.Errorf("insufficient data for client cancel")
	}

	pos := 0

	// Read query ID length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for query ID length")
	}
	queryIDLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read query ID
	if pos+int(queryIDLen) > len(data) {
		return fmt.Errorf("insufficient data for query ID")
	}
	c.QueryID = string(data[pos : pos+int(queryIDLen)])

	return nil
}

// Size returns the estimated size of the packed message
func (c *ClientCancel) Size() int {
	return 4 + len(c.QueryID) // 4 bytes for length + string length
}

// NewClientCancel creates a new client cancel message
func NewClientCancel(queryID string) *ClientCancel {
	return &ClientCancel{
		QueryID: queryID,
	}
}
