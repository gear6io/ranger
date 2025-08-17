package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
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
