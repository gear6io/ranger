package protocol

import (
	"io"
)

// SignalType represents the type of a protocol signal
type SignalType byte

// Signal represents a protocol message that can be packed and unpacked
type Signal interface {
	// Type returns the signal type identifier
	Type() SignalType

	// Pack serializes the signal to bytes for transmission
	Pack() ([]byte, error)

	// Unpack deserializes the signal from bytes
	Unpack(data []byte) error

	// Size returns the size of the packed signal (for pre-allocation)
	Size() int

	// Register registers this signal type in both registry and factory
	Register(registry *Registry, factory *SignalFactory) error
}

// Message represents a complete protocol message with length and type
type Message struct {
	Length  uint32
	Type    SignalType
	Payload []byte
}

// Codec handles encoding and decoding of protocol messages
type Codec interface {
	// EncodeMessage encodes a signal into a complete message
	EncodeMessage(signal Signal) (*Message, error)

	// DecodeMessage decodes a complete message from bytes
	DecodeMessage(data []byte) (*Message, error)

	// ReadMessage reads a complete message from an io.Reader
	ReadMessage(reader io.Reader) (*Message, error)

	// WriteMessage writes a complete message to an io.Writer
	WriteMessage(writer io.Writer, message *Message) error
}

// Direction indicates whether a signal is sent from client to server or vice versa
type Direction int

const (
	ClientToServer Direction = iota
	ServerToClient
)

// SignalInfo provides metadata about a signal type
type SignalInfo struct {
	Type      SignalType
	Direction Direction
	Name      string
	Version   int
}
