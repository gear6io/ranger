package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
)

// DefaultCodec implements the Codec interface for the unified protocol
type DefaultCodec struct {
	registry *Registry
	factory  *SignalFactory
}

// NewDefaultCodec creates a new default codec
func NewDefaultCodec(registry *Registry, factory *SignalFactory) *DefaultCodec {
	return &DefaultCodec{
		registry: registry,
		factory:  factory,
	}
}

// EncodeMessage encodes a signal into a complete message
func (c *DefaultCodec) EncodeMessage(signal Signal) (*Message, error) {
	payload, err := signal.Pack()
	if err != nil {
		return nil, fmt.Errorf("failed to pack signal: %w", err)
	}

	// Message format: [4 bytes length][1 byte type][payload]
	messageLength := uint32(1 + len(payload)) // 1 byte for type + payload

	return &Message{
		Length:  messageLength,
		Type:    signal.Type(),
		Payload: payload,
	}, nil
}

// DecodeMessage decodes a complete message from bytes
func (c *DefaultCodec) DecodeMessage(data []byte) (*Message, error) {
	if len(data) < 5 { // minimum: 4 bytes length + 1 byte type
		return nil, fmt.Errorf("message too short: expected at least 5 bytes, got %d", len(data))
	}

	// Read message length (4 bytes, big endian)
	messageLength := binary.BigEndian.Uint32(data[:4])

	// Read message type (1 byte)
	messageType := SignalType(data[4])

	// Extract payload
	var payload []byte
	if len(data) > 5 {
		payload = data[5:]
	}

	// Validate payload length
	if uint32(len(payload)) != messageLength-1 {
		return nil, fmt.Errorf("payload length mismatch: expected %d, got %d", messageLength-1, len(payload))
	}

	return &Message{
		Length:  messageLength,
		Type:    messageType,
		Payload: payload,
	}, nil
}

// ReadMessage reads a complete message from an io.Reader
func (c *DefaultCodec) ReadMessage(reader io.Reader) (*Message, error) {
	// Read message length (4 bytes, big endian)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBuf); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	messageLength := binary.BigEndian.Uint32(lengthBuf)

	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(reader, typeBuf); err != nil {
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}
	messageType := SignalType(typeBuf[0])

	// Read payload
	var payload []byte
	if messageLength > 1 {
		payload = make([]byte, messageLength-1)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("failed to read message payload: %w", err)
		}
	}

	return &Message{
		Length:  messageLength,
		Type:    messageType,
		Payload: payload,
	}, nil
}

// WriteMessage writes a complete message to an io.Writer
func (c *DefaultCodec) WriteMessage(writer io.Writer, message *Message) error {
	// Write message length (4 bytes, big endian)
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, message.Length)
	if _, err := writer.Write(lengthBuf); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message type (1 byte)
	if _, err := writer.Write([]byte{byte(message.Type)}); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// Write payload if present
	if len(message.Payload) > 0 {
		if _, err := writer.Write(message.Payload); err != nil {
			return fmt.Errorf("failed to write message payload: %w", err)
		}
	}

	return nil
}

// UnpackSignal unpacks a message into a signal using the registry
func (c *DefaultCodec) UnpackSignal(message *Message) (Signal, error) {
	// Determine if this is a client or server signal
	if IsClientSignal(message.Type) {
		// Server receives client signal, so it needs to unpack it
		signal, err := c.registry.GetClientSignal(message.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to get client signal for type %d: %w", message.Type, err)
		}

		// Create a new instance and unpack
		newSignal, err := c.createSignalInstance(signal)
		if err != nil {
			return nil, fmt.Errorf("failed to create signal instance: %w", err)
		}

		if err := newSignal.Unpack(message.Payload); err != nil {
			return nil, fmt.Errorf("failed to unpack client signal: %w", err)
		}

		return newSignal, nil
	} else if IsServerSignal(message.Type) {
		// Client receives server signal, so it needs to unpack it
		signal, err := c.registry.GetServerSignal(message.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to get server signal for type %d: %w", message.Type, err)
		}

		// Create a new instance and unpack
		newSignal, err := c.createSignalInstance(signal)
		if err != nil {
			return nil, fmt.Errorf("failed to create signal instance: %w", err)
		}

		if err := newSignal.Unpack(message.Payload); err != nil {
			return nil, fmt.Errorf("failed to unpack server signal: %w", err)
		}

		return newSignal, nil
	}

	return nil, fmt.Errorf("unknown signal type: %d", message.Type)
}

// createSignalInstance creates a new instance of a signal
func (c *DefaultCodec) createSignalInstance(signal Signal) (Signal, error) {
	if c.factory != nil {
		return c.factory.CreateSignalFromInstance(signal)
	}

	// Fallback to reflection-based creation
	signalType := reflect.TypeOf(signal)
	if signalType.Kind() == reflect.Ptr {
		signalType = signalType.Elem()
	}

	newInstance := reflect.New(signalType)
	if newSignal, ok := newInstance.Interface().(Signal); ok {
		return newSignal, nil
	}

	return nil, fmt.Errorf("failed to create signal instance")
}
