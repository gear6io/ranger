package signals

import (
	"fmt"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

// ServerException represents a server exception/error message
type ServerException struct {
	ErrorCode    string
	ErrorMessage string
	StackTrace   string
}

// Type returns the signal type
func (e *ServerException) Type() protocol.SignalType {
	return protocol.ServerException
}

// Pack serializes the exception message to bytes
func (e *ServerException) Pack() ([]byte, error) {
	// Calculate total size
	size := e.Size()
	buf := make([]byte, 0, size)

	// Pack error code (uvarint length + string)
	codeBytes := []byte(e.ErrorCode)
	codeLen := uint64(len(codeBytes))
	for codeLen >= 0x80 {
		buf = append(buf, byte(codeLen)|0x80)
		codeLen >>= 7
	}
	buf = append(buf, byte(codeLen))
	buf = append(buf, codeBytes...)

	// Pack error message (uvarint length + string)
	msgBytes := []byte(e.ErrorMessage)
	msgLen := uint64(len(msgBytes))
	for msgLen >= 0x80 {
		buf = append(buf, byte(msgLen)|0x80)
		msgLen >>= 7
	}
	buf = append(buf, byte(msgLen))
	buf = append(buf, msgBytes...)

	// Pack stack trace (uvarint length + string)
	stackBytes := []byte(e.StackTrace)
	stackLen := uint64(len(stackBytes))
	for stackLen >= 0x80 {
		buf = append(buf, byte(stackLen)|0x80)
		stackLen >>= 7
	}
	buf = append(buf, byte(stackLen))
	buf = append(buf, stackBytes...)

	return buf, nil
}

// Unpack deserializes the exception message from bytes
func (e *ServerException) Unpack(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty server exception message")
	}

	pos := 0

	// Read error code length (uvarint)
	codeLen, bytesRead := e.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read error code length")
	}
	pos += bytesRead

	// Read error code
	if pos+int(codeLen) > len(data) {
		return fmt.Errorf("insufficient data for error code")
	}
	e.ErrorCode = string(data[pos : pos+int(codeLen)])
	pos += int(codeLen)

	// Read error message length (uvarint)
	msgLen, bytesRead := e.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read error message length")
	}
	pos += bytesRead

	// Read error message
	if pos+int(msgLen) > len(data) {
		return fmt.Errorf("insufficient data for error message")
	}
	e.ErrorMessage = string(data[pos : pos+int(msgLen)])
	pos += int(msgLen)

	// Read stack trace length (uvarint)
	stackLen, bytesRead := e.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read stack trace length")
	}
	pos += bytesRead

	// Read stack trace
	if pos+int(stackLen) > len(data) {
		return fmt.Errorf("insufficient data for stack trace")
	}
	e.StackTrace = string(data[pos : pos+int(stackLen)])

	return nil
}

// Size returns the estimated size of the packed message
func (e *ServerException) Size() int {
	// Error code (uvarint length + string) + error message (uvarint length + string) + stack trace (uvarint length + string)
	return 8 + len(e.ErrorCode) + 8 + len(e.ErrorMessage) + 8 + len(e.StackTrace)
}

// readUvarint reads a variable-length integer from the beginning of data
func (e *ServerException) readUvarint(data []byte) (uint64, int) {
	var value uint64
	var shift uint

	for i, b := range data {
		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return value, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, 0 // overflow
		}
	}

	return 0, 0 // incomplete
}

// NewServerException creates a new server exception message
func NewServerException(errorCode string, errorMessage, stackTrace string) *ServerException {
	return &ServerException{
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
		StackTrace:   stackTrace,
	}
}

// Register registers this signal type in both registry and factory
func (e *ServerException) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(e, &protocol.SignalInfo{Name: "ServerException"}); err != nil {
		return fmt.Errorf("failed to register ServerException in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerException, func() protocol.Signal {
		return &ServerException{}
	})

	return nil
}
