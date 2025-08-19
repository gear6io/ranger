package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ClientHello represents a client hello message
type ClientHello struct {
	ClientName      string
	MajorVersion    uint64
	MinorVersion    uint64
	ProtocolVersion uint64
	Database        string
	User            string
	Password        string
}

// Type returns the signal type
func (h *ClientHello) Type() protocol.SignalType {
	return protocol.ClientHello
}

// Pack serializes the hello message to bytes
func (h *ClientHello) Pack() ([]byte, error) {
	// Calculate total size
	size := h.Size()
	buf := make([]byte, 0, size)

	// Pack client name
	nameBytes := []byte(h.ClientName)
	buf = append(buf, byte(len(nameBytes)))
	buf = append(buf, nameBytes...)

	// Pack major version (varint)
	for h.MajorVersion >= 0x80 {
		buf = append(buf, byte(h.MajorVersion)|0x80)
		h.MajorVersion >>= 7
	}
	buf = append(buf, byte(h.MajorVersion))

	// Pack minor version (varint)
	for h.MinorVersion >= 0x80 {
		buf = append(buf, byte(h.MinorVersion)|0x80)
		h.MinorVersion >>= 7
	}
	buf = append(buf, byte(h.MinorVersion))

	// Pack protocol version (varint)
	for h.ProtocolVersion >= 0x80 {
		buf = append(buf, byte(h.ProtocolVersion)|0x80)
		h.ProtocolVersion >>= 7
	}
	buf = append(buf, byte(h.ProtocolVersion))

	// Pack database
	dbBytes := []byte(h.Database)
	buf = append(buf, byte(len(dbBytes)))
	buf = append(buf, dbBytes...)

	// Pack user
	userBytes := []byte(h.User)
	buf = append(buf, byte(len(userBytes)))
	buf = append(buf, userBytes...)

	// Pack password
	pwdBytes := []byte(h.Password)
	buf = append(buf, byte(len(pwdBytes)))
	buf = append(buf, pwdBytes...)

	return buf, nil
}

// Unpack deserializes the hello message from bytes
func (h *ClientHello) Unpack(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty hello message")
	}

	pos := 0

	// Read client name
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for client name length")
	}
	nameLen := int(data[pos])
	pos++

	if pos+nameLen > len(data) {
		return fmt.Errorf("insufficient data for client name")
	}
	h.ClientName = string(data[pos : pos+nameLen])
	pos += nameLen

	// Read major version (varint)
	major, bytesRead := h.readVarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read major version")
	}
	h.MajorVersion = major
	pos += bytesRead

	// Read minor version (varint)
	minor, bytesRead := h.readVarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read minor version")
	}
	h.MinorVersion = minor
	pos += bytesRead

	// Read protocol version (varint)
	protocol, bytesRead := h.readVarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read protocol version")
	}
	h.ProtocolVersion = protocol
	pos += bytesRead

	// Read database
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for database length")
	}
	dbLen := int(data[pos])
	pos++

	if pos+dbLen > len(data) {
		return fmt.Errorf("insufficient data for database")
	}
	h.Database = string(data[pos : pos+dbLen])
	pos += dbLen

	// Read user
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for user length")
	}
	userLen := int(data[pos])
	pos++

	if pos+userLen > len(data) {
		return fmt.Errorf("insufficient data for user")
	}
	h.User = string(data[pos : pos+userLen])
	pos += userLen

	// Read password
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for password length")
	}
	pwdLen := int(data[pos])
	pos++

	if pos+pwdLen > len(data) {
		return fmt.Errorf("insufficient data for password")
	}
	h.Password = string(data[pos : pos+pwdLen])

	return nil
}

// Size returns the estimated size of the packed message
func (h *ClientHello) Size() int {
	// Rough estimate: 1 byte per length + string lengths + varint sizes
	return 1 + len(h.ClientName) + 1 + len(h.Database) + 1 + len(h.User) + 1 + len(h.Password) + 12 // 12 for varints
}

// readVarint reads a variable-length integer from the beginning of data
func (h *ClientHello) readVarint(data []byte) (uint64, int) {
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

// NewClientHello creates a new hello message
func NewClientHello(clientName, database, user, password string) *ClientHello {
	return &ClientHello{
		ClientName:      clientName,
		MajorVersion:    1,
		MinorVersion:    0,
		ProtocolVersion: protocol.DBMS_TCP_PROTOCOL_VERSION,
		Database:        database,
		User:            user,
		Password:        password,
	}
}

// Register registers this signal type in both registry and factory
func (h *ClientHello) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterClientSignal(h, &protocol.SignalInfo{Name: "ClientHello"}); err != nil {
		return fmt.Errorf("failed to register ClientHello in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ClientHello, func() protocol.Signal {
		return &ClientHello{}
	})

	return nil
}
