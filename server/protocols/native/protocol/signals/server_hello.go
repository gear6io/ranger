package signals

import (
	"fmt"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

// ServerHello represents a server hello response message
type ServerHello struct {
	ServerName   string
	MajorVersion uint64
	MinorVersion uint64
	Revision     uint64
	Timezone     string
	DisplayName  string
	VersionPatch uint64
}

// Type returns the signal type
func (h *ServerHello) Type() protocol.SignalType {
	return protocol.ServerHello
}

// Pack serializes the hello message to bytes
func (h *ServerHello) Pack() ([]byte, error) {
	// Calculate total size
	size := h.Size()
	buf := make([]byte, 0, size)

	// Pack server name (4 bytes length + string)
	nameBytes := []byte(h.ServerName)
	nameLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(nameLenBytes, uint32(len(nameBytes)))
	buf = append(buf, nameLenBytes...)
	buf = append(buf, nameBytes...)

	// Pack major version (1 byte)
	buf = append(buf, byte(h.MajorVersion))

	// Pack minor version (1 byte)
	buf = append(buf, byte(h.MinorVersion))

	// Pack revision (varint)
	for h.Revision >= 0x80 {
		buf = append(buf, byte(h.Revision)|0x80)
		h.Revision >>= 7
	}
	buf = append(buf, byte(h.Revision))

	// Pack timezone (4 bytes length + string)
	tzBytes := []byte(h.Timezone)
	tzLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(tzLenBytes, uint32(len(tzBytes)))
	buf = append(buf, tzLenBytes...)
	buf = append(buf, tzBytes...)

	// Pack display name (4 bytes length + string)
	displayBytes := []byte(h.DisplayName)
	displayLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(displayLenBytes, uint32(len(displayBytes)))
	buf = append(buf, displayLenBytes...)
	buf = append(buf, displayBytes...)

	// Pack version patch (varint)
	for h.VersionPatch >= 0x80 {
		buf = append(buf, byte(h.VersionPatch)|0x80)
		h.VersionPatch >>= 7
	}
	buf = append(buf, byte(h.VersionPatch))

	return buf, nil
}

// Unpack deserializes the hello message from bytes
func (h *ServerHello) Unpack(data []byte) error {
	if len(data) < 8 { // minimum: 4 bytes name length + 1 byte major + 1 byte minor + 1 byte revision
		return fmt.Errorf("insufficient data for server hello")
	}

	pos := 0

	// Read server name length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for server name length")
	}
	nameLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read server name
	if pos+int(nameLen) > len(data) {
		return fmt.Errorf("insufficient data for server name")
	}
	h.ServerName = string(data[pos : pos+int(nameLen)])
	pos += int(nameLen)

	// Read major version (1 byte)
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for major version")
	}
	h.MajorVersion = uint64(data[pos])
	pos++

	// Read minor version (1 byte)
	if pos >= len(data) {
		return fmt.Errorf("insufficient data for minor version")
	}
	h.MinorVersion = uint64(data[pos])
	pos++

	// Read revision (varint)
	revision, bytesRead := h.readVarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read revision")
	}
	h.Revision = revision
	pos += bytesRead

	// Read timezone length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for timezone length")
	}
	tzLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read timezone
	if pos+int(tzLen) > len(data) {
		return fmt.Errorf("insufficient data for timezone")
	}
	h.Timezone = string(data[pos : pos+int(tzLen)])
	pos += int(tzLen)

	// Read display name length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for display name length")
	}
	displayLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read display name
	if pos+int(displayLen) > len(data) {
		return fmt.Errorf("insufficient data for display name")
	}
	h.DisplayName = string(data[pos : pos+int(displayLen)])
	pos += int(displayLen)

	// Read version patch (varint)
	if pos < len(data) {
		patch, bytesRead := h.readVarint(data[pos:])
		if bytesRead > 0 {
			h.VersionPatch = patch
		}
	}

	return nil
}

// Size returns the estimated size of the packed message
func (h *ServerHello) Size() int {
	// 4 bytes per length + string lengths + 1 byte per version + varint sizes
	return 4 + len(h.ServerName) + 1 + 1 + 8 + 4 + len(h.Timezone) + 4 + len(h.DisplayName) + 8
}

// readVarint reads a variable-length integer from the beginning of data
func (h *ServerHello) readVarint(data []byte) (uint64, int) {
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

// NewServerHello creates a new server hello message
func NewServerHello(serverName, timezone, displayName string) *ServerHello {
	return &ServerHello{
		ServerName:   serverName,
		MajorVersion: 1,
		MinorVersion: 0,
		Revision:     protocol.DBMS_TCP_PROTOCOL_VERSION,
		Timezone:     timezone,
		DisplayName:  displayName,
		VersionPatch: 0,
	}
}

// Register registers this signal type in both registry and factory
func (h *ServerHello) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(h, &protocol.SignalInfo{Name: "ServerHello"}); err != nil {
		return fmt.Errorf("failed to register ServerHello in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerHello, func() protocol.Signal {
		return &ServerHello{}
	})

	return nil
}
