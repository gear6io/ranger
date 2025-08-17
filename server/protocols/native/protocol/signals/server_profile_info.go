package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ServerProfileInfo represents a server profile info message for query profiling
type ServerProfileInfo struct {
	RowsReadBeforeLimit  uint64
	BytesReadBeforeLimit uint64
	AppliedLimit         uint64
	AppliedOffset        uint64
	RowsRead             uint64
	BytesRead            uint64
}

// Type returns the signal type
func (p *ServerProfileInfo) Type() protocol.SignalType {
	return protocol.ServerProfileInfo
}

// Pack serializes the profile info message to bytes
func (p *ServerProfileInfo) Pack() ([]byte, error) {
	// Calculate total size
	size := p.Size()
	buf := make([]byte, 0, size)

	// Pack rows read before limit (uvarint)
	for p.RowsReadBeforeLimit >= 0x80 {
		buf = append(buf, byte(p.RowsReadBeforeLimit)|0x80)
		p.RowsReadBeforeLimit >>= 7
	}
	buf = append(buf, byte(p.RowsReadBeforeLimit))

	// Pack bytes read before limit (uvarint)
	for p.BytesReadBeforeLimit >= 0x80 {
		buf = append(buf, byte(p.BytesReadBeforeLimit)|0x80)
		p.BytesReadBeforeLimit >>= 7
	}
	buf = append(buf, byte(p.BytesReadBeforeLimit))

	// Pack applied limit (uvarint)
	for p.AppliedLimit >= 0x80 {
		buf = append(buf, byte(p.AppliedLimit)|0x80)
		p.AppliedLimit >>= 7
	}
	buf = append(buf, byte(p.AppliedLimit))

	// Pack applied offset (uvarint)
	for p.AppliedOffset >= 0x80 {
		buf = append(buf, byte(p.AppliedOffset)|0x80)
		p.AppliedOffset >>= 7
	}
	buf = append(buf, byte(p.AppliedOffset))

	// Pack rows read (uvarint)
	for p.RowsRead >= 0x80 {
		buf = append(buf, byte(p.RowsRead)|0x80)
		p.RowsRead >>= 7
	}
	buf = append(buf, byte(p.RowsRead))

	// Pack bytes read (uvarint)
	for p.BytesRead >= 0x80 {
		buf = append(buf, byte(p.BytesRead)|0x80)
		p.BytesRead >>= 7
	}
	buf = append(buf, byte(p.BytesRead))

	return buf, nil
}

// Unpack deserializes the profile info message from bytes
func (p *ServerProfileInfo) Unpack(data []byte) error {
	if len(data) < 6 { // minimum: 6 uvarints (at least 1 byte each)
		return fmt.Errorf("insufficient data for server profile info")
	}

	pos := 0

	// Read rows read before limit (uvarint)
	rowsReadBeforeLimit, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read rows read before limit")
	}
	p.RowsReadBeforeLimit = rowsReadBeforeLimit
	pos += bytesRead

	// Read bytes read before limit (uvarint)
	bytesReadBeforeLimit, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read bytes read before limit")
	}
	p.BytesReadBeforeLimit = bytesReadBeforeLimit
	pos += bytesRead

	// Read applied limit (uvarint)
	appliedLimit, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read applied limit")
	}
	p.AppliedLimit = appliedLimit
	pos += bytesRead

	// Read applied offset (uvarint)
	appliedOffset, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read applied offset")
	}
	p.AppliedOffset = appliedOffset
	pos += bytesRead

	// Read rows read (uvarint)
	rowsRead, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read rows read")
	}
	p.RowsRead = rowsRead
	pos += bytesRead

	// Read bytes read (uvarint)
	bytesReadVal, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read bytes read")
	}
	p.BytesRead = bytesReadVal

	return nil
}

// Size returns the estimated size of the packed message
func (p *ServerProfileInfo) Size() int {
	return 48 // 6 uvarints (8 bytes each)
}

// readUvarint reads a variable-length integer from the beginning of data
func (p *ServerProfileInfo) readUvarint(data []byte) (uint64, int) {
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

// NewServerProfileInfo creates a new server profile info message
func NewServerProfileInfo(rowsReadBeforeLimit, bytesReadBeforeLimit, appliedLimit, appliedOffset, rowsRead, bytesRead uint64) *ServerProfileInfo {
	return &ServerProfileInfo{
		RowsReadBeforeLimit:  rowsReadBeforeLimit,
		BytesReadBeforeLimit: bytesReadBeforeLimit,
		AppliedLimit:         appliedLimit,
		AppliedOffset:        appliedOffset,
		RowsRead:             rowsRead,
		BytesRead:            bytesRead,
	}
}
