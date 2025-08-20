package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ServerProgress represents a server progress message for query execution
type ServerProgress struct {
	RowsRead    uint64
	BytesRead   uint64
	TotalRows   uint64
	TotalBytes  uint64
	ElapsedTime float64
}

// Type returns the signal type
func (p *ServerProgress) Type() protocol.SignalType {
	return protocol.ServerProgress
}

// Pack serializes the progress message to bytes
func (p *ServerProgress) Pack() ([]byte, error) {
	// Calculate total size
	size := p.Size()
	buf := make([]byte, 0, size)

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

	// Pack total rows (uvarint)
	for p.TotalRows >= 0x80 {
		buf = append(buf, byte(p.TotalRows)|0x80)
		p.TotalRows >>= 7
	}
	buf = append(buf, byte(p.TotalRows))

	// Pack total bytes (uvarint)
	for p.TotalBytes >= 0x80 {
		buf = append(buf, byte(p.TotalBytes)|0x80)
		p.TotalBytes >>= 7
	}
	buf = append(buf, byte(p.TotalBytes))

	// Pack elapsed time (8 bytes, big endian)
	elapsedBytes := make([]byte, 8)
	protocol.WriteUint64BigEndian(elapsedBytes, uint64(p.ElapsedTime*1000000)) // Convert to microseconds
	buf = append(buf, elapsedBytes...)

	return buf, nil
}

// Unpack deserializes the progress message from bytes
func (p *ServerProgress) Unpack(data []byte) error {
	if len(data) < 8 { // minimum: 8 bytes for elapsed time
		return fmt.Errorf("insufficient data for server progress")
	}

	pos := 0

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
	pos += bytesRead

	// Read total rows (uvarint)
	totalRows, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read total rows")
	}
	p.TotalRows = totalRows
	pos += bytesRead

	// Read total bytes (uvarint)
	totalBytes, bytesRead := p.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read total bytes")
	}
	p.TotalBytes = totalBytes
	pos += bytesRead

	// Read elapsed time (8 bytes, big endian)
	if pos+8 > len(data) {
		return fmt.Errorf("insufficient data for elapsed time")
	}
	elapsedMicroseconds := protocol.ReadUint64BigEndian(data[pos:])
	p.ElapsedTime = float64(elapsedMicroseconds) / 1000000.0 // Convert from microseconds

	return nil
}

// Size returns the estimated size of the packed message
func (p *ServerProgress) Size() int {
	return 32 + 8 // 4 uvarints (8 bytes each) + 8 bytes for elapsed time
}

// readUvarint reads a variable-length integer from the beginning of data
func (p *ServerProgress) readUvarint(data []byte) (uint64, int) {
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

// NewServerProgress creates a new server progress message
func NewServerProgress(rowsRead, bytesRead, totalRows, totalBytes uint64, elapsedTime float64) *ServerProgress {
	return &ServerProgress{
		RowsRead:    rowsRead,
		BytesRead:   bytesRead,
		TotalRows:   totalRows,
		TotalBytes:  totalBytes,
		ElapsedTime: elapsedTime,
	}
}

// Register registers this signal type in both registry and factory
func (p *ServerProgress) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(p, &protocol.SignalInfo{Name: "ServerProgress"}); err != nil {
		return fmt.Errorf("failed to register ServerProgress in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerProgress, func() protocol.Signal {
		return &ServerProgress{}
	})

	return nil
}
