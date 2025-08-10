package native

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PacketReader reads protocol packets
type PacketReader struct {
	conn io.Reader
}

// NewPacketReader creates a new packet reader
func NewPacketReader(conn io.Reader) *PacketReader {
	return &PacketReader{conn: conn}
}

// ReadByte reads a single byte
func (r *PacketReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	if _, err := r.conn.Read(buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// ReadUint32 reads a 32-bit unsigned integer (big endian)
func (r *PacketReader) ReadUint32() (uint32, error) {
	buf := make([]byte, 4)
	if _, err := r.conn.Read(buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

// ReadString reads a string with length prefix (4 bytes, big endian)
func (r *PacketReader) ReadString() (string, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	if _, err := r.conn.Read(buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

// ReadBytes reads bytes with length prefix
func (r *PacketReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	if _, err := r.conn.Read(buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// ReadUVarInt reads a variable-length unsigned integer
func (r *PacketReader) ReadUVarInt() (uint64, error) {
	var value uint64
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	return value, nil
}

// PacketWriter writes protocol packets
type PacketWriter struct {
	conn io.Writer
}

// NewPacketWriter creates a new packet writer
func NewPacketWriter(conn io.Writer) *PacketWriter {
	return &PacketWriter{conn: conn}
}

// WriteByte writes a single byte
func (w *PacketWriter) WriteByte(b byte) error {
	_, err := w.conn.Write([]byte{b})
	return err
}

// WriteUint32 writes a 32-bit unsigned integer (big endian)
func (w *PacketWriter) WriteUint32(n uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, n)
	_, err := w.conn.Write(buf)
	return err
}

// WriteString writes a string with length prefix
func (w *PacketWriter) WriteString(s string) error {
	if err := w.WriteUint32(uint32(len(s))); err != nil {
		return err
	}
	if len(s) > 0 {
		_, err := w.conn.Write([]byte(s))
		return err
	}
	return nil
}

// WriteBytes writes bytes with length prefix
func (w *PacketWriter) WriteBytes(data []byte) error {
	if err := w.WriteUint32(uint32(len(data))); err != nil {
		return err
	}
	if len(data) > 0 {
		_, err := w.conn.Write(data)
		return err
	}
	return nil
}

// WriteMessage writes a complete message with length prefix
func (w *PacketWriter) WriteMessage(msgType byte, payload []byte) error {
	// Calculate total message length (type + payload)
	totalLength := 1 + len(payload)

	// Write message length (4 bytes, big endian)
	if err := w.WriteUint32(uint32(totalLength)); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message type
	if err := w.WriteByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// Write payload if any
	if len(payload) > 0 {
		if _, err := w.conn.Write(payload); err != nil {
			return fmt.Errorf("failed to write payload: %w", err)
		}
	}

	return nil
}

// WriteUVarInt writes a variable-length unsigned integer
func (w *PacketWriter) WriteUVarInt(n uint64) error {
	for n >= 0x80 {
		if err := w.WriteByte(byte(n) | 0x80); err != nil {
			return err
		}
		n >>= 7
	}
	return w.WriteByte(byte(n))
}
