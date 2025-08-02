package native

import (
	"encoding/binary"
	"io"
	"net"
)

// PacketReader reads packets from the connection
type PacketReader struct {
	conn net.Conn
}

// NewPacketReader creates a new packet reader
func NewPacketReader(conn net.Conn) *PacketReader {
	return &PacketReader{
		conn: conn,
	}
}

// ReadByte reads a single byte
func (r *PacketReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r.conn, buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

// ReadUvarint reads an unsigned variable-length integer
func (r *PacketReader) ReadUvarint() (uint64, error) {
	return binary.ReadUvarint(&netConnReader{r.conn})
}

// ReadString reads a string
func (r *PacketReader) ReadString() (string, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(r.conn, buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// ReadBytes reads a byte array
func (r *PacketReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(r.conn, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// PacketWriter writes packets to the connection
type PacketWriter struct {
	conn net.Conn
	buf  []byte
	pos  int
}

// NewPacketWriter creates a new packet writer
func NewPacketWriter(conn net.Conn) *PacketWriter {
	return &PacketWriter{
		conn: conn,
		buf:  make([]byte, 1024),
		pos:  0,
	}
}

// WriteByte writes a single byte
func (w *PacketWriter) WriteByte(b byte) error {
	if w.pos >= len(w.buf) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.buf[w.pos] = b
	w.pos++
	return nil
}

// WriteUvarint writes an unsigned variable-length integer
func (w *PacketWriter) WriteUvarint(v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return w.WriteBytes(buf[:n])
}

// WriteString writes a string
func (w *PacketWriter) WriteString(s string) error {
	if err := w.WriteUvarint(uint64(len(s))); err != nil {
		return err
	}
	return w.WriteBytes([]byte(s))
}

// WriteBytes writes a byte array
func (w *PacketWriter) WriteBytes(b []byte) error {
	if w.pos+len(b) > len(w.buf) {
		if err := w.Flush(); err != nil {
			return err
		}
		// If still too large, write directly
		if len(b) > len(w.buf) {
			_, err := w.conn.Write(b)
			return err
		}
	}
	copy(w.buf[w.pos:], b)
	w.pos += len(b)
	return nil
}

// Flush flushes the buffer to the connection
func (w *PacketWriter) Flush() error {
	if w.pos > 0 {
		_, err := w.conn.Write(w.buf[:w.pos])
		w.pos = 0
		return err
	}
	return nil
}

// netConnReader implements io.ByteReader for net.Conn
type netConnReader struct {
	conn net.Conn
}

func (r *netConnReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r.conn, buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}
