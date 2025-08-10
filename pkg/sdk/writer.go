package sdk

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/go-faster/errors"
)

// Writer handles writing protocol data to the server
type Writer struct {
	conn net.Conn
	buf  []byte
	pos  int
}

// NewWriter creates a new protocol writer
func NewWriter(conn net.Conn) *Writer {
	return &Writer{
		conn: conn,
		buf:  make([]byte, 4096),
		pos:  0,
	}
}

// WriteClientHello writes client hello message
func (w *Writer) WriteClientHello(clientName string, protocolVersion int, database, user, password string) error {
	// Write packet type (ClientHello = 0)
	if err := w.writeByte(0); err != nil {
		return errors.Wrap(err, "write packet type")
	}

	// Write client name
	if err := w.writeString(clientName); err != nil {
		return errors.Wrap(err, "write client name")
	}

	// Write protocol version
	if err := w.writeInt(protocolVersion); err != nil {
		return errors.Wrap(err, "write protocol version")
	}

	// Write database
	if err := w.writeString(database); err != nil {
		return errors.Wrap(err, "write database")
	}

	// Write user
	if err := w.writeString(user); err != nil {
		return errors.Wrap(err, "write user")
	}

	// Write password
	if err := w.writeString(password); err != nil {
		return errors.Wrap(err, "write password")
	}

	return nil
}

// WriteQuery writes a query
func (w *Writer) WriteQuery(query Query, settings Settings) error {
	// Write packet type (Query = 1)
	if err := w.writeByte(1); err != nil {
		return errors.Wrap(err, "write packet type")
	}

	// Write query ID
	if err := w.writeString(query.QueryID); err != nil {
		return errors.Wrap(err, "write query ID")
	}

	// Write query body
	if err := w.writeString(query.Body); err != nil {
		return errors.Wrap(err, "write query body")
	}

	// Write settings count
	if err := w.writeInt(len(settings)); err != nil {
		return errors.Wrap(err, "write settings count")
	}

	// Write settings
	for key, value := range settings {
		if err := w.writeString(key); err != nil {
			return errors.Wrap(err, "write setting key")
		}
		if err := w.writeString(fmt.Sprintf("%v", value)); err != nil {
			return errors.Wrap(err, "write setting value")
		}
	}

	return nil
}

// WritePing writes a ping message
func (w *Writer) WritePing() error {
	// Write packet type (Ping = 2)
	return w.writeByte(2)
}

// WriteDataBlock writes a data block for batch insertion
func (w *Writer) WriteDataBlock(tableName string, columns []string, row []interface{}) error {
	// Write packet type (Data = 3)
	if err := w.writeByte(3); err != nil {
		return errors.Wrap(err, "write packet type")
	}

	// Write table name
	if err := w.writeString(tableName); err != nil {
		return errors.Wrap(err, "write table name")
	}

	// Write column count
	if err := w.writeInt(len(columns)); err != nil {
		return errors.Wrap(err, "write column count")
	}

	// Write column names
	for _, col := range columns {
		if err := w.writeString(col); err != nil {
			return errors.Wrap(err, "write column name")
		}
	}

	// Write row count (1 for single row)
	if err := w.writeInt(1); err != nil {
		return errors.Wrap(err, "write row count")
	}

	// Write row data
	for _, val := range row {
		if err := w.writeValue(val); err != nil {
			return errors.Wrap(err, "write value")
		}
	}

	return nil
}

// WriteEndOfStream writes end of stream marker
func (w *Writer) WriteEndOfStream() error {
	// Write packet type (EndOfStream = 4)
	return w.writeByte(4)
}

// writeValue writes a value in the appropriate format
func (w *Writer) writeValue(val interface{}) error {
	switch v := val.(type) {
	case string:
		return w.writeString(v)
	case int:
		return w.writeInt(v)
	case int64:
		return w.writeInt64(v)
	case float64:
		return w.writeFloat64(v)
	case bool:
		return w.writeBool(v)
	case time.Time:
		return w.writeTime(v)
	case nil:
		return w.writeNull()
	default:
		return errors.Errorf("unsupported value type: %T", val)
	}
}

// writeByte writes a single byte
func (w *Writer) writeByte(b byte) error {
	if w.pos >= len(w.buf) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.buf[w.pos] = b
	w.pos++
	return nil
}

// writeInt writes a 32-bit integer
func (w *Writer) writeInt(i int) error {
	if w.pos+4 > len(w.buf) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.pos:], uint32(i))
	w.pos += 4
	return nil
}

// writeInt64 writes a 64-bit integer
func (w *Writer) writeInt64(i int64) error {
	if w.pos+8 > len(w.buf) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(w.buf[w.pos:], uint64(i))
	w.pos += 8
	return nil
}

// writeFloat64 writes a 64-bit float
func (w *Writer) writeFloat64(f float64) error {
	if w.pos+8 > len(w.buf) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(w.buf[w.pos:], uint64(f))
	w.pos += 8
	return nil
}

// writeBool writes a boolean
func (w *Writer) writeBool(b bool) error {
	var val byte
	if b {
		val = 1
	}
	return w.writeByte(val)
}

// writeTime writes a time value
func (w *Writer) writeTime(t time.Time) error {
	// Write as Unix timestamp
	return w.writeInt64(t.Unix())
}

// writeNull writes a null value
func (w *Writer) writeNull() error {
	// Write null marker
	return w.writeByte(0)
}

// writeString writes a string
func (w *Writer) writeString(s string) error {
	// Write string length
	if err := w.writeInt(len(s)); err != nil {
		return errors.Wrap(err, "write string length")
	}

	// Write string data
	bytes := []byte(s)
	for len(bytes) > 0 {
		available := len(w.buf) - w.pos
		if available == 0 {
			if err := w.flush(); err != nil {
				return err
			}
			available = len(w.buf)
		}

		n := len(bytes)
		if n > available {
			n = available
		}

		copy(w.buf[w.pos:], bytes[:n])
		w.pos += n
		bytes = bytes[n:]
	}

	return nil
}

// Flush flushes the buffer to the connection
func (w *Writer) Flush() error {
	return w.flush()
}

// flush flushes the internal buffer
func (w *Writer) flush() error {
	if w.pos == 0 {
		return nil
	}

	if _, err := w.conn.Write(w.buf[:w.pos]); err != nil {
		return errors.Wrap(err, "write to connection")
	}

	w.pos = 0
	return nil
}

// SetWriteTimeout sets the write timeout
func (w *Writer) SetWriteTimeout(timeout time.Duration) error {
	return w.conn.SetWriteDeadline(time.Now().Add(timeout))
}

// ClearWriteTimeout clears the write timeout
func (w *Writer) ClearWriteTimeout() error {
	return w.conn.SetWriteDeadline(time.Time{})
}
