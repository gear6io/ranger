package sdk

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/go-faster/errors"
)

// Reader handles reading protocol data from the server
type Reader struct {
	conn net.Conn
	buf  []byte
}

// NewReader creates a new protocol reader
func NewReader(conn net.Conn) *Reader {
	return &Reader{
		conn: conn,
		buf:  make([]byte, 4096),
	}
}

// ReadServerHello reads server hello message
func (r *Reader) ReadServerHello() (*ServerVersion, error) {
	// Read packet type
	packetType, err := r.readByte()
	if err != nil {
		return nil, errors.Wrap(err, "read packet type")
	}

	if packetType != 0 { // ServerHello packet type
		return nil, errors.New("unexpected packet type")
	}

	// Read server info
	info := &ServerVersion{}

	// Read server name
	if info.Name, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read server name")
	}

	// Read version
	if info.Major, err = r.readInt(); err != nil {
		return nil, errors.Wrap(err, "read major version")
	}
	if info.Minor, err = r.readInt(); err != nil {
		return nil, errors.Wrap(err, "read minor version")
	}
	if info.Patch, err = r.readInt(); err != nil {
		return nil, errors.Wrap(err, "read patch version")
	}
	if info.Revision, err = r.readInt(); err != nil {
		return nil, errors.Wrap(err, "read revision")
	}

	// Read interface
	if info.Interface, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read interface")
	}

	// Read default database
	if info.DefaultDB, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read default database")
	}

	// Read timezone
	if info.Timezone, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read timezone")
	}

	// Read display name
	if info.DisplayName, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read display name")
	}

	// Read version string
	if info.Version, err = r.readString(); err != nil {
		return nil, errors.Wrap(err, "read version string")
	}

	// Read protocol version
	if info.Protocol, err = r.readInt(); err != nil {
		return nil, errors.Wrap(err, "read protocol version")
	}

	return info, nil
}

// ReadPong reads pong response
func (r *Reader) ReadPong() error {
	packetType, err := r.readByte()
	if err != nil {
		return errors.Wrap(err, "read packet type")
	}

	if packetType != 2 { // Pong packet type
		return errors.New("unexpected packet type, expected pong")
	}

	return nil
}

// ReadExecResponse reads execution response
func (r *Reader) ReadExecResponse() error {
	// Read packet type
	packetType, err := r.readByte()
	if err != nil {
		return errors.Wrap(err, "read packet type")
	}

	switch packetType {
	case 1: // Exception packet
		return r.readException()
	case 2: // Progress packet
		return r.readProgress()
	case 3: // Pong packet
		return r.ReadPong()
	case 4: // End of stream packet
		return nil
	default:
		return errors.Errorf("unexpected packet type: %d", packetType)
	}
}

// readException reads server exception
func (r *Reader) readException() error {
	// Read exception code
	code, err := r.readInt()
	if err != nil {
		return errors.Wrap(err, "read exception code")
	}

	// Read exception name
	name, err := r.readString()
	if err != nil {
		return errors.Wrap(err, "read exception name")
	}

	// Read exception message
	message, err := r.readString()
	if err != nil {
		return errors.Wrap(err, "read exception message")
	}

	// Read exception stack
	stack, err := r.readString()
	if err != nil {
		return errors.Wrap(err, "read exception stack")
	}

	return &Exception{
		Code:    code,
		Name:    name,
		Message: message,
		Stack:   stack,
	}
}

// readProgress reads progress information
func (r *Reader) readProgress() error {
	// Read progress data
	readRows, err := r.readUint64()
	if err != nil {
		return errors.Wrap(err, "read read rows")
	}

	readBytes, err := r.readUint64()
	if err != nil {
		return errors.Wrap(err, "read read bytes")
	}

	totalRows, err := r.readUint64()
	if err != nil {
		return errors.Wrap(err, "read total rows")
	}

	writtenRows, err := r.readUint64()
	if err != nil {
		return errors.Wrap(err, "read written rows")
	}

	writtenBytes, err := r.readUint64()
	if err != nil {
		return errors.Wrap(err, "read written bytes")
	}

	// For now, we'll just discard the progress data
	_ = readRows
	_ = readBytes
	_ = totalRows
	_ = writtenRows
	_ = writtenBytes

	return nil
}

// readByte reads a single byte
func (r *Reader) readByte() (byte, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r.conn, buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// readInt reads a 32-bit integer
func (r *Reader) readInt() (int, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.conn, buf); err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(buf)), nil
}

// readUint64 reads a 64-bit unsigned integer
func (r *Reader) readUint64() (uint64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r.conn, buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// readString reads a string
func (r *Reader) readString() (string, error) {
	// Read string length
	length, err := r.readInt()
	if err != nil {
		return "", errors.Wrap(err, "read string length")
	}

	if length < 0 {
		return "", errors.New("negative string length")
	}

	// Read string data
	buf := make([]byte, length)
	if _, err := io.ReadFull(r.conn, buf); err != nil {
		return "", errors.Wrap(err, "read string data")
	}

	return string(buf), nil
}

// readBytes reads raw bytes
func (r *Reader) readBytes(length int) ([]byte, error) {
	buf := make([]byte, length)
	if _, err := io.ReadFull(r.conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// SetReadTimeout sets the read timeout
func (r *Reader) SetReadTimeout(timeout time.Duration) error {
	return r.conn.SetReadDeadline(time.Now().Add(timeout))
}

// ClearReadTimeout clears the read timeout
func (r *Reader) ClearReadTimeout() error {
	return r.conn.SetReadDeadline(time.Time{})
}
