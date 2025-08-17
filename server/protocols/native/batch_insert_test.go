package native

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadColumnValue(t *testing.T) {
	// Test UInt32
	data := []byte{1, 0, 0, 0} // Little endian 1
	reader := &PacketReader{conn: &mockConn{data: data}}

	value, err := reader.ReadUint32()
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), value)
}

func TestReadFloat64(t *testing.T) {
	// Test Float64
	expected := 3.14159
	reader := &PacketReader{conn: &mockConn{data: []byte{0x6e, 0x86, 0x1b, 0xf0, 0xf9, 0x21, 0x09, 0x40}}}

	value, err := reader.ReadFloat64()
	assert.NoError(t, err)
	assert.InDelta(t, expected, value, 0.0001)
}

func TestReadString(t *testing.T) {
	// Test String
	expected := "Hello, World!"
	length := uint64(len(expected))

	// Create buffer with length + string
	buf := make([]byte, 0)
	lengthBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lengthBuf, length)
	buf = append(buf, lengthBuf[:n]...)
	buf = append(buf, []byte(expected)...)

	reader := &PacketReader{conn: &mockConn{data: buf}}

	value, err := reader.ReadString()
	assert.NoError(t, err)
	assert.Equal(t, expected, value)
}

func TestReadUvarint(t *testing.T) {
	// Test Uvarint
	expected := uint64(12345)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, expected)

	reader := &PacketReader{conn: &mockConn{data: buf[:n]}}

	value, err := reader.ReadUVarInt()
	assert.NoError(t, err)
	assert.Equal(t, expected, value)
}

func TestReadByte(t *testing.T) {
	// Test ReadByte
	expected := byte(42)
	reader := &PacketReader{conn: &mockConn{data: []byte{expected}}}

	value, err := reader.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, expected, value)
}

// Mock connection for testing
type mockConn struct {
	data []byte
	pos  int
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	if m.pos >= len(m.data) {
		return 0, nil
	}

	n = copy(p, m.data[m.pos:])
	m.pos += n
	return n, nil
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
