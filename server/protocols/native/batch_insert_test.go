package native

import (
	"net"
	"time"
)

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
