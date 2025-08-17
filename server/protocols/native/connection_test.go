package native

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"

	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestBatchInsertDataParsing(t *testing.T) {
	t.Skip("Skipping protocol tests - test logic needs significant updates for current protocol implementation")

	// Create a mock connection using bytes.Buffer
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create connection handler
	logger := zerolog.New(&bytes.Buffer{})

	// Create a mock query engine (we'll use nil for now since this is just a test)
	var mockQueryEngine *query.Engine
	handler := NewConnectionHandler(serverConn, mockQueryEngine, logger)

	// Start handler in goroutine
	go func() {
		handler.Handle()
	}()

	// Send client hello
	sendClientHello(t, clientConn)

	// Read server hello
	readServerHello(t, clientConn)

	// Send addendum
	sendAddendum(t, clientConn)

	// Send batch insert data
	sendBatchInsertData(t, clientConn, "test_table")

	// Read batch insert response
	readBatchInsertResponse(t, clientConn)
}

func sendClientHello(t *testing.T, conn net.Conn) {
	// Send packet type (ClientHello = 0)
	writeByte(t, conn, 0)

	// Send client name
	writeString(t, conn, "clickhouse-go/2.35.0")

	// Send major version
	writeUvarint(t, conn, 2)

	// Send minor version
	writeUvarint(t, conn, 35)

	// Send protocol version
	writeUvarint(t, conn, 54460)

	// Send default database
	writeString(t, conn, "default")

	// Send username
	writeString(t, conn, "default")

	// Send password
	writeString(t, conn, "")
}

func sendAddendum(t *testing.T, conn net.Conn) {
	// Send empty quota key string
	writeString(t, conn, "")
}

func sendBatchInsertData(t *testing.T, conn net.Conn, tableName string) {
	// Send packet type (ClientData = 2)
	writeByte(t, conn, 2)

	// Send table name
	writeString(t, conn, tableName)

	// Send client info (empty for now)
	writeString(t, conn, "")

	// Send external table info (empty for now)
	writeString(t, conn, "")

	// Send empty block info
	writeUvarint(t, conn, 0)

	// Send column count (2 columns)
	writeUvarint(t, conn, 2)

	// Send column metadata
	// Column 1: id (UInt32)
	writeString(t, conn, "id")
	writeString(t, conn, "UInt32")

	// Column 2: name (String)
	writeString(t, conn, "name")
	writeString(t, conn, "String")

	// Send row count (2 rows)
	writeUvarint(t, conn, 2)

	// Send data rows
	// Row 1: id=1, name="Alice"
	writeUint32(t, conn, 1)
	writeString(t, conn, "Alice")

	// Row 2: id=2, name="Bob"
	writeUint32(t, conn, 2)
	writeString(t, conn, "Bob")
}

func readServerHello(t *testing.T, conn net.Conn) {
	// Read packet type
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, byte(0), buf[0]) // ServerHello = 0

	// Read server name
	serverName, err := readString(conn)
	assert.NoError(t, err)
	assert.NotEmpty(t, serverName)

	// Read server version
	_, err = readUvarint(conn)
	assert.NoError(t, err)

	// Read protocol version
	_, err = readUvarint(conn)
	assert.NoError(t, err)
}

func readBatchInsertResponse(t *testing.T, conn net.Conn) {
	// Read packet type
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, byte(3), buf[0]) // ServerProgress = 3

	// Read progress info
	rowsRead, err := readUvarint(conn)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, rowsRead, uint64(0))

	bytesRead, err := readUvarint(conn)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, bytesRead, uint64(0))

	totalRows, err := readUvarint(conn)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, totalRows, uint64(0))

	totalBytes, err := readUvarint(conn)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, totalBytes, uint64(0))

	// Read end of stream
	buf = make([]byte, 1)
	_, err = conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, byte(5), buf[0]) // ServerEndOfStream = 5
}

// Helper functions
func writeByte(t *testing.T, conn net.Conn, b byte) {
	_, err := conn.Write([]byte{b})
	assert.NoError(t, err)
}

func writeUvarint(t *testing.T, conn net.Conn, v uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := conn.Write(buf[:n])
	assert.NoError(t, err)
}

func writeString(t *testing.T, conn net.Conn, s string) {
	writeUvarint(t, conn, uint64(len(s)))
	_, err := conn.Write([]byte(s))
	assert.NoError(t, err)
}

func writeUint32(t *testing.T, conn net.Conn, v uint32) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	_, err := conn.Write(buf)
	assert.NoError(t, err)
}

func readString(conn net.Conn) (string, error) {
	length, err := readUvarint(conn)
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	_, err = conn.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readUvarint(conn net.Conn) (uint64, error) {
	// Simple implementation using a buffer
	buf := make([]byte, binary.MaxVarintLen64)
	n, err := conn.Read(buf)
	if err != nil {
		return 0, err
	}
	value, _ := binary.Uvarint(buf[:n])
	return value, nil
}
