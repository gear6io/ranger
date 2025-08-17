package sdk_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TFMV/icebox/pkg/sdk"
)

// MockIceboxServer simulates an Icebox server for testing
type MockIceboxServer struct {
	listener net.Listener
	addr     string
	quit     chan struct{}
}

// NewMockServer creates a new mock server
func NewMockServer() (*MockIceboxServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &MockIceboxServer{
		listener: listener,
		addr:     listener.Addr().String(),
		quit:     make(chan struct{}),
	}

	go server.serve()
	return server, nil
}

// Addr returns the server address
func (s *MockIceboxServer) Addr() string {
	return s.addr
}

// Close stops the server
func (s *MockIceboxServer) Close() error {
	close(s.quit)
	return s.listener.Close()
}

// serve handles incoming connections
func (s *MockIceboxServer) serve() {
	for {
		select {
		case <-s.quit:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}
			go s.handleConnection(conn)
		}
	}
}

// handleConnection handles a single client connection
func (s *MockIceboxServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read client hello
	if err := s.readClientHello(conn); err != nil {
		return
	}

	// Send server hello
	if err := s.sendServerHello(conn); err != nil {
		return
	}

	// Handle queries
	for {
		select {
		case <-s.quit:
			return
		default:
			if err := s.handleQuery(conn); err != nil {
				return
			}
		}
	}
}

// readClientHello reads and validates client hello
func (s *MockIceboxServer) readClientHello(conn net.Conn) error {
	// Read message length
	lengthBuf := make([]byte, 4)
	if _, err := conn.Read(lengthBuf); err != nil {
		return err
	}
	messageLength := binary.BigEndian.Uint32(lengthBuf)

	// Read message type
	messageTypeBuf := make([]byte, 1)
	if _, err := conn.Read(messageTypeBuf); err != nil {
		return err
	}
	messageType := messageTypeBuf[0]

	// Validate message type (ClientHello = 0)
	if messageType != 0 {
		return fmt.Errorf("expected ClientHello (0), got %d", messageType)
	}

	// Read payload (client name, version info)
	payloadSize := messageLength - 1
	if payloadSize > 0 {
		payload := make([]byte, payloadSize)
		if _, err := conn.Read(payload); err != nil {
			return err
		}
	}

	return nil
}

// sendServerHello sends server hello response
func (s *MockIceboxServer) sendServerHello(conn net.Conn) error {
	// Server hello payload: server name, version, protocol version
	serverName := "Icebox Mock Server"
	serverVersion := "1.0.0"
	protocolVersion := "1.0"

	payload := []byte(serverName + serverVersion + protocolVersion)
	messageLength := 1 + len(payload) // 1 byte for message type

	// Write message length (4 bytes, big endian)
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(messageLength))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}

	// Write message type (ServerHello = 1)
	if _, err := conn.Write([]byte{1}); err != nil {
		return err
	}

	// Write payload
	if _, err := conn.Write(payload); err != nil {
		return err
	}

	return nil
}

// handleQuery handles a client query
func (s *MockIceboxServer) handleQuery(conn net.Conn) error {
	// Read message length
	lengthBuf := make([]byte, 4)
	if _, err := conn.Read(lengthBuf); err != nil {
		return err
	}
	messageLength := binary.BigEndian.Uint32(lengthBuf)

	// Read message type
	messageTypeBuf := make([]byte, 1)
	if _, err := conn.Read(messageTypeBuf); err != nil {
		return err
	}
	messageType := messageTypeBuf[0]

	// Handle different message types
	switch messageType {
	case 3: // ClientQuery
		return s.handleClientQuery(conn, int(messageLength-1))
	case 4: // ClientPing
		return s.handleClientPing(conn)
	default:
		return fmt.Errorf("unknown message type: %d", messageType)
	}
}

// handleClientQuery handles a client query
func (s *MockIceboxServer) handleClientQuery(conn net.Conn, payloadSize int) error {
	// Read query payload
	queryBuf := make([]byte, payloadSize)
	if _, err := conn.Read(queryBuf); err != nil {
		return err
	}
	query := string(queryBuf)

	// Simulate different query responses based on query content
	if s.shouldReturnError(query) {
		return s.sendErrorResponse(conn, 1001, "Table 'nonexistent' does not exist")
	}

	return s.sendQueryResponse(conn, query)
}

// handleClientPing handles a client ping
func (s *MockIceboxServer) handleClientPing(conn net.Conn) error {
	// Send pong response
	messageLength := 1 // Just message type

	// Write message length
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(messageLength))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}

	// Write message type (ServerPong = 5)
	if _, err := conn.Write([]byte{5}); err != nil {
		return err
	}

	return nil
}

// shouldReturnError determines if a query should return an error
func (s *MockIceboxServer) shouldReturnError(query string) bool {
	// Return error for specific queries to test error handling
	errorQueries := []string{
		"SELECT * FROM nonexistent",
		"SHOW TABLES FROM nonexistent",
		"DROP TABLE nonexistent",
		"INVALID SQL SYNTAX",
	}

	for _, errorQuery := range errorQueries {
		if query == errorQuery {
			return true
		}
	}
	return false
}

// sendErrorResponse sends a server exception response
func (s *MockIceboxServer) sendErrorResponse(conn net.Conn, code int, message string) error {
	// Create exception payload
	exceptionPayload := fmt.Sprintf("%s\x00%d", message, code)
	messageLength := 1 + len(exceptionPayload) // 1 byte for message type

	// Write message length
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(messageLength))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}

	// Write message type (ServerException = 2)
	if _, err := conn.Write([]byte{2}); err != nil {
		return err
	}

	// Write exception payload
	if _, err := conn.Write([]byte(exceptionPayload)); err != nil {
		return err
	}

	return nil
}

// sendQueryResponse sends a successful query response
func (s *MockIceboxServer) sendQueryResponse(conn net.Conn, query string) error {
	// Send data response
	if err := s.sendDataResponse(conn, query); err != nil {
		return err
	}

	// Send end of stream
	return s.sendEndOfStream(conn)
}

// sendDataResponse sends a data response
func (s *MockIceboxServer) sendDataResponse(conn net.Conn, query string) error {
	// Create mock data based on query
	var columns []string
	var data [][]interface{}

	if query == "SHOW DATABASES" {
		columns = []string{"Database"}
		data = [][]interface{}{
			{"default"},
			{"system"},
			{"test"},
		}
	} else if query == "SHOW TABLES" {
		columns = []string{"Table"}
		data = [][]interface{}{
			{"users"},
			{"orders"},
		}
	} else {
		columns = []string{"result"}
		data = [][]interface{}{
			{"Query executed successfully"},
		}
	}

	// Send each column as a separate data packet
	for i, column := range columns {
		if err := s.sendColumnData(conn, column, "String", data, i); err != nil {
			return err
		}
	}

	return nil
}

// sendColumnData sends data for a single column
func (s *MockIceboxServer) sendColumnData(conn net.Conn, columnName, columnType string, data [][]interface{}, columnIndex int) error {
	// Create data string for this column
	var values []string
	for _, row := range data {
		if columnIndex < len(row) {
			values = append(values, fmt.Sprintf("%v", row[columnIndex]))
		}
	}
	dataString := fmt.Sprintf("%s", values)

	// Calculate payload size
	payload := fmt.Sprintf("%d%s%s%d%d%s", 1, columnName, columnType, 1, len(values), dataString)
	messageLength := 1 + len(payload) // 1 byte for message type

	// Write message length
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(messageLength))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}

	// Write message type (ServerData = 3)
	if _, err := conn.Write([]byte{3}); err != nil {
		return err
	}

	// Write payload
	if _, err := conn.Write([]byte(payload)); err != nil {
		return err
	}

	return nil
}

// sendEndOfStream sends end of stream response
func (s *MockIceboxServer) sendEndOfStream(conn net.Conn) error {
	// End of stream has no payload
	messageLength := 1 // Just message type

	// Write message length
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(messageLength))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}

	// Write message type (ServerEndOfStream = 6)
	if _, err := conn.Write([]byte{6}); err != nil {
		return err
	}

	return nil
}

// TestMockServerErrorHandling tests error handling with the mock server
func TestMockServerErrorHandling(t *testing.T) {
	// Start mock server
	server, err := NewMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Create client connected to mock server
	client, err := sdk.NewClient(&sdk.Options{
		Addr: []string{server.Addr()},
		Auth: sdk.Auth{
			Username: "test",
			Password: "test",
			Database: "test",
		},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	t.Run("SuccessfulQuery", func(t *testing.T) {
		ctx := context.Background()
		rows, err := client.Query(ctx, "SHOW DATABASES")
		require.NoError(t, err)
		defer rows.Close()

		// Should have no error
		assert.NoError(t, rows.Err())

		// Should have columns
		assert.Len(t, rows.Cols, 1)
		assert.Equal(t, "Database", rows.Cols[0].Name)
	})

	t.Run("ErrorQuery", func(t *testing.T) {
		ctx := context.Background()
		rows, err := client.Query(ctx, "SELECT * FROM nonexistent")
		require.NoError(t, err) // Query execution succeeds, but rows contain error
		defer rows.Close()

		// Should have error in rows
		assert.Error(t, rows.Err())
		assert.Contains(t, rows.Err().Error(), "Table 'nonexistent' does not exist")
	})

	t.Run("InvalidSQLQuery", func(t *testing.T) {
		ctx := context.Background()
		rows, err := client.Query(ctx, "INVALID SQL SYNTAX")
		require.NoError(t, err) // Query execution succeeds, but rows contain error
		defer rows.Close()

		// Should have error in rows
		assert.Error(t, rows.Err())
		assert.Contains(t, rows.Err().Error(), "Table 'nonexistent' does not exist")
	})
}

// TestMockServerConnection tests basic connection functionality
func TestMockServerConnection(t *testing.T) {
	// Start mock server
	server, err := NewMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Create client connected to mock server
	client, err := sdk.NewClient(&sdk.Options{
		Addr: []string{server.Addr()},
		Auth: sdk.Auth{
			Username: "test",
			Password: "test",
			Database: "test",
		},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	t.Run("Ping", func(t *testing.T) {
		ctx := context.Background()
		err := client.Ping(ctx)
		assert.NoError(t, err)
	})

	t.Run("ShowDatabases", func(t *testing.T) {
		ctx := context.Background()
		rows, err := client.Query(ctx, "SHOW DATABASES")
		require.NoError(t, err)
		defer rows.Close()

		// Should have no error
		assert.NoError(t, rows.Err())

		// Should have data
		assert.Len(t, rows.Cols, 1)
		assert.Len(t, rows.Data, 3) // default, system, test
	})

	t.Run("ShowTables", func(t *testing.T) {
		ctx := context.Background()
		rows, err := client.Query(ctx, "SHOW TABLES")
		require.NoError(t, err)
		defer rows.Close()

		// Should have no error
		assert.NoError(t, rows.Err())

		// Should have data
		assert.Len(t, rows.Cols, 1)
		assert.Len(t, rows.Data, 2) // users, orders
	})
}
