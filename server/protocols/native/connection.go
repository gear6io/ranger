package native

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// ConnectionHandler handles a single client connection
type ConnectionHandler struct {
	conn      net.Conn
	logger    zerolog.Logger
	reader    *PacketReader
	writer    *PacketWriter
	helloSent bool
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:      conn,
		logger:    logger,
		reader:    NewPacketReader(conn),
		writer:    NewPacketWriter(conn),
		helloSent: false,
	}
}

// Handle handles the client connection
func (h *ConnectionHandler) Handle() error {
	defer h.conn.Close()

	// Set connection timeout
	h.conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Handle client packets
	for {
		// Reset deadline
		h.conn.SetDeadline(time.Now().Add(30 * time.Second))

		// Read packet type
		packetType, err := h.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil // Client disconnected
			}
			return fmt.Errorf("failed to read packet type: %w", err)
		}

		// Handle packet based on type
		switch packetType {
		case ClientHello:
			if err := h.handleClientHello(); err != nil {
				return fmt.Errorf("failed to handle client hello: %w", err)
			}
			// Send server hello immediately after client hello
			if err := h.sendHello(); err != nil {
				return fmt.Errorf("failed to send hello: %w", err)
			}
			h.helloSent = true

			// After handshake, check for addendum (quota key)
			// This is sent as a string, not as a packet type
			if err := h.handleAddendum(); err != nil {
				return fmt.Errorf("failed to handle addendum: %w", err)
			}
		case ClientQuery:
			if err := h.handleQuery(); err != nil {
				return fmt.Errorf("failed to handle query: %w", err)
			}
		case ClientCancel:
			if err := h.handleCancel(); err != nil {
				return fmt.Errorf("failed to handle cancel: %w", err)
			}
		case ClientPing:
			if err := h.handlePing(); err != nil {
				return fmt.Errorf("failed to handle ping: %w", err)
			}
		case ClientData:
			if err := h.handleData(); err != nil {
				return fmt.Errorf("failed to handle data: %w", err)
			}
		default:
			// Handle unknown packet types - might be addendum or other protocol extensions
			h.logger.Debug().Uint8("packet_type", packetType).Msg("Unknown packet type received")
			if err := h.handleUnknownPacket(packetType); err != nil {
				return fmt.Errorf("failed to handle unknown packet %d: %w", packetType, err)
			}
		}
	}
}

// sendHello sends server hello packet
func (h *ConnectionHandler) sendHello() error {
	// Write packet type
	if err := h.writer.WriteByte(ServerHello); err != nil {
		return err
	}
	// Write server name
	if err := h.writer.WriteString("Icebox"); err != nil {
		return err
	}
	// Write major version
	if err := h.writer.WriteUvarint(22); err != nil {
		return err
	}
	// Write minor version
	if err := h.writer.WriteUvarint(3); err != nil {
		return err
	}
	// Write revision
	if err := h.writer.WriteUvarint(54460); err != nil {
		return err
	}
	// Write timezone
	if err := h.writer.WriteString("UTC"); err != nil {
		return err
	}
	// Write display name
	if err := h.writer.WriteString("Icebox"); err != nil {
		return err
	}
	// Write version patch
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}
	return h.writer.Flush()
}

// handleClientHello handles client hello packet
func (h *ConnectionHandler) handleClientHello() error {
	// Read client name
	clientName, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read major version
	majorVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read minor version
	minorVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read protocol version (revision)
	protocolVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read default database
	database, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read username
	username, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read password
	password, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().
		Str("client", clientName).
		Uint64("major", majorVersion).
		Uint64("minor", minorVersion).
		Uint64("protocol_version", protocolVersion).
		Str("database", database).
		Str("username", username).
		Str("password", "***").
		Msg("Client hello received")

	// TODO: Validate password if authentication is implemented
	_ = password

	// No response needed - server hello will be sent by the main handler
	return nil
}

// handleQuery handles client query packet
func (h *ConnectionHandler) handleQuery() error {
	// Read query ID
	queryID, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read client info
	clientInfo, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read query
	query, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().
		Str("query_id", queryID).
		Str("client_info", clientInfo).
		Str("query", query).
		Msg("Query received")

	// TODO: Execute query using engine
	// For now, send a simple response
	return h.sendQueryResponse(query)
}

// handlePing handles client ping packet
func (h *ConnectionHandler) handlePing() error {
	h.logger.Debug().Msg("Ping received")
	return h.sendPong()
}

// handleData handles client data packet
func (h *ConnectionHandler) handleData() error {
	// Read table name
	tableName, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().Str("table", tableName).Msg("Data packet received")

	// TODO: Handle data insertion
	// For now, just acknowledge
	return h.sendDataResponse()
}

// handleCancel handles client cancel packet
func (h *ConnectionHandler) handleCancel() error {
	h.logger.Debug().Msg("Cancel received")
	// TODO: Cancel running query
	return nil
}

// handleAddendum handles the addendum (quota key) sent after handshake
func (h *ConnectionHandler) handleAddendum() error {
	// Try to read addendum as a string (quota key)
	// This is sent by ClickHouse Go client after handshake
	quotaKey, err := h.reader.ReadString()
	if err != nil {
		// If no addendum, that's fine
		return nil
	}
	h.logger.Debug().Str("quota_key", quotaKey).Msg("Addendum received")
	return nil
}

// handleUnknownPacket handles unknown packet types (like addendum)
func (h *ConnectionHandler) handleUnknownPacket(packetType byte) error {
	// Handle addendum (quota key) - this is sent as a string after handshake
	if packetType == 110 { // This might be the addendum packet type
		quotaKey, err := h.reader.ReadString()
		if err != nil {
			return err
		}
		h.logger.Debug().Str("quota_key", quotaKey).Msg("Addendum received")
		return nil
	}

	// For other unknown packets, just ignore them
	h.logger.Debug().Uint8("packet_type", packetType).Msg("Ignoring unknown packet type")
	return nil
}

// sendPong sends pong response
func (h *ConnectionHandler) sendPong() error {
	if err := h.writer.WriteByte(ServerPong); err != nil {
		return err
	}
	return h.writer.Flush()
}

// sendQueryResponse sends query response with mock data based on query content
func (h *ConnectionHandler) sendQueryResponse(query string) error {
	// Parse query to determine appropriate mock response
	mockResponse := h.generateMockResponse(query)

	// Send data packet type
	if err := h.writer.WriteByte(ServerData); err != nil {
		return err
	}

	// Send column count
	if err := h.writer.WriteUvarint(uint64(len(mockResponse.columns))); err != nil {
		return err
	}

	// Send column names and types
	for _, col := range mockResponse.columns {
		if err := h.writer.WriteString(col.name); err != nil {
			return err
		}
		if err := h.writer.WriteString(col.dataType); err != nil {
			return err
		}
	}

	// Send data block
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send row count
	if err := h.writer.WriteUvarint(uint64(len(mockResponse.rows))); err != nil {
		return err
	}

	// Send data row by row
	for _, row := range mockResponse.rows {
		for i, col := range mockResponse.columns {
			if err := h.writeColumnValue(col.dataType, row[i]); err != nil {
				return err
			}
		}
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}

// MockColumn represents a column in mock response
type MockColumn struct {
	name     string
	dataType string
}

// MockRow represents a row of data
type MockRow []interface{}

// MockResponse represents a complete mock response
type MockResponse struct {
	columns []MockColumn
	rows    []MockRow
}

// generateMockResponse generates appropriate mock response based on query
func (h *ConnectionHandler) generateMockResponse(query string) MockResponse {
	// Convert query to lowercase for easier matching
	queryLower := strings.ToLower(query)

	switch {
	case strings.Contains(queryLower, "select 1"):
		return MockResponse{
			columns: []MockColumn{{name: "1", dataType: "UInt8"}},
			rows:    []MockRow{{uint8(1)}},
		}
	case strings.Contains(queryLower, "select 42"):
		return MockResponse{
			columns: []MockColumn{{name: "42", dataType: "UInt8"}},
			rows:    []MockRow{{uint8(42)}},
		}
	case strings.Contains(queryLower, "select 'hello'"):
		return MockResponse{
			columns: []MockColumn{{name: "'hello'", dataType: "String"}},
			rows:    []MockRow{{"hello"}},
		}
	case strings.Contains(queryLower, "select now()"):
		return MockResponse{
			columns: []MockColumn{{name: "now()", dataType: "DateTime"}},
			rows:    []MockRow{{time.Now()}},
		}
	case strings.Contains(queryLower, "select count(*)"):
		return MockResponse{
			columns: []MockColumn{{name: "count()", dataType: "UInt64"}},
			rows:    []MockRow{{uint64(100)}},
		}
	case strings.Contains(queryLower, "select * from users"):
		return MockResponse{
			columns: []MockColumn{
				{name: "id", dataType: "UInt32"},
				{name: "name", dataType: "String"},
				{name: "email", dataType: "String"},
				{name: "created_at", dataType: "DateTime"},
			},
			rows: []MockRow{
				{uint32(1), "Alice", "alice@example.com", time.Now().Add(-24 * time.Hour)},
				{uint32(2), "Bob", "bob@example.com", time.Now().Add(-12 * time.Hour)},
				{uint32(3), "Charlie", "charlie@example.com", time.Now()},
			},
		}
	case strings.Contains(queryLower, "select * from orders"):
		return MockResponse{
			columns: []MockColumn{
				{name: "order_id", dataType: "UInt64"},
				{name: "customer_id", dataType: "UInt32"},
				{name: "amount", dataType: "Float64"},
				{name: "status", dataType: "String"},
			},
			rows: []MockRow{
				{uint64(1001), uint32(1), 99.99, "completed"},
				{uint64(1002), uint32(2), 149.50, "pending"},
				{uint64(1003), uint32(1), 75.25, "shipped"},
			},
		}
	default:
		// Default response for unknown queries
		return MockResponse{
			columns: []MockColumn{{name: "result", dataType: "String"}},
			rows:    []MockRow{{fmt.Sprintf("Mock response for: %s", query)}},
		}
	}
}

// writeColumnValue writes a value for a specific column type
func (h *ConnectionHandler) writeColumnValue(dataType string, value interface{}) error {
	switch dataType {
	case "UInt8":
		if v, ok := value.(uint8); ok {
			return h.writer.WriteByte(v)
		}
		return h.writer.WriteByte(0)
	case "UInt32":
		if v, ok := value.(uint32); ok {
			return h.writer.WriteUint32(v)
		}
		return h.writer.WriteUint32(0)
	case "UInt64":
		if v, ok := value.(uint64); ok {
			return h.writer.WriteUvarint(v)
		}
		return h.writer.WriteUvarint(0)
	case "Float64":
		if v, ok := value.(float64); ok {
			return h.writer.WriteFloat64(v)
		}
		return h.writer.WriteFloat64(0.0)
	case "String":
		if v, ok := value.(string); ok {
			return h.writer.WriteString(v)
		}
		return h.writer.WriteString("")
	case "DateTime":
		if v, ok := value.(time.Time); ok {
			return h.writer.WriteUint32(uint32(v.Unix()))
		}
		return h.writer.WriteUint32(uint32(time.Now().Unix()))
	default:
		// Default to string representation
		return h.writer.WriteString(fmt.Sprintf("%v", value))
	}
}

// sendDataResponse sends data response
func (h *ConnectionHandler) sendDataResponse() error {
	// Send progress
	if err := h.writer.WriteByte(ServerProgress); err != nil {
		return err
	}

	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}
