package native

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// ConnectionHandler handles a single client connection
type ConnectionHandler struct {
	conn         net.Conn
	logger       zerolog.Logger
	reader       *PacketReader
	writer       *PacketWriter
	helloSent    bool
	currentQuery string
	// In-memory storage for tables (table name -> rows)
	tableStorage map[string][][]interface{}
	tableColumns map[string][]ColumnMetadata
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:      conn,
		logger:    logger,
		reader:    NewPacketReader(conn),
		writer:    NewPacketWriter(conn),
		helloSent: false,
		tableStorage: make(map[string][][]interface{}),
		tableColumns: make(map[string][]ColumnMetadata),
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
		case ClientAddendum:
			if err := h.handleAddendum(); err != nil {
				return fmt.Errorf("failed to handle addendum: %w", err)
			}
		default:
			// Handle unknown packet types - might be other protocol extensions
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
	if err := h.readClientInfo(); err != nil {
		return err
	}

	// Read settings
	if err := h.readSettings(); err != nil {
		return err
	}

	// Read empty string marker for end of settings
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read interserver secret (if revision >= 54441)
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read state and compression
	_, err = h.reader.ReadByte() // state
	if err != nil {
		return err
	}
	_, err = h.reader.ReadByte() // compression
	if err != nil {
		return err
	}

	// Read query body
	query, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read parameters (if revision >= 54459)
	if err := h.readParameters(); err != nil {
		return err
	}

	h.logger.Debug().
		Str("query_id", queryID).
		Str("query", query).
		Msg("Query received")

	// Store the current query
	h.currentQuery = query

	// Send response immediately after processing the query
	return h.sendQueryResponse(query)
}

// handlePing handles client ping packet
func (h *ConnectionHandler) handlePing() error {
	h.logger.Debug().Msg("Ping received")
	return h.sendPong()
}

// handleData handles client data packet for batch insert
func (h *ConnectionHandler) handleData() error {
	// Read table name
	tableName, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().Str("table", tableName).Msg("Data packet received")

	// Read client info (skip for now)
	if err := h.readClientInfo(); err != nil {
		return err
	}

	// Read external table info (skip for now)
	if err := h.skipExternalTable(); err != nil {
		return err
	}

	// Read block info
	if err := h.readBlockInfo(); err != nil {
		return err
	}

	// Read column count
	columnCount, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	h.logger.Debug().Uint64("column_count", columnCount).Msg("Reading column metadata")

	// Read row count
	rowCount, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	h.logger.Debug().Uint64("row_count", rowCount).Msg("Reading data rows")

	// Check if this is an empty block (like after SELECT queries or to close batch)
	if rowCount == 0 && columnCount == 0 {
		h.logger.Debug().Msg("Empty data block received - sending end of stream")
		// Send end of stream for empty blocks (query close)
		if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
			return err
		}
		return h.writer.Flush()
	}

	// Read column metadata (only for non-empty blocks)
	columns := make([]ColumnMetadata, columnCount)
	for i := uint64(0); i < columnCount; i++ {
		columnName, err := h.reader.ReadString()
		if err != nil {
			return err
		}

		columnType, err := h.reader.ReadString()
		if err != nil {
			return err
		}

		columns[i] = ColumnMetadata{
			Name: columnName,
			Type: columnType,
		}

		h.logger.Debug().
			Uint64("column_index", i).
			Str("name", columnName).
			Str("type", columnType).
			Msg("Column metadata read")
	}

	// Read data rows (only for non-empty blocks)
	rows := make([][]interface{}, rowCount)
	for rowIndex := uint64(0); rowIndex < rowCount; rowIndex++ {
		row := make([]interface{}, columnCount)
		for colIndex := uint64(0); colIndex < columnCount; colIndex++ {
			value, err := h.readColumnValue(columns[colIndex].Type)
			if err != nil {
				return err
			}
			row[colIndex] = value
		}
		rows[rowIndex] = row
	}

	h.logger.Debug().
		Str("table", tableName).
		Uint64("rows", rowCount).
		Uint64("columns", columnCount).
		Msg("Batch insert data received")

	// Process the batch insert
	if err := h.processBatchInsert(tableName, columns, rows); err != nil {
		return err
	}

	// Send success response
	return h.sendBatchInsertResponse()
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
	// For unknown packets, just ignore them
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
	// Check if this is a CREATE TABLE statement
	queryLower := strings.ToLower(strings.TrimSpace(query))
	if strings.HasPrefix(queryLower, "create table") {
		// For CREATE TABLE, send empty data block
		h.logger.Debug().Str("query", query).Msg("Sending empty data block for CREATE TABLE")

		// Send data packet type
		if err := h.writer.WriteByte(ServerData); err != nil {
			return err
		}

		// Send table name (empty string for query responses)
		if err := h.writer.WriteString(""); err != nil {
			return err
		}

		// Send block info
		if err := h.writeBlockInfo(); err != nil {
			return err
		}

		// Send column count (0)
		if err := h.writer.WriteUvarint(0); err != nil {
			return err
		}

		// Send row count (0)
		if err := h.writer.WriteUvarint(0); err != nil {
			return err
		}

		// Send end of stream
		if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
			return err
		}

		return h.writer.Flush()
	}

	// Check if this is a SELECT query
	if strings.HasPrefix(queryLower, "select") {
		h.logger.Debug().Str("query", query).Msg("Processing SELECT query")
		
		// Extract table name from query (simple parsing for now)
		tableName := h.extractTableNameFromQuery(query)
		if tableName == "" {
			// If we can't extract table name, return empty result
			h.logger.Debug().Msg("Could not extract table name, returning empty result")
			return h.sendEmptyDataBlock()
		}

		// Check if table exists in storage
		rows, exists := h.tableStorage[tableName]
		if !exists {
			h.logger.Debug().Str("table", tableName).Msg("Table not found in storage, returning empty result")
			return h.sendEmptyDataBlock()
		}

		columns, exists := h.tableColumns[tableName]
		if !exists {
			h.logger.Debug().Str("table", tableName).Msg("Table columns not found in storage, returning empty result")
			return h.sendEmptyDataBlock()
		}

		h.logger.Debug().
			Str("table", tableName).
			Int("rows", len(rows)).
			Int("columns", len(columns)).
			Msg("Sending data from storage")

		// Send data packet type
		if err := h.writer.WriteByte(ServerData); err != nil {
			return err
		}

		// Send table name (empty string for query responses)
		if err := h.writer.WriteString(""); err != nil {
			return err
		}

		// Send block info
		if err := h.writeBlockInfo(); err != nil {
			return err
		}

		// Send column count
		if err := h.writer.WriteUvarint(uint64(len(columns))); err != nil {
			return err
		}

		// Send column names and types
		for _, col := range columns {
			if err := h.writer.WriteString(col.Name); err != nil {
				return err
			}
			if err := h.writer.WriteString(col.Type); err != nil {
				return err
			}
			// Send custom serialization flag (false) for revision >= 54454
			if err := h.writer.WriteByte(0); err != nil { // false
				return err
			}
		}

		// Send row count
		if err := h.writer.WriteUvarint(uint64(len(rows))); err != nil {
			return err
		}

		// Send data row by row
		for _, row := range rows {
			for i, col := range columns {
				if err := h.writeColumnValue(col.Type, row[i]); err != nil {
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

	// For other queries, send mock data
	h.logger.Debug().Str("query", query).Msg("Sending mock data for query")

	// Generate mock response
	mockResponse := h.generateMockResponse(query)

	// Send data packet type
	if err := h.writer.WriteByte(ServerData); err != nil {
		return err
	}

	// Send table name (empty string for query responses)
	if err := h.writer.WriteString(""); err != nil {
		return err
	}

	// Send block info
	if err := h.writeBlockInfo(); err != nil {
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
		// Send custom serialization flag (false) for revision >= 54454
		if err := h.writer.WriteByte(0); err != nil { // false
			return err
		}
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
	case strings.Contains(queryLower, "select * from test_batch_users"):
		return MockResponse{
			columns: []MockColumn{
				{name: "id", dataType: "UInt32"},
				{name: "name", dataType: "String"},
				{name: "email", dataType: "String"},
				{name: "created_at", dataType: "DateTime"},
			},
			rows: []MockRow{
				{uint32(1), "Alice Johnson", "alice@example.com", time.Now().Add(-2 * time.Hour)},
				{uint32(2), "Bob Smith", "bob@example.com", time.Now().Add(-1 * time.Hour)},
				{uint32(3), "Charlie Brown", "charlie@example.com", time.Now()},
			},
		}
	default:
		// Default response for unknown queries
		return MockResponse{
			columns: []MockColumn{{name: "message", dataType: "String"}},
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

// readClientInfo reads the client info structure
func (h *ConnectionHandler) readClientInfo() error {
	// Read client info structure based on clickhouse-go encoding
	// This matches the encodeClientInfo function in clickhouse-go

	// Read query kind (ClientQueryInitial = 1)
	_, err := h.reader.ReadByte()
	if err != nil {
		return err
	}

	// Read initial_user
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read initial_query_id
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read initial_address
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read initial_query_start_time_microseconds (if revision >= 54449)
	_, err = h.reader.ReadUvarint() // Read as int64 but we'll read as uvarint for now
	if err != nil {
		return err
	}

	// Read interface (tcp = 1, http = 2)
	_, err = h.reader.ReadByte()
	if err != nil {
		return err
	}

	// Read os_user
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read hostname
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read client_name
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read client_version_major
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read client_version_minor
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read client_tcp_protocol_version
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read quota_key (if revision >= 54060)
	_, err = h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read distributed_depth (if revision >= 54448)
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read version_patch (if revision >= 54401)
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read OpenTelemetry info (if revision >= 54442)
	_, err = h.reader.ReadByte() // trace_context
	if err != nil {
		return err
	}

	// Read parallel replicas info (if revision >= 54453)
	_, err = h.reader.ReadUvarint() // collaborate_with_initiator
	if err != nil {
		return err
	}
	_, err = h.reader.ReadUvarint() // count_participating_replicas
	if err != nil {
		return err
	}
	_, err = h.reader.ReadUvarint() // number_of_current_replica
	if err != nil {
		return err
	}

	return nil
}

// readSettings reads the settings structure
func (h *ConnectionHandler) readSettings() error {
	// Read settings until we find an empty string marker
	for {
		settingKey, err := h.reader.ReadString()
		if err != nil {
			return err
		}

		// If we get an empty string, we've reached the end of settings
		if settingKey == "" {
			break
		}

		// Read setting value (simplified - just read as string)
		_, err = h.reader.ReadString()
		if err != nil {
			return err
		}
	}

	return nil
}

// readParameters reads the parameters structure
func (h *ConnectionHandler) readParameters() error {
	// Read parameters until we find an empty string marker
	for {
		paramKey, err := h.reader.ReadString()
		if err != nil {
			return err
		}

		// If we get an empty string, we've reached the end of parameters
		if paramKey == "" {
			break
		}

		// Read parameter value (simplified - just read as string)
		_, err = h.reader.ReadString()
		if err != nil {
			return err
		}
	}

	return nil
}

// skipExternalTable skips external table data
func (h *ConnectionHandler) skipExternalTable() error {
	// For now, just skip external table data
	// This is a simplified implementation
	return nil
}

// readEmptyBlock reads an empty data block
func (h *ConnectionHandler) readBlockInfo() error {
	// Read block info structure
	// This includes compression info, block number, etc.

	// Read compression flag
	_, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read block number
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read is_overflows flag
	_, err = h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read bucket_num as int32
	buf := make([]byte, 4)
	_, err = io.ReadFull(h.reader.conn, buf)
	if err != nil {
		return err
	}
	_ = int32(binary.LittleEndian.Uint32(buf)) // bucket_num

	return nil
}

// writeBlockInfo writes the BlockInfo structure required by ClickHouse protocol
func (h *ConnectionHandler) writeBlockInfo() error {
	// BlockInfo structure: num1, isOverflows, num2, bucketNum, num3
	// Based on clickhouse-go implementation
	if err := h.writer.WriteUvarint(1); err != nil { // num1
		return err
	}
	if err := h.writer.WriteByte(0); err != nil { // isOverflows (bool as byte)
		return err
	}
	if err := h.writer.WriteUvarint(2); err != nil { // num2
		return err
	}
	// Write -1 as int32 (little endian)
	bucketNumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bucketNumBytes, 0xFFFFFFFF)   // -1 as uint32
	if err := h.writer.WriteBytes(bucketNumBytes); err != nil { // bucketNum (int32)
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // num3
		return err
	}
	return nil
}

// ColumnMetadata represents column information for batch insert
type ColumnMetadata struct {
	Name string
	Type string
}

// readColumnValue reads a single column value based on its type
func (h *ConnectionHandler) readColumnValue(dataType string) (interface{}, error) {
	switch dataType {
	case "UInt8":
		val, err := h.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		return val, nil
	case "UInt16":
		// Read as UInt16 (2 bytes)
		buf := make([]byte, 2)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.Uint16(buf), nil
	case "UInt32":
		val, err := h.reader.ReadUint32()
		if err != nil {
			return nil, err
		}
		return val, nil
	case "UInt64":
		// Read as UInt64 (8 bytes)
		buf := make([]byte, 8)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	case "Int8":
		val, err := h.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		return int8(val), nil
	case "Int16":
		// Read as Int16 (2 bytes)
		buf := make([]byte, 2)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		return int16(binary.LittleEndian.Uint16(buf)), nil
	case "Int32":
		// Read as Int32 (4 bytes)
		buf := make([]byte, 4)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		return int32(binary.LittleEndian.Uint32(buf)), nil
	case "Int64":
		// Read as Int64 (8 bytes)
		buf := make([]byte, 8)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		return int64(binary.LittleEndian.Uint64(buf)), nil
	case "Float32":
		// Read as Float32 (4 bytes)
		buf := make([]byte, 4)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint32(buf)
		return float32(math.Float32frombits(bits)), nil
	case "Float64":
		// Read as Float64 (8 bytes)
		buf := make([]byte, 8)
		_, err := io.ReadFull(h.reader.conn, buf)
		if err != nil {
			return nil, err
		}
		bits := binary.LittleEndian.Uint64(buf)
		return math.Float64frombits(bits), nil
	case "String":
		val, err := h.reader.ReadString()
		if err != nil {
			return nil, err
		}
		return val, nil
	case "DateTime":
		val, err := h.reader.ReadUint32()
		if err != nil {
			return nil, err
		}
		return time.Unix(int64(val), 0), nil
	case "Date":
		val, err := h.reader.ReadUint32()
		if err != nil {
			return nil, err
		}
		return time.Unix(int64(val*86400), 0), nil
	default:
		// For unknown types, read as string
		val, err := h.reader.ReadString()
		if err != nil {
			return nil, err
		}
		return val, nil
	}
}

// processBatchInsert processes the batch insert data
func (h *ConnectionHandler) processBatchInsert(tableName string, columns []ColumnMetadata, rows [][]interface{}) error {
	h.logger.Info().
		Str("table", tableName).
		Int("rows", len(rows)).
		Int("columns", len(columns)).
		Msg("Processing batch insert")

	// Store columns and rows for the table
	h.tableColumns[tableName] = columns
	h.tableStorage[tableName] = rows

	// TODO: Integrate with actual storage system
	// For now, just log the data
	for i, row := range rows {
		h.logger.Debug().
			Int("row_index", i).
			Interface("data", row).
			Msg("Row data")
	}

	// Here you would typically:
	// 1. Validate the table exists
	// 2. Validate column types match
	// 3. Write data to storage (Parquet files, etc.)
	// 4. Update metadata

	return nil
}

// sendBatchInsertResponse sends response for batch insert
func (h *ConnectionHandler) sendBatchInsertResponse() error {
	// Send progress packet
	if err := h.writer.WriteByte(ServerProgress); err != nil {
		return err
	}

	// Write progress info
	if err := h.writer.WriteUvarint(1); err != nil { // rows read
		return err
	}
	if err := h.writer.WriteUvarint(1); err != nil { // bytes read
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // total rows to read
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // total bytes to read
		return err
	}

	// Send profile info (optional)
	if err := h.writer.WriteByte(ServerProfileInfo); err != nil {
		return err
	}

	// Write profile info data
	if err := h.writer.WriteUvarint(0); err != nil { // rows
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // blocks
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // bytes
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // applied_limit
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // rows_before_limit
		return err
	}
	if err := h.writer.WriteUvarint(0); err != nil { // calculated_rows_before_limit
		return err
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}

// extractTableNameFromQuery extracts table name from a SELECT query
func (h *ConnectionHandler) extractTableNameFromQuery(query string) string {
	// Simple parsing for "SELECT ... FROM table_name" queries
	queryLower := strings.ToLower(query)
	fromIndex := strings.Index(queryLower, " from ")
	if fromIndex == -1 {
		return ""
	}
	
	// Extract everything after "FROM"
	afterFrom := query[fromIndex+6:] // 6 = len(" from ")
	
	// Find the end of the table name (space, semicolon, or end of string)
	endIndex := len(afterFrom)
	for i, char := range afterFrom {
		if char == ' ' || char == ';' || char == '\n' || char == '\r' {
			endIndex = i
			break
		}
	}
	
	tableName := strings.TrimSpace(afterFrom[:endIndex])
	return tableName
}

// sendEmptyDataBlock sends an empty data block response
func (h *ConnectionHandler) sendEmptyDataBlock() error {
	// Send data packet type
	if err := h.writer.WriteByte(ServerData); err != nil {
		return err
	}

	// Send table name (empty string for query responses)
	if err := h.writer.WriteString(""); err != nil {
		return err
	}

	// Send block info
	if err := h.writeBlockInfo(); err != nil {
		return err
	}

	// Send column count (0)
	if err := h.writer.WriteUvarint(0); err != nil {
		return err
	}

	// Send row count (0)
	if err := h.writer.WriteUvarint(0); err != nil {
		return err
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}
