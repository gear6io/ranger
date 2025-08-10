package native

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

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
	// In-memory storage for tables
	tables map[string][][]interface{}
	mu     sync.RWMutex
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:   conn,
		logger: logger,
		reader: NewPacketReader(conn),
		writer: NewPacketWriter(conn),
		tables: make(map[string][][]interface{}),
	}
}

// Handle handles the client connection
func (h *ConnectionHandler) Handle() error {
	defer h.conn.Close()
	h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("New client connected")

	for {
		// Read message length (4 bytes, big endian)
		messageLength, err := h.reader.ReadUint32()
		if err != nil {
			h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("Client disconnected")
			return nil
		}

		// Read message type (1 byte)
		messageType, err := h.reader.ReadByte()
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to read message type")
			return err
		}

		// Read message payload
		var payload []byte
		if messageLength > 1 {
			// Read the payload directly (without length prefix)
			expectedPayloadLength := int(messageLength - 1)
			h.logger.Info().
				Uint32("message_length", messageLength).
				Int("expected_payload_length", expectedPayloadLength).
				Msg("About to read payload")

			payload = make([]byte, expectedPayloadLength)
			h.logger.Info().Msg("Created payload buffer, about to call io.ReadFull")

			if _, err := io.ReadFull(h.conn, payload); err != nil {
				h.logger.Error().
					Err(err).
					Uint32("message_length", messageLength).
					Int("expected_payload_length", expectedPayloadLength).
					Msg("Failed to read message payload")
				return err
			}
			h.logger.Info().
				Int("payload_length", len(payload)).
				Bytes("payload_bytes", payload).
				Msg("Read message payload")
		}

		h.logger.Debug().Uint8("message_type", messageType).Uint32("length", messageLength).Msg("Received message")

		// Handle message based on type
		switch messageType {
		case ClientHello:
			h.logger.Debug().Msg("Handling ClientHello message")
			if err := h.handleClientHello(payload); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client hello")
				return err
			}
		case ClientQuery:
			h.logger.Debug().Msg("Handling ClientQuery message")
			if err := h.handleClientQuery(payload); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client query")
				return err
			}
		case ClientData:
			h.logger.Debug().Msg("Handling ClientData message")
			if err := h.handleClientData(payload); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client data")
				return err
			}
		case ClientPing:
			h.logger.Debug().Msg("Handling ClientPing message")
			if err := h.handleClientPing(); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client ping")
				return err
			}
		default:
			h.logger.Warn().Uint8("message_type", messageType).Msg("Unknown message type")
			if err := h.sendException(fmt.Sprintf("Unknown message type: %d", messageType)); err != nil {
				return err
			}
		}
	}
}

// handleClientHello handles client hello message
func (h *ConnectionHandler) handleClientHello(payload []byte) error {
	// Parse ClientHello message according to ClickHouse protocol
	// Format: client_name (string) + major_version (varint) + minor_version (varint) + protocol_version (varint)

	h.logger.Debug().
		Int("payload_length", len(payload)).
		Bytes("payload_bytes", payload).
		Msg("Processing ClientHello payload")

	reader := &payloadReader{data: payload}

	// Read client name
	clientName, err := h.readStringFromReader(reader)
	if err != nil {
		return fmt.Errorf("failed to read client name: %w", err)
	}

	// Read client version major
	clientVersionMajor, err := h.readUVarIntFromReader(reader)
	if err != nil {
		return fmt.Errorf("failed to read client version major: %w", err)
	}

	// Read client version minor
	clientVersionMinor, err := h.readUVarIntFromReader(reader)
	if err != nil {
		return fmt.Errorf("failed to read client version minor: %w", err)
	}

	// Read protocol version
	protocolVersion, err := h.readUVarIntFromReader(reader)
	if err != nil {
		return fmt.Errorf("failed to read protocol version: %w", err)
	}

	h.logger.Debug().
		Str("client", clientName).
		Uint64("version", clientVersionMajor).
		Uint64("minor", clientVersionMinor).
		Uint64("protocol", protocolVersion).
		Msg("Client hello received")

	h.logger.Debug().Msg("About to call sendServerHello()")

	// Send server hello response
	err = h.sendServerHello()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to send ServerHello")
		return err
	}

	h.logger.Debug().Msg("sendServerHello() completed successfully")
	return nil
}

// handleClientQuery handles client query message
func (h *ConnectionHandler) handleClientQuery(payload []byte) error {
	// Parse ClientQuery payload according to ClickHouse protocol
	// Format: query string only (no length prefix)

	// Convert payload to query string
	query := string(payload)

	h.logger.Debug().
		Int("payload_length", len(payload)).
		Str("query", query).
		Msg("Query received")

	// Store current query for batch operations
	h.currentQuery = query

	// Check if this is a SELECT query
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		// Execute SELECT query
		if err := h.executeSelectQuery(query); err != nil {
			h.logger.Error().Err(err).Msg("Failed to execute SELECT query")
			return h.sendException(fmt.Sprintf("Failed to execute query: %v", err))
		}
	} else {
		// For non-SELECT queries, send acknowledgment and EndOfStream
		h.logger.Debug().Msg("Sending acknowledgment for non-SELECT query")

		// Send acknowledgment
		ack := fmt.Sprintf("Query received: %s", query)
		if err := h.writer.WriteMessage(ServerData, []byte(ack)); err != nil {
			h.logger.Error().Err(err).Msg("Failed to send query acknowledgment")
			return err
		}

		// Send EndOfStream
		if err := h.writer.WriteMessage(ServerEndOfStream, nil); err != nil {
			h.logger.Error().Err(err).Msg("Failed to send EndOfStream")
			return err
		}

		h.logger.Debug().Msg("Query acknowledgment sent successfully")
	}

	return nil
}

// executeSelectQuery executes a SELECT query and returns the results
func (h *ConnectionHandler) executeSelectQuery(query string) error {
	h.logger.Debug().Str("query", query).Msg("Executing SELECT query")

	// Handle simple queries like "SELECT 1" or "SELECT 'hello'"
	queryLower := strings.ToLower(strings.TrimSpace(query))

	// Check if this is a simple expression query (no FROM clause)
	if !strings.Contains(queryLower, " from ") {
		h.logger.Debug().Msg("Simple expression query detected")
		return h.executeSimpleExpressionQuery(query)
	}

	// Simple query parsing - extract table name from "SELECT ... FROM table_name"
	// This is a very basic implementation
	fromIndex := strings.Index(queryLower, " from ")
	// Note: fromIndex should never be -1 here since we already checked above

	// Extract table name (everything after "FROM " until the end or next space)
	tablePart := query[fromIndex+6:] // Skip "FROM "
	tableName := strings.TrimSpace(tablePart)

	// Remove any trailing parts (ORDER BY, LIMIT, etc.)
	if spaceIndex := strings.Index(tableName, " "); spaceIndex != -1 {
		tableName = tableName[:spaceIndex]
	}

	h.logger.Debug().Str("table", tableName).Msg("Extracted table name from query")

	// Get data from memory
	h.mu.RLock()
	rows, exists := h.tables[tableName]
	h.mu.RUnlock()

	if !exists {
		h.logger.Warn().Str("table", tableName).Msg("Table not found")
		// Return empty result set
		return h.sendEmptyResultSet()
	}

	h.logger.Info().
		Str("table", tableName).
		Int("rows", len(rows)).
		Msg("Sending query results")

	// Send the data rows
	return h.sendQueryResults(tableName, rows)
}

// executeSimpleExpressionQuery handles simple SELECT queries without FROM clauses
func (h *ConnectionHandler) executeSimpleExpressionQuery(query string) error {
	h.logger.Debug().Str("query", query).Msg("Executing simple expression query")

	// Extract the expression part (everything after "SELECT ")
	selectIndex := strings.Index(strings.ToUpper(query), "SELECT")
	if selectIndex == -1 {
		return fmt.Errorf("invalid SELECT query")
	}

	expression := strings.TrimSpace(query[selectIndex+6:]) // Skip "SELECT "
	h.logger.Debug().Str("expression", expression).Msg("Extracted expression")

	// For now, handle simple cases like "SELECT 1" or "SELECT 'hello'"
	// Create a simple result with one column and one row
	columnName := "result"
	columnType := "String"

	// Try to determine the type and value
	var value string
	if expression == "1" {
		value = "1"
		columnType = "UInt32"
	} else if strings.HasPrefix(expression, "'") && strings.HasSuffix(expression, "'") {
		// String literal
		value = expression[1 : len(expression)-1] // Remove quotes
		columnType = "String"
	} else {
		// Default to string representation
		value = expression
		columnType = "String"
	}

	h.logger.Debug().
		Str("column_name", columnName).
		Str("column_type", columnType).
		Str("value", value).
		Msg("Simple expression result")

	// Send the result
	return h.sendSimpleExpressionResult(columnName, columnType, value)
}

// sendSimpleExpressionResult sends a simple expression result
func (h *ConnectionHandler) sendSimpleExpressionResult(columnName, columnType, value string) error {
	h.logger.Debug().Msg("Sending simple expression result")

	// Send ServerData with column metadata and data
	// Format: columnCount (uvarint) + columnName (string) + columnType (string) + dataBlock (uvarint) + rowCount (uvarint) + data (string)

	// Create payload
	payload := make([]byte, 0, 256)

	// Column count = 1
	payload = append(payload, 1) // uvarint 1

	// Column name (4-byte length + content)
	nameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(nameLen, uint32(len(columnName)))
	payload = append(payload, nameLen...)
	payload = append(payload, []byte(columnName)...)

	// Column type (4-byte length + content)
	typeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(typeLen, uint32(len(columnType)))
	payload = append(payload, typeLen...)
	payload = append(payload, []byte(columnType)...)

	// Data block = 0
	payload = append(payload, 0) // uvarint 0

	// Row count = 1
	payload = append(payload, 1) // uvarint 1

	// Data (comma-separated values for the column)
	payload = append(payload, []byte(value)...)

	if err := h.writer.WriteMessage(ServerData, payload); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerData message")
		return err
	}

	// Send EndOfStream
	if err := h.writer.WriteMessage(ServerEndOfStream, nil); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerEndOfStream message")
		return err
	}

	h.logger.Debug().Msg("Simple expression result sent successfully")
	return nil
}

// sendEmptyResultSet sends an empty result set
func (h *ConnectionHandler) sendEmptyResultSet() error {
	h.logger.Debug().Msg("Sending empty result set")

	// Send ServerData with empty content
	if err := h.writer.WriteMessage(ServerData, []byte{}); err != nil {
		return err
	}

	// Send EndOfStream
	return h.writer.WriteMessage(ServerEndOfStream, nil)
}

// sendQueryResults sends query results as ServerData packets
func (h *ConnectionHandler) sendQueryResults(tableName string, rows [][]interface{}) error {
	h.logger.Debug().Str("table", tableName).Int("rows", len(rows)).Msg("Sending query results")

	if len(rows) == 0 {
		h.logger.Debug().Msg("No rows to send, sending empty result set")
		return h.sendEmptyResultSet()
	}

	// Get column names and types from the first row
	// For now, we'll use the hardcoded schema since that's what we stored
	columnNames := []string{"id", "name", "email", "created_at"}
	columnTypes := []string{"UInt32", "String", "String", "DateTime"}

	// For each column, send a ServerData packet
	for colIdx, columnName := range columnNames {
		var payload []byte

		// Column count (uvarint) - always 1 for single column
		columnCount := uint64(1)
		columnCountBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(columnCountBytes, columnCount)
		payload = append(payload, columnCountBytes[:n]...)

		// Column name (string) - 4-byte length + string content
		columnNameLength := uint32(len(columnName))
		columnNameLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(columnNameLengthBytes, columnNameLength)
		payload = append(payload, columnNameLengthBytes...)
		payload = append(payload, []byte(columnName)...)

		// Column type (string) - 4-byte length + string content
		columnType := columnTypes[colIdx]
		columnTypeLength := uint32(len(columnType))
		columnTypeLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(columnTypeLengthBytes, columnTypeLength)
		payload = append(payload, columnTypeLengthBytes...)
		payload = append(payload, []byte(columnType)...)

		// Data block (uvarint) - placeholder for now
		dataBlock := uint64(1)
		dataBlockBytes := make([]byte, binary.MaxVarintLen64)
		n = binary.PutUvarint(dataBlockBytes, dataBlock)
		payload = append(payload, dataBlockBytes[:n]...)

		// Row count (uvarint)
		rowCount := uint64(len(rows))
		rowCountBytes := make([]byte, binary.MaxVarintLen64)
		n = binary.PutUvarint(rowCountBytes, rowCount)
		payload = append(payload, rowCountBytes[:n]...)

		// Data (string) - values for this column, comma-separated
		var dataBuilder strings.Builder
		for i, row := range rows {
			if colIdx < len(row) {
				dataBuilder.WriteString(fmt.Sprintf("%v", row[colIdx]))
			}
			if i < len(rows)-1 {
				dataBuilder.WriteString(",")
			}
		}
		data := dataBuilder.String()
		dataLength := uint32(len(data))
		dataLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(dataLengthBytes, dataLength)
		payload = append(payload, dataLengthBytes...)
		payload = append(payload, []byte(data)...)

		h.logger.Debug().
			Str("column", columnName).
			Str("type", columnType).
			Int("rows", len(rows)).
			Str("data", data).
			Msg("Sending ServerData for column")

		// Send the result as ServerData for this column
		if err := h.writer.WriteMessage(ServerData, payload); err != nil {
			h.logger.Error().Err(err).Str("column", columnName).Msg("Failed to write ServerData message")
			return err
		}
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream after all columns")
	// Send EndOfStream after all columns
	return h.writer.WriteMessage(ServerEndOfStream, nil)
}

// handleClientData handles client data message
func (h *ConnectionHandler) handleClientData(payload []byte) error {
	h.logger.Debug().Msg("Client data received")

	// Parse the data block
	block, err := h.parseDataBlock(payload)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to parse data block")
		return h.sendException(fmt.Sprintf("Failed to parse data block: %v", err))
	}

	h.logger.Info().
		Str("table", block.TableName).
		Int("columns", len(block.Columns)).
		Int("rows", len(block.Rows)).
		Msg("Processing data block")

	// Process the data block
	if err := h.processDataBlock(block); err != nil {
		h.logger.Error().Err(err).Msg("Failed to process data block")
		return h.sendException(fmt.Sprintf("Failed to process data block: %v", err))
	}

	// Send success response
	return h.sendDataResponse()
}

// handleClientPing handles client ping message
func (h *ConnectionHandler) handleClientPing() error {
	h.logger.Debug().Msg("Ping received")
	return h.sendPong()
}

// parseDataBlock parses the data block from payload
func (h *ConnectionHandler) parseDataBlock(payload []byte) (*DataBlock, error) {
	// Create a reader for the payload
	reader := &payloadReader{data: payload}

	// Read table name
	tableName, err := h.readStringFromReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read table name: %w", err)
	}

	// Read column count (4 bytes, big endian)
	if len(reader.data)-reader.pos < 4 {
		return nil, fmt.Errorf("insufficient data for column count")
	}
	columnCount := binary.BigEndian.Uint32(reader.data[reader.pos : reader.pos+4])
	reader.pos += 4

	// Read row count (4 bytes, big endian)
	if len(reader.data)-reader.pos < 4 {
		return nil, fmt.Errorf("insufficient data for row count")
	}
	rowCount := binary.BigEndian.Uint32(reader.data[reader.pos : reader.pos+4])
	reader.pos += 4

	// Read column names
	columns := make([]string, columnCount)
	for i := uint32(0); i < columnCount; i++ {
		columns[i], err = h.readStringFromReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read column name %d: %w", i, err)
		}
	}

	// Read column types (simplified - all strings for now)
	columnTypes := make([]string, columnCount)
	for i := uint32(0); i < columnCount; i++ {
		columnTypes[i] = "String"
	}

	// Read row data
	rows := make([][]interface{}, rowCount)
	for i := uint32(0); i < rowCount; i++ {
		rows[i] = make([]interface{}, columnCount)
		for j := uint32(0); j < columnCount; j++ {
			value, err := h.readStringFromReader(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to read value at row %d, col %d: %w", i, j, err)
			}
			rows[i][j] = value
		}
	}

	return &DataBlock{
		TableName:   tableName,
		Columns:     columns,
		ColumnTypes: columnTypes,
		Rows:        rows,
	}, nil
}

// processDataBlock processes the received data block
func (h *ConnectionHandler) processDataBlock(block *DataBlock) error {
	h.logger.Info().
		Str("table", block.TableName).
		Interface("columns", block.Columns).
		Interface("rows", len(block.Rows)).
		Msg("Processing data block")

	// Store the data in memory
	h.mu.Lock()
	defer h.mu.Unlock()

	// Initialize table if it doesn't exist
	if h.tables[block.TableName] == nil {
		h.tables[block.TableName] = make([][]interface{}, 0)
	}

	// Append the new rows
	h.tables[block.TableName] = append(h.tables[block.TableName], block.Rows...)

	h.logger.Info().
		Str("table", block.TableName).
		Int("total_rows", len(h.tables[block.TableName])).
		Msg("Data stored successfully")

	return nil
}

// sendServerHello sends server hello response
func (h *ConnectionHandler) sendServerHello() error {
	h.logger.Debug().Msg("Starting to send ServerHello response")

	// Create ServerHello message according to ClickHouse protocol
	// Format: server_name (string) + major_version (varint) + minor_version (varint) + revision (varint) + timezone (string) + display_name (string) + version_patch (varint)

	// Build payload
	var payload []byte

	// Server name
	serverName := "Icebox Server"
	serverNameBytes := []byte(serverName)
	serverNameLen := uint32(len(serverNameBytes))

	// Write server name length (4 bytes, big endian)
	nameLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nameLenBytes, serverNameLen)
	payload = append(payload, nameLenBytes...)

	// Write server name
	payload = append(payload, serverNameBytes...)

	// Server version major (1.0.0)
	payload = append(payload, 1)

	// Server version minor
	payload = append(payload, 0)

	// Server revision (using DBMS_TCP_PROTOCOL_VERSION)
	revision := uint64(DBMS_TCP_PROTOCOL_VERSION)
	for revision >= 0x80 {
		payload = append(payload, byte(revision)|0x80)
		revision >>= 7
	}
	payload = append(payload, byte(revision))

	// Timezone
	timezone := "UTC"
	timezoneBytes := []byte(timezone)
	timezoneLen := uint32(len(timezoneBytes))
	timezoneLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(timezoneLenBytes, timezoneLen)
	payload = append(payload, timezoneLenBytes...)
	payload = append(payload, timezoneBytes...)

	// Display name
	displayName := "Icebox"
	displayNameBytes := []byte(displayName)
	displayNameLen := uint32(len(displayNameBytes))
	displayNameLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(displayNameLenBytes, displayNameLen)
	payload = append(payload, displayNameLenBytes...)
	payload = append(payload, displayNameBytes...)

	// Version patch
	versionPatch := uint64(0)
	for versionPatch >= 0x80 {
		payload = append(payload, byte(versionPatch)|0x80)
		versionPatch >>= 7
	}
	payload = append(payload, byte(versionPatch))

	h.logger.Debug().
		Int("payload_length", len(payload)).
		Bytes("payload_bytes", payload).
		Msg("Sending ServerHello response")

	err := h.writer.WriteMessage(ServerHello, payload)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to send ServerHello")
		return err
	}

	h.logger.Info().Msg("ServerHello sent successfully")
	h.logger.Debug().Msg("sendServerHello function completed successfully")
	return nil
}

// sendQueryResponse sends query response
func (h *ConnectionHandler) sendQueryResponse(query string) error {
	// For now, just send a simple acknowledgment
	response := fmt.Sprintf("Query received: %s", query)
	payload := []byte(response)

	h.logger.Debug().Msg("Sending ServerData response")
	// Send the data response
	if err := h.writer.WriteMessage(ServerData, payload); err != nil {
		return err
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream response")
	// Send EndOfStream to indicate completion
	return h.writer.WriteMessage(ServerEndOfStream, nil)
}

// sendDataResponse sends data processing response
func (h *ConnectionHandler) sendDataResponse() error {
	response := "Data block processed successfully"
	payload := []byte(response)

	h.logger.Debug().Msg("Sending ServerData response")
	if err := h.writer.WriteMessage(ServerData, payload); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerData message")
		return err
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream response")
	// Send EndOfStream to indicate completion
	if err := h.writer.WriteMessage(ServerEndOfStream, nil); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerEndOfStream message")
		return err
	}

	h.logger.Debug().Msg("sendDataResponse completed successfully")
	return nil
}

// sendPong sends pong response
func (h *ConnectionHandler) sendPong() error {
	h.logger.Debug().
		Uint8("packet_type", ServerPong).
		Msg("Sending pong response")
	return h.writer.WriteMessage(ServerPong, nil)
}

// sendException sends exception response
func (h *ConnectionHandler) sendException(errorMsg string) error {
	payload := []byte(errorMsg)
	return h.writer.WriteMessage(ServerException, payload)
}

// payloadReader is a helper to read from a byte slice
type payloadReader struct {
	data []byte
	pos  int
}

func (r *payloadReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// readStringFromReader reads a string from a payload reader
func (h *ConnectionHandler) readStringFromReader(reader *payloadReader) (string, error) {
	// Read string length (4 bytes, big endian)
	if len(reader.data)-reader.pos < 4 {
		return "", fmt.Errorf("insufficient data for string length")
	}

	length := binary.BigEndian.Uint32(reader.data[reader.pos : reader.pos+4])
	reader.pos += 4

	h.logger.Debug().
		Int("string_length", int(length)).
		Int("remaining_data", len(reader.data)-reader.pos).
		Int("position", reader.pos).
		Msg("Reading string from payload")

	if length == 0 {
		return "", nil
	}

	if len(reader.data)-reader.pos < int(length) {
		return "", fmt.Errorf("insufficient data for string content")
	}

	str := string(reader.data[reader.pos : reader.pos+int(length)])
	reader.pos += int(length)
	return str, nil
}

// readUVarIntFromReader reads a varint from a payload reader
func (h *ConnectionHandler) readUVarIntFromReader(reader *payloadReader) (uint64, error) {
	var value uint64
	var shift uint

	for {
		if reader.pos >= len(reader.data) {
			return 0, fmt.Errorf("unexpected end of data while reading varint")
		}

		b := reader.data[reader.pos]
		reader.pos++

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
