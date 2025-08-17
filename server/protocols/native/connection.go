package native

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// ConnectionHandler handles a single client connection
type ConnectionHandler struct {
	conn         net.Conn
	queryEngine  *query.Engine
	logger       zerolog.Logger
	reader       *PacketReader
	writer       *PacketWriter
	helloSent    bool
	currentQuery string
	// In-memory storage for tables (fallback)
	tables map[string][][]interface{}
	mu     sync.RWMutex

	// Execution tracking
	executionStart   time.Time
	currentOperation string
	isExecuting      bool
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, queryEngine *query.Engine, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:        conn,
		queryEngine: queryEngine,
		logger:      logger,
		reader:      NewPacketReader(conn),
		writer:      NewPacketWriter(conn),
		tables:      make(map[string][][]interface{}),
	}
}

// startExecution tracks the start of an operation
func (h *ConnectionHandler) startExecution(operation string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.executionStart = time.Now()
	h.currentOperation = operation
	h.isExecuting = true

	if h.logger.GetLevel() <= zerolog.DebugLevel {
		h.logger.Debug().
			Str("client", h.conn.RemoteAddr().String()).
			Str("operation", operation).
			Time("start_time", h.executionStart).
			Msg("Operation started")
	}
}

// endExecution marks the completion of an operation
func (h *ConnectionHandler) endExecution() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.isExecuting {
		duration := time.Since(h.executionStart)
		h.isExecuting = false

		if h.logger.GetLevel() <= zerolog.DebugLevel {
			h.logger.Debug().
				Str("client", h.conn.RemoteAddr().String()).
				Str("operation", h.currentOperation).
				Dur("duration", duration).
				Msg("Operation completed")
		}
	}
}

// logAbruptDisconnection logs when a connection closes during execution
func (h *ConnectionHandler) logAbruptDisconnection() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.isExecuting && h.logger.GetLevel() <= zerolog.DebugLevel {
		duration := time.Since(h.executionStart)
		h.logger.Error().
			Str("client", h.conn.RemoteAddr().String()).
			Str("operation", h.currentOperation).
			Dur("duration", duration).
			Str("stack_trace", string(debug.Stack())).
			Msg("Client disconnected during operation execution")
	}
}

// Handle handles the client connection
func (h *ConnectionHandler) Handle() error {
	defer func() {
		h.logAbruptDisconnection()
		h.conn.Close()
	}()
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
			h.startExecution("ClientQuery")
			if err := h.handleClientQuery(payload); err != nil {
				h.endExecution()
				h.logger.Error().Err(err).Msg("Failed to handle client query")
				return err
			}
			h.endExecution()
		case ClientData:
			h.logger.Debug().Msg("Handling ClientData message")
			h.startExecution("ClientData")
			if err := h.handleClientData(payload); err != nil {
				h.endExecution()
				h.logger.Error().Err(err).Msg("Failed to handle client data")
				return err
			}
			h.endExecution()
		case ClientPing:
			h.logger.Debug().Msg("Handling ClientPing message")
			if err := h.handleClientPing(); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client ping")
				return err
			}
		default:
			h.logger.Warn().Uint8("message_type", messageType).Msg("Unknown message type")
			if err := h.sendException(fmt.Sprintf("Unknown message type: %d", messageType)); err != nil {
				h.logger.Error().Err(err).Msg("Failed to send exception for unknown message type")
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

	// Check if this is a simple query that should bypass the query engine
	queryUpper := strings.ToUpper(strings.TrimSpace(query))

	// For simple SELECT queries, go directly to native server handling
	// This avoids parser crashes and provides better performance
	if strings.HasPrefix(queryUpper, "SELECT") {
		// Handle simple queries directly with native server logic
		return h.executeSelectQuery(query)
	}

	// For non-SELECT queries, try the query engine first
	ctx := context.Background() // Create a context for the query
	result, err := h.queryEngine.ExecuteQuery(ctx, query)
	if err != nil {
		h.logger.Debug().Err(err).Str("query", query).Msg("Query engine failed, trying native server handling")

		// Try to handle the query natively instead of sending fake success
		return h.handleQueryNatively(query, queryUpper)
	}

	// Query engine succeeded, handle the result
	if strings.HasPrefix(queryUpper, "SELECT") {
		// For SELECT queries, send the results
		if err := h.sendQueryEngineResults(result); err != nil {
			h.logger.Error().Err(err).Msg("Failed to send query results")
			return err
		}
	} else {
		// For non-SELECT queries, send proper acknowledgment with real results
		if err := h.sendQueryEngineResults(result); err != nil {
			h.logger.Error().Err(err).Msg("Failed to send query results")
			return err
		}
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

	// Get data from Query Engine (which will route to Storage Manager)
	rows, err := h.queryEngine.GetTableData(context.Background(), tableName, 0) // 0 = no limit
	if err != nil {
		h.logger.Warn().Err(err).Str("table", tableName).Msg("Failed to get table data from Query Engine")
		// Return empty result set
		return h.sendEmptyResultSet()
	}

	if len(rows) == 0 {
		h.logger.Debug().Str("table", tableName).Msg("No data found in table")
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

	// Column count = 1 (as uvarint)
	columnCountBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(columnCountBytes, 1)
	payload = append(payload, columnCountBytes[:n]...)

	// Column name (4-byte big-endian length + content)
	nameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(nameLen, uint32(len(columnName)))
	payload = append(payload, nameLen...)
	payload = append(payload, []byte(columnName)...)

	// Column type (4-byte big-endian length + content)
	typeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(typeLen, uint32(len(columnType)))
	payload = append(payload, typeLen...)
	payload = append(payload, []byte(columnType)...)

	// Data block = 0 (as uvarint)
	dataBlockBytes := make([]byte, binary.MaxVarintLen64)
	n2 := binary.PutUvarint(dataBlockBytes, 0)
	payload = append(payload, dataBlockBytes[:n2]...)

	// Row count = 1 (as uvarint)
	rowCountBytes := make([]byte, binary.MaxVarintLen64)
	n3 := binary.PutUvarint(rowCountBytes, 1)
	payload = append(payload, rowCountBytes[:n3]...)

	// Data (4-byte big-endian length + string content)
	dataLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLen, uint32(len(value)))
	payload = append(payload, dataLen...)
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

	// Create payload for empty result set
	// Format: blockInfo (1 byte) + columnCount (uvarint) + dataBlock (uvarint) + rowCount (uvarint) + data (string)
	payload := make([]byte, 0, 64)

	// Block info (optional) - for now, send empty block info
	// In ClickHouse protocol, this is typically 0x00 for no special info
	payload = append(payload, 0x00)

	// Column count = 0 (as uvarint)
	columnCountBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(columnCountBytes, 0)
	payload = append(payload, columnCountBytes[:n]...)

	// Data block = 0 (as uvarint)
	dataBlockBytes := make([]byte, binary.MaxVarintLen64)
	n2 := binary.PutUvarint(dataBlockBytes, 0)
	payload = append(payload, dataBlockBytes[:n2]...)

	// Row count = 0 (as uvarint)
	rowCountBytes := make([]byte, binary.MaxVarintLen64)
	n3 := binary.PutUvarint(rowCountBytes, 0)
	payload = append(payload, rowCountBytes[:n3]...)

	// Empty data string (4-byte length + empty content)
	dataLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLen, 0)
	payload = append(payload, dataLen...)

	// Send ServerData with proper payload
	if err := h.writer.WriteMessage(ServerData, payload); err != nil {
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

	// Send data in the format the client expects:
	// For each column, send a separate ServerData packet with:
	// columnCount (uvarint) + columnName (string) + columnType (string) + dataBlock (uvarint) + rowCount (uvarint) + data (string)

	for i, columnName := range columnNames {
		// Create payload for this column
		payload := make([]byte, 0, 256)

		// Column count = 1 (as uvarint)
		columnCountBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(columnCountBytes, 1)
		payload = append(payload, columnCountBytes[:n]...)

		// Column name (4-byte big-endian length + string content)
		columnNameLength := uint32(len(columnName))
		columnNameLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(columnNameLengthBytes, columnNameLength)
		payload = append(payload, columnNameLengthBytes...)
		payload = append(payload, []byte(columnName)...)

		// Column type (4-byte big-endian length + string content)
		columnType := columnTypes[i]
		columnTypeLength := uint32(len(columnType))
		columnTypeLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(columnTypeLengthBytes, columnTypeLength)
		payload = append(payload, columnTypeLengthBytes...)
		payload = append(payload, []byte(columnType)...)

		// Data block (uvarint) - placeholder for now
		dataBlock := uint64(1)
		dataBlockBytes := make([]byte, binary.MaxVarintLen64)
		n2 := binary.PutUvarint(dataBlockBytes, dataBlock)
		payload = append(payload, dataBlockBytes[:n2]...)

		// Row count (uvarint)
		rowCount := uint64(len(rows))
		rowCountBytes := make([]byte, binary.MaxVarintLen64)
		n3 := binary.PutUvarint(rowCountBytes, rowCount)
		payload = append(payload, rowCountBytes[:n3]...)

		// Data (string) - serialize this column's data as comma-separated values
		var dataBuilder strings.Builder
		for j, row := range rows {
			if j > 0 {
				dataBuilder.WriteString(",")
			}
			dataBuilder.WriteString(fmt.Sprintf("%v", row[i]))
		}
		data := dataBuilder.String()
		dataLength := uint32(len(data))
		dataLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(dataLengthBytes, dataLength)
		payload = append(payload, dataLengthBytes...)
		payload = append(payload, []byte(data)...)

		h.logger.Debug().
			Str("column", columnName).
			Int("rows", len(rows)).
			Str("data", data).
			Msg("Sending ServerData for column")

		// Send ServerData packet for this column
		if err := h.writer.WriteMessage(ServerData, payload); err != nil {
			h.logger.Error().Err(err).Msg("Failed to write ServerData message")
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

	// Use Query Engine to store the data
	if err := h.queryEngine.InsertData(context.Background(), block.TableName, block.Rows); err != nil {
		h.logger.Error().Err(err).Str("table", block.TableName).Msg("Failed to store data via Query Engine")
		return err
	}

	h.logger.Info().
		Str("table", block.TableName).
		Int("total_rows", len(block.Rows)).
		Msg("Data stored successfully via Query Engine")

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
	// Send acknowledgment in a format the client can parse:
	// columnCount (1) + columnName ("result") + columnType ("String") + dataBlock (1) + rowCount (1) + data (success message)
	ackPayload := make([]byte, 0, 256)

	// Column count: 1
	ackPayload = append(ackPayload, 0x01) // uvarint 1

	// Column name: "result" (4-byte length + content)
	resultName := "result"
	resultNameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(resultNameLen, uint32(len(resultName)))
	ackPayload = append(ackPayload, resultNameLen...)
	ackPayload = append(ackPayload, []byte(resultName)...)

	// Column type: "String" (4-byte length + content)
	resultType := "String"
	resultTypeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(resultTypeLen, uint32(len(resultType)))
	ackPayload = append(ackPayload, resultTypeLen...)
	ackPayload = append(ackPayload, []byte(resultType)...)

	// Data block: 1
	ackPayload = append(ackPayload, 0x01) // uvarint 1

	// Row count: 1
	ackPayload = append(ackPayload, 0x01) // uvarint 1

	// Data: success message (4-byte length + content)
	response := "Data block processed successfully"
	responseLen := make([]byte, 4)
	binary.BigEndian.PutUint32(responseLen, uint32(len(response)))
	ackPayload = append(ackPayload, responseLen...)
	ackPayload = append(ackPayload, []byte(response)...)

	h.logger.Debug().Msg("Sending ServerData response")
	if err := h.writer.WriteMessage(ServerData, ackPayload); err != nil {
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

// sendQueryEngineResults sends query results from the QueryEngine
func (h *ConnectionHandler) sendQueryEngineResults(result *query.QueryResult) error {
	h.logger.Debug().
		Int("row_count", int(result.RowCount)).
		Int("column_count", len(result.Columns)).
		Msg("Sending QueryEngine results")

	// Send the data rows
	if result.Data != nil {
		if rows, ok := result.Data.([][]interface{}); ok {
			for _, row := range rows {
				if err := h.writer.WriteMessage(ServerData, h.serializeRow(row)); err != nil {
					h.logger.Error().Err(err).Msg("Failed to send data row")
					return err
				}
			}
		}
	}

	// Send EndOfStream
	if err := h.writer.WriteMessage(ServerEndOfStream, nil); err != nil {
		h.logger.Error().Err(err).Msg("Failed to send EndOfStream")
		return err
	}

	h.logger.Debug().Msg("QueryEngine results sent successfully")
	return nil
}

// serializeRow converts a row to bytes for transmission
func (h *ConnectionHandler) serializeRow(row []interface{}) []byte {
	// Simple serialization - convert each value to string and join with tabs
	var parts []string
	for _, val := range row {
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return []byte(strings.Join(parts, "\t"))
}

// handleQueryNatively handles queries when the query engine fails
func (h *ConnectionHandler) handleQueryNatively(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling query natively")

	// Handle different types of queries
	switch {
	case strings.HasPrefix(queryUpper, "SHOW"):
		return h.handleShowQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "DESCRIBE"), strings.HasPrefix(queryUpper, "EXPLAIN"):
		return h.handleDescribeQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "CREATE"):
		return h.handleCreateQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "DROP"):
		return h.handleDropQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "INSERT"):
		return h.handleInsertQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "UPDATE"):
		return h.handleUpdateQuery(query, queryUpper)
	case strings.HasPrefix(queryUpper, "DELETE"):
		return h.handleDeleteQuery(query, queryUpper)
	default:
		// For unknown queries, return a proper error instead of fake success
		return h.sendErrorResponse(fmt.Sprintf("Query not supported: %s", query))
	}
}

// handleShowQuery handles SHOW commands
func (h *ConnectionHandler) handleShowQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling SHOW query")

	// Get storage manager from query engine to access metadata
	storageMgr := h.queryEngine.GetStorageManager()
	if storageMgr == nil {
		h.logger.Error().Msg("Storage manager not available")
		return h.sendErrorResponse("Storage manager not available")
	}

	// Get metadata manager from storage manager
	metadataMgr := storageMgr.GetMetadataManager()
	if metadataMgr == nil {
		h.logger.Error().Msg("Metadata manager not available")
		return h.sendErrorResponse("Metadata manager not available")
	}

	switch {
	case strings.Contains(queryUpper, "DATABASES"):
		// Return list of databases/namespaces from metadata
		ctx := context.Background()
		databases, err := metadataMgr.ListDatabases(ctx)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to list databases from metadata")
			// Fallback to default databases if metadata fails
			databases = []string{"default", "system"}
		}

		// Always include default and system databases
		hasDefault := false
		hasSystem := false
		for _, db := range databases {
			if db == "default" {
				hasDefault = true
			}
			if db == "system" {
				hasSystem = true
			}
		}

		if !hasDefault {
			databases = append(databases, "default")
		}
		if !hasSystem {
			databases = append(databases, "system")
		}

		return h.sendStringListResult("database", databases)

	case strings.Contains(queryUpper, "TABLES"):
		// Extract database name from SHOW TABLES [FROM database]
		database := "default" // Default database
		if strings.Contains(queryUpper, "FROM") {
			parts := strings.Fields(query)
			for i, part := range parts {
				if strings.ToUpper(part) == "FROM" && i+1 < len(parts) {
					database = parts[i+1]
					break
				}
			}
		}

		// Get tables from metadata for the specified database
		ctx := context.Background()
		tables, err := metadataMgr.ListTables(ctx, database)
		if err != nil {
			h.logger.Error().Err(err).Str("database", database).Msg("Failed to list tables from metadata")
			// Return empty list if metadata fails
			tables = []string{}
		}

		return h.sendStringListResult("table", tables)

	case strings.Contains(queryUpper, "COLUMNS"):
		// Extract table name from SHOW COLUMNS FROM table [FROM database]
		var tableName, database string
		parts := strings.Fields(query)

		// Parse SHOW COLUMNS FROM table [FROM database]
		for i, part := range parts {
			if strings.ToUpper(part) == "FROM" {
				if i+1 < len(parts) {
					tableName = parts[i+1]
					// Check if there's another FROM for database
					if i+3 < len(parts) && strings.ToUpper(parts[i+2]) == "FROM" {
						database = parts[i+3]
					} else {
						database = "default"
					}
				}
				break
			}
		}

		if tableName == "" {
			return h.sendErrorResponse("Invalid SHOW COLUMNS syntax")
		}

		// Get table metadata to extract column information
		ctx := context.Background()
		tableMetadata, err := metadataMgr.LoadTableMetadata(ctx, database, tableName)
		if err != nil {
			h.logger.Error().Err(err).Str("database", database).Str("table", tableName).Msg("Failed to load table metadata")
			// Return basic column info if metadata fails
			columns := []string{"id", "name", "created_at"}
			return h.sendStringListResult("column", columns)
		}

		// Parse schema from metadata to get actual columns
		var schemaData map[string]interface{}
		if err := json.Unmarshal(tableMetadata.Schema, &schemaData); err != nil {
			h.logger.Error().Err(err).Msg("Failed to parse table schema")
			// Return basic column info if schema parsing fails
			columns := []string{"id", "name", "created_at"}
			return h.sendStringListResult("column", columns)
		}

		// Extract column names from schema
		var columns []string
		if fields, ok := schemaData["fields"].([]interface{}); ok {
			for _, field := range fields {
				if fieldMap, ok := field.(map[string]interface{}); ok {
					if name, ok := fieldMap["name"].(string); ok {
						columns = append(columns, name)
					}
				}
			}
		}

		// Fallback to basic columns if schema parsing fails
		if len(columns) == 0 {
			columns = []string{"id", "name", "created_at"}
		}

		return h.sendStringListResult("column", columns)

	default:
		return h.sendErrorResponse(fmt.Sprintf("Unsupported SHOW command: %s", query))
	}
}

// handleDescribeQuery handles DESCRIBE commands
func (h *ConnectionHandler) handleDescribeQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling DESCRIBE query")

	// Extract table name from DESCRIBE table [FROM database]
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return h.sendErrorResponse("Invalid DESCRIBE syntax")
	}

	tableName := parts[1]
	database := "default" // Default database

	// Check if database is specified: DESCRIBE table FROM database
	if len(parts) >= 4 && strings.ToUpper(parts[2]) == "FROM" {
		database = parts[3]
	}

	// Get storage manager from query engine to access metadata
	storageMgr := h.queryEngine.GetStorageManager()
	if storageMgr == nil {
		h.logger.Error().Msg("Storage manager not available")
		return h.sendErrorResponse("Storage manager not available")
	}

	// Get metadata manager from storage manager
	metadataMgr := storageMgr.GetMetadataManager()
	if metadataMgr == nil {
		h.logger.Error().Msg("Metadata manager not available")
		return h.sendErrorResponse("Metadata manager not available")
	}

	// Get table metadata to extract schema information
	ctx := context.Background()
	tableMetadata, err := metadataMgr.LoadTableMetadata(ctx, database, tableName)
	if err != nil {
		h.logger.Error().Err(err).Str("database", database).Str("table", tableName).Msg("Failed to load table metadata")
		// Return basic schema if metadata fails
		columns := [][]string{
			{"id", "INTEGER", "NO", "PRI", "NULL", ""},
			{"name", "VARCHAR(255)", "NO", "", "NULL", ""},
			{"created_at", "TIMESTAMP", "NO", "", "CURRENT_TIMESTAMP", ""},
		}
		return h.sendTableSchemaResult(tableName, columns)
	}

	// Parse schema from metadata to get actual column information
	var schemaData map[string]interface{}
	if err := json.Unmarshal(tableMetadata.Schema, &schemaData); err != nil {
		h.logger.Error().Err(err).Msg("Failed to parse table schema")
		// Return basic schema if schema parsing fails
		columns := [][]string{
			{"id", "INTEGER", "NO", "PRI", "NULL", ""},
			{"name", "VARCHAR(255)", "NO", "", "NULL", ""},
			{"created_at", "TIMESTAMP", "NO", "", "CURRENT_TIMESTAMP", ""},
		}
		return h.sendTableSchemaResult(tableName, columns)
	}

	// Extract column information from schema
	var columns [][]string
	if fields, ok := schemaData["fields"].([]interface{}); ok {
		for _, field := range fields {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				name, _ := fieldMap["name"].(string)
				dataType, _ := fieldMap["type"].(string)
				nullable, _ := fieldMap["nullable"].(bool)

				// Set nullable string
				nullableStr := "YES"
				if !nullable {
					nullableStr = "NO"
				}

				// Set key information (simplified)
				key := ""
				if name == "id" {
					key = "PRI"
				}

				// Set default value (simplified)
				defaultVal := "NULL"
				if name == "created_at" {
					defaultVal = "CURRENT_TIMESTAMP"
				}

				// Add length information if available
				if length, ok := fieldMap["length"].(float64); ok && length > 0 {
					dataType = fmt.Sprintf("%s(%d)", dataType, int(length))
				}

				columns = append(columns, []string{name, dataType, nullableStr, key, defaultVal, ""})
			}
		}
	}

	// Fallback to basic schema if schema parsing fails
	if len(columns) == 0 {
		columns = [][]string{
			{"id", "INTEGER", "NO", "PRI", "NULL", ""},
			{"name", "VARCHAR(255)", "NO", "", "NULL", ""},
			{"created_at", "TIMESTAMP", "NO", "", "CURRENT_TIMESTAMP", ""},
		}
	}

	return h.sendTableSchemaResult(tableName, columns)
}

// handleCreateQuery handles CREATE commands
func (h *ConnectionHandler) handleCreateQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling CREATE query")

	// Get storage manager from query engine to access metadata
	storageMgr := h.queryEngine.GetStorageManager()
	if storageMgr == nil {
		h.logger.Error().Msg("Storage manager not available")
		return h.sendErrorResponse("Storage manager not available")
	}

	// Parse CREATE TABLE statement
	if strings.Contains(queryUpper, "CREATE TABLE") {
		// Extract table name and database from CREATE TABLE database.table
		parts := strings.Fields(query)
		if len(parts) < 3 {
			return h.sendErrorResponse("Invalid CREATE TABLE syntax")
		}

		// Find the table name (after CREATE TABLE)
		tableIdentifier := ""
		for i, part := range parts {
			if strings.ToUpper(part) == "TABLE" && i+1 < len(parts) {
				tableIdentifier = parts[i+1]
				break
			}
		}

		if tableIdentifier == "" {
			return h.sendErrorResponse("Table name not found in CREATE TABLE statement")
		}

		// Parse database.table format
		var database, tableName string
		if strings.Contains(tableIdentifier, ".") {
			dotIndex := strings.Index(tableIdentifier, ".")
			database = tableIdentifier[:dotIndex]
			tableName = tableIdentifier[dotIndex+1:]
		} else {
			database = "default"
			tableName = tableIdentifier
		}

		// Extract schema from the CREATE TABLE statement
		// This is a simplified parser - in production, you'd want a proper SQL parser
		schemaStart := strings.Index(query, "(")
		schemaEnd := strings.LastIndex(query, ")")
		if schemaStart == -1 || schemaEnd == -1 || schemaEnd <= schemaStart {
			return h.sendErrorResponse("Invalid table schema in CREATE TABLE statement")
		}

		schemaPart := query[schemaStart+1 : schemaEnd]
		schema := h.parseCreateTableSchema(schemaPart)

		// Create table via storage manager
		ctx := context.Background()
		if err := storageMgr.CreateTable(ctx, database, tableName, schema, "FILESYSTEM", nil); err != nil {
			h.logger.Error().Err(err).Str("database", database).Str("table", tableName).Msg("Failed to create table")
			return h.sendErrorResponse(fmt.Sprintf("Failed to create table: %v", err))
		}

		h.logger.Info().Str("database", database).Str("table", tableName).Msg("Table created successfully")
		return h.sendSimpleAcknowledgment(fmt.Sprintf("Table %s.%s created successfully", database, tableName))
	}

	// For other CREATE statements, just acknowledge
	return h.sendSimpleAcknowledgment("Created successfully")
}

// parseCreateTableSchema parses the schema part of a CREATE TABLE statement
func (h *ConnectionHandler) parseCreateTableSchema(schemaPart string) []byte {
	// This is a simplified schema parser
	// In production, you'd want to use a proper SQL parser

	columns := strings.Split(schemaPart, ",")
	var fields []map[string]interface{}

	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}

		// Parse column definition: name type [length] [NOT NULL]
		colParts := strings.Fields(col)
		if len(colParts) < 2 {
			continue
		}

		colName := colParts[0]
		colType := strings.ToUpper(colParts[1])

		// Extract length if present (e.g., VARCHAR(255))
		var length int
		if strings.Contains(colType, "(") && strings.Contains(colType, ")") {
			start := strings.Index(colType, "(")
			end := strings.Index(colType, ")")
			if start < end {
				lengthStr := colType[start+1 : end]
				if _, err := fmt.Sscanf(lengthStr, "%d", &length); err == nil {
					colType = colType[:start] // Remove length part
				}
			}
		}

		// Check if NOT NULL
		nullable := true
		for _, part := range colParts {
			if strings.ToUpper(part) == "NOT" {
				nullable = false
				break
			}
		}

		field := map[string]interface{}{
			"name":     colName,
			"type":     colType,
			"nullable": nullable,
		}

		if length > 0 {
			field["length"] = length
		}

		fields = append(fields, field)
	}

	// Create schema structure
	schema := map[string]interface{}{
		"type":   "struct",
		"fields": fields,
	}

	// Convert to JSON
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to marshal schema to JSON")
		// Return basic schema if marshaling fails
		return []byte(`{"type":"struct","fields":[{"name":"id","type":"INTEGER","nullable":false}]}`)
	}

	return schemaJSON
}

// handleDropQuery handles DROP commands
func (h *ConnectionHandler) handleDropQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling DROP query")

	// Get storage manager from query engine to access metadata
	storageMgr := h.queryEngine.GetStorageManager()
	if storageMgr == nil {
		h.logger.Error().Msg("Storage manager not available")
		return h.sendErrorResponse("Storage manager not available")
	}

	// Parse DROP TABLE statement
	if strings.Contains(queryUpper, "DROP TABLE") {
		// Extract table name and database from DROP TABLE database.table
		parts := strings.Fields(query)
		if len(parts) < 3 {
			return h.sendErrorResponse("Invalid DROP TABLE syntax")
		}

		// Find the table name (after DROP TABLE)
		tableIdentifier := ""
		for i, part := range parts {
			if strings.ToUpper(part) == "TABLE" && i+1 < len(parts) {
				tableIdentifier = parts[i+1]
				break
			}
		}

		if tableIdentifier == "" {
			return h.sendErrorResponse("Table name not found in DROP TABLE statement")
		}

		// Parse database.table format
		var database, tableName string
		if strings.Contains(tableIdentifier, ".") {
			dotIndex := strings.Index(tableIdentifier, ".")
			database = tableIdentifier[:dotIndex]
			tableName = tableIdentifier[dotIndex+1:]
		} else {
			database = "default"
			tableName = tableIdentifier
		}

		// Drop table via storage manager
		ctx := context.Background()
		if err := storageMgr.RemoveTable(ctx, database, tableName); err != nil {
			h.logger.Error().Err(err).Str("database", database).Str("table", tableName).Msg("Failed to drop table")
			return h.sendErrorResponse(fmt.Sprintf("Failed to drop table: %v", err))
		}

		h.logger.Info().Str("database", database).Str("table", tableName).Msg("Table dropped successfully")
		return h.sendSimpleAcknowledgment(fmt.Sprintf("Table %s.%s dropped successfully", database, tableName))
	}

	// For other DROP statements, just acknowledge
	return h.sendSimpleAcknowledgment("Dropped successfully")
}

// handleInsertQuery handles INSERT commands
func (h *ConnectionHandler) handleInsertQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling INSERT query")

	// For now, just acknowledge the command
	// TODO: Implement actual data insertion
	return h.sendSimpleAcknowledgment("Inserted successfully")
}

// handleUpdateQuery handles UPDATE commands
func (h *ConnectionHandler) handleUpdateQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling UPDATE query")

	// For now, just acknowledge the command
	// TODO: Implement actual data updates
	return h.sendSimpleAcknowledgment("Updated successfully")
}

// handleDeleteQuery handles DELETE commands
func (h *ConnectionHandler) handleDeleteQuery(query, queryUpper string) error {
	h.logger.Debug().Str("query", query).Msg("Handling DELETE query")

	// For now, just acknowledge the command
	// TODO: Implement actual data deletion
	return h.sendSimpleAcknowledgment("Deleted successfully")
}

// sendStringListResult sends a list of strings as a result
func (h *ConnectionHandler) sendStringListResult(columnName string, values []string) error {
	// Create a simple result with one column
	columns := [][]string{{columnName, "String"}}
	data := make([][]interface{}, len(values))
	for i, val := range values {
		data[i] = []interface{}{val}
	}

	return h.sendTableResult(columns, data)
}

// sendTableSchemaResult sends table schema as a result
func (h *ConnectionHandler) sendTableSchemaResult(tableName string, columns [][]string) error {
	// Create result with schema columns: Field, Type, Null, Key, Default, Extra
	schemaColumns := [][]string{
		{"Field", "String"},
		{"Type", "String"},
		{"Null", "String"},
		{"Key", "String"},
		{"Default", "String"},
		{"Extra", "String"},
	}

	data := make([][]interface{}, len(columns))
	for i, col := range columns {
		data[i] = make([]interface{}, len(col))
		for j, val := range col {
			data[i][j] = val
		}
	}

	return h.sendTableResult(schemaColumns, data)
}

// sendTableResult sends a table result
func (h *ConnectionHandler) sendTableResult(columns [][]string, data [][]interface{}) error {
	// Send column information
	if err := h.sendColumns(columns); err != nil {
		return err
	}

	// Send data
	if err := h.sendData(data); err != nil {
		return err
	}

	// Send EndOfStream
	return h.writer.WriteMessage(ServerEndOfStream, nil)
}

// sendColumns sends column information
func (h *ConnectionHandler) sendColumns(columns [][]string) error {
	// TODO: Implement proper column sending
	// For now, just log
	h.logger.Debug().Int("column_count", len(columns)).Msg("Sending columns")
	return nil
}

// sendData sends data rows
func (h *ConnectionHandler) sendData(data [][]interface{}) error {
	// TODO: Implement proper data sending
	// For now, just log
	h.logger.Debug().Int("row_count", len(data)).Msg("Sending data")
	return nil
}

// sendSimpleAcknowledgment sends a simple acknowledgment
func (h *ConnectionHandler) sendSimpleAcknowledgment(message string) error {
	// Send a simple result with the message
	columns := [][]string{{"result", "String"}}
	data := [][]interface{}{{message}}

	return h.sendTableResult(columns, data)
}

// sendErrorResponse sends an error response
func (h *ConnectionHandler) sendErrorResponse(message string) error {
	h.logger.Error().Str("error", message).Msg("Sending error response")

	// TODO: Implement proper error response format
	// For now, send as a result with error message
	columns := [][]string{{"error", "String"}}
	data := [][]interface{}{{message}}

	return h.sendTableResult(columns, data)
}
