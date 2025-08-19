package native

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/protocols/native/middleware"
	"github.com/TFMV/icebox/server/protocols/native/protocol"
	"github.com/TFMV/icebox/server/protocols/native/protocol/signals"
	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// DataBlock represents a block of data for transfer
type DataBlock struct {
	TableName string
	Columns   []string
	Rows      [][]interface{}
}

// Protocol constants for backward compatibility
const (
	DBMS_TCP_PROTOCOL_VERSION = 54460
)

// ConnectionHandler handles a single client connection
type ConnectionHandler struct {
	conn         net.Conn
	queryEngine  *query.Engine
	logger       zerolog.Logger
	codec        *protocol.DefaultCodec
	helloSent    bool
	currentQuery string
	// In-memory storage for tables (fallback)
	tables map[string][][]interface{}
	mu     sync.RWMutex

	// Middleware system
	connCtx    *middleware.ConnectionContext
	middleware *middleware.Chain
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, queryEngine *query.Engine, logger zerolog.Logger, middlewareChain *middleware.Chain) *ConnectionHandler {
	// Create registry and factory
	registry := protocol.NewRegistry()
	factory := protocol.NewSignalFactory()

	// Create signal instances for auto-registration
	signals := []protocol.Signal{
		&signals.ClientHello{},
		&signals.ClientQuery{},
		&signals.ClientData{},
		&signals.ClientCancel{},
		&signals.ClientPing{},
		&signals.ServerHello{},
		&signals.ServerData{},
		&signals.ServerException{},
		&signals.ServerProgress{},
		&signals.ServerPong{},
		&signals.ServerEndOfStream{},
		&signals.ServerProfileInfo{},
	}

	// Auto-register all signals with proper error handling
	if err := protocol.RegisterMultipleSignals(signals, registry, factory); err != nil {
		logger.Error().Err(err).Msg("Failed to register signals")
		// In a real implementation, you might want to handle this error more gracefully
		// For now, we'll log it and continue, but this could cause issues
	}

	// Create codec with factory
	codec := protocol.NewDefaultCodec(registry, factory)

	// Create connection context
	connCtx := &middleware.ConnectionContext{
		ClientAddr:   conn.RemoteAddr().String(),
		ConnectionID: generateConnectionID(),
		StartTime:    time.Now(),
		State:        middleware.StateHandshaking,
	}

	return &ConnectionHandler{
		conn:        conn,
		queryEngine: queryEngine,
		logger:      logger,
		codec:       codec,
		tables:      make(map[string][][]interface{}),
		connCtx:     connCtx,
		middleware:  middlewareChain,
	}
}

// generateConnectionID generates a unique connection ID
func generateConnectionID() string {
	// Simple implementation - in production you might want a more robust ID
	return fmt.Sprintf("conn_%d", time.Now().UnixNano())
}

// Handle handles the client connection
func (h *ConnectionHandler) Handle() error {
	defer func() {
		// Notify middleware of disconnection
		h.middleware.Execute(context.Background(), h.connCtx, middleware.EventDisconnected, nil)
		h.conn.Close()
	}()

	// Notify middleware of connection
	if err := h.middleware.Execute(context.Background(), h.connCtx, middleware.EventConnected, nil); err != nil {
		h.logger.Error().Err(err).Msg("Middleware rejected connection")
		return err
	}

	h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("New client connected")

	for {
		// Notify middleware before read
		if err := h.middleware.ExecuteRead(context.Background(), h.connCtx); err != nil {
			h.logger.Error().Err(err).Msg("Middleware rejected read operation")
			return err
		}

		// Read and decode message using unified protocol
		message, err := h.codec.ReadMessage(h.conn)
		if err != nil {
			if err == io.EOF {
				h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("Client disconnected")
				return nil
			}

			// Notify middleware of read error
			h.middleware.ExecuteError(context.Background(), h.connCtx, err)
			h.logger.Error().Err(err).Msg("Failed to read message")
			return err
		}

		// Unpack the message into a signal
		signal, err := h.codec.UnpackSignal(message)
		if err != nil {
			// Notify middleware of protocol error
			h.middleware.ExecuteError(context.Background(), h.connCtx, err)
			h.logger.Error().Err(err).Msg("Failed to unpack message")
			return err
		}

		h.logger.Debug().Uint8("message_type", uint8(signal.Type())).Msg("Received message")

		// Handle message based on type
		switch signal.Type() {
		case protocol.ClientHello:
			h.logger.Debug().Msg("Handling ClientHello message")
			if err := h.handleClientHelloSignal(signal.(*signals.ClientHello)); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client hello")
				return err
			}
		case protocol.ClientQuery:
			h.logger.Debug().Msg("Handling ClientQuery message")
			if err := h.handleClientQuerySignal(context.Background(), signal.(*signals.ClientQuery)); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client query")
				return err
			}
		case protocol.ClientData:
			h.logger.Debug().Msg("Handling ClientData message")
			if err := h.handleClientDataSignal(signal.(*signals.ClientData)); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client data")
				return err
			}
		case protocol.ClientPing:
			h.logger.Debug().Msg("Handling ClientPing message")
			if err := h.handleClientPing(); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client ping")
				return err
			}
		case protocol.ClientCancel:
			h.logger.Debug().Msg("Handling ClientCancel message")
			if err := h.handleClientCancelSignal(signal.(*signals.ClientCancel)); err != nil {
				h.logger.Error().Err(err).Msg("Failed to handle client cancel")
				return err
			}
		default:
			h.logger.Warn().Uint8("message_type", uint8(signal.Type())).Msg("Unknown message type")
			if err := h.sendException(fmt.Sprintf("Unknown message type: %d", signal.Type())); err != nil {
				h.logger.Error().Err(err).Msg("Failed to send exception for unknown message type")
				return err
			}
		}
	}
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

	// Empty data string (as uvarint length + empty content)
	dataLenBytes := make([]byte, binary.MaxVarintLen64)
	dataLenN := binary.PutUvarint(dataLenBytes, 0)
	payload = append(payload, dataLenBytes[:dataLenN]...)

	// Send ServerData with proper payload
	if err := h.sendServerDataWithPayload(payload); err != nil {
		return err
	}

	// Send EndOfStream
	return h.sendServerEndOfStreamSignal()
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

		// Column name (as uvarint length + string content)
		columnNameLengthBytes := make([]byte, binary.MaxVarintLen64)
		columnNameLengthN := binary.PutUvarint(columnNameLengthBytes, uint64(len(columnName)))
		payload = append(payload, columnNameLengthBytes[:columnNameLengthN]...)
		payload = append(payload, []byte(columnName)...)

		// Column type (as uvarint length + string content)
		columnType := columnTypes[i]
		columnTypeLengthBytes := make([]byte, binary.MaxVarintLen64)
		columnTypeLengthN := binary.PutUvarint(columnTypeLengthBytes, uint64(len(columnType)))
		payload = append(payload, columnTypeLengthBytes[:columnTypeLengthN]...)
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
		dataLengthBytes := make([]byte, binary.MaxVarintLen64)
		dataLengthN := binary.PutUvarint(dataLengthBytes, uint64(len(data)))
		payload = append(payload, dataLengthBytes[:dataLengthN]...)
		payload = append(payload, []byte(data)...)

		h.logger.Debug().
			Str("column", columnName).
			Int("rows", len(rows)).
			Str("data", data).
			Msg("Sending ServerData for column")

		// Send ServerData packet for this column
		if err := h.sendServerDataWithPayload(payload); err != nil {
			h.logger.Error().Err(err).Msg("Failed to write ServerData message")
			return err
		}
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream after all columns")
	// Send EndOfStream after all columns
	return h.sendServerEndOfStreamSignal()
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
	return h.sendServerPongSignal()
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
		TableName: tableName,
		Columns:   columns,
		Rows:      rows,
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

	err := h.sendServerHelloSignal()
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
	if err := h.sendServerDataWithPayload(payload); err != nil {
		return err
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream response")
	// Send EndOfStream to indicate completion
	return h.sendServerEndOfStreamSignal()
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
	if err := h.sendServerDataWithPayload(ackPayload); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerData message")
		return err
	}

	h.logger.Debug().Msg("Sending ServerEndOfStream response")
	// Send EndOfStream to indicate completion
	if err := h.sendServerEndOfStreamSignal(); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerEndOfStream message")
		return err
	}

	h.logger.Debug().Msg("sendDataResponse completed successfully")
	return nil
}

// sendException sends exception response
func (h *ConnectionHandler) sendException(errorMsg string) error {
	return h.sendExceptionSignal(errorMsg)
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

	// Check if we have data to send
	if result.Data == nil || len(result.Columns) == 0 {
		h.logger.Debug().Msg("No data or columns to send, sending empty result set")
		return h.sendEmptyResultSet()
	}

	// Extract rows from the result
	var rows [][]interface{}
	if dataRows, ok := result.Data.([][]interface{}); ok {
		rows = dataRows
	} else {
		h.logger.Warn().Msg("Result data is not in expected format, sending empty result set")
		return h.sendEmptyResultSet()
	}

	if len(rows) == 0 {
		h.logger.Debug().Msg("No rows to send, sending empty result set")
		return h.sendEmptyResultSet()
	}

	// Send data in the format the client expects:
	// For each column, send a separate ServerData packet with:
	// columnCount (uvarint) + columnName (string) + columnType (string) + dataBlock (uvarint) + rowCount (uvarint) + data (string)

	for i, columnName := range result.Columns {
		// Determine column type (for now, use String as default)
		columnType := "String"

		// Extract values for this column from all rows
		var columnValues []string
		for _, row := range rows {
			if i < len(row) {
				// Convert the value to string
				value := fmt.Sprintf("%v", row[i])
				columnValues = append(columnValues, value)
			} else {
				// If row doesn't have enough columns, add NULL
				columnValues = append(columnValues, "NULL")
			}
		}

		// Join all values for this column with commas
		dataString := strings.Join(columnValues, ",")

		// Create payload for this column
		payload := make([]byte, 0, 256)

		// Column count = 1 (as uvarint)
		columnCountBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(columnCountBytes, 1)
		payload = append(payload, columnCountBytes[:n]...)

		// Column name (as uvarint length + content)
		nameLenBytes := make([]byte, binary.MaxVarintLen64)
		n2 := binary.PutUvarint(nameLenBytes, uint64(len(columnName)))
		payload = append(payload, nameLenBytes[:n2]...)
		payload = append(payload, []byte(columnName)...)

		// Column type (as uvarint length + content)
		typeLenBytes := make([]byte, binary.MaxVarintLen64)
		n3 := binary.PutUvarint(typeLenBytes, uint64(len(columnType)))
		payload = append(payload, typeLenBytes[:n3]...)
		payload = append(payload, []byte(columnType)...)

		// Data block = 0 (as uvarint)
		dataBlockBytes := make([]byte, binary.MaxVarintLen64)
		n4 := binary.PutUvarint(dataBlockBytes, 0)
		payload = append(payload, dataBlockBytes[:n4]...)

		// Row count (as uvarint)
		rowCountBytes := make([]byte, binary.MaxVarintLen64)
		n5 := binary.PutUvarint(rowCountBytes, uint64(len(rows)))
		payload = append(payload, rowCountBytes[:n5]...)

		// Data (as uvarint length + content)
		dataLenBytes := make([]byte, binary.MaxVarintLen64)
		n6 := binary.PutUvarint(dataLenBytes, uint64(len(dataString)))
		payload = append(payload, dataLenBytes[:n6]...)
		payload = append(payload, []byte(dataString)...)

		// Send ServerData for this column
		if err := h.sendServerDataWithPayload(payload); err != nil {
			h.logger.Error().Err(err).Msg("Failed to send ServerData for column")
			return err
		}
	}

	// Send EndOfStream
	if err := h.sendServerEndOfStreamSignal(); err != nil {
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
	return h.sendServerEndOfStreamSignal()
}

// sendColumns sends column information
func (h *ConnectionHandler) sendColumns(columns [][]string) error {
	// Send column metadata using unified protocol
	h.logger.Debug().Int("column_count", len(columns)).Msg("Sending columns")

	// Create empty data for column metadata message
	emptyData := [][]interface{}{}
	return h.sendServerDataSignal(columns, emptyData)
}

// sendData sends data rows
func (h *ConnectionHandler) sendData(data [][]interface{}) error {
	// Send data rows using unified protocol
	h.logger.Debug().Int("row_count", len(data)).Msg("Sending data")

	if len(data) == 0 {
		// Send empty data
		return h.sendServerDataSignal([][]string{}, [][]interface{}{})
	}

	// Create simple columns based on data structure
	columns := [][]string{}
	if len(data) > 0 {
		for i := range data[0] {
			columns = append(columns, []string{fmt.Sprintf("col_%d", i), "String"})
		}
	}

	return h.sendServerDataSignal(columns, data)
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

	// Send ServerException packet according to native protocol
	// Format: error message (4-byte length + string) + error code (uvarint)
	payload := make([]byte, 0, len(message)+20)

	// Add error message with 4-byte length prefix
	messageBytes := []byte(message)
	messageLen := make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(messageBytes)))
	payload = append(payload, messageLen...)
	payload = append(payload, messageBytes...)

	// Add error code as uvarint
	errorCode := uint64(1001) // Generic error code
	codeBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(codeBytes, errorCode)
	payload = append(payload, codeBytes[:n]...)

	// Send ServerException packet
	if err := h.sendExceptionSignal(string(messageBytes)); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerException message")
		return err
	}

	// Send EndOfStream to indicate end of response
	if err := h.sendServerEndOfStreamSignal(); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write ServerEndOfStream message")
		return err
	}

	h.logger.Debug().Msg("Error response sent successfully")
	return nil
}

// handleClientHelloSignal handles client hello message using decoded signal
func (h *ConnectionHandler) handleClientHelloSignal(hello *signals.ClientHello) error {
	h.logger.Debug().
		Str("client_name", hello.ClientName).
		Uint64("major_version", hello.MajorVersion).
		Uint64("minor_version", hello.MinorVersion).
		Uint64("protocol_version", hello.ProtocolVersion).
		Str("database", hello.Database).
		Str("user", hello.User).
		Msg("Processing ClientHello signal")

	// Send server hello response
	return h.sendServerHelloSignal()
}

// handleClientQuerySignal handles client query message using decoded signal
func (h *ConnectionHandler) handleClientQuerySignal(ctx context.Context, query *signals.ClientQuery) error {
	h.logger.Debug().
		Str("query", query.Query).
		Str("query_id", query.QueryID).
		Str("database", query.Database).
		Str("user", query.User).
		Msg("Processing ClientQuery signal")

	// Store current query
	h.currentQuery = query.Query

	// Execute query using query engine
	result, err := h.queryEngine.ExecuteQuery(ctx, query.Query)
	if err != nil {
		h.logger.Error().Err(err).Str("query", query.Query).Msg("Query execution failed")
		return h.sendExceptionSignal(fmt.Sprintf("Query execution failed: %v", err))
	}

	// Send query results
	return h.sendQueryEngineResultsSignal(result)
}

// handleClientDataSignal handles client data message using decoded signal
func (h *ConnectionHandler) handleClientDataSignal(data *signals.ClientData) error {
	h.logger.Debug().
		Str("table_name", data.TableName).
		Int("column_count", len(data.Columns)).
		Int("row_count", len(data.Rows)).
		Msg("Processing ClientData signal")

	// Process the data block
	block := &DataBlock{
		TableName: data.TableName,
		Columns:   data.Columns,
		Rows:      data.Rows,
	}

	return h.processDataBlock(block)
}

// handleClientCancelSignal handles client cancel message using decoded signal
func (h *ConnectionHandler) handleClientCancelSignal(cancel *signals.ClientCancel) error {
	h.logger.Debug().
		Str("query_id", cancel.QueryID).
		Msg("Processing ClientCancel signal")

	// Implement query cancellation logic using query engine
	h.logger.Info().Str("query_id", cancel.QueryID).Msg("Query cancellation requested")

	// Try to cancel the query using the query engine
	if err := h.queryEngine.CancelQuery(cancel.QueryID); err != nil {
		h.logger.Warn().Err(err).Str("query_id", cancel.QueryID).Msg("Failed to cancel query")
		return h.sendSimpleAcknowledgment(fmt.Sprintf("Query %s cancellation failed: %v", cancel.QueryID, err))
	}

	// Send acknowledgment
	return h.sendSimpleAcknowledgment(fmt.Sprintf("Query %s cancelled successfully", cancel.QueryID))
}

// sendServerHelloSignal sends server hello using unified protocol
func (h *ConnectionHandler) sendServerHelloSignal() error {
	hello := signals.NewServerHello("Icebox Server", "UTC", "Icebox Database Server")

	message, err := h.codec.EncodeMessage(hello)
	if err != nil {
		return fmt.Errorf("failed to encode server hello: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendExceptionSignal sends server exception using unified protocol
func (h *ConnectionHandler) sendExceptionSignal(errorMsg string) error {
	exception := signals.NewServerException(1001, errorMsg, "")

	message, err := h.codec.EncodeMessage(exception)
	if err != nil {
		return fmt.Errorf("failed to encode server exception: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendQueryEngineResultsSignal sends query results using unified protocol
func (h *ConnectionHandler) sendQueryEngineResultsSignal(result *query.QueryResult) error {
	// Implement proper result sending using unified protocol
	h.logger.Debug().
		Int("column_count", len(result.Columns)).
		Int64("row_count", result.RowCount).
		Msg("Sending query results using unified protocol")

	// Convert query result to protocol format
	columns := make([][]string, len(result.Columns))
	for i, colName := range result.Columns {
		columns[i] = []string{colName, "String"} // Default type to String
	}

	// Convert data interface{} to [][]interface{}
	var data [][]interface{}
	if result.Data != nil {
		// Try to assert as [][]interface{} first
		if rowData, ok := result.Data.([][]interface{}); ok {
			data = rowData
		} else {
			// Fallback: create single row with the data as string
			data = [][]interface{}{{fmt.Sprintf("%v", result.Data)}}
		}
	}

	// Send the data
	if err := h.sendServerDataSignal(columns, data); err != nil {
		return err
	}

	// Send end of stream
	return h.sendServerEndOfStreamSignal()
}

// sendServerDataSignal sends server data using unified protocol
func (h *ConnectionHandler) sendServerDataSignal(columns [][]string, data [][]interface{}) error {
	// Convert [][]string to []signals.Column
	signalColumns := make([]signals.Column, len(columns))
	for i, col := range columns {
		if len(col) >= 2 {
			signalColumns[i] = signals.Column{
				Name: col[0],
				Type: col[1],
			}
		} else {
			signalColumns[i] = signals.Column{
				Name: "unknown",
				Type: "String",
			}
		}
	}

	serverData := signals.NewServerData(signalColumns, data)

	message, err := h.codec.EncodeMessage(serverData)
	if err != nil {
		return fmt.Errorf("failed to encode server data: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendServerEndOfStreamSignal sends server end of stream using unified protocol
func (h *ConnectionHandler) sendServerEndOfStreamSignal() error {
	eos := signals.NewServerEndOfStream()

	message, err := h.codec.EncodeMessage(eos)
	if err != nil {
		return fmt.Errorf("failed to encode server end of stream: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendServerPongSignal sends server pong using unified protocol
func (h *ConnectionHandler) sendServerPongSignal() error {
	pong := signals.NewServerPong(time.Now().Unix())

	message, err := h.codec.EncodeMessage(pong)
	if err != nil {
		return fmt.Errorf("failed to encode server pong: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendServerDataWithPayload sends server data with custom payload (for backward compatibility)
func (h *ConnectionHandler) sendServerDataWithPayload(payload []byte) error {
	// Parse payload and create appropriate ServerData signal
	// For backward compatibility, we'll create a simple single-column result with the payload as string data
	h.logger.Debug().Int("payload_size", len(payload)).Msg("Sending server data with custom payload")

	columns := []signals.Column{
		{Name: "data", Type: "String"},
	}

	// Convert payload to string and send as single row
	data := [][]interface{}{
		{string(payload)},
	}

	serverData := signals.NewServerData(columns, data)

	message, err := h.codec.EncodeMessage(serverData)
	if err != nil {
		return fmt.Errorf("failed to encode server data: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}

// sendServerExceptionWithPayload sends server exception with custom payload (for backward compatibility)
func (h *ConnectionHandler) sendServerExceptionWithPayload(payload []byte) error {
	// Parse payload to extract error information
	// For backward compatibility, assume the payload contains the error message
	h.logger.Debug().Int("payload_size", len(payload)).Msg("Sending server exception with custom payload")

	errorMsg := string(payload)
	if len(errorMsg) == 0 {
		errorMsg = "Unknown error occurred"
	}

	// Create exception with parsed error message
	exception := signals.NewServerException(1001, errorMsg, "")

	message, err := h.codec.EncodeMessage(exception)
	if err != nil {
		return fmt.Errorf("failed to encode server exception: %w", err)
	}

	return h.codec.WriteMessage(h.conn, message)
}
