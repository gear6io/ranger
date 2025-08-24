package native

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gear6io/ranger/server/protocols/native/middleware"
	"github.com/gear6io/ranger/server/protocols/native/protocol"
	"github.com/gear6io/ranger/server/protocols/native/protocol/signals"
	"github.com/gear6io/ranger/server/query"
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

	// Server context for graceful shutdown
	serverCtx context.Context
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, queryEngine *query.Engine, logger zerolog.Logger, middlewareChain *middleware.Chain, serverCtx context.Context) *ConnectionHandler {
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
		serverCtx:   serverCtx,
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
		// Check if server is shutting down
		select {
		case <-h.serverCtx.Done():
			h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("Server shutting down, closing connection")
			return nil
		default:
			// Continue with normal operation
		}

		// Set a read timeout to allow graceful shutdown
		if err := h.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			h.logger.Error().Err(err).Msg("Failed to set read deadline")
			return err
		}

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

			// Check for timeout errors (these are expected during shutdown)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// This is a timeout, check if server is shutting down
				select {
				case <-h.serverCtx.Done():
					h.logger.Debug().Str("client", h.conn.RemoteAddr().String()).Msg("Server shutting down, closing connection")
					return nil
				default:
					// Not shutting down, continue with next iteration
					continue
				}
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

// handleClientPing handles client ping message
func (h *ConnectionHandler) handleClientPing() error {
	h.logger.Debug().Msg("Ping received")
	return h.sendServerPongSignal()
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
	hello := signals.NewServerHello("Ranger Server", "UTC", "Ranger Database Server")

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
