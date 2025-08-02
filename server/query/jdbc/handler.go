package jdbc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/rs/zerolog"
)

// JDBCHandler handles JDBC protocol communication and SQL execution
type JDBCHandler struct {
	engine *duckdb.Engine
	logger zerolog.Logger
	ctx    context.Context
}

// NewJDBCHandler creates a new JDBC handler
func NewJDBCHandler(engine *duckdb.Engine, logger zerolog.Logger, ctx context.Context) *JDBCHandler {
	return &JDBCHandler{
		engine: engine,
		logger: logger,
		ctx:    ctx,
	}
}

// HandleConnection handles a JDBC connection
func (h *JDBCHandler) HandleConnection(conn io.ReadWriteCloser) error {
	defer conn.Close()

	// Handle startup
	if err := h.handleStartup(conn); err != nil {
		h.logger.Error().Err(err).Msg("Startup failed")
		return err
	}

	// Main message loop
	for {
		msg, err := ReadMessage(conn)
		if err != nil {
			if err == io.EOF {
				h.logger.Debug().Msg("Client disconnected")
				return nil
			}
			h.logger.Error().Err(err).Msg("Failed to read message")
			return err
		}

		if err := h.handleMessage(conn, msg); err != nil {
			h.logger.Error().Err(err).Msg("Failed to handle message")
			// Send error response but continue processing
			WriteErrorResponse(conn, "XX000", err.Error())
		}
	}
}

// handleStartup handles the initial connection startup
func (h *JDBCHandler) handleStartup(conn io.ReadWriteCloser) error {
	// Read startup message
	params, err := ParseStartupMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to parse startup message: %w", err)
	}

	h.logger.Debug().Interface("params", params).Msg("Startup parameters")

	// Send startup response
	if err := WriteStartupResponse(conn); err != nil {
		return fmt.Errorf("failed to write startup response: %w", err)
	}

	return nil
}

// handleMessage handles individual PostgreSQL messages
func (h *JDBCHandler) handleMessage(conn io.WriteCloser, msg *Message) error {
	switch msg.Type {
	case MessageTypeQuery:
		return h.handleQuery(conn, msg)
	case MessageTypeParse:
		return h.handleParse(conn, msg)
	case MessageTypeBind:
		return h.handleBind(conn, msg)
	case MessageTypeExecute:
		return h.handleExecute(conn, msg)
	case MessageTypeDescribe:
		return h.handleDescribe(conn, msg)
	case MessageTypeClose:
		return h.handleClose(conn, msg)
	case MessageTypeSync:
		return h.handleSync(conn, msg)
	case MessageTypeTerminate:
		return h.handleTerminate(conn, msg)
	default:
		h.logger.Warn().Str("type", string(msg.Type)).Msg("Unknown message type")
		return WriteErrorResponse(conn, "XX000", fmt.Sprintf("Unknown message type: %c", msg.Type))
	}
}

// handleQuery handles a simple query
func (h *JDBCHandler) handleQuery(conn io.WriteCloser, msg *Message) error {
	// Extract SQL query from message
	query := strings.TrimRight(string(msg.Data), "\x00")

	h.logger.Debug().Str("query", query).Msg("Executing query")

	// Execute query using DuckDB engine
	result, err := h.engine.ExecuteQuery(h.ctx, query)
	if err != nil {
		return WriteErrorResponse(conn, "XX000", err.Error())
	}

	// Send row description if there are columns
	if len(result.Columns) > 0 {
		columns := make([]ColumnDescription, len(result.Columns))
		for i, col := range result.Columns {
			columns[i] = ColumnDescription{
				Name:     col,
				TypeOID:  h.getTypeOID(col), // Map to PostgreSQL type OID
				TypeSize: -1,                // Variable length
			}
		}

		if err := WriteRowDescription(conn, columns); err != nil {
			return err
		}
	}

	// Send data rows
	for _, row := range result.Rows {
		if err := WriteDataRow(conn, row); err != nil {
			return err
		}
	}

	// Send command complete
	tag := fmt.Sprintf("SELECT %d", len(result.Rows))
	if err := WriteCommandComplete(conn, tag); err != nil {
		return err
	}

	// Send ready for query
	return WriteReadyForQuery(conn, 'I')
}

// handleParse handles a parse message (prepared statement)
func (h *JDBCHandler) handleParse(conn io.WriteCloser, msg *Message) error {
	// For now, just acknowledge the parse
	// In a full implementation, you would store the prepared statement
	return WriteMessage(conn, ResponseTypeParseComplete, nil)
}

// handleBind handles a bind message (bind parameters to prepared statement)
func (h *JDBCHandler) handleBind(conn io.WriteCloser, msg *Message) error {
	// For now, just acknowledge the bind
	// In a full implementation, you would bind parameters
	return WriteMessage(conn, ResponseTypeBindComplete, nil)
}

// handleExecute handles an execute message (execute prepared statement)
func (h *JDBCHandler) handleExecute(conn io.WriteCloser, msg *Message) error {
	// For now, just send no data
	// In a full implementation, you would execute the prepared statement
	return WriteMessage(conn, ResponseTypeNoData, nil)
}

// handleDescribe handles a describe message
func (h *JDBCHandler) handleDescribe(conn io.WriteCloser, msg *Message) error {
	// For now, just send no data
	// In a full implementation, you would describe the prepared statement or portal
	return WriteMessage(conn, ResponseTypeNoData, nil)
}

// handleClose handles a close message
func (h *JDBCHandler) handleClose(conn io.WriteCloser, msg *Message) error {
	// For now, just acknowledge the close
	// In a full implementation, you would close the prepared statement or portal
	return WriteMessage(conn, ResponseTypeCloseComplete, nil)
}

// handleSync handles a sync message
func (h *JDBCHandler) handleSync(conn io.WriteCloser, msg *Message) error {
	// Send ready for query
	return WriteReadyForQuery(conn, 'I')
}

// handleTerminate handles a terminate message
func (h *JDBCHandler) handleTerminate(conn io.WriteCloser, msg *Message) error {
	// Client is terminating, just return
	return nil
}

// getTypeOID maps DuckDB column names to PostgreSQL type OIDs
func (h *JDBCHandler) getTypeOID(columnName string) int32 {
	// This is a simplified mapping
	// In a real implementation, you would get the actual data type from DuckDB
	lowerName := strings.ToLower(columnName)

	switch {
	case strings.Contains(lowerName, "int"):
		return 23 // int4
	case strings.Contains(lowerName, "bigint"):
		return 20 // int8
	case strings.Contains(lowerName, "float"):
		return 701 // float8
	case strings.Contains(lowerName, "double"):
		return 701 // float8
	case strings.Contains(lowerName, "decimal"):
		return 1700 // numeric
	case strings.Contains(lowerName, "bool"):
		return 16 // bool
	case strings.Contains(lowerName, "date"):
		return 1082 // date
	case strings.Contains(lowerName, "time"):
		return 1083 // time
	case strings.Contains(lowerName, "timestamp"):
		return 1114 // timestamp
	case strings.Contains(lowerName, "varchar"):
		return 1043 // varchar
	case strings.Contains(lowerName, "text"):
		return 25 // text
	default:
		return 25 // text (default)
	}
}

// ExecuteQuery executes a SQL query and returns results
func (h *JDBCHandler) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// Use the DuckDB engine to execute the query
	result, err := h.engine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// Convert to our format
	return &QueryResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		RowCount: result.RowCount,
		Duration: result.Duration,
	}, nil
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int64
	Duration time.Duration
}
