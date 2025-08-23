package jdbc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/query"
	"github.com/rs/zerolog"
)

// JDBCHandler handles JDBC protocol communication and SQL execution
type JDBCHandler struct {
	queryEngine *query.Engine
	logger      zerolog.Logger
	ctx         context.Context
}

// NewJDBCHandler creates a new JDBC handler
func NewJDBCHandler(queryEngine *query.Engine, logger zerolog.Logger, ctx context.Context) *JDBCHandler {
	return &JDBCHandler{
		queryEngine: queryEngine,
		logger:      logger,
		ctx:         ctx,
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
		return errors.New(ErrStartupMessageParseFailed, "failed to parse startup message", err)
	}

	h.logger.Debug().Interface("params", params).Msg("Startup parameters")

	// Send startup response
	if err := WriteStartupResponse(conn); err != nil {
		return errors.New(ErrStartupResponseWriteFailed, "failed to write startup response", err)
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
	queryStr := strings.TrimSpace(string(msg.Data))
	h.logger.Debug().Str("query", queryStr).Msg("Executing query using QueryEngine")

	// Execute query using the QueryEngine
	result, err := h.queryEngine.ExecuteQuery(h.ctx, queryStr)
	if err != nil {
		h.logger.Error().Err(err).Str("query", queryStr).Msg("Query execution failed")
		return WriteErrorResponse(conn, "XX000", fmt.Sprintf("Query execution failed: %v", err))
	}

	// Convert QueryEngine result to JDBC format
	columns := make([]ColumnDescription, len(result.Columns))
	for i, colName := range result.Columns {
		columns[i] = ColumnDescription{
			Name:     colName,
			TypeOID:  25, // text type for now, could be enhanced with proper type mapping
			TypeSize: -1,
		}
	}

	// Send row description
	if err := WriteRowDescription(conn, columns); err != nil {
		return errors.New(ErrRowDescriptionWriteFailed, "failed to write row description", err)
	}

	// Send data rows
	if result.Data != nil {
		if rows, ok := result.Data.([][]interface{}); ok {
			for _, row := range rows {
				if err := WriteDataRow(conn, row); err != nil {
					return errors.New(ErrDataRowWriteFailed, "failed to write data row", err)
				}
			}
		}
	}

	// Send command complete with appropriate message
	commandMsg := "SELECT"
	if result.RowCount > 0 {
		commandMsg = fmt.Sprintf("SELECT %d", result.RowCount)
	}
	if err := WriteCommandComplete(conn, commandMsg); err != nil {
		return errors.New(ErrCommandCompleteWriteFailed, "failed to write command complete", err)
	}

	// Send ready for query
	return WriteReadyForQuery(conn, 'I')
}

// handleParse handles a parse message (prepared statement)
func (h *JDBCHandler) handleParse(conn io.WriteCloser, msg *Message) error {
	// TODO: Implement prepared statement parsing
	h.logger.Debug().Msg("Parse message received (not implemented)")
	return WriteMessage(conn, ResponseTypeParseComplete, nil)
}

// handleBind handles a bind message (parameter binding)
func (h *JDBCHandler) handleBind(conn io.WriteCloser, msg *Message) error {
	// TODO: Implement parameter binding
	h.logger.Debug().Msg("Bind message received (not implemented)")
	return WriteMessage(conn, ResponseTypeBindComplete, nil)
}

// handleExecute handles an execute message
func (h *JDBCHandler) handleExecute(conn io.WriteCloser, msg *Message) error {
	// TODO: Implement prepared statement execution
	h.logger.Debug().Msg("Execute message received (not implemented)")
	return WriteCommandComplete(conn, "SELECT 0")
}

// handleDescribe handles a describe message
func (h *JDBCHandler) handleDescribe(conn io.WriteCloser, msg *Message) error {
	// TODO: Implement describe functionality
	h.logger.Debug().Msg("Describe message received (not implemented)")
	return WriteMessage(conn, ResponseTypeNoData, nil)
}

// handleClose handles a close message
func (h *JDBCHandler) handleClose(conn io.WriteCloser, msg *Message) error {
	// TODO: Implement close functionality
	h.logger.Debug().Msg("Close message received (not implemented)")
	return WriteMessage(conn, ResponseTypeCloseComplete, nil)
}

// handleSync handles a sync message
func (h *JDBCHandler) handleSync(conn io.WriteCloser, msg *Message) error {
	return WriteReadyForQuery(conn, 'I')
}

// handleTerminate handles a terminate message
func (h *JDBCHandler) handleTerminate(conn io.WriteCloser, msg *Message) error {
	h.logger.Debug().Msg("Terminate message received")
	return nil
}

// ExecuteQuery executes a SQL query (for testing)
func (h *JDBCHandler) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// Execute query using the QueryEngine
	result, err := h.queryEngine.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.New(ErrQueryExecutionFailed, "query execution failed", err)
	}

	// Convert QueryEngine result to JDBC QueryResult format
	var rows [][]interface{}
	if result.Data != nil {
		if dataRows, ok := result.Data.([][]interface{}); ok {
			rows = dataRows
		}
	}

	return &QueryResult{
		Columns:  result.Columns,
		Rows:     rows,
		RowCount: result.RowCount,
		Duration: time.Duration(0), // QueryEngine doesn't provide duration yet
	}, nil
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int64
	Duration time.Duration
}
