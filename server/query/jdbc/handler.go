package jdbc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gear6io/ranger/server/query/duckdb"
	"github.com/rs/zerolog"
)

// QueryEngineInterface defines the interface for query execution
type QueryEngineInterface interface {
	ExecuteQuery(ctx context.Context, query string) (*duckdb.QueryResult, error)
}

// JDBCHandler handles JDBC protocol communication and SQL execution
type JDBCHandler struct {
	engine        QueryEngineInterface
	logger        zerolog.Logger
	ctx           context.Context
	sqlMiddleware *SQLParserMiddleware
}

// NewJDBCHandler creates a new JDBC handler
func NewJDBCHandler(engine QueryEngineInterface, logger zerolog.Logger, ctx context.Context) *JDBCHandler {
	return &JDBCHandler{
		engine:        engine,
		logger:        logger,
		ctx:           ctx,
		sqlMiddleware: NewSQLParserMiddleware(logger),
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
	// Extract SQL query from message
	query := strings.TrimRight(string(msg.Data), "\x00")

	h.logger.Debug().Str("query", query).Msg("Executing query")

	// Step 1: Analyze the query using SQL parser middleware
	analysis, err := h.sqlMiddleware.AnalyzeQuery(query)
	if err != nil {
		h.logger.Warn().Err(err).Str("query", query).Msg("Query analysis failed")
		// Continue execution even if analysis fails - don't block queries
	} else {
		// Step 2: Validate the query for security and business rules
		if validationErr := h.sqlMiddleware.ValidateQuery(analysis); validationErr != nil {
			h.logger.Warn().Err(validationErr).Str("query", query).Msg("Query validation failed")
			// For now, log the validation error but continue execution
			// In production, you might want to block these queries
		}

		// Step 3: Log detailed query information for monitoring
		h.logger.Info().
			Str("statementType", analysis.StatementType).
			Str("complexity", analysis.Complexity).
			Int("tableCount", len(analysis.Tables)).
			Int("columnCount", len(analysis.Columns)).
			Bool("hasJoins", analysis.HasJoins).
			Bool("hasSubqueries", analysis.HasSubqueries).
			Bool("hasAggregations", analysis.HasAggregations).
			Dur("parseTime", analysis.ParseTime).
			Msg("Query analyzed successfully")

		// Step 4: Log optimization hints if available
		if len(analysis.OptimizationHints) > 0 {
			h.logger.Warn().
				Strs("optimizationHints", analysis.OptimizationHints).
				Str("query", query).
				Msg("Query optimization suggestions available")
		}
	}

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

	// Send command complete with enhanced tag from SQL parser middleware
	var tag string
	if analysis != nil && analysis.IsValid {
		// Use enhanced command complete tag from middleware
		tag = h.sqlMiddleware.GenerateCommandCompleteTag(analysis, len(result.Rows))
	} else {
		// Fallback to basic tag if analysis failed
		tag = fmt.Sprintf("SELECT %d", len(result.Rows))
	}

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

// ExecuteQuery executes a SQL query and returns results with SQL parser analysis
func (h *JDBCHandler) ExecuteQuery(query string) (*duckdb.QueryResult, error) {
	// Analyze the query using SQL parser middleware
	analysis, err := h.sqlMiddleware.AnalyzeQuery(query)
	if err != nil {
		h.logger.Debug().Err(err).Str("query", query).Msg("Query analysis failed during execution")
	} else {
		h.logger.Debug().
			Str("statementType", analysis.StatementType).
			Str("complexity", analysis.Complexity).
			Msg("Query analysis completed during execution")
	}

	// Execute the query using the engine
	return h.engine.ExecuteQuery(h.ctx, query)
}

// GetQueryAnalysis analyzes a SQL query and returns detailed analysis
func (h *JDBCHandler) GetQueryAnalysis(query string) (*QueryAnalysis, error) {
	return h.sqlMiddleware.AnalyzeQuery(query)
}

// IsQueryAllowed checks if a query is allowed based on security and business rules
func (h *JDBCHandler) IsQueryAllowed(query string) (bool, error) {
	analysis, err := h.sqlMiddleware.AnalyzeQuery(query)
	if err != nil {
		return false, err
	}

	validationErr := h.sqlMiddleware.ValidateQuery(analysis)
	return validationErr == nil, validationErr
}

// GetQueryComplexity returns the complexity level of a query
func (h *JDBCHandler) GetQueryComplexity(query string) (string, error) {
	analysis, err := h.sqlMiddleware.AnalyzeQuery(query)
	if err != nil {
		return "UNKNOWN", err
	}

	return analysis.Complexity, nil
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int64
	Duration time.Duration
}
