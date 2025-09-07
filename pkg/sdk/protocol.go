package sdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"net"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/google/uuid"

	// Unified protocol package
	"github.com/gear6io/ranger/server/protocols/native/protocol"
	"github.com/gear6io/ranger/server/protocols/native/protocol/signals"
)

// ServerVersion represents server version information
type ServerVersion struct {
	Name        string
	Major       int
	Minor       int
	Patch       int
	Revision    int
	Interface   string
	DefaultDB   string
	Timezone    string
	DisplayName string
	Version     string
	Protocol    int
}

// Query represents a query to be executed
type Query struct {
	Body    string
	QueryID string
}

// Row represents a single row result
type Row struct {
	rows *Rows
	err  error
}

// Scan scans the current row into the provided values
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// Rows represents query results
type Rows struct {
	Client   *Client
	Query    Query
	Cols     []Column
	Data     [][]interface{}
	Current  int
	Closed   bool
	QueryErr error // Store any error encountered during query execution
	onClose  func()
}

// Column represents a column in query results
type Column struct {
	Name string
	Type string
}

// BatchOption represents batch preparation options
type BatchOption func(*BatchOptions)

// BatchOptions represents batch preparation options
type BatchOptions struct {
	CloseOnFlush      bool
	ReleaseConnection bool
}

// WithCloseOnFlush sets the batch to close on flush
func WithCloseOnFlush() BatchOption {
	return func(opts *BatchOptions) {
		opts.CloseOnFlush = true
	}
}

// WithReleaseConnection sets the batch to release connection on close
func WithReleaseConnection() BatchOption {
	return func(opts *BatchOptions) {
		opts.ReleaseConnection = true
	}
}

// Batch represents a batch for insertion
type Batch struct {
	Client    *Client
	Query     Query
	TableName string
	Columns   []string
	Data      [][]interface{}
	Sent      bool
	onClose   func()
}

// BatchColumn represents a column in a batch
type BatchColumn struct {
	Batch   *Batch
	Column  int
	release func(error)
}

// Progress represents query progress
type Progress struct {
	ReadRows     uint64
	ReadBytes    uint64
	TotalRows    uint64
	WrittenRows  uint64
	WrittenBytes uint64
}

// Exception represents server exception
type Exception struct {
	Code    int
	Name    string
	Message string
	Stack   string
}

// Error implements the error interface
func (e *Exception) Error() string {
	return fmt.Sprintf("ranger exception [%d]: %s - %s", e.Code, e.Name, e.Message)
}

// GenerateQueryID generates a unique query ID
func GenerateQueryID() string {
	return uuid.New().String()
}

// ParseInsertQuery parses an INSERT query to extract table name and columns
func ParseInsertQuery(query string) (string, []string, error) {
	// Simple regex to extract table name and columns from INSERT query
	insertMatch := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([^\s(]+)(?:\s*\(([^)]+)\))?`)
	matches := insertMatch.FindStringSubmatch(query)

	if len(matches) < 2 {
		return "", nil, errors.New("invalid INSERT query")
	}

	tableName := strings.TrimSpace(matches[1])
	var columns []string

	if len(matches) > 2 && matches[2] != "" {
		// Parse columns
		colStr := strings.TrimSpace(matches[2])
		for _, col := range strings.Split(colStr, ",") {
			columns = append(columns, strings.TrimSpace(col))
		}
	}

	return tableName, columns, nil
}

// Next advances to the next row
func (r *Rows) Next() bool {
	if r.Closed || r.Current >= len(r.Data) {
		return false
	}
	r.Current++
	return r.Current <= len(r.Data)
}

// Scan scans the current row into the provided values
func (r *Rows) Scan(dest ...interface{}) error {
	if r.Closed {
		return errors.New("rows are closed")
	}
	if r.Current <= 0 || r.Current > len(r.Data) {
		return errors.New("no current row")
	}

	row := r.Data[r.Current-1]
	if len(dest) != len(row) {
		return errors.New("destination count does not match column count")
	}

	for i, val := range row {
		// All values from readQueryResponse are stored as strings
		strVal, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string value, got %T", val)
		}

		switch d := dest[i].(type) {
		case *string:
			*d = strVal
		case *uint32:
			// Parse string to uint32
			parsed, err := strconv.ParseUint(strVal, 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse uint32 from '%s': %w", strVal, err)
			}
			*d = uint32(parsed)
		case *int:
			// Parse string to int
			parsed, err := strconv.Atoi(strVal)
			if err != nil {
				return fmt.Errorf("failed to parse int from '%s': %w", strVal, err)
			}
			*d = parsed
		case *int64:
			// Parse string to int64
			parsed, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse int64 from '%s': %w", strVal, err)
			}
			*d = parsed
		case *float64:
			// Parse string to float64
			parsed, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return fmt.Errorf("failed to parse float64 from '%s': %w", strVal, err)
			}
			*d = parsed
		case *bool:
			// Parse string to bool
			parsed, err := strconv.ParseBool(strVal)
			if err != nil {
				return fmt.Errorf("failed to parse bool from '%s': %w", strVal, err)
			}
			*d = parsed
		case *time.Time:
			// Parse string to time.Time
			// Try multiple time formats
			var parsed time.Time
			var err error

			// Try Go's default time format first (includes timezone and monotonic clock)
			// This format looks like: "2025-08-10 18:14:22.049865 +0530 IST m=+0.004783126"
			// We need to extract just the time part before the monotonic clock info
			if idx := strings.Index(strVal, " m="); idx != -1 {
				// Remove the monotonic clock part
				strVal = strVal[:idx]
			}

			// Try RFC3339 format first
			parsed, err = time.Parse(time.RFC3339, strVal)
			if err != nil {
				// Try "2006-01-02 15:04:05" format
				parsed, err = time.Parse("2006-01-02 15:04:05", strVal)
				if err != nil {
					// Try "2006-01-02 15:04:05.000000000" format
					parsed, err = time.Parse("2006-01-02 15:04:05.000000000", strVal)
					if err != nil {
						// Try "2006-01-02 15:04:05 -0700 MST" format (Go's default without monotonic clock)
						parsed, err = time.Parse("2006-01-02 15:04:05 -0700 MST", strVal)
						if err != nil {
							return fmt.Errorf("failed to parse time from '%s': %w", strVal, err)
						}
					}
				}
			}
			*d = parsed
		default:
			return fmt.Errorf("unsupported scan destination type: %T", dest[i])
		}
	}

	return nil
}

// Columns returns the column names
func (r *Rows) Columns() ([]string, error) {
	if r.Closed {
		return nil, errors.New("rows are closed")
	}

	names := make([]string, len(r.Cols))
	for i, col := range r.Cols {
		names[i] = col.Name
	}
	return names, nil
}

// Close closes the rows
func (r *Rows) Close() error {
	if r.Closed {
		return nil
	}
	r.Closed = true
	if r.onClose != nil {
		r.onClose()
	}
	return nil
}

// Err returns any error encountered during iteration
func (r *Rows) Err() error {
	return r.QueryErr
}

// Append adds a row to the batch
func (b *Batch) Append(values ...interface{}) error {
	if b.Sent {
		return errors.New("batch already sent")
	}

	if len(values) != len(b.Columns) {
		return errors.New("value count does not match column count")
	}

	b.Data = append(b.Data, values)
	return nil
}

// Send sends the batch to the server
func (b *Batch) Send() error {
	if b.Sent {
		return errors.New("batch already sent")
	}

	// Get a connection from the client
	conn, err := b.Client.acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer b.Client.release(conn, nil)

	// Send the data using ClickHouse native protocol
	if err := conn.sendBatchData(b); err != nil {
		return fmt.Errorf("failed to send batch data: %w", err)
	}

	// Read the server's response to the batch data
	// This is crucial to keep the protocol stream synchronized
	if err := conn.readExecResponse(); err != nil {
		return fmt.Errorf("failed to read batch response: %w", err)
	}

	// Mark as sent
	b.Sent = true
	return nil
}

// Flush flushes the batch without closing
func (b *Batch) Flush() error {
	return b.Send()
}

// Close closes the batch
func (b *Batch) Close() error {
	if !b.Sent {
		return b.Send()
	}
	if b.onClose != nil {
		b.onClose()
	}
	return nil
}

// Rows returns the number of rows in the batch
func (b *Batch) Rows() int {
	return len(b.Data)
}

// IsSent returns true if the batch has been sent
func (b *Batch) IsSent() bool {
	return b.Sent
}

// Column returns a batch column for direct manipulation
func (b *Batch) Column(idx int) *BatchColumn {
	if idx < 0 || idx >= len(b.Columns) {
		return nil
	}

	return &BatchColumn{
		Batch:  b,
		Column: idx,
		release: func(err error) {
			// Handle column release
		},
	}
}

// Append adds a value to the column
func (bc *BatchColumn) Append(value interface{}) error {
	if bc.Batch.Sent {
		return errors.New("batch already sent")
	}

	// Ensure we have enough rows
	for len(bc.Batch.Data) <= bc.Batch.Rows() {
		row := make([]interface{}, len(bc.Batch.Columns))
		bc.Batch.Data = append(bc.Batch.Data, row)
	}

	bc.Batch.Data[bc.Batch.Rows()-1][bc.Column] = value
	return nil
}

func (c *connection) readExecResponse() error {
	// Read response packets using unified protocol until EndOfStream
	for {
		// Read and decode message using unified protocol
		message, err := c.codec.ReadMessage(c.conn)
		if err != nil {
			if err == io.EOF {
				break // Client disconnected, stop reading
			}
			return fmt.Errorf("failed to read message: %w", err)
		}

		// Unpack the message into a signal
		signal, err := c.codec.UnpackSignal(message)
		if err != nil {
			return fmt.Errorf("failed to unpack message: %w", err)
		}

		switch signal.Type() {
		case protocol.ServerEndOfStream:
			return nil
		case protocol.ServerException:
			// Handle exception signal
			exception := signal.(*signals.ServerException)
			return fmt.Errorf("server exception [%s]: %s", exception.ErrorCode, exception.ErrorMessage)
		case protocol.ServerClose:
			// Handle server close signal
			close := signal.(*signals.ServerClose)
			c.bad = true // Mark connection as bad
			return fmt.Errorf("server closed connection: %s", close.Reason)
		case protocol.ServerData:
			// Handle data signal - just continue reading
			continue
		case protocol.ServerProgress:
			// Handle progress signal - just continue reading
			continue
		default:
			// Skip other packet types for now
			continue
		}
	}

	return nil
}

// netConnReader implements io.ByteReader for reading from net.Conn
type netConnReader struct {
	conn net.Conn
}

func (r *netConnReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := r.conn.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

// sendBatchData sends batch data to the server using unified protocol
func (c *connection) sendBatchData(batch *Batch) error {
	// Create ClientData signal using unified protocol
	clientData := signals.NewClientData(
		batch.TableName,
		batch.Columns,
		batch.Data,
	)

	// Use unified codec to encode and send the message
	message, err := c.codec.EncodeMessage(clientData)
	if err != nil {
		return fmt.Errorf("failed to encode client data: %w", err)
	}

	if err := c.codec.WriteMessage(c.conn, message); err != nil {
		return fmt.Errorf("failed to send client data: %w", err)
	}

	// Read response packets using unified protocol until EndOfStream
	for {
		// Read and decode message using unified protocol
		message, err := c.codec.ReadMessage(c.conn)
		if err != nil {
			if err == io.EOF {
				break // Client disconnected, stop reading
			}
			return fmt.Errorf("failed to read message: %w", err)
		}

		// Unpack the message into a signal
		signal, err := c.codec.UnpackSignal(message)
		if err != nil {
			return fmt.Errorf("failed to unpack message: %w", err)
		}

		switch signal.Type() {
		case protocol.ServerEndOfStream:
			// End of response, batch operation successful
			return nil
		case protocol.ServerException:
			// Handle exception signal
			exception := signal.(*signals.ServerException)
			return fmt.Errorf("server exception [%s]: %s", exception.ErrorCode, exception.ErrorMessage)
		case protocol.ServerClose:
			// Handle server close signal
			close := signal.(*signals.ServerClose)
			c.bad = true // Mark connection as bad
			return fmt.Errorf("server closed connection: %s", close.Reason)
		case protocol.ServerData:
			// Handle data signal - just continue reading for batch operations
			continue
		case protocol.ServerProgress:
			// Handle progress signal - just continue reading
			continue
		default:
			// Skip other packet types for now
			continue
		}
	}

	return nil
}

// Connector creates a database/sql.Connector
func Connector(opt *Options) driver.Connector {
	return &connector{opt: opt}
}

// connector implements database/sql.Connector
type connector struct {
	opt *Options
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := NewClient(c.opt)
	if err != nil {
		return nil, err
	}

	return &sqlConn{client: client}, nil
}

func (c *connector) Driver() driver.Driver {
	return &sqlDriver{}
}

// sqlConn implements database/sql.Conn
type sqlConn struct {
	client *Client
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	batch, err := c.client.PrepareBatch(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{batch: batch}, nil
}

func (c *sqlConn) Close() error {
	return c.client.Close()
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Ranger doesn't support transactions, so we return a no-op transaction
	return &sqlTx{conn: c}, nil
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Convert args to interface{} slice
	interfaceArgs := make([]interface{}, len(args))
	for i, arg := range args {
		interfaceArgs[i] = arg.Value
	}

	err := c.client.Exec(ctx, query, interfaceArgs...)
	if err != nil {
		return nil, err
	}

	return &sqlResult{}, nil
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Convert args to interface{} slice
	interfaceArgs := make([]interface{}, len(args))
	for i, arg := range args {
		interfaceArgs[i] = arg.Value
	}

	rows, err := c.client.Query(ctx, query, interfaceArgs...)
	if err != nil {
		return nil, err
	}

	return &sqlRows{rows: rows}, nil
}

// sqlStmt implements database/sql.Stmt
type sqlStmt struct {
	batch *Batch
}

func (s *sqlStmt) Close() error {
	return s.batch.Close()
}

func (s *sqlStmt) NumInput() int {
	return -1 // Variable number of inputs
}

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	// Convert driver.Value to driver.NamedValue
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{Value: arg}
	}
	return s.ExecContext(context.Background(), namedArgs)
}

func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Convert args to interface{} slice
	interfaceArgs := make([]interface{}, len(args))
	for i, arg := range args {
		interfaceArgs[i] = arg.Value
	}

	// Add args to batch
	if err := s.batch.Append(interfaceArgs...); err != nil {
		return nil, err
	}

	// Send batch
	if err := s.batch.Send(); err != nil {
		return nil, err
	}

	return &sqlResult{}, nil
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Convert driver.Value to driver.NamedValue
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{Value: arg}
	}
	return s.QueryContext(context.Background(), namedArgs)
}

func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// For prepared statements, we need to execute the query
	// This is a simplified implementation
	return nil, errors.New("prepared statement queries not implemented")
}

// sqlRows implements database/sql.Rows
type sqlRows struct {
	rows *Rows
}

func (r *sqlRows) Columns() []string {
	cols, err := r.rows.Columns()
	if err != nil {
		return nil
	}
	return cols
}

func (r *sqlRows) Close() error {
	return r.rows.Close()
}

func (r *sqlRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		return io.EOF
	}

	// Convert dest to interface{} slice for Scan
	interfaceDest := make([]interface{}, len(dest))
	for i := range dest {
		interfaceDest[i] = &dest[i]
	}

	return r.rows.Scan(interfaceDest...)
}

// sqlTx implements database/sql.Tx
type sqlTx struct {
	conn *sqlConn
}

func (t *sqlTx) Commit() error {
	// No-op for Ranger
	return nil
}

func (t *sqlTx) Rollback() error {
	// No-op for Ranger
	return nil
}

// sqlResult implements database/sql.Result
type sqlResult struct{}

func (r *sqlResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported")
}

func (r *sqlResult) RowsAffected() (int64, error) {
	return 0, errors.New("RowsAffected not supported")
}

// sqlDriver implements database/sql.Driver
type sqlDriver struct{}

func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	opt, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}

	client, err := NewClient(opt)
	if err != nil {
		return nil, err
	}

	return &sqlConn{client: client}, nil
}
