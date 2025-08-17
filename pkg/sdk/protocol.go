package sdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"encoding/binary"
	"net"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
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
	return fmt.Sprintf("icebox exception [%d]: %s - %s", e.Code, e.Name, e.Message)
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

// readResults reads query results from the server
func (r *Rows) readResults(ctx context.Context) error {
	// This is a simplified implementation
	// In a real implementation, this would read the actual protocol packets

	// For now, we'll create some dummy data
	r.Cols = []Column{
		{Name: "id", Type: "Int32"},
		{Name: "name", Type: "String"},
		{Name: "value", Type: "Float64"},
	}

	r.Data = [][]interface{}{
		{1, "test1", 1.23},
		{2, "test2", 4.56},
		{3, "test3", 7.89},
	}

	return nil
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

// generateRandomBytes generates random bytes of the specified length
func generateRandomBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	return bytes, err
}

// Protocol constants matching ClickHouse native protocol
// See https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Protocol.h
const (
	// Client message types
	ClientHello  byte = 0
	ClientQuery  byte = 1
	ClientData   byte = 2
	ClientCancel byte = 3
	ClientPing   byte = 4

	// Server message types
	ServerHello               byte = 0
	ServerData                byte = 1
	ServerException           byte = 2
	ServerProgress            byte = 3
	ServerPong                byte = 4
	ServerEndOfStream         byte = 5
	ServerProfileInfo         byte = 6
	ServerTotals              byte = 7
	ServerExtremes            byte = 8
	ServerTablesStatus        byte = 9
	ServerLog                 byte = 10
	ServerTableColumns        byte = 11
	ServerPartUUIDs           byte = 12
	ServerReadTaskRequest     byte = 13
	ServerProfileEvents       byte = 14
	ServerTreeReadTaskRequest byte = 15

	// Protocol versions
	DBMS_TCP_PROTOCOL_VERSION = 54460
)

// connection methods (these will be implemented in a separate connection.go file)
func (c *connection) ping(ctx context.Context) error {
	// Send ping packet
	if err := c.sendPing(); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	// Read pong response
	if err := c.readPong(); err != nil {
		return fmt.Errorf("failed to read pong: %w", err)
	}

	return nil
}

func (c *connection) query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	// Send query packet
	if err := c.sendQuery(query); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Read query response
	rows, err := c.readQueryResponse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to read query response: %w", err)
	}

	return rows, nil
}

func (c *connection) exec(ctx context.Context, query string, args ...interface{}) error {
	// Send query packet
	if err := c.sendQuery(query); err != nil {
		return fmt.Errorf("failed to send query: %w", err)
	}

	// Read query response (for DDL statements, we expect EndOfStream)
	if err := c.readExecResponse(); err != nil {
		return fmt.Errorf("failed to read exec response: %w", err)
	}

	return nil
}

func (c *connection) prepareBatch(ctx context.Context, query string, opts ...BatchOption) (*Batch, error) {
	// Parse the INSERT query to extract table name and columns
	tableName, columns, err := ParseInsertQuery(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse insert query: %w", err)
	}

	// Create batch with parsed information
	batch := &Batch{
		Client:    c.client,
		Query:     Query{Body: query, QueryID: GenerateQueryID()},
		TableName: tableName,
		Columns:   columns,
		Data:      make([][]interface{}, 0),
		Sent:      false,
	}

	// Note: Batch options are not currently stored in the Batch struct
	// This is a limitation that can be addressed in future versions

	return batch, nil
}

func (c *connection) asyncInsert(ctx context.Context, query string, wait bool, args ...interface{}) error {
	// For now, just delegate to exec since async insert is not fully implemented
	return c.exec(ctx, query, args...)
}

func (c *connection) serverVersion() (*ServerVersion, error) {
	// Return a default server version for now
	return &ServerVersion{
		Name:        "Icebox",
		Major:       1,
		Minor:       0,
		Patch:       0,
		Revision:    0,
		Interface:   "Native",
		DefaultDB:   "default",
		Timezone:    "UTC",
		DisplayName: "Icebox Server",
		Version:     "1.0.0",
		Protocol:    1,
	}, nil
}

func (c *connection) handshake(ctx context.Context) error {
	// Send client hello
	if err := c.sendClientHello(); err != nil {
		return fmt.Errorf("failed to send client hello: %w", err)
	}

	// Read server hello
	if err := c.readServerHello(); err != nil {
		return fmt.Errorf("failed to read server hello: %w", err)
	}

	return nil
}

// Protocol helper methods

func (c *connection) sendClientHello() error {
	// ClickHouse ClientHello format: client_name + major_version + minor_version + protocol_version
	// Note: The server only expects these 4 fields, not the additional default_database, username, password

	// Create a buffer for the payload
	payload := make([]byte, 0, 256)

	// Client name (4-byte big-endian length + content)
	clientName := "Icebox Go Client"
	clientNameBytes := []byte(clientName)
	clientNameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(clientNameLen, uint32(len(clientNameBytes)))
	payload = append(payload, clientNameLen...)
	payload = append(payload, clientNameBytes...)

	// Major version (varint)
	majorVersion := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(majorVersion, 1)
	payload = append(payload, majorVersion[:n]...)

	// Minor version (varint)
	minorVersion := make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(minorVersion, 0)
	payload = append(payload, minorVersion[:n]...)

	// Protocol version (varint)
	protocolVersion := make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(protocolVersion, 54460)
	payload = append(payload, protocolVersion[:n]...)

	// Now write the complete message with correct length
	// Message length = 1 (message type) + payload length
	totalLength := 1 + len(payload)

	// Debug logging

	// Write message length (4 bytes, big endian)
	if err := c.writeUint32BigEndian(uint32(totalLength)); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message type
	if err := c.writeByte(ClientHello); err != nil {
		return err
	}

	// Write payload
	if _, err := c.conn.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}

func (c *connection) sendQuery(query string) error {
	// ClickHouse ClientQuery format: query string only
	// The payload should contain just the query string, not length-prefixed

	// Convert query to bytes
	queryBytes := []byte(query)

	// Now write the complete message with correct length
	// Message length = 1 (message type) + payload length
	totalLength := 1 + len(queryBytes)

	// Write message length (4 bytes, big endian)
	if err := c.writeUint32BigEndian(uint32(totalLength)); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message type
	if err := c.writeByte(ClientQuery); err != nil {
		return err
	}

	// Write payload (just the query string)
	if _, err := c.conn.Write(queryBytes); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}

func (c *connection) sendPing() error {
	// Ping has no payload, so message length = 1 (just the message type)

	// Write message length (4 bytes, big endian)
	if err := c.writeUint32BigEndian(1); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write message type
	if err := c.writeByte(ClientPing); err != nil {
		return err
	}

	return nil
}

func (c *connection) readUint32BigEndian() (uint32, error) {
	buf := make([]byte, 4)
	n, err := c.conn.Read(buf)
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, fmt.Errorf("expected to read 4 bytes, got %d", n)
	}
	value := binary.BigEndian.Uint32(buf)

	return value, nil
}

func (c *connection) readServerHello() error {
	// Read message length (4 bytes, big endian)
	_, err := c.readUint32BigEndian()
	if err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	// Read packet type
	packetType, err := c.readByte()
	if err != nil {
		return err
	}

	if packetType != ServerHello {
		return fmt.Errorf("expected ServerHello packet, got %d", packetType)
	}

	// Read server name
	_, err = c.readString()
	if err != nil {
		return err
	}

	// Read server version (as single bytes, not varints)
	_, err = c.readByte() // Major
	if err != nil {
		return err
	}

	_, err = c.readByte() // Minor
	if err != nil {
		return err
	}

	// Read revision (varint)
	_, err = c.readUvarint()
	if err != nil {
		return err
	}

	// Read timezone
	_, err = c.readString()
	if err != nil {
		return err
	}

	// Read display name
	_, err = c.readString()
	if err != nil {
		return err
	}

	// Read version patch (varint)
	_, err = c.readUvarint()
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) readPong() error {
	// Read message length (4 bytes, big endian)
	_, err := c.readUint32BigEndian()
	if err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}

	// Read packet type
	packetType, err := c.readByte()
	if err != nil {
		return err
	}

	if packetType != ServerPong {
		return fmt.Errorf("expected ServerPong packet, got %d", packetType)
	}

	return nil
}

func (c *connection) readQueryResponse(query string) (*Rows, error) {
	// Read response packets until EndOfStream
	var allColumns []Column
	var allData [][]interface{}
	var queryErr error // Store any error encountered during query execution

	for {
		// Read message length (4 bytes, big endian)
		_, err := c.readUint32BigEndian()
		if err != nil {
			return nil, fmt.Errorf("failed to read message length: %w", err)
		}

		// Read packet type
		packetType, err := c.readByte()
		if err != nil {
			return nil, err
		}

		switch packetType {
		case ServerEndOfStream:
			// Return the accumulated rows
			return &Rows{
				Client:   c.client,
				Query:    Query{Body: query, QueryID: GenerateQueryID()},
				Cols:     allColumns,
				Data:     allData,
				Current:  0,
				Closed:   false,
				QueryErr: queryErr, // Store any error encountered
			}, nil
		case ServerException:
			// Read exception details and store the error
			errorMsg, err := c.readString() // Error message
			if err != nil {
				return nil, err
			}
			errorCode, err := c.readUvarint() // Error code
			if err != nil {
				return nil, err
			}
			queryErr = fmt.Errorf("server exception [%d]: %s", errorCode, errorMsg)
			// Continue reading to get EndOfStream
			continue
		case ServerData:
			// Parse the data payload according to the format sent by the server
			// Format: column_count (uvarint) + column_name (string) + column_type (string) + data_block (uvarint) + row_count (uvarint) + data (string)

			// Read column count (uvarint) - should always be 1 for single column
			_, err = c.readUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read column count: %w", err)
			}

			// Read column name (string)
			columnName, err := c.readString()
			if err != nil {
				return nil, fmt.Errorf("failed to read column name: %w", err)
			}

			// Read column type (string)
			columnType, err := c.readString()
			if err != nil {
				return nil, fmt.Errorf("failed to read column type: %w", err)
			}

			// Read data block (uvarint)
			_, err = c.readUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read data block: %w", err)
			}

			// Read row count (uvarint)
			_, err = c.readUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read row count: %w", err)
			}

			// Read data (string)
			data, err := c.readString()
			if err != nil {
				return nil, fmt.Errorf("failed to read data: %w", err)
			}

			// Add this column to our collection
			allColumns = append(allColumns, Column{Name: columnName, Type: columnType})

			// Parse the data string to extract row values for this column
			// The server sends data like "1,2,3" for the id column
			rowValues := strings.Split(data, ",")

			// Initialize allData if this is the first column
			if len(allData) == 0 {
				allData = make([][]interface{}, len(rowValues))
				for i := range allData {
					allData[i] = make([]interface{}, 0, 4) // 4 columns: id, name, email, created_at
				}
			}

			// Add the values for this column to each row
			for i, value := range rowValues {
				if i < len(allData) {
					allData[i] = append(allData[i], strings.TrimSpace(value))
				}
			}

			// Continue reading for more packets (should be more ServerData packets, then ServerEndOfStream)
			continue
		default:
			// Skip other packet types for now
			continue
		}
	}
}

func (c *connection) readExecResponse() error {
	// Read response packets until EndOfStream
	for {
		// Read message length (4 bytes, big endian) - we need this to know packet boundaries
		_, err := c.readUint32BigEndian()
		if err != nil {
			return fmt.Errorf("failed to read message length: %w", err)
		}

		// Read packet type
		packetType, err := c.readByte()
		if err != nil {
			return err
		}

		switch packetType {
		case ServerEndOfStream:
			return nil
		case ServerException:
			// Read exception details
			errorMsg, err := c.readString() // Error message
			if err != nil {
				return err
			}
			errorCode, err := c.readUvarint() // Error code
			if err != nil {
				return err
			}
			return fmt.Errorf("server exception [%d]: %s", errorCode, errorMsg)
		case ServerData:
			// Parse the ServerData packet according to the server's format:
			// columnCount (uvarint) + columnName (string) + columnType (string) + dataBlock (uvarint) + rowCount (uvarint) + data (string)

			// Read column count
			columnCount, err := c.readUvarint()
			if err != nil {
				return fmt.Errorf("failed to read column count: %w", err)
			}

			// Read column names and types
			for i := uint64(0); i < columnCount; i++ {
				// Read column name
				_, err = c.readString()
				if err != nil {
					return fmt.Errorf("failed to read column %d name: %w", i, err)
				}

				// Read column type
				_, err = c.readString()
				if err != nil {
					return fmt.Errorf("failed to read column %d type: %w", i, err)
				}
			}

			// Read data block
			_, err = c.readUvarint()
			if err != nil {
				return fmt.Errorf("failed to read data block: %w", err)
			}

			// Read row count
			_, err = c.readUvarint()
			if err != nil {
				return fmt.Errorf("failed to read row count: %w", err)
			}

			// Read data (raw string)
			_, err = c.readString()
			if err != nil {
				return fmt.Errorf("failed to read data: %w", err)
			}

			// Continue reading for more packets
			continue
		default:
			// Skip other packet types for now
			continue
		}
	}
}

// Helper methods for reading/writing protocol data

func (c *connection) writeByte(b byte) error {
	_, err := c.conn.Write([]byte{b})
	return err
}

func (c *connection) writeUvarint(v uint64) error {
	buf := make([]byte, 10)
	n := binary.PutUvarint(buf, v)
	_, err := c.conn.Write(buf[:n])
	return err
}

func (c *connection) writeUint32(n uint32) error {
	// Write uint32 as 4 bytes, big endian
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, n)
	if _, err := c.conn.Write(buf); err != nil {
		return fmt.Errorf("failed to write uint32: %w", err)
	}
	return nil
}

func (c *connection) writeUint32BigEndian(value uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	_, err := c.conn.Write(buf)
	return err
}

func (c *connection) writeString(s string) error {
	if err := c.writeUint32BigEndian(uint32(len(s))); err != nil {
		return err
	}
	_, err := c.conn.Write([]byte(s))
	return err
}

func (c *connection) readByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := c.conn.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (c *connection) readUvarint() (uint64, error) {
	reader := &netConnReader{conn: c.conn}
	return binary.ReadUvarint(reader)
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

func (c *connection) readString() (string, error) {
	// Read string length as 4 bytes, big endian (not varint)
	length, err := c.readUint32BigEndian()
	if err != nil {
		return "", err
	}

	buf := make([]byte, length)
	_, err = c.conn.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (c *connection) isBad() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bad
}

func (c *connection) markBad() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bad = true
}

func (c *connection) updateLastUsed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastUsed = time.Now()
}

func (c *connection) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}

// sendBatchData sends batch data to the server using ClickHouse native protocol
func (c *connection) sendBatchData(batch *Batch) error {
	// First, we need to calculate the total payload size
	// Start with table name (4 bytes for length + string content)
	payloadSize := 4 + len(batch.TableName)

	// Add column count (4 bytes)
	payloadSize += 4

	// Add row count (4 bytes)
	payloadSize += 4

	// Add column names (4 bytes for length + string content for each column)
	for _, col := range batch.Columns {
		colSize := 4 + len(col)
		payloadSize += colSize
	}

	// Add row data (4 bytes for length + string content for each value)
	for _, row := range batch.Data {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			valueSize := 4 + len(strValue)
			payloadSize += valueSize
		}
	}

	// Total message length = 1 byte (packet type) + payload
	// The 4-byte length prefix itself is NOT included in this calculation
	totalMessageLength := 1 + payloadSize

	// Write the total message length (4 bytes, big endian)
	if err := binary.Write(c.conn, binary.BigEndian, uint32(totalMessageLength)); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write the packet type
	if _, err := c.conn.Write([]byte{ClientData}); err != nil {
		return fmt.Errorf("failed to write packet type: %w", err)
	}

	// Write table name
	if err := c.writeString(batch.TableName); err != nil {
		return fmt.Errorf("failed to write table name: %w", err)
	}

	// Write column count
	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(batch.Columns))); err != nil {
		return fmt.Errorf("failed to write column count: %w", err)
	}

	// Write row count
	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(batch.Data))); err != nil {
		return fmt.Errorf("failed to write row count: %w", err)
	}

	// Write column names
	for _, col := range batch.Columns {
		if err := c.writeString(col); err != nil {
			return fmt.Errorf("failed to write column name %s: %w", col, err)
		}
	}

	// Write row data
	for _, row := range batch.Data {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			if err := c.writeString(strValue); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
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
	// Icebox doesn't support transactions, so we return a no-op transaction
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
	// No-op for Icebox
	return nil
}

func (t *sqlTx) Rollback() error {
	// No-op for Icebox
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
