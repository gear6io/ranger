package sdk

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	// Unified protocol package
	"github.com/TFMV/icebox/server/protocols/native/protocol"
	"github.com/TFMV/icebox/server/protocols/native/protocol/signals"
)

// CompressionMethod represents compression methods
type CompressionMethod byte

func (c CompressionMethod) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionZSTD:
		return "zstd"
	case CompressionLZ4:
		return "lz4"
	case CompressionLZ4HC:
		return "lz4hc"
	case CompressionGZIP:
		return "gzip"
	case CompressionDeflate:
		return "deflate"
	case CompressionBrotli:
		return "br"
	default:
		return ""
	}
}

const (
	CompressionNone    = CompressionMethod(0)
	CompressionLZ4     = CompressionMethod(1)
	CompressionLZ4HC   = CompressionMethod(2)
	CompressionZSTD    = CompressionMethod(3)
	CompressionGZIP    = CompressionMethod(4)
	CompressionDeflate = CompressionMethod(5)
	CompressionBrotli  = CompressionMethod(6)
)

// Protocol represents connection protocol
type Protocol int

const (
	Native Protocol = iota
	HTTP
)

func (p Protocol) String() string {
	switch p {
	case Native:
		return "native"
	case HTTP:
		return "http"
	default:
		return "unknown"
	}
}

// ConnOpenStrategy represents connection opening strategy
type ConnOpenStrategy uint8

const (
	ConnOpenInOrder ConnOpenStrategy = iota
	ConnOpenRoundRobin
	ConnOpenRandom
)

// Auth represents authentication information
type Auth struct {
	Database string
	Username string
	Password string
}

// Compression represents compression settings
type Compression struct {
	Method CompressionMethod
	Level  int // applies to lz4, lz4hc, zlib, and brotli
}

// Settings represents query and connection settings
type Settings map[string]interface{}

// Get returns a setting value with type assertion
func (s Settings) Get(key string) interface{} {
	return s[key]
}

// GetString returns a string setting value
func (s Settings) GetString(key string) string {
	if v, ok := s[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// GetInt returns an int setting value
func (s Settings) GetInt(key string) int {
	if v, ok := s[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case int64:
			return int(val)
		case float64:
			return int(val)
		case string:
			if i, err := strconv.Atoi(val); err == nil {
				return i
			}
		}
	}
	return 0
}

// GetBool returns a boolean setting value
func (s Settings) GetBool(key string) bool {
	if v, ok := s[key]; ok {
		switch val := v.(type) {
		case bool:
			return val
		case string:
			return strings.ToLower(val) == "true" || val == "1"
		case int:
			return val != 0
		}
	}
	return false
}

// Set sets a setting value
func (s Settings) Set(key string, value interface{}) {
	s[key] = value
}

// Dial represents a dial function
type Dial func(ctx context.Context, addr string, opt *Options) (net.Conn, error)

// DialResult represents a dial result
type DialResult struct {
	conn net.Conn
}

// Options represents comprehensive client options
type Options struct {
	// Protocol and connection
	Protocol     Protocol
	Addr         []string
	Auth         Auth
	DialContext  func(ctx context.Context, addr string) (net.Conn, error)
	DialStrategy func(ctx context.Context, connID int, options *Options, dial Dial) (DialResult, error)

	// TLS and security
	TLS *tls.Config

	// Compression
	Compression *Compression

	// Connection pooling
	MaxOpenConns     int           // default 10
	MaxIdleConns     int           // default 5
	ConnMaxLifetime  time.Duration // default 1 hour
	ConnOpenStrategy ConnOpenStrategy

	// Timeouts
	DialTimeout  time.Duration // default 30 seconds
	ReadTimeout  time.Duration // default 3 seconds
	WriteTimeout time.Duration // default 3 seconds

	// Settings and configuration
	Settings Settings
	Debug    bool
	Debugf   func(format string, v ...any)

	// HTTP specific (for HTTP protocol)
	HTTPHeaders         map[string]string
	HTTPURLPath         string
	HTTPMaxConnsPerHost int
	HTTPProxyURL        *url.URL

	// Performance
	BlockBufferSize      uint8 // default 2
	MaxCompressionBuffer int   // default 10MB
	FreeBufOnConnRelease bool  // default false

	// Logging
	Logger *zap.Logger

	// Internal
	scheme string
}

// Client represents an Icebox client with connection pooling
type Client struct {
	opt    *Options
	idle   chan *connection
	open   chan struct{}
	exit   chan struct{}
	connID int64
	mu     sync.RWMutex
	closed bool
}

// connection represents a pooled connection
type connection struct {
	conn      net.Conn
	client    *Client
	connID    int
	createdAt time.Time
	lastUsed  time.Time
	released  bool
	bad       bool
	mu        sync.Mutex

	// Unified protocol components
	codec    *protocol.DefaultCodec
	registry *protocol.Registry
	factory  *protocol.SignalFactory
}

// NewClient creates a new Icebox client
func NewClient(opt *Options) (*Client, error) {
	if opt == nil {
		opt = &Options{}
	}

	o := opt.SetDefaults()

	client := &Client{
		opt:  o,
		idle: make(chan *connection, o.MaxIdleConns),
		open: make(chan struct{}, o.MaxOpenConns),
		exit: make(chan struct{}),
	}

	go client.startAutoCloseIdleConnections()
	return client, nil
}

// Open is a convenience function to create a client and open a connection
func Open(opt *Options) (*Client, error) {
	client, err := NewClient(opt)
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}

// OpenDB creates a database/sql.DB compatible connection
func OpenDB(opt *Options) (*sql.DB, error) {
	connector := Connector(opt)
	return sql.OpenDB(connector), nil
}

// SetDefaults sets default values for options
func (o *Options) SetDefaults() *Options {
	if o.Protocol == 0 {
		o.Protocol = Native
	}

	if len(o.Addr) == 0 {
		o.Addr = []string{"127.0.0.1:2849"}
	}

	// Only set username default if it's truly unset (not explicitly empty from DSN)
	// We can detect this by checking if the Options was just created vs parsed from DSN
	if o.Auth.Username == "" && o.scheme == "" {
		o.Auth.Username = "default"
	}

	if o.Auth.Database == "" {
		o.Auth.Database = "default"
	}

	if o.DialTimeout == 0 {
		o.DialTimeout = 30 * time.Second
	}

	if o.ReadTimeout == 0 {
		o.ReadTimeout = 3 * time.Second
	}

	if o.WriteTimeout == 0 {
		o.WriteTimeout = 3 * time.Second
	}

	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = 10
	}

	if o.MaxIdleConns == 0 {
		o.MaxIdleConns = 5
	}

	if o.ConnMaxLifetime == 0 {
		o.ConnMaxLifetime = time.Hour
	}

	if o.BlockBufferSize == 0 {
		o.BlockBufferSize = 2
	}

	if o.MaxCompressionBuffer == 0 {
		o.MaxCompressionBuffer = 10 * 1024 * 1024 // 10MB
	}

	if o.Settings == nil {
		o.Settings = make(Settings)
	}

	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}

	return o
}

// ParseDSN parses a DSN string into Options
func ParseDSN(dsn string) (*Options, error) {
	opt := &Options{}
	// Parse DSN first to set the scheme, then apply defaults
	if err := opt.fromDSN(dsn); err != nil {
		return nil, err
	}
	opt = opt.SetDefaults() // Initialize defaults including Settings map
	return opt, nil
}

// fromDSN parses DSN string
func (o *Options) fromDSN(dsn string) error {
	// Parse DSN format: icebox://[username:password@]host:port/database?param=value
	if !strings.HasPrefix(dsn, "icebox://") {
		return errors.New("invalid DSN format, must start with icebox://")
	}

	// Mark that this Options was created from DSN parsing
	o.scheme = "icebox"

	// Initialize Settings map if it's nil
	if o.Settings == nil {
		o.Settings = make(Settings)
	}

	dsn = strings.TrimPrefix(dsn, "icebox://")

	// Check if DSN has auth part (contains @)
	if strings.Contains(dsn, "@") {
		// Split into auth and host parts
		parts := strings.SplitN(dsn, "@", 2)
		if len(parts) != 2 {
			return errors.New("invalid DSN format")
		}

		// Parse auth
		auth := parts[0]
		if auth != "" {
			authParts := strings.SplitN(auth, ":", 2)
			if len(authParts) == 2 {
				o.Auth.Username = authParts[0]
				o.Auth.Password = authParts[1]
			} else {
				o.Auth.Username = authParts[0]
			}
		}

		// Parse host and database
		hostDB := parts[1]
		hostDBParts := strings.SplitN(hostDB, "/", 2)

		o.Addr = []string{hostDBParts[0]}

		if len(hostDBParts) > 1 {
			dbParts := strings.SplitN(hostDBParts[1], "?", 2)
			o.Auth.Database = dbParts[0]

			// Parse query parameters
			if len(dbParts) > 1 {
				params := dbParts[1]
				query, err := url.ParseQuery(params)
				if err != nil {
					return errors.Wrap(err, "parse query parameters")
				}

				for key, values := range query {
					if len(values) > 0 {
						o.Settings.Set(key, values[0])
					}
				}
			}
		}
	} else {
		// No auth part, parse directly as host:port/database
		hostDBParts := strings.SplitN(dsn, "/", 2)

		o.Addr = []string{hostDBParts[0]}

		if len(hostDBParts) > 1 {
			dbParts := strings.SplitN(hostDBParts[1], "?", 2)
			o.Auth.Database = dbParts[0]

			// Parse query parameters
			if len(dbParts) > 1 {
				params := dbParts[1]
				query, err := url.ParseQuery(params)
				if err != nil {
					return errors.Wrap(err, "parse query parameters")
				}

				for key, values := range query {
					if len(values) > 0 {
						o.Settings.Set(key, values[0])
					}
				}
			}
		}
	}

	return nil
}

// Ping sends a ping to the server
func (c *Client) Ping(ctx context.Context) error {
	conn, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer c.release(conn, err)

	return conn.ping(ctx)
}

// Query executes a query and returns results
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	conn, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := conn.query(ctx, query, args...)
	if err != nil {
		c.release(conn, err)
		return nil, err
	}

	// Release connection when rows are closed
	rows.onClose = func() {
		c.release(conn, nil)
	}

	return rows, nil
}

// ExecuteQuery executes a query and returns results (alias for Query for compatibility)
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*Rows, error) {
	return c.Query(ctx, query)
}

// QueryRow executes a query and returns a single row
func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return &Row{err: err}
	}

	if !rows.Next() {
		rows.Close()
		return &Row{err: sql.ErrNoRows}
	}

	return &Row{rows: rows}
}

// Exec executes a query without returning results
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	conn, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer c.release(conn, err)

	return conn.exec(ctx, query, args...)
}

// PrepareBatch prepares a batch for insertion
func (c *Client) PrepareBatch(ctx context.Context, query string, opts ...BatchOption) (*Batch, error) {
	conn, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}

	batch, err := conn.prepareBatch(ctx, query, opts...)
	if err != nil {
		c.release(conn, err)
		return nil, err
	}

	// Release connection when batch is closed
	batch.onClose = func() {
		c.release(conn, nil)
	}

	return batch, nil
}

// AsyncInsert performs an asynchronous insert
func (c *Client) AsyncInsert(ctx context.Context, query string, wait bool, args ...interface{}) error {
	conn, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer c.release(conn, err)

	return conn.asyncInsert(ctx, query, wait, args...)
}

// ServerVersion returns server version information
func (c *Client) ServerVersion() (*ServerVersion, error) {
	conn, err := c.acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer c.release(conn, err)

	return conn.serverVersion()
}

// Stats returns connection statistics
func (c *Client) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return Stats{
		MaxOpenConnections: c.opt.MaxOpenConns,
		OpenConnections:    len(c.open),
		InUse:              c.opt.MaxOpenConns - len(c.open),
		Idle:               len(c.idle),
	}
}

// Close closes the client and all connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	close(c.exit)

	// Close all idle connections
	for {
		select {
		case conn := <-c.idle:
			conn.close()
		default:
			return nil
		}
	}
}

// acquire acquires a connection from the pool
func (c *Client) acquire(ctx context.Context) (*connection, error) {
	// Try to get an idle connection
	select {
	case conn := <-c.idle:
		if conn.isBad() {
			conn.close()
			return c.dial(ctx)
		}
		return conn, nil
	default:
	}

	// Try to open a new connection
	select {
	case c.open <- struct{}{}:
		return c.dial(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Wait for a connection to become available
		select {
		case conn := <-c.idle:
			if conn.isBad() {
				conn.close()
				return c.dial(ctx)
			}
			return conn, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// release releases a connection back to the pool
func (c *Client) release(conn *connection, err error) {
	if err != nil {
		conn.markBad()
		conn.close()
		<-c.open
		return
	}

	conn.updateLastUsed()

	select {
	case c.idle <- conn:
	default:
		conn.close()
		<-c.open
	}
}

// dial creates a new connection
func (c *Client) dial(ctx context.Context) (*connection, error) {
	connID := int(atomic.AddInt64(&c.connID, 1))

	var conn net.Conn
	var err error

	// Choose address based on strategy
	addr := c.chooseAddr(connID)

	if c.opt.DialContext != nil {
		conn, err = c.opt.DialContext(ctx, addr)
	} else {
		dialer := &net.Dialer{Timeout: c.opt.DialTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}

	if err != nil {
		return nil, err
	}

	connection := newConnection(conn, c, connID)

	// Perform handshake
	if err := connection.handshake(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	return connection, nil
}

// newConnection creates a new connection with initialized protocol components
func newConnection(conn net.Conn, client *Client, connID int) *connection {
	// Initialize unified protocol components
	registry := protocol.NewRegistry()
	factory := protocol.NewSignalFactory()
	codec := protocol.NewDefaultCodec(registry, factory)

	connection := &connection{
		conn:      conn,
		client:    client,
		connID:    connID,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		codec:     codec,
		registry:  registry,
		factory:   factory,
	}

	// Register constructors for all signals
	connection.registerSignalConstructors()

	return connection
}

// handshake performs the initial protocol handshake with the server
func (c *connection) handshake(ctx context.Context) error {
	// Protocol components are already initialized in connection creation

	// Send ClientHello
	clientHello := signals.NewClientHello(
		"Icebox SDK",
		c.client.opt.Auth.Database,
		c.client.opt.Auth.Username,
		c.client.opt.Auth.Password,
	)

	// Encode and send the message
	message, err := c.codec.EncodeMessage(clientHello)
	if err != nil {
		return errors.Wrap(err, "failed to encode client hello")
	}

	if err := c.codec.WriteMessage(c.conn, message); err != nil {
		return errors.Wrap(err, "failed to send client hello")
	}

	// Read and process ServerHello response
	response, err := c.codec.ReadMessage(c.conn)
	if err != nil {
		return errors.Wrap(err, "failed to read server hello")
	}

	signal, err := c.codec.UnpackSignal(response)
	if err != nil {
		return errors.Wrap(err, "failed to unpack server hello")
	}

	if signal.Type() != protocol.ServerHello {
		return errors.Errorf("expected server hello, got signal type %d", signal.Type())
	}

	return nil
}

// registerSignalConstructors registers constructors for all signal types using the new clean auto-registration pattern
func (c *connection) registerSignalConstructors() {
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

	// Register signals
	for _, signal := range signals {
		if err := signal.Register(c.registry, c.factory); err != nil {
			fmt.Printf("Warning: Failed to register signal %T: %v\n", signal, err)
		}
	}
}

// chooseAddr chooses an address based on the connection strategy
func (c *Client) chooseAddr(connID int) string {
	if len(c.opt.Addr) == 1 {
		return c.opt.Addr[0]
	}

	var index int
	switch c.opt.ConnOpenStrategy {
	case ConnOpenInOrder:
		index = connID % len(c.opt.Addr)
	case ConnOpenRoundRobin:
		index = connID % len(c.opt.Addr)
	case ConnOpenRandom:
		index = rand.Intn(len(c.opt.Addr))
	default:
		index = connID % len(c.opt.Addr)
	}

	return c.opt.Addr[index]
}

// startAutoCloseIdleConnections starts the background goroutine to close idle connections
func (c *Client) startAutoCloseIdleConnections() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.closeIdleExpired()
		case <-c.exit:
			return
		}
	}
}

// closeIdleExpired closes expired idle connections
func (c *Client) closeIdleExpired() {
	now := time.Now()

	for {
		select {
		case conn := <-c.idle:
			if now.Sub(conn.lastUsed) > c.opt.ConnMaxLifetime {
				conn.close()
				<-c.open
			} else {
				// Put it back
				select {
				case c.idle <- conn:
				default:
					conn.close()
					<-c.open
				}
			}
		default:
			return
		}
	}
}

// Stats represents connection statistics
type Stats struct {
	MaxOpenConnections int
	OpenConnections    int
	InUse              int
	Idle               int
}

// Errors
var (
	ErrBatchInvalid              = errors.New("icebox: batch is invalid. check appended data is correct")
	ErrBatchAlreadySent          = errors.New("icebox: batch has already been sent")
	ErrBatchNotSent              = errors.New("icebox: invalid retry, batch not sent yet")
	ErrAcquireConnTimeout        = errors.New("icebox: acquire conn timeout. you can increase the number of max open conn or the dial timeout")
	ErrUnsupportedServerRevision = errors.New("icebox: unsupported server revision")
	ErrBindMixedParamsFormats    = errors.New("icebox [bind]: mixed named, numeric or positional parameters")
	ErrAcquireConnNoAddress      = errors.New("icebox: no valid address supplied")
	ErrServerUnexpectedData      = errors.New("code: 101, message: Unexpected packet Data received from client")
)

// ping sends a ping to the server using unified protocol
func (c *connection) ping(ctx context.Context) error {
	// Create and send ClientPing signal
	ping := signals.NewClientPing(time.Now().Unix())

	message, err := c.codec.EncodeMessage(ping)
	if err != nil {
		return errors.Wrap(err, "failed to encode client ping")
	}

	if err := c.codec.WriteMessage(c.conn, message); err != nil {
		return errors.Wrap(err, "failed to send client ping")
	}

	// Read and process ServerPong response
	response, err := c.codec.ReadMessage(c.conn)
	if err != nil {
		return errors.Wrap(err, "failed to read server pong")
	}

	signal, err := c.codec.UnpackSignal(response)
	if err != nil {
		return errors.Wrap(err, "failed to unpack server pong")
	}

	if signal.Type() != protocol.ServerPong {
		return errors.Errorf("expected server pong, got signal type %d", signal.Type())
	}

	return nil
}

// query executes a query using unified protocol
func (c *connection) query(ctx context.Context, queryStr string, args ...interface{}) (*Rows, error) {
	// Create and send ClientQuery signal
	queryID := generateQueryID()
	query := signals.NewClientQuery(queryStr, queryID, c.client.opt.Auth.Database, c.client.opt.Auth.Username, c.client.opt.Auth.Password)

	message, err := c.codec.EncodeMessage(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode client query")
	}

	if err := c.codec.WriteMessage(c.conn, message); err != nil {
		return nil, errors.Wrap(err, "failed to send client query")
	}

	// Read and process server response
	rows := &Rows{
		Client: c.client,
		Query: Query{
			Body:    queryStr,
			QueryID: queryID,
		},
		Cols:    []Column{},
		Data:    [][]interface{}{},
		Current: 0,
		Closed:  false,
	}

	// Read response packets using unified protocol until EndOfStream
	for {
		// Read and decode message using unified protocol
		message, err := c.codec.ReadMessage(c.conn)
		if err != nil {
			if err == io.EOF {
				break // Client disconnected, stop reading
			}
			return nil, errors.Wrap(err, "failed to read message")
		}

		// Unpack the message into a signal
		signal, err := c.codec.UnpackSignal(message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unpack message")
		}

		switch signal.Type() {
		case protocol.ServerEndOfStream:
			// End of response, return the rows
			return rows, nil
		case protocol.ServerException:
			// Handle exception signal
			exception := signal.(*signals.ServerException)
			return nil, fmt.Errorf("server exception [%d]: %s", exception.ErrorCode, exception.ErrorMessage)
		case protocol.ServerData:
			// Handle data signal - extract column information and data
			serverData := signal.(*signals.ServerData)

			// Extract column information
			if len(rows.Cols) == 0 {
				rows.Cols = make([]Column, len(serverData.Columns))
				for i, col := range serverData.Columns {
					rows.Cols[i] = Column{
						Name: col.Name,
						Type: col.Type,
					}
				}
			}

			// Extract row data
			if len(serverData.Rows) > 0 {
				rows.Data = append(rows.Data, serverData.Rows...)
			}

			continue
		case protocol.ServerProgress:
			// Handle progress signal - just continue reading
			continue
		default:
			// Skip other packet types for now
			continue
		}
	}

	return rows, nil
}

// exec executes a query without returning results using unified protocol
func (c *connection) exec(ctx context.Context, queryStr string, args ...interface{}) error {
	// Create and send ClientQuery signal
	queryID := generateQueryID()
	query := signals.NewClientQuery(queryStr, queryID, c.client.opt.Auth.Database, c.client.opt.Auth.Username, c.client.opt.Auth.Password)

	message, err := c.codec.EncodeMessage(query)
	if err != nil {
		return errors.Wrap(err, "failed to encode client query")
	}

	if err := c.codec.WriteMessage(c.conn, message); err != nil {
		return errors.Wrap(err, "failed to send client query")
	}

	// Read response packets using unified protocol until EndOfStream
	for {
		// Read and decode message using unified protocol
		message, err := c.codec.ReadMessage(c.conn)
		if err != nil {
			if err == io.EOF {
				break // Client disconnected, stop reading
			}
			return errors.Wrap(err, "failed to read message")
		}

		// Unpack the message into a signal
		signal, err := c.codec.UnpackSignal(message)
		if err != nil {
			return errors.Wrap(err, "failed to unpack message")
		}

		switch signal.Type() {
		case protocol.ServerEndOfStream:
			// End of response, execution successful
			return nil
		case protocol.ServerException:
			// Handle exception signal
			exception := signal.(*signals.ServerException)
			return fmt.Errorf("server exception [%d]: %s", exception.ErrorCode, exception.ErrorMessage)
		case protocol.ServerData:
			// Handle data signal - just continue reading for exec
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

// prepareBatch prepares a batch for insertion using unified protocol
func (c *connection) prepareBatch(ctx context.Context, queryStr string, opts ...BatchOption) (*Batch, error) {
	// Create batch with proper initialization
	batch := &Batch{
		Query: Query{
			Body:    queryStr,
			QueryID: generateQueryID(),
		},
		TableName: "",
		Columns:   []string{},
		Data:      [][]interface{}{},
		Sent:      false,
	}

	// Apply options
	options := &BatchOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return batch, nil
}

// asyncInsert performs an asynchronous insert using unified protocol
func (c *connection) asyncInsert(ctx context.Context, queryStr string, wait bool, args ...interface{}) error {
	// For now, just execute synchronously with proper response handling
	return c.exec(ctx, queryStr, args...)
}

// close closes the connection
func (c *connection) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.released {
		c.conn.Close()
		c.released = true
	}
}

// generateQueryID generates a unique query ID
func generateQueryID() string {
	return fmt.Sprintf("query_%d", time.Now().UnixNano())
}

// isBad checks if the connection is marked as bad
func (c *connection) isBad() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bad
}

// markBad marks the connection as bad
func (c *connection) markBad() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bad = true
}

// updateLastUsed updates the last used timestamp
func (c *connection) updateLastUsed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastUsed = time.Now()
}

// serverVersion returns server version information using unified protocol
func (c *connection) serverVersion() (*ServerVersion, error) {
	// For now, return a default server version
	// TODO: Implement proper server version retrieval using unified protocol
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
