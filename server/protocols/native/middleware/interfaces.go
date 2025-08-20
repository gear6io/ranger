package middleware

import (
	"context"
	"time"
)

// ConnectionEvent represents different connection lifecycle events
type ConnectionEvent int

const (
	EventConnected ConnectionEvent = iota
	EventDisconnected
	EventReadError
	EventWriteError
	EventProtocolError
	EventTimeout
	EventAuthenticated
	EventQueryStarted
	EventQueryCompleted
	EventQueryFailed
	EventResourceExceeded
)

// ConnectionState represents the current state of a connection
type ConnectionState int

const (
	StateHandshaking ConnectionState = iota
	StateAuthenticating
	StateAuthenticated
	StateQuerying
	StateIdle
	StateClosing
	StateClosed
)

// ConnectionContext holds connection state and metadata
type ConnectionContext struct {
	ClientAddr    string
	ConnectionID  string
	UserAgent     string
	Database      string
	Username      string
	StartTime     time.Time
	LastActivity  time.Time
	ErrorCount    int
	State         ConnectionState
	AuthToken     string
	QueryCount    int64
	ResourceUsage ResourceUsage
}

// ResourceUsage tracks resource consumption
type ResourceUsage struct {
	MemoryBytes   int64
	CPUTime       time.Duration
	QueryDuration time.Duration
	RowsProcessed int64
}

// Middleware defines the interface for connection middleware
type Middleware interface {
	// OnEvent is called for each connection event
	OnEvent(ctx context.Context, connCtx *ConnectionContext, event ConnectionEvent, err error) error

	// OnRead is called before reading from connection
	OnRead(ctx context.Context, connCtx *ConnectionContext) error

	// OnWrite is called before writing to connection
	OnWrite(ctx context.Context, connCtx *ConnectionContext) error

	// OnError is called when an error occurs
	OnError(ctx context.Context, connCtx *ConnectionContext, err error) error

	// OnQuery is called before executing a query
	OnQuery(ctx context.Context, connCtx *ConnectionContext, query string) error
}

// Chain represents a chain of middleware
type Chain struct {
	middlewares []Middleware
}

// NewChain creates a new middleware chain
func NewChain(middlewares ...Middleware) *Chain {
	return &Chain{middlewares: middlewares}
}

// Execute runs the middleware chain for an event
func (c *Chain) Execute(ctx context.Context, connCtx *ConnectionContext, event ConnectionEvent, err error) error {
	for _, middleware := range c.middlewares {
		if err := middleware.OnEvent(ctx, connCtx, event, err); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteRead runs the middleware chain before reading
func (c *Chain) ExecuteRead(ctx context.Context, connCtx *ConnectionContext) error {
	for _, middleware := range c.middlewares {
		if err := middleware.OnRead(ctx, connCtx); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteWrite runs the middleware chain before writing
func (c *Chain) ExecuteWrite(ctx context.Context, connCtx *ConnectionContext) error {
	for _, middleware := range c.middlewares {
		if err := middleware.OnWrite(ctx, connCtx); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteQuery runs the middleware chain before query execution
func (c *Chain) ExecuteQuery(ctx context.Context, connCtx *ConnectionContext, query string) error {
	for _, middleware := range c.middlewares {
		if err := middleware.OnQuery(ctx, connCtx, query); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteError runs the middleware chain for error handling
func (c *Chain) ExecuteError(ctx context.Context, connCtx *ConnectionContext, err error) error {
	for _, middleware := range c.middlewares {
		if err := middleware.OnError(ctx, connCtx, err); err != nil {
			return err
		}
	}
	return nil
}
