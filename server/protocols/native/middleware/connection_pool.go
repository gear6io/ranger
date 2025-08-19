package middleware

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// ConnectionPool manages connection lifecycle and pooling
type ConnectionPool struct {
	mu                sync.RWMutex
	activeConnections map[string]*ConnectionContext
	maxConnections    int
	idleTimeout       time.Duration
	cleanupInterval   time.Duration
	logger            zerolog.Logger

	// Stats
	totalConnections    int64
	peakConnections     int64
	rejectedConnections int64

	// Cleanup
	stopCleanup chan struct{}
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxConnections int, idleTimeout, cleanupInterval time.Duration, logger zerolog.Logger) *ConnectionPool {
	pool := &ConnectionPool{
		activeConnections: make(map[string]*ConnectionContext),
		maxConnections:    maxConnections,
		idleTimeout:       idleTimeout,
		cleanupInterval:   cleanupInterval,
		logger:            logger,
		stopCleanup:       make(chan struct{}),
	}

	// Start cleanup goroutine
	go pool.cleanupRoutine()

	return pool
}

// OnEvent handles connection lifecycle events
func (p *ConnectionPool) OnEvent(ctx context.Context, connCtx *ConnectionContext, event ConnectionEvent, err error) error {
	switch event {
	case EventConnected:
		return p.handleConnection(connCtx)
	case EventDisconnected:
		return p.handleDisconnection(connCtx)
	case EventAuthenticated:
		connCtx.State = StateAuthenticated
		return nil
	case EventQueryStarted:
		connCtx.State = StateQuerying
		connCtx.LastActivity = time.Now()
		return nil
	case EventQueryCompleted, EventQueryFailed:
		connCtx.State = StateIdle
		connCtx.LastActivity = time.Now()
		return nil
	}
	return nil
}

// OnRead updates last activity time
func (p *ConnectionPool) OnRead(ctx context.Context, connCtx *ConnectionContext) error {
	connCtx.LastActivity = time.Now()
	return nil
}

// OnWrite updates last activity time
func (p *ConnectionPool) OnWrite(ctx context.Context, connCtx *ConnectionContext) error {
	connCtx.LastActivity = time.Now()
	return nil
}

// OnError tracks error count
func (p *ConnectionPool) OnError(ctx context.Context, connCtx *ConnectionContext, err error) error {
	connCtx.ErrorCount++
	return err
}

// OnQuery tracks query count
func (p *ConnectionPool) OnQuery(ctx context.Context, connCtx *ConnectionContext, query string) error {
	atomic.AddInt64(&connCtx.QueryCount, 1)
	return nil
}

// handleConnection attempts to add a new connection to the pool
func (p *ConnectionPool) handleConnection(connCtx *ConnectionContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if we're at capacity
	if len(p.activeConnections) >= p.maxConnections {
		atomic.AddInt64(&p.rejectedConnections, 1)
		p.logger.Warn().
			Str("client", connCtx.ClientAddr).
			Int("max_connections", p.maxConnections).
			Int("active_connections", len(p.activeConnections)).
			Msg("Connection rejected - pool at capacity")

		return fmt.Errorf("connection pool at capacity (%d)", p.maxConnections)
	}

	// Add connection to pool
	p.activeConnections[connCtx.ConnectionID] = connCtx
	atomic.AddInt64(&p.totalConnections, 1)

	// Update peak connections
	if current := int64(len(p.activeConnections)); current > atomic.LoadInt64(&p.peakConnections) {
		atomic.StoreInt64(&p.peakConnections, current)
	}

	p.logger.Debug().
		Str("client", connCtx.ClientAddr).
		Str("connection_id", connCtx.ConnectionID).
		Int("active_connections", len(p.activeConnections)).
		Msg("Connection added to pool")

	return nil
}

// handleDisconnection removes a connection from the pool
func (p *ConnectionPool) handleDisconnection(connCtx *ConnectionContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.activeConnections[connCtx.ConnectionID]; exists {
		delete(p.activeConnections, connCtx.ConnectionID)

		p.logger.Debug().
			Str("client", connCtx.ClientAddr).
			Str("connection_id", connCtx.ConnectionID).
			Int("active_connections", len(p.activeConnections)).
			Msg("Connection removed from pool")
	}

	return nil
}

// cleanupRoutine periodically removes idle connections
func (p *ConnectionPool) cleanupRoutine() {
	ticker := time.NewTicker(p.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cleanupIdleConnections()
		case <-p.stopCleanup:
			return
		}
	}
}

// cleanupIdleConnections removes connections that have been idle too long
func (p *ConnectionPool) cleanupIdleConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-p.idleTimeout)

	var toRemove []string

	for connID, connCtx := range p.activeConnections {
		if connCtx.LastActivity.Before(cutoff) && connCtx.State == StateIdle {
			toRemove = append(toRemove, connID)
		}
	}

	for _, connID := range toRemove {
		connCtx := p.activeConnections[connID]
		delete(p.activeConnections, connID)

		p.logger.Info().
			Str("client", connCtx.ClientAddr).
			Str("connection_id", connID).
			Dur("idle_time", now.Sub(connCtx.LastActivity)).
			Msg("Removing idle connection")
	}
}

// GetStats returns connection pool statistics
func (p *ConnectionPool) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"active_connections":   len(p.activeConnections),
		"max_connections":      p.maxConnections,
		"total_connections":    atomic.LoadInt64(&p.totalConnections),
		"peak_connections":     atomic.LoadInt64(&p.peakConnections),
		"rejected_connections": atomic.LoadInt64(&p.rejectedConnections),
		"idle_timeout":         p.idleTimeout,
		"cleanup_interval":     p.cleanupInterval,
	}
}

// Close stops the connection pool and cleans up resources
func (p *ConnectionPool) Close() error {
	close(p.stopCleanup)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Log final stats
	p.logger.Info().
		Interface("final_stats", p.GetStats()).
		Msg("Connection pool closed")

	return nil
}
