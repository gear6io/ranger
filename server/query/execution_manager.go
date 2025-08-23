package query

import (
	"context"
	"sync"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/rs/zerolog"
)

// QueryStatus represents the status of a query
type QueryStatus string

const (
	QueryStatusPending   QueryStatus = "pending"
	QueryStatusRunning   QueryStatus = "running"
	QueryStatusCompleted QueryStatus = "completed"
	QueryStatusCancelled QueryStatus = "cancelled"
	QueryStatusFailed    QueryStatus = "failed"
)

// QueryInfo represents information about a running query
type QueryInfo struct {
	ID         string             `json:"id"`
	Query      string             `json:"query"`
	Status     QueryStatus        `json:"status"`
	StartTime  time.Time          `json:"start_time"`
	EndTime    *time.Time         `json:"end_time,omitempty"`
	Duration   *time.Duration     `json:"duration,omitempty"`
	User       string             `json:"user"`
	ClientAddr string             `json:"client_addr"`
	CancelFunc context.CancelFunc `json:"-"`
	Error      error              `json:"error,omitempty"`
	RowCount   int64              `json:"row_count,omitempty"`
}

// ExecutionManager manages running queries and provides cancellation capabilities
type ExecutionManager struct {
	queries map[string]*QueryInfo
	mu      sync.RWMutex
	logger  zerolog.Logger
}

// NewExecutionManager creates a new execution manager
func NewExecutionManager(logger zerolog.Logger) *ExecutionManager {
	return &ExecutionManager{
		queries: make(map[string]*QueryInfo),
		logger:  logger,
	}
}

// StartQuery starts tracking a new query
func (em *ExecutionManager) StartQuery(ctx context.Context, queryID, query, user, clientAddr string) (*QueryInfo, context.Context) {
	em.mu.Lock()
	defer em.mu.Unlock()

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)

	queryInfo := &QueryInfo{
		ID:         queryID,
		Query:      query,
		Status:     QueryStatusPending,
		StartTime:  time.Now(),
		User:       user,
		ClientAddr: clientAddr,
		CancelFunc: cancel,
	}

	em.queries[queryID] = queryInfo
	em.logger.Debug().Str("query_id", queryID).Msg("Query started tracking")

	// Update status to running
	queryInfo.Status = QueryStatusRunning

	return queryInfo, ctx
}

// CompleteQuery marks a query as completed
func (em *ExecutionManager) CompleteQuery(queryID string, rowCount int64, err error) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	queryInfo, exists := em.queries[queryID]
	if !exists {
		return errors.New(ErrQueryNotFound, "query not found", nil).AddContext("query_id", queryID)
	}

	now := time.Now()
	duration := now.Sub(queryInfo.StartTime)

	queryInfo.EndTime = &now
	queryInfo.Duration = &duration
	queryInfo.RowCount = rowCount

	if err != nil {
		queryInfo.Status = QueryStatusFailed
		queryInfo.Error = err
	} else {
		queryInfo.Status = QueryStatusCompleted
	}

	em.logger.Debug().
		Str("query_id", queryID).
		Str("status", string(queryInfo.Status)).
		Dur("duration", duration).
		Int64("row_count", rowCount).
		Msg("Query completed")

	return nil
}

// CancelQuery cancels a running query
func (em *ExecutionManager) CancelQuery(queryID string) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	queryInfo, exists := em.queries[queryID]
	if !exists {
		return errors.New(ErrQueryNotFound, "query not found", nil).AddContext("query_id", queryID)
	}

	if queryInfo.Status != QueryStatusRunning {
		return errors.New(ErrQueryNotRunning, "query is not running", nil).AddContext("query_id", queryID).AddContext("status", string(queryInfo.Status))
	}

	// Cancel the context
	queryInfo.CancelFunc()

	// Update status
	now := time.Now()
	duration := now.Sub(queryInfo.StartTime)
	queryInfo.EndTime = &now
	queryInfo.Duration = &duration
	queryInfo.Status = QueryStatusCancelled

	em.logger.Info().
		Str("query_id", queryID).
		Dur("duration", duration).
		Msg("Query cancelled")

	return nil
}

// GetQueryInfo returns information about a specific query
func (em *ExecutionManager) GetQueryInfo(queryID string) (*QueryInfo, error) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	queryInfo, exists := em.queries[queryID]
	if !exists {
		return nil, errors.New(ErrQueryNotFound, "query not found", nil).AddContext("query_id", queryID)
	}

	return queryInfo, nil
}

// ListQueries returns a list of all queries (running and completed)
func (em *ExecutionManager) ListQueries() []*QueryInfo {
	em.mu.RLock()
	defer em.mu.RUnlock()

	queries := make([]*QueryInfo, 0, len(em.queries))
	for _, query := range em.queries {
		queries = append(queries, query)
	}

	return queries
}

// ListRunningQueries returns only running queries
func (em *ExecutionManager) ListRunningQueries() []*QueryInfo {
	em.mu.RLock()
	defer em.mu.RUnlock()

	var runningQueries []*QueryInfo
	for _, query := range em.queries {
		if query.Status == QueryStatusRunning {
			runningQueries = append(runningQueries, query)
		}
	}

	return runningQueries
}

// CleanupCompletedQueries removes completed queries older than the specified duration
func (em *ExecutionManager) CleanupCompletedQueries(maxAge time.Duration) int {
	em.mu.Lock()
	defer em.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-maxAge)
	removed := 0

	for queryID, queryInfo := range em.queries {
		if queryInfo.Status == QueryStatusCompleted ||
			queryInfo.Status == QueryStatusCancelled ||
			queryInfo.Status == QueryStatusFailed {
			if queryInfo.EndTime != nil && queryInfo.EndTime.Before(cutoff) {
				delete(em.queries, queryID)
				removed++
			}
		}
	}

	if removed > 0 {
		em.logger.Debug().Int("removed", removed).Msg("Cleaned up completed queries")
	}

	return removed
}

// GetStats returns statistics about queries
func (em *ExecutionManager) GetStats() map[string]interface{} {
	em.mu.RLock()
	defer em.mu.RUnlock()

	stats := map[string]interface{}{
		"total":     0,
		"running":   0,
		"completed": 0,
		"cancelled": 0,
		"failed":    0,
		"pending":   0,
	}

	for _, query := range em.queries {
		stats["total"] = stats["total"].(int) + 1
		switch query.Status {
		case QueryStatusRunning:
			stats["running"] = stats["running"].(int) + 1
		case QueryStatusCompleted:
			stats["completed"] = stats["completed"].(int) + 1
		case QueryStatusCancelled:
			stats["cancelled"] = stats["cancelled"].(int) + 1
		case QueryStatusFailed:
			stats["failed"] = stats["failed"].(int) + 1
		case QueryStatusPending:
			stats["pending"] = stats["pending"].(int) + 1
		}
	}

	return stats
}
