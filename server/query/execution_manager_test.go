package query

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionManager(t *testing.T) {
	// Create a logger
	logger := zerolog.Nop()

	// Create execution manager
	em := NewExecutionManager(logger)
	require.NotNil(t, em)
	ctx := context.Background()

	t.Run("StartQuery", func(t *testing.T) {
		queryInfo, ctx := em.StartQuery(ctx, "test_query_1", "SELECT * FROM test", "testuser", "127.0.0.1")

		assert.NotNil(t, queryInfo)
		assert.NotNil(t, ctx)
		assert.Equal(t, "test_query_1", queryInfo.ID)
		assert.Equal(t, "SELECT * FROM test", queryInfo.Query)
		assert.Equal(t, QueryStatusRunning, queryInfo.Status)
		assert.Equal(t, "testuser", queryInfo.User)
		assert.Equal(t, "127.0.0.1", queryInfo.ClientAddr)

		// Check that context is cancellable
		select {
		case <-ctx.Done():
			t.Fatal("Context should not be done yet")
		default:
			// Expected
		}
	})

	t.Run("CompleteQuery", func(t *testing.T) {
		// Start a query
		_, _ = em.StartQuery(ctx, "test_query_2", "SELECT * FROM test2", "testuser", "127.0.0.1")

		// Complete it successfully
		err := em.CompleteQuery("test_query_2", 100, nil)
		require.NoError(t, err)

		// Check status
		info, err := em.GetQueryInfo("test_query_2")
		require.NoError(t, err)
		assert.Equal(t, QueryStatusCompleted, info.Status)
		assert.Equal(t, int64(100), info.RowCount)
		assert.NotNil(t, info.EndTime)
		assert.NotNil(t, info.Duration)
	})

	t.Run("CancelQuery", func(t *testing.T) {
		// Start a query
		_, ctx := em.StartQuery(ctx, "test_query_3", "SELECT * FROM test3", "testuser", "127.0.0.1")

		// Cancel it
		err := em.CancelQuery("test_query_3")
		require.NoError(t, err)

		// Check that context is cancelled
		select {
		case <-ctx.Done():
			// Expected
		default:
			t.Fatal("Context should be cancelled")
		}

		// Check status
		info, err := em.GetQueryInfo("test_query_3")
		require.NoError(t, err)
		assert.Equal(t, QueryStatusCancelled, info.Status)
		assert.NotNil(t, info.EndTime)
		assert.NotNil(t, info.Duration)
	})

	t.Run("ListQueries", func(t *testing.T) {
		// Start a few queries
		em.StartQuery(ctx, "test_query_4", "SELECT * FROM test4", "testuser", "127.0.0.1")
		em.StartQuery(ctx, "test_query_5", "SELECT * FROM test5", "testuser", "127.0.0.1")

		// List all queries
		queries := em.ListQueries()
		assert.GreaterOrEqual(t, len(queries), 5) // Should have at least our test queries

		// List running queries
		runningQueries := em.ListRunningQueries()
		assert.GreaterOrEqual(t, len(runningQueries), 2) // Should have at least our new test queries
	})

	t.Run("GetStats", func(t *testing.T) {
		stats := em.GetStats()

		assert.Contains(t, stats, "total")
		assert.Contains(t, stats, "running")
		assert.Contains(t, stats, "completed")
		assert.Contains(t, stats, "cancelled")
		assert.Contains(t, stats, "failed")
		assert.Contains(t, stats, "pending")

		// Should have some queries
		assert.Greater(t, stats["total"].(int), 0)
	})

	t.Run("CleanupCompletedQueries", func(t *testing.T) {
		// Complete a query
		em.CompleteQuery("test_query_4", 50, nil)

		// Cleanup queries older than 1 nanosecond (should remove completed ones)
		removed := em.CleanupCompletedQueries(1 * time.Nanosecond)
		assert.Greater(t, removed, 0)
	})
}
