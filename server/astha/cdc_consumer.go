package astha

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/rs/zerolog"
)

// CDCConsumer polls the CDC log table and processes changes
type CDCConsumer struct {
	db           *sql.DB
	logTable     string
	batchSize    int
	pollInterval time.Duration
	logger       zerolog.Logger
	eventStore   EventStore
}

// NewCDCConsumer creates a new CDC consumer
func NewCDCConsumer(db *sql.DB, eventStore EventStore, logger zerolog.Logger) *CDCConsumer {
	return &CDCConsumer{
		db:           db,
		logTable:     "__cdc_log",
		batchSize:    256,
		pollInterval: 100 * time.Millisecond,
		logger:       logger,
		eventStore:   eventStore,
	}
}

// Start begins the CDC consumer processing loop
func (c *CDCConsumer) Start(ctx context.Context) error {
	c.logger.Info().
		Str("log_table", c.logTable).
		Int("batch_size", c.batchSize).
		Dur("poll_interval", c.pollInterval).
		Msg("Starting CDC consumer")

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("CDC consumer stopped")
			return nil
		case <-ticker.C:
			if err := c.processChanges(ctx); err != nil {
				c.logger.Error().Err(err).Msg("Failed to process CDC changes")
			}
		}
	}
}

// processChanges processes available changes with immediate cleanup
func (c *CDCConsumer) processChanges(ctx context.Context) error {
	for {
		// 1. Get unprocessed changes (ordered by ID, limited by batch size)
		changes, err := c.getUnprocessedChanges(ctx)
		if err != nil {
			return fmt.Errorf("failed to get unprocessed changes: %w", err)
		}

		if len(changes) == 0 {
			return nil // No changes to process
		}

		c.logger.Debug().
			Int("batch_size", len(changes)).
			Int64("first_id", changes[0].ID).
			Int64("last_id", changes[len(changes)-1].ID).
			Msg("Processing CDC changes batch")

		// 2. Process the batch
		if err := c.processBatch(ctx, changes); err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		// 3. ONLY if processing succeeds, delete processed logs
		if err := c.deleteProcessedLogs(ctx, changes); err != nil {
			return fmt.Errorf("failed to delete processed logs: %w", err)
		}

		c.logger.Debug().
			Int("processed_count", len(changes)).
			Msg("Successfully processed and cleaned up CDC batch")
	}
}

// getUnprocessedChanges retrieves unprocessed changes from CDC log
func (c *CDCConsumer) getUnprocessedChanges(ctx context.Context) ([]registry.CDCLogEntry, error) {
	query := fmt.Sprintf(`
		SELECT id, timestamp, tablename, operation, before, after, created_at 
		FROM %s 
		ORDER BY id ASC 
		LIMIT ?
	`, c.logTable)

	rows, err := c.db.QueryContext(ctx, query, c.batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query CDC log: %w", err)
	}
	defer rows.Close()

	var changes []registry.CDCLogEntry
	for rows.Next() {
		var change registry.CDCLogEntry
		if err := rows.Scan(&change.ID, &change.Timestamp, &change.TableName,
			&change.Operation, &change.Before, &change.After, &change.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan CDC log entry: %w", err)
		}
		changes = append(changes, change)
	}

	return changes, nil
}

// processBatch processes a batch of CDC changes
func (c *CDCConsumer) processBatch(ctx context.Context, changes []registry.CDCLogEntry) error {
	for _, change := range changes {
		// Convert CDC change to Astha event
		event, err := c.convertChangeToEvent(change)
		if err != nil {
			c.logger.Error().
				Err(err).
				Int64("change_id", change.ID).
				Str("table", change.TableName).
				Str("operation", change.Operation).
				Msg("Failed to convert CDC change to event")
			continue // Skip this change but continue processing others
		}

		// Store event for processing
		if err := c.eventStore.StoreEvent(ctx, event); err != nil {
			c.logger.Error().
				Err(err).
				Int64("change_id", change.ID).
				Str("table", change.TableName).
				Msg("Failed to store event")
			continue
		}

		c.logger.Trace().
			Int64("change_id", change.ID).
			Str("table", change.TableName).
			Str("operation", change.Operation).
			Msg("Successfully converted and stored CDC change")
	}

	return nil
}

// deleteProcessedLogs deletes processed logs (immediate cleanup)
func (c *CDCConsumer) deleteProcessedLogs(ctx context.Context, changes []registry.CDCLogEntry) error {
	if len(changes) == 0 {
		return nil
	}

	// Get the highest ID we processed
	maxID := changes[len(changes)-1].ID

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete logs up to the highest processed ID
	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id <= ?`, c.logTable)
	result, err := tx.ExecContext(ctx, deleteQuery, maxID)
	if err != nil {
		return fmt.Errorf("failed to delete processed logs: %w", err)
	}

	deletedCount, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get deleted row count: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit deletion: %w", err)
	}

	c.logger.Debug().
		Int64("max_id", maxID).
		Int("expected_deleted", len(changes)).
		Int64("actual_deleted", deletedCount).
		Msg("Deleted processed CDC logs")

	return nil
}

// convertChangeToEvent converts a CDC change to an Astha event
func (c *CDCConsumer) convertChangeToEvent(change registry.CDCLogEntry) (Event[any], error) {
	// Parse timestamp with subsecond precision
	timestamp, err := time.Parse("2006-01-02 15:04:05.999999999", change.Timestamp)
	if err != nil {
		// Try without subseconds
		timestamp, err = time.Parse("2006-01-02 15:04:05", change.Timestamp)
		if err != nil {
			return Event[any]{}, fmt.Errorf("failed to parse timestamp: %w", err)
		}
	}

	// Parse created_at
	createdAt, err := time.Parse("2006-01-02 15:04:05.999999999", change.CreatedAt)
	if err != nil {
		createdAt, err = time.Parse("2006-01-02 15:04:05", change.CreatedAt)
		if err != nil {
			return Event[any]{}, fmt.Errorf("failed to parse created_at: %w", err)
		}
	}

	// Parse table-specific data based on operation
	var data any
	switch change.TableName {
	case "tables":
		data = c.parseTableStats(change)
	case "table_files":
		data = c.parseTableFile(change)
	case "file_statistics":
		data = c.parseFileStatistics(change)
	case "table_metadata":
		data = c.parseTableMetadata(change)
	default:
		// For unknown tables, use raw JSON data
		if change.Operation == "INSERT" || change.Operation == "UPDATE" {
			data = change.After
		} else {
			data = change.Before
		}
	}

	return Event[any]{
		ID:        change.ID,
		Table:     change.TableName,
		Operation: change.Operation,
		Data:      data,
		Timestamp: timestamp,
		CreatedAt: createdAt,
	}, nil
}

// parseTableStats parses table statistics from CDC change
// Note: This will use the actual Registry table structure
func (c *CDCConsumer) parseTableStats(change registry.CDCLogEntry) map[string]interface{} {
	var stats map[string]interface{}

	switch change.Operation {
	case "INSERT", "UPDATE":
		if change.After != "" {
			if err := json.Unmarshal([]byte(change.After), &stats); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table stats from CDC change")
			}
		}
	case "DELETE":
		if change.Before != "" {
			if err := json.Unmarshal([]byte(change.Before), &stats); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table stats from CDC change")
			}
		}
	}

	return stats
}

// parseTableFile parses table file information from CDC change
// Note: This will use the actual Registry table structure
func (c *CDCConsumer) parseTableFile(change registry.CDCLogEntry) map[string]interface{} {
	var file map[string]interface{}

	switch change.Operation {
	case "INSERT", "UPDATE":
		if change.After != "" {
			if err := json.Unmarshal([]byte(change.After), &file); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table file from CDC change")
			}
		}
	case "DELETE":
		if change.Before != "" {
			if err := json.Unmarshal([]byte(change.Before), &file); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table file from CDC change")
			}
		}
	}

	return file
}

// parseFileStatistics parses file statistics from CDC change
// Note: This will use the actual Registry table structure
func (c *CDCConsumer) parseFileStatistics(change registry.CDCLogEntry) map[string]interface{} {
	var stats map[string]interface{}

	switch change.Operation {
	case "INSERT", "UPDATE":
		if change.After != "" {
			if err := json.Unmarshal([]byte(change.After), &stats); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse file statistics from CDC change")
			}
		}
	case "DELETE":
		if change.Before != "" {
			if err := json.Unmarshal([]byte(change.Before), &stats); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse file statistics from CDC change")
			}
		}
	}

	return stats
}

// parseTableMetadata parses table metadata from CDC change
// Note: This will use the actual Registry table structure
func (c *CDCConsumer) parseTableMetadata(change registry.CDCLogEntry) map[string]interface{} {
	var metadata map[string]interface{}

	switch change.Operation {
	case "INSERT", "UPDATE":
		if change.After != "" {
			if err := json.Unmarshal([]byte(change.After), &metadata); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table metadata from CDC change")
			}
		}
	case "DELETE":
		if change.Before != "" {
			if err := json.Unmarshal([]byte(change.Before), &metadata); err != nil {
				c.logger.Error().Err(err).Msg("Failed to parse table metadata from CDC change")
			}
		}
	}

	return metadata
}

// SetBatchSize sets the batch size for processing
func (c *CDCConsumer) SetBatchSize(size int) {
	if size > 0 {
		c.batchSize = size
	}
}

// SetPollInterval sets the polling interval
func (c *CDCConsumer) SetPollInterval(interval time.Duration) {
	if interval > 0 {
		c.pollInterval = interval
	}
}
