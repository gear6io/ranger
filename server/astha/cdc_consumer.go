package astha

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
)

// =============================================================================
// TYPE ALIASES FOR CLEANER USAGE
// =============================================================================

// TableEvent represents an event with Table data
type TableEvent = Event[regtypes.Table]

// TableFileEvent represents an event with TableFile data
type TableFileEvent = Event[regtypes.TableFile]

// TableMetadataEvent represents an event with TableMetadata data
type TableMetadataEvent = Event[regtypes.TableMetadata]

// TableStatisticEvent represents an event with TableStatistic data
type TableStatisticEvent = Event[regtypes.TableStatistic]

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
			return err
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
			return err
		}

		// 3. ONLY if processing succeeds, delete processed logs
		if err := c.deleteProcessedLogs(ctx, changes); err != nil {
			return err
		}

		c.logger.Debug().
			Int("processed_count", len(changes)).
			Msg("Successfully processed and cleaned up CDC batch")
	}
}

// getUnprocessedChanges retrieves unprocessed changes from CDC log
func (c *CDCConsumer) getUnprocessedChanges(ctx context.Context) ([]regtypes.ChangeLog, error) {
	query := "SELECT id, timestamp, tablename, operation, before, after, created_at FROM " + c.logTable + " ORDER BY id ASC LIMIT ?"

	rows, err := c.db.QueryContext(ctx, query, c.batchSize)
	if err != nil {
		return nil, errors.New(ErrCDCQueryLogFailed, "failed to query CDC log", err)
	}
	defer rows.Close()

	var changes []regtypes.ChangeLog
	for rows.Next() {
		var change regtypes.ChangeLog
		if err := rows.Scan(&change.ID, &change.Timestamp, &change.TableName,
			&change.Operation, &change.Before, &change.After, &change.CreatedAt); err != nil {
			return nil, errors.New(ErrCDCScanLogFailed, "failed to scan CDC log entry", err)
		}
		changes = append(changes, change)
	}

	return changes, nil
}

// processBatch processes a batch of CDC changes
func (c *CDCConsumer) processBatch(ctx context.Context, changes []regtypes.ChangeLog) error {
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
func (c *CDCConsumer) deleteProcessedLogs(ctx context.Context, changes []regtypes.ChangeLog) error {
	if len(changes) == 0 {
		return nil
	}

	// Get the highest ID we processed
	maxID := changes[len(changes)-1].ID

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New(ErrCDCTransactionFailed, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	// Delete logs up to the highest processed ID
	deleteQuery := "DELETE FROM " + c.logTable + " WHERE id <= ?"
	result, err := tx.ExecContext(ctx, deleteQuery, maxID)
	if err != nil {
		return errors.New(ErrCDCDeleteLogsFailed, "failed to delete processed logs", err)
	}

	deletedCount, err := result.RowsAffected()
	if err != nil {
		return errors.New(ErrCDCTransactionFailed, "failed to get deleted row count", err)
	}

	if err = tx.Commit(); err != nil {
		return errors.New(ErrCDCTransactionFailed, "failed to commit deletion", err)
	}

	c.logger.Debug().
		Int64("max_id", maxID).
		Int("expected_deleted", len(changes)).
		Int64("actual_deleted", deletedCount).
		Msg("Deleted processed CDC logs")

	return nil
}

// convertChangeToEvent converts a CDC change to an Astha event with type safety
func (c *CDCConsumer) convertChangeToEvent(change regtypes.ChangeLog) (any, error) {

	// Use the new generic functions for full type safety
	switch change.TableName {
	case "tables":
		event, err := convertChangeToEventGeneric[regtypes.Table](change)
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("table", change.TableName).
				Str("operation", change.Operation).
				Msg("Failed to parse table data")
			return nil, errors.New(ErrCDCParseDataFailed, "failed to parse table data", err)
		}
		return event, nil

	case "table_files":
		event, err := convertChangeToEventGeneric[regtypes.TableFile](change)
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("table", change.TableName).
				Str("operation", change.Operation).
				Msg("Failed to parse table data")
			return nil, errors.New(ErrCDCParseDataFailed, "failed to parse table data", err)
		}
		return event, nil

	case "table_metadata":
		event, err := convertChangeToEventGeneric[regtypes.TableMetadata](change)
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("table", change.TableName).
				Str("operation", change.Operation).
				Msg("Failed to parse table data")
			return nil, errors.New(ErrCDCParseDataFailed, "failed to parse table data", err)
		}
		return event, nil

	case "table_statistics":
		event, err := convertChangeToEventGeneric[regtypes.TableStatistic](change)
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("table", change.TableName).
				Str("operation", change.Operation).
				Msg("Failed to parse table data")
			return nil, errors.New(ErrCDCParseDataFailed, "failed to parse table data", err)
		}
		return event, nil

	default:
		return nil, errors.New(ErrCDCUnknownTableType, "unknown table type", nil).AddContext("table_type", change.TableName)
	}
}

// =============================================================================
// STANDALONE GENERIC FUNCTIONS FOR TYPE SAFETY
// =============================================================================

// convertChangeToEventGeneric is a standalone generic function that converts CDC changes to typed events
func convertChangeToEventGeneric[T any](change regtypes.ChangeLog) (Event[T], error) {
	// Parse timestamp with subsecond precision
	timestamp, err := time.Parse("2006-01-02 15:04:05.999999999", change.Timestamp)
	if err != nil {
		// Try without subseconds
		timestamp, err = time.Parse("2006-01-02 15:04:05", change.Timestamp)
		if err != nil {
			return Event[T]{}, errors.New(ErrCDCTimestampParseFailed, "failed to parse timestamp", err)
		}
	}

	// Parse created_at
	createdAt, err := time.Parse("2006-01-02 15:04:05.999999999", change.CreatedAt)
	if err != nil {
		createdAt, err = time.Parse("2006-01-02 15:04:05", change.CreatedAt)
		if err != nil {
			return Event[T]{}, errors.New(ErrCDCCreatedAtParseFailed, "failed to parse created_at", err)
		}
	}

	// Parse data to type T directly
	var data T
	if err := parseDataToType[T](change, &data); err != nil {
		return Event[T]{}, errors.New(ErrCDCParseDataFailed, "failed to parse data to type", err).AddContext("data_type", fmt.Sprintf("%T", data))
	}

	return Event[T]{
		ID:        change.ID,
		Table:     change.TableName,
		Operation: change.Operation,
		Data:      data, // This is type T, maintaining full type safety!
		Timestamp: timestamp,
		CreatedAt: createdAt,
	}, nil
}

// parseDataToType is a standalone generic function that parses JSON data to any type T
func parseDataToType[T any](change regtypes.ChangeLog, result *T) error {
	// Determine which data to parse based on operation
	var jsonData *string
	switch change.Operation {
	case "INSERT", "UPDATE":
		jsonData = change.After
	case "DELETE":
		jsonData = change.Before
	default:
		return errors.New(ErrCDCUnknownOperation, "unknown operation", nil).AddContext("operation", change.Operation)
	}

	if jsonData == nil || *jsonData == "" {
		return errors.New(ErrCDCNoDataAvailable, "no data available for operation", nil).AddContext("operation", change.Operation)
	}

	// Parse directly to type T
	if err := json.Unmarshal([]byte(*jsonData), result); err != nil {
		return errors.New(ErrCDCParseDataFailed, "failed to parse data to type", err).AddContext("data_type", fmt.Sprintf("%T", result))
	}

	return nil
}

// =============================================================================
// TYPE-SAFE CONVERSION HELPERS
// =============================================================================

// ConvertToTableEvent converts a CDC change to a TableEvent with type safety
func ConvertToTableEvent(change regtypes.ChangeLog) (TableEvent, error) {
	return convertChangeToEventGeneric[regtypes.Table](change)
}

// ConvertToTableFileEvent converts a CDC change to a TableFileEvent with type safety
func ConvertToTableFileEvent(change regtypes.ChangeLog) (TableFileEvent, error) {
	return convertChangeToEventGeneric[regtypes.TableFile](change)
}

// ConvertToTableMetadataEvent converts a CDC change to a TableMetadataEvent with type safety
func ConvertToTableMetadataEvent(change regtypes.ChangeLog) (TableMetadataEvent, error) {
	return convertChangeToEventGeneric[regtypes.TableMetadata](change)
}

// ConvertToTableStatisticEvent converts a CDC change to a TableStatisticEvent with type safety
func ConvertToTableStatisticEvent(change regtypes.ChangeLog) (TableStatisticEvent, error) {
	return convertChangeToEventGeneric[regtypes.TableStatistic](change)
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
