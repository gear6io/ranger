package astha

import (
	"context"
	"fmt"
	"sync"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
)

// MemoryEventStore implements EventStore interface using in-memory storage
type MemoryEventStore struct {
	mu        sync.RWMutex
	events    map[string][]any // table -> events (can be any typed event)
	processed map[int64]bool   // eventID -> processed status
	logger    zerolog.Logger
}

// NewMemoryEventStore creates a new in-memory event store
func NewMemoryEventStore(logger zerolog.Logger) *MemoryEventStore {
	return &MemoryEventStore{
		events:    make(map[string][]any),
		processed: make(map[int64]bool),
		logger:    logger,
	}
}

// StoreEvent stores an event for processing
func (s *MemoryEventStore) StoreEvent(ctx context.Context, event any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Extract table and ID from the event using reflection or type assertion
	var table string
	var eventID int64
	var operation string

	// Try to extract information from the event
	switch e := event.(type) {
	case Event[regtypes.Table]:
		table = e.Table
		eventID = e.ID
		operation = e.Operation
	case Event[regtypes.TableFile]:
		table = e.Table
		eventID = e.ID
		operation = e.Operation
	case Event[regtypes.TableMetadata]:
		table = e.Table
		eventID = e.ID
		operation = e.Operation
	case Event[regtypes.TableStatistic]:
		table = e.Table
		eventID = e.ID
		operation = e.Operation
	default:
		return errors.New(ErrUnsupportedEventType, "unsupported event type", nil).AddContext("event_type", fmt.Sprintf("%T", event))
	}

	if s.events[table] == nil {
		s.events[table] = make([]any, 0)
	}

	s.events[table] = append(s.events[table], event)
	s.processed[eventID] = false

	s.logger.Debug().
		Int64("event_id", eventID).
		Str("table", table).
		Str("operation", operation).
		Msg("Stored event in memory store")

	return nil
}

// GetEvents retrieves events for a specific table
func (s *MemoryEventStore) GetEvents(ctx context.Context, table string, limit int) ([]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, exists := s.events[table]
	if !exists {
		return []any{}, nil
	}

	// Return unprocessed events up to the limit
	var unprocessed []any
	count := 0
	for _, event := range events {
		if count >= limit {
			break
		}

		// Extract event ID using type assertion
		var eventID int64
		switch e := event.(type) {
		case Event[regtypes.Table]:
			eventID = e.ID
		case Event[regtypes.TableFile]:
			eventID = e.ID
		case Event[regtypes.TableMetadata]:
			eventID = e.ID
		case Event[regtypes.TableStatistic]:
			eventID = e.ID
		default:
			continue // Skip unknown event types
		}

		if !s.processed[eventID] {
			unprocessed = append(unprocessed, event)
			count++
		}
	}

	return unprocessed, nil
}

// MarkEventProcessed marks an event as successfully processed
func (s *MemoryEventStore) MarkEventProcessed(ctx context.Context, eventID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.processed[eventID] = true

	s.logger.Debug().
		Int64("event_id", eventID).
		Msg("Marked event as processed")

	return nil
}

// CleanupProcessedEvents removes processed events to prevent memory bloat
func (s *MemoryEventStore) CleanupProcessedEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for table, events := range s.events {
		var unprocessed []any
		for _, event := range events {
			// Extract event ID using type assertion
			var eventID int64
			switch e := event.(type) {
			case Event[regtypes.Table]:
				eventID = e.ID
			case Event[regtypes.TableFile]:
				eventID = e.ID
			case Event[regtypes.TableMetadata]:
				eventID = e.ID
			case Event[regtypes.TableStatistic]:
				eventID = e.ID
			default:
				continue // Skip unknown event types
			}

			if !s.processed[eventID] {
				unprocessed = append(unprocessed, event)
			} else {
				// Remove from processed map
				delete(s.processed, eventID)
			}
		}
		s.events[table] = unprocessed
	}

	s.logger.Debug().Msg("Cleaned up processed events from memory store")
}

// GetEventCount returns the count of events by table
func (s *MemoryEventStore) GetEventCount() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	counts := make(map[string]int)
	for table, events := range s.events {
		counts[table] = len(events)
	}

	return counts
}

// GetProcessedCount returns the count of processed events
func (s *MemoryEventStore) GetProcessedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, processed := range s.processed {
		if processed {
			count++
		}
	}

	return count
}

// Reset clears all events and processed status
func (s *MemoryEventStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = make(map[string][]any)
	s.processed = make(map[int64]bool)

	s.logger.Info().Msg("Reset memory event store")
}
