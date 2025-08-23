package astha

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
)

// MemoryEventStore implements EventStore interface using in-memory storage
type MemoryEventStore struct {
	mu       sync.RWMutex
	events   map[string][]Event[any] // table -> events
	processed map[int64]bool          // eventID -> processed status
	logger   zerolog.Logger
}

// NewMemoryEventStore creates a new in-memory event store
func NewMemoryEventStore(logger zerolog.Logger) *MemoryEventStore {
	return &MemoryEventStore{
		events:    make(map[string][]Event[any]),
		processed: make(map[int64]bool),
		logger:    logger,
	}
}

// StoreEvent stores an event for processing
func (s *MemoryEventStore) StoreEvent(ctx context.Context, event Event[any]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	table := event.Table
	if s.events[table] == nil {
		s.events[table] = make([]Event[any], 0)
	}

	s.events[table] = append(s.events[table], event)
	s.processed[event.ID] = false

	s.logger.Debug().
		Int64("event_id", event.ID).
		Str("table", event.Table).
		Str("operation", event.Operation).
		Msg("Stored event in memory store")

	return nil
}

// GetEvents retrieves events for a specific table
func (s *MemoryEventStore) GetEvents(ctx context.Context, table string, limit int) ([]Event[any], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, exists := s.events[table]
	if !exists {
		return []Event[any]{}, nil
	}

	// Return unprocessed events up to the limit
	var unprocessed []Event[any]
	count := 0
	for _, event := range events {
		if count >= limit {
			break
		}
		if !s.processed[event.ID] {
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
		var unprocessed []Event[any]
		for _, event := range events {
			if !s.processed[event.ID] {
				unprocessed = append(unprocessed, event)
			} else {
				// Remove from processed map
				delete(s.processed, event.ID)
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

	s.events = make(map[string][]Event[any])
	s.processed = make(map[int64]bool)

	s.logger.Info().Msg("Reset memory event store")
}
