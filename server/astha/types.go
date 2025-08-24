package astha

import (
	"context"
	"time"
)

// Event represents a change event with generic data type
type Event[T any] struct {
	ID        int64     `json:"id"`
	Table     string    `json:"table"`
	Operation string    `json:"operation"` // INSERT, UPDATE, DELETE
	Data      T         `json:"data"`
	Timestamp time.Time `json:"timestamp"`
	CreatedAt time.Time `json:"created_at"`
}

// Subscriber interface that all components must implement
type Subscriber[T any] interface {
	// OnEvent handles incoming events for this component
	OnEvent(ctx context.Context, event Event[T]) error

	// OnHealth check for component health
	OnHealth(ctx context.Context) error

	// OnRefresh tells component to refresh from Registry
	OnRefresh(ctx context.Context) error
}

// Note: CDCLogEntry type will be defined in the Registry package
// when we implement the CDC table creation. For now, we'll use
// a generic map[string]interface{} to represent CDC changes.

// Note: TableStats, TableFile, FileStatistics, and TableMetadata types
// are already defined in the Registry schema. Astha will use those types
// directly from the Registry package to avoid duplication.

// ComponentInfo represents component registration information
type ComponentInfo struct {
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	Status        string    `json:"status"` // active, inactive, error
	LastSeen      time.Time `json:"last_seen"`
	Subscriptions []string  `json:"subscriptions"` // Tables this component subscribes to
}

// EventStore interface for storing and retrieving events
type EventStore interface {
	// StoreEvent stores an event for processing
	StoreEvent(ctx context.Context, event any) error

	// GetEvents retrieves events for a specific table
	GetEvents(ctx context.Context, table string, limit int) ([]any, error)

	// MarkEventProcessed marks an event as successfully processed
	MarkEventProcessed(ctx context.Context, eventID int64) error

	// GetEventCount returns the count of events by table
	GetEventCount() map[string]int

	// GetProcessedCount returns the count of processed events
	GetProcessedCount() int
}
