package astha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
)

// Scheduler manages component subscriptions and event distribution
type Scheduler struct {
	mu                 sync.RWMutex
	components         map[string]ComponentInfo
	componentInstances map[string]Subscriber[any] // Store actual component instances
	subscriptions      map[string][]string        // table -> component names
	eventStore         EventStore
	cdcConsumer        *CDCConsumer
	logger             zerolog.Logger
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
}

// NewScheduler creates a new Astha scheduler
func NewScheduler(eventStore EventStore, cdcConsumer *CDCConsumer, logger zerolog.Logger) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		components:         make(map[string]ComponentInfo),
		componentInstances: make(map[string]Subscriber[any]),
		subscriptions:      make(map[string][]string),
		eventStore:         eventStore,
		cdcConsumer:        cdcConsumer,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start begins the scheduler and CDC consumer
func (s *Scheduler) Start() error {
	s.logger.Info().Msg("Starting Astha scheduler")

	// Start CDC consumer
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.cdcConsumer.Start(s.ctx); err != nil {
			s.logger.Error().Err(err).Msg("CDC consumer stopped with error")
		}
	}()

	// Start event distribution loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.eventDistributionLoop()
	}()

	// Start cleanup loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.cleanupLoop()
	}()

	s.logger.Info().Msg("Astha scheduler started successfully")
	return nil
}

// Stop gracefully stops the scheduler
func (s *Scheduler) Stop() error {
	s.logger.Info().Msg("Stopping Astha scheduler")

	s.cancel()
	s.wg.Wait()

	s.logger.Info().Msg("Astha scheduler stopped")
	return nil
}

// RegisterComponent registers a component with the scheduler
func (s *Scheduler) RegisterComponent(info ComponentInfo, instance Subscriber[any]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate component info
	if info.Name == "" {
		return errors.New(ErrComponentNameEmpty, "component name cannot be empty", nil)
	}

	if len(info.Subscriptions) == 0 {
		return errors.New(ErrComponentNoSubscriptions, "component must subscribe to at least one table", nil)
	}

	if instance == nil {
		return errors.New(ErrComponentInstanceNil, "component instance cannot be nil", nil)
	}

	// Update component info
	info.Status = "active"
	info.LastSeen = time.Now()
	s.components[info.Name] = info
	s.componentInstances[info.Name] = instance

	// Update subscriptions
	for _, table := range info.Subscriptions {
		if s.subscriptions[table] == nil {
			s.subscriptions[table] = make([]string, 0)
		}
		s.subscriptions[table] = append(s.subscriptions[table], info.Name)
	}

	s.logger.Info().
		Str("component", info.Name).
		Strs("subscriptions", info.Subscriptions).
		Msg("Component registered with scheduler")

	return nil
}

// RegisterComponentInfo registers only component metadata (for backward compatibility)
func (s *Scheduler) RegisterComponentInfo(info ComponentInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate component info
	if info.Name == "" {
		return errors.New(ErrComponentNameEmpty, "component name cannot be empty", nil)
	}

	if len(info.Subscriptions) == 0 {
		return errors.New(ErrComponentNoSubscriptions, "component must subscribe to at least one table", nil)
	}

	// Update component info
	info.Status = "active"
	info.LastSeen = time.Now()
	s.components[info.Name] = info

	// Update subscriptions
	for _, table := range info.Subscriptions {
		if s.subscriptions[table] == nil {
			s.subscriptions[table] = make([]string, 0)
		}
		s.subscriptions[table] = append(s.subscriptions[table], info.Name)
	}

	s.logger.Info().
		Str("component", info.Name).
		Strs("subscriptions", info.Subscriptions).
		Msg("Component metadata registered with scheduler (no instance)")

	return nil
}

// UnregisterComponent removes a component from the scheduler
func (s *Scheduler) UnregisterComponent(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	component, exists := s.components[name]
	if !exists {
		return errors.New(ErrComponentNotFound, "component not found", nil).AddContext("component_name", name)
	}

	// Remove from subscriptions
	for _, table := range component.Subscriptions {
		if subscribers, exists := s.subscriptions[table]; exists {
			var newSubscribers []string
			for _, subscriber := range subscribers {
				if subscriber != name {
					newSubscribers = append(newSubscribers, subscriber)
				}
			}
			s.subscriptions[table] = newSubscribers
		}
	}

	// Remove component and instance
	delete(s.components, name)
	delete(s.componentInstances, name)

	s.logger.Info().
		Str("component", name).
		Msg("Component unregistered from scheduler")

	return nil
}

// UpdateComponentHealth updates component health status
func (s *Scheduler) UpdateComponentHealth(name string, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	component, exists := s.components[name]
	if !exists {
		return errors.New(ErrComponentNotFound, "component not found", nil).AddContext("component_name", name)
	}

	component.Status = status
	component.LastSeen = time.Now()
	s.components[name] = component

	s.logger.Debug().
		Str("component", name).
		Str("status", status).
		Msg("Updated component health status")

	return nil
}

// eventDistributionLoop continuously distributes events to components
func (s *Scheduler) eventDistributionLoop() {
	ticker := time.NewTicker(50 * time.Millisecond) // Check for events every 50ms
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.distributeEvents(); err != nil {
				s.logger.Error().Err(err).Msg("Failed to distribute events")
			}
		}
	}
}

// distributeEvents distributes available events to subscribed components
func (s *Scheduler) distributeEvents() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get all tables with events
	eventCounts := s.eventStore.GetEventCount()

	for table, count := range eventCounts {
		if count == 0 {
			continue
		}

		// Get events for this table
		events, err := s.eventStore.GetEvents(s.ctx, table, 100) // Process up to 100 events at a time
		if err != nil {
			s.logger.Error().Err(err).Str("table", table).Msg("Failed to get events for table")
			continue
		}

		if len(events) == 0 {
			continue
		}

		// Get subscribers for this table
		subscribers, exists := s.subscriptions[table]
		if !exists || len(subscribers) == 0 {
			s.logger.Debug().Str("table", table).Msg("No subscribers for table, skipping event distribution")
			continue
		}

		// Distribute events to all subscribers
		for _, event := range events {
			s.distributeEventToSubscribers(event, subscribers)
		}
	}

	return nil
}

// distributeEventToSubscribers distributes a single event to all subscribed components
func (s *Scheduler) distributeEventToSubscribers(event any, subscribers []string) {
	for _, subscriberName := range subscribers {
		component, exists := s.components[subscriberName]
		if !exists {
			s.logger.Warn().
				Str("subscriber", subscriberName).
				Msg("Subscriber not found, skipping event distribution")
			continue
		}

		if component.Status != "active" {
			s.logger.Debug().
				Str("subscriber", subscriberName).
				Str("status", component.Status).
				Msg("Subscriber not active, skipping event distribution")
			continue
		}

		// Get the actual component instance to call OnEvent
		instance, instanceExists := s.componentInstances[subscriberName]
		if !instanceExists {
			s.logger.Warn().
				Str("subscriber", subscriberName).
				Msg("Component instance not found, skipping event distribution")
			continue
		}

		// Extract event information using type assertion
		var eventID int64
		var table string
		var operation string

		switch e := event.(type) {
		case Event[regtypes.Table]:
			eventID = e.ID
			table = e.Table
			operation = e.Operation
		case Event[regtypes.TableFile]:
			eventID = e.ID
			table = e.Table
			operation = e.Operation
		case Event[regtypes.TableMetadata]:
			eventID = e.ID
			table = e.Table
			operation = e.Operation
		case Event[regtypes.TableStatistic]:
			eventID = e.ID
			table = e.Table
			operation = e.Operation
		default:
			s.logger.Warn().
				Str("subscriber", subscriberName).
				Str("event_type", fmt.Sprintf("%T", event)).
				Msg("Unknown event type, skipping")
			continue
		}

		// Convert the event to Event[any] for the component
		var eventAny Event[any]
		switch e := event.(type) {
		case Event[regtypes.Table]:
			eventAny = Event[any]{
				ID:        e.ID,
				Table:     e.Table,
				Operation: e.Operation,
				Data:      e.Data,
				Timestamp: e.Timestamp,
				CreatedAt: e.CreatedAt,
			}
		case Event[regtypes.TableFile]:
			eventAny = Event[any]{
				ID:        e.ID,
				Table:     e.Table,
				Operation: e.Operation,
				Data:      e.Data,
				Timestamp: e.Timestamp,
				CreatedAt: e.CreatedAt,
			}
		case Event[regtypes.TableMetadata]:
			eventAny = Event[any]{
				ID:        e.ID,
				Table:     e.Table,
				Operation: e.Operation,
				Data:      e.Data,
				Timestamp: e.Timestamp,
				CreatedAt: e.CreatedAt,
			}
		case Event[regtypes.TableStatistic]:
			eventAny = Event[any]{
				ID:        e.ID,
				Table:     e.Table,
				Operation: e.Operation,
				Data:      e.Data,
				Timestamp: e.Timestamp,
				CreatedAt: e.CreatedAt,
			}
		default:
			s.logger.Warn().
				Str("subscriber", subscriberName).
				Str("event_type", fmt.Sprintf("%T", event)).
				Msg("Unknown event type, skipping")
			continue
		}

		// Call the component's OnEvent method
		if err := instance.OnEvent(s.ctx, eventAny); err != nil {
			s.logger.Error().
				Err(err).
				Int64("event_id", eventID).
				Str("table", table).
				Str("operation", operation).
				Str("subscriber", subscriberName).
				Msg("Failed to process event in component")

			// Mark component as having an error
			s.UpdateComponentHealth(subscriberName, "error")
			continue
		}

		// Mark event as processed
		if err := s.eventStore.MarkEventProcessed(s.ctx, eventID); err != nil {
			s.logger.Error().
				Err(err).
				Int64("event_id", eventID).
				Str("subscriber", subscriberName).
				Msg("Failed to mark event as processed")
		}

		s.logger.Debug().
			Int64("event_id", eventID).
			Str("table", table).
			Str("subscriber", subscriberName).
			Msg("Event successfully processed by component")
	}
}

// cleanupLoop periodically cleans up processed events
func (s *Scheduler) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.eventStore.(*MemoryEventStore).CleanupProcessedEvents()
		}
	}
}

// GetComponentInfo returns information about a specific component
func (s *Scheduler) GetComponentInfo(name string) (ComponentInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	component, exists := s.components[name]
	return component, exists
}

// GetAllComponents returns information about all registered components
func (s *Scheduler) GetAllComponents() []ComponentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var components []ComponentInfo
	for _, component := range s.components {
		components = append(components, component)
	}

	return components
}

// GetTableSubscribers returns all components subscribed to a specific table
func (s *Scheduler) GetTableSubscribers(table string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subscribers, exists := s.subscriptions[table]
	if !exists {
		return []string{}
	}

	// Return a copy to avoid race conditions
	result := make([]string, len(subscribers))
	copy(result, subscribers)
	return result
}

// GetSchedulerStats returns scheduler statistics
func (s *Scheduler) GetSchedulerStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	eventCounts := s.eventStore.GetEventCount()
	processedCount := s.eventStore.GetProcessedCount()

	return map[string]interface{}{
		"component_count":   len(s.components),
		"table_count":       len(s.subscriptions),
		"event_counts":      eventCounts,
		"processed_events":  processedCount,
		"active_components": s.getActiveComponentCount(),
	}
}

// getActiveComponentCount returns the count of active components
func (s *Scheduler) getActiveComponentCount() int {
	count := 0
	for _, component := range s.components {
		if component.Status == "active" {
			count++
		}
	}
	return count
}
