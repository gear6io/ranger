package astha

import (
	"context"
	"database/sql"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/rs/zerolog"
)

// Astha represents the main CDC scheduler
type Astha struct {
	scheduler *Scheduler
	cdcSetup  *registry.CDCSetup
	logger    zerolog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
}

// Config holds Astha configuration
type Config struct {
	Database     *sql.DB
	Logger       zerolog.Logger
	BatchSize    int
	PollInterval int // milliseconds
}

// NewAstha creates a new Astha CDC scheduler
func NewAstha(cfg *Config) (*Astha, error) {
	if cfg.Database == nil {
		return nil, errors.New(ErrDatabaseConnectionRequired, "database connection is required", nil)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create CDC setup
	cdcSetup := registry.NewCDCSetup(cfg.Database, cfg.Logger)

	// Create event store
	eventStore := NewMemoryEventStore(cfg.Logger)

	// Create CDC consumer
	cdcConsumer := NewCDCConsumer(cfg.Database, eventStore, cfg.Logger)
	if cfg.BatchSize > 0 {
		cdcConsumer.SetBatchSize(cfg.BatchSize)
	}
	if cfg.PollInterval > 0 {
		cdcConsumer.SetPollInterval(time.Duration(cfg.PollInterval) * time.Millisecond)
	}

	// Create scheduler
	scheduler := NewScheduler(eventStore, cdcConsumer, cfg.Logger)

	return &Astha{
		scheduler: scheduler,
		cdcSetup:  cdcSetup,
		logger:    cfg.Logger,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Start begins the Astha scheduler and CDC infrastructure
func (a *Astha) Start() error {
	a.logger.Info().Msg("Starting Astha CDC scheduler")

	// Setup CDC infrastructure
	if err := a.cdcSetup.SetupCDC(a.ctx); err != nil {
		return err
	}

	// Start scheduler
	if err := a.scheduler.Start(); err != nil {
		return err
	}

	a.logger.Info().Msg("Astha CDC scheduler started successfully")
	return nil
}

// Stop gracefully stops the Astha scheduler
func (a *Astha) Stop() error {
	a.logger.Info().Msg("Stopping Astha CDC scheduler")

	// Stop scheduler
	if err := a.scheduler.Stop(); err != nil {
		a.logger.Error().Err(err).Msg("Failed to stop scheduler")
	}

	// Cancel context
	a.cancel()

	a.logger.Info().Msg("Astha CDC scheduler stopped")
	return nil
}

// RegisterComponent registers a component with the scheduler
func (a *Astha) RegisterComponent(info ComponentInfo) error {
	return a.scheduler.RegisterComponentInfo(info)
}

// RegisterComponentWithInstance registers a component with both metadata and instance
func (a *Astha) RegisterComponentWithInstance(info ComponentInfo, instance Subscriber[any]) error {
	return a.scheduler.RegisterComponent(info, instance)
}

// UnregisterComponent removes a component from the scheduler
func (a *Astha) UnregisterComponent(name string) error {
	return a.scheduler.UnregisterComponent(name)
}

// GetSchedulerStats returns scheduler statistics
func (a *Astha) GetSchedulerStats() map[string]interface{} {
	return a.scheduler.GetSchedulerStats()
}

// GetComponentInfo returns information about a specific component
func (a *Astha) GetComponentInfo(name string) (ComponentInfo, bool) {
	return a.scheduler.GetComponentInfo(name)
}

// GetAllComponents returns information about all registered components
func (a *Astha) GetAllComponents() []ComponentInfo {
	return a.scheduler.GetAllComponents()
}

// GetTableSubscribers returns all components subscribed to a specific table
func (a *Astha) GetTableSubscribers(table string) []string {
	return a.scheduler.GetTableSubscribers(table)
}

// GetMonitoredTables returns the list of monitored tables
func (a *Astha) GetMonitoredTables() []string {
	return a.cdcSetup.GetMonitoredTables()
}
