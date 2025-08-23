package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/icebox/server/astha"
	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/rs/zerolog"
)

// IcebergComponent implements the Subscriber interface for Astha events
type IcebergComponent struct {
	manager *Manager
	logger  zerolog.Logger
}

// NewIcebergComponent creates a new Iceberg component
func NewIcebergComponent(manager *Manager, logger zerolog.Logger) *IcebergComponent {
	return &IcebergComponent{
		manager: manager,
		logger:  logger,
	}
}

// OnEvent handles incoming events for this component
func (ic *IcebergComponent) OnEvent(ctx context.Context, event astha.Event[registry.FileInfo]) error {
	ic.logger.Debug().
		Str("table", event.Table).
		Str("operation", event.Operation).
		Int64("file_id", event.Data.ID).
		Msg("Received file event")

	switch event.Operation {
	case "INSERT":
		return ic.handleFileInsert(ctx, event.Data)
	case "UPDATE":
		return ic.handleFileUpdate(ctx, event.Data)
	case "DELETE":
		return ic.handleFileDelete(ctx, event.Data)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

// OnHealth check for component health
func (ic *IcebergComponent) OnHealth(ctx context.Context) error {
	// Check if the Iceberg manager is running
	if ic.manager == nil {
		return fmt.Errorf("iceberg manager is nil")
	}

	// Get stats to verify manager is responsive
	stats := ic.manager.GetStats()
	if stats == nil {
		return fmt.Errorf("failed to get iceberg manager stats")
	}

	ic.logger.Debug().
		Int64("files_processed", stats.FilesProcessed).
		Int64("batches_processed", stats.BatchesProcessed).
		Msg("Iceberg component health check passed")

	return nil
}

// OnRefresh tells component to refresh from Registry
func (ic *IcebergComponent) OnRefresh(ctx context.Context) error {
	ic.logger.Info().Msg("Refreshing Iceberg component from Registry")

	// Get pending files from the manager's queue
	pendingFiles := ic.manager.GetPendingFiles()
	
	ic.logger.Info().
		Int("pending_count", len(pendingFiles)).
		Msg("Refreshed pending files count")

	// Process any pending files that might have been missed
	for _, file := range pendingFiles {
		if err := ic.manager.ProcessFile(file); err != nil {
			ic.logger.Warn().
				Err(err).
				Int64("file_id", file.ID).
				Msg("Failed to process pending file during refresh")
		}
	}

	return nil
}

// handleFileInsert processes file insert events
func (ic *IcebergComponent) handleFileInsert(ctx context.Context, fileInfo registry.FileInfo) error {
	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("Processing file insert event")

	// Add file to Iceberg manager for processing
	if err := ic.manager.ProcessFile(fileInfo); err != nil {
		return fmt.Errorf("failed to process file insert: %w", err)
	}

	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Msg("File insert event processed successfully")

	return nil
}

// handleFileUpdate processes file update events
func (ic *IcebergComponent) handleFileUpdate(ctx context.Context, fileInfo registry.FileInfo) error {
	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("Processing file update event")

	// For updates, we need to check if the file needs reprocessing
	// This could happen if file size, row count, or other metadata changed
	if fileInfo.State != registry.IcebergMetadataGenerationStateCompleted {
		// File needs metadata regeneration
		if err := ic.manager.ProcessFile(fileInfo); err != nil {
			return fmt.Errorf("failed to process file update: %w", err)
		}

		ic.logger.Debug().
			Int64("file_id", fileInfo.ID).
			Msg("File update event processed - metadata regeneration triggered")
	} else {
		ic.logger.Debug().
			Int64("file_id", fileInfo.ID).
			Msg("File update event processed - no metadata regeneration needed")
	}

	return nil
}

// handleFileDelete processes file delete events
func (ic *IcebergComponent) handleFileDelete(ctx context.Context, fileInfo registry.FileInfo) error {
	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("Processing file delete event")

	// For deletes, we need to update the Iceberg metadata to reflect the removal
	// This would typically involve creating a new snapshot with the file marked as deleted
	// For now, we'll log the event and handle it in future phases
	
	ic.logger.Info().
		Int64("file_id", fileInfo.ID).
		Msg("File delete event received - deletion handling will be implemented in future phases")

	return nil
}

// GetSubscribedTables returns the tables this component subscribes to
func (ic *IcebergComponent) GetSubscribedTables() []string {
	return []string{"table_files"}
}

// GetComponentInfo returns component information for Astha registration
func (ic *IcebergComponent) GetComponentInfo() astha.ComponentInfo {
	return astha.ComponentInfo{
		Name:          "iceberg_metadata_manager",
		Version:       "1.0.0",
		Status:        "active",
		LastSeen:      time.Now(),
		Subscriptions: []string{"table_files"},
	}
}
