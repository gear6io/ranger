package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/astha"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/rs/zerolog"
)

// Package-specific error codes for iceberg component
var (
	IcebergComponentUnknownOperation = errors.MustNewCode("iceberg.component.unknown_operation")
	IcebergComponentManagerNil       = errors.MustNewCode("iceberg.component.manager_nil")
	IcebergComponentStatsFailed      = errors.MustNewCode("iceberg.component.stats_failed")
	IcebergComponentProcessingFailed = errors.MustNewCode("iceberg.component.processing_failed")
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
func (ic *IcebergComponent) OnEvent(ctx context.Context, event astha.Event[*regtypes.TableFile]) error {
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
		return errors.New(IcebergComponentUnknownOperation, "unknown operation", nil).AddContext("operation", event.Operation)
	}
}

// OnHealth check for component health
func (ic *IcebergComponent) OnHealth(ctx context.Context) error {
	// Check if the Iceberg manager is running
	if ic.manager == nil {
		return errors.New(IcebergComponentManagerNil, "iceberg manager is nil", nil)
	}

	// Get stats to verify manager is responsive
	stats := ic.manager.GetStats()
	if stats == nil {
		return errors.New(IcebergComponentStatsFailed, "failed to get iceberg manager stats", nil)
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
func (ic *IcebergComponent) handleFileInsert(ctx context.Context, fileInfo *regtypes.TableFile) error {
	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("Processing file insert event")

	// Add file to Iceberg manager for processing
	if err := ic.manager.ProcessFile(fileInfo); err != nil {
		return err
	}

	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Msg("File insert event processed successfully")

	return nil
}

// handleFileUpdate processes file update events
func (ic *IcebergComponent) handleFileUpdate(ctx context.Context, fileInfo *regtypes.TableFile) error {
	ic.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("Processing file update event")

	// For updates, we need to check if the file needs reprocessing
	// This could happen if file size, row count, or other metadata changed
	if fileInfo.IcebergMetadataState != regtypes.IcebergMetadataGenerationStateCompleted {
		// File needs metadata regeneration
		if err := ic.manager.ProcessFile(fileInfo); err != nil {
			return err
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
func (ic *IcebergComponent) handleFileDelete(ctx context.Context, fileInfo *regtypes.TableFile) error {
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

// AsSubscriberAny returns a type adapter that implements Subscriber[any]
func (ic *IcebergComponent) AsSubscriberAny() astha.Subscriber[any] {
	return &icebergComponentAdapter{component: ic}
}

// icebergComponentAdapter adapts IcebergComponent to Subscriber[any]
type icebergComponentAdapter struct {
	component *IcebergComponent
}

// OnEvent handles incoming events with type conversion
func (a *icebergComponentAdapter) OnEvent(ctx context.Context, event astha.Event[any]) error {
	// Check if this is a table_files event
	if event.Table != "table_files" {
		// Skip events for other tables
		return nil
	}

	// Convert the generic data to FileInfo
	fileInfo, ok := event.Data.(*regtypes.TableFile)
	if !ok {
		// Try to parse from map if it's a map
		if dataMap, ok := event.Data.(map[string]interface{}); ok {
			// Convert map to FileInfo
			fileInfo = &regtypes.TableFile{}
			if id, ok := dataMap["id"].(float64); ok {
				fileInfo.ID = int64(id)
			}
			if tableID, ok := dataMap["table_id"].(float64); ok {
				fileInfo.TableID = int64(tableID)
			}
			if fileName, ok := dataMap["file_name"].(string); ok {
				fileInfo.FileName = fileName
			}
			if filePath, ok := dataMap["file_path"].(string); ok {
				fileInfo.FilePath = filePath
			}
			if fileSize, ok := dataMap["file_size"].(float64); ok {
				fileInfo.FileSize = int64(fileSize)
			}
			if fileType, ok := dataMap["file_type"].(string); ok {
				fileInfo.FileType = fileType
			}
			if partitionPath, ok := dataMap["partition_path"].(string); ok {
				fileInfo.PartitionPath = partitionPath
			}
			if rowCount, ok := dataMap["row_count"].(float64); ok {
				fileInfo.RowCount = int64(rowCount)
			}
			if checksum, ok := dataMap["checksum"].(string); ok {
				fileInfo.Checksum = checksum
			}
			if isCompressed, ok := dataMap["is_compressed"].(bool); ok {
				fileInfo.IsCompressed = isCompressed
			}
			if createdAt, ok := dataMap["created_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
					fileInfo.CreatedAt = t
				}
			}
			if modifiedAt, ok := dataMap["modified_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, modifiedAt); err == nil {
					fileInfo.UpdatedAt = t
				}
			}
			if state, ok := dataMap["iceberg_metadata_state"].(string); ok {
				fileInfo.IcebergMetadataState = state
			}
		} else {
			return errors.New(IcebergComponentProcessingFailed, "failed to convert event data to FileInfo", nil).AddContext("unexpected_type", fmt.Sprintf("%T", event.Data))
		}
	}

	// Create a typed event for the component
	typedEvent := astha.Event[*regtypes.TableFile]{
		ID:        event.ID,
		Table:     event.Table,
		Operation: event.Operation,
		Data:      fileInfo,
		Timestamp: event.Timestamp,
		CreatedAt: event.CreatedAt,
	}

	// Call the component's OnEvent method
	return a.component.OnEvent(ctx, typedEvent)
}

// OnHealth check for component health
func (a *icebergComponentAdapter) OnHealth(ctx context.Context) error {
	return a.component.OnHealth(ctx)
}

// OnRefresh tells component to refresh from Registry
func (a *icebergComponentAdapter) OnRefresh(ctx context.Context) error {
	return a.component.OnRefresh(ctx)
}
