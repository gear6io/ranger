package iceberg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/metadata/registry"
)

// ProcessFileTask processes a single file for Iceberg metadata generation
type ProcessFileTask struct {
	FileInfo registry.FileInfo
	Manager  *Manager
}

// Execute processes the file task
func (t *ProcessFileTask) Execute(ctx context.Context) error {
	// Mark file as processing
	if err := t.Manager.fileQueue.MarkCompleted(t.FileInfo.ID); err != nil {
		return fmt.Errorf("failed to mark file as completed: %w", err)
	}

	// TODO: Implement actual file processing
	// This would involve:
	// 1. Reading file statistics
	// 2. Generating Iceberg manifest entry
	// 3. Updating metadata

	return nil
}

// GetID returns the task identifier
func (t *ProcessFileTask) GetID() string {
	return fmt.Sprintf("file-%d", t.FileInfo.ID)
}

// ProcessBatchTask processes a batch of files together
type ProcessBatchTask struct {
	Batch   BatchInfo
	Manager *Manager
}

// Execute processes the batch task
func (t *ProcessBatchTask) Execute(ctx context.Context) error {
	// Generate manifest for the batch
	manifestPath, err := t.Manager.generateManifest(t.Batch)
	if err != nil {
		return fmt.Errorf("failed to generate manifest: %w", err)
	}

	// Update metadata file with new snapshot
	if err := t.Manager.updateMetadataFile(t.Batch, manifestPath); err != nil {
		return fmt.Errorf("failed to update metadata file: %w", err)
	}

	// Mark all files in batch as completed
	for _, file := range t.Batch.Files {
		if err := t.Manager.fileQueue.MarkCompleted(file.ID); err != nil {
			return fmt.Errorf("failed to mark file %d as completed: %w", file.ID, err)
		}
	}

	return nil
}

// GetID returns the task identifier
func (t *ProcessBatchTask) GetID() string {
	return fmt.Sprintf("batch-%s", t.Batch.ID)
}

// BatchProcessor handles batching logic for optimal metadata generation
type BatchProcessor struct {
	maxFilesPerBatch int
	minBatchSize     int64
	maxBatchSize     int64
	mu               sync.RWMutex
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor() *BatchProcessor {
	return &BatchProcessor{
		maxFilesPerBatch: 10,                     // From constants
		minBatchSize:     100 * 1024 * 1024,      // 100MB
		maxBatchSize:     1 * 1024 * 1024 * 1024, // 1GB
	}
}

// CreateBatches creates optimal batches from a list of files
func (bp *BatchProcessor) CreateBatches(files []registry.FileInfo) []BatchInfo {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	if len(files) == 0 {
		return nil
	}

	var batches []BatchInfo
	var currentBatch []registry.FileInfo
	var currentSize int64

	for _, file := range files {
		// Check if adding this file would exceed file count limit
		if len(currentBatch) >= bp.maxFilesPerBatch {
			// Create batch and start new one
			if len(currentBatch) > 0 {
				batches = append(batches, bp.createBatchInfo(currentBatch))
				currentBatch = nil
				currentSize = 0
			}
		}

		// Check if adding this file would exceed size limits
		if currentSize+file.FileSize > bp.maxBatchSize {
			// Create batch and start new one
			if len(currentBatch) > 0 {
				batches = append(batches, bp.createBatchInfo(currentBatch))
				currentBatch = nil
				currentSize = 0
			}
		}

		// Add file to current batch
		currentBatch = append(currentBatch, file)
		currentSize += file.FileSize

		// Check if we should create a batch now (size threshold reached)
		if currentSize >= bp.minBatchSize && len(currentBatch) >= 2 {
			batches = append(batches, bp.createBatchInfo(currentBatch))
			currentBatch = nil
			currentSize = 0
		}
	}

	// Add remaining files as final batch
	if len(currentBatch) > 0 {
		batches = append(batches, bp.createBatchInfo(currentBatch))
	}

	return batches
}

// createBatchInfo creates a BatchInfo from a slice of files
func (bp *BatchProcessor) createBatchInfo(files []registry.FileInfo) BatchInfo {
	return BatchInfo{
		ID:        fmt.Sprintf("batch-%d", time.Now().UnixNano()),
		Files:     files,
		CreatedAt: time.Now(),
		Status:    "pending",
	}
}
