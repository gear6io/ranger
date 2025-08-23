package iceberg

import (
	"fmt"
	"sync"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry"
)

// Package-specific error codes for file queue
var (
	FileQueueAlreadyQueued = errors.MustNewCode("iceberg.file_queue.already_queued")
	FileQueueNotProcessing = errors.MustNewCode("iceberg.file_queue.not_processing")
)

// FileQueue manages a FIFO queue of files for Iceberg metadata processing
type FileQueue struct {
	pending    []registry.FileInfo
	processing map[int64]registry.FileInfo // file ID -> file info
	mu         sync.RWMutex
	stats      *QueueStats
}

// QueueStats tracks queue performance metrics
type QueueStats struct {
	PendingCount    int   `json:"pending_count"`
	ProcessingCount int   `json:"processing_count"`
	TotalEnqueued   int64 `json:"total_enqueued"`
	TotalDequeued   int64 `json:"total_dequeued"`
	TotalCompleted  int64 `json:"total_completed"`
	TotalFailed     int64 `json:"total_failed"`
}

// NewFileQueue creates a new file queue
func NewFileQueue() *FileQueue {
	return &FileQueue{
		pending:    make([]registry.FileInfo, 0),
		processing: make(map[int64]registry.FileInfo),
		stats:      &QueueStats{},
	}
}

// Enqueue adds a file to the pending queue
func (q *FileQueue) Enqueue(fileInfo registry.FileInfo) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if file is already in queue or processing
	if q.isFileInQueue(fileInfo.ID) || q.isFileProcessing(fileInfo.ID) {
		return errors.New(FileQueueAlreadyQueued, "file is already queued or processing").AddContext("file_id", fmt.Sprintf("%d", fileInfo.ID))
	}

	q.pending = append(q.pending, fileInfo)
	q.stats.TotalEnqueued++
	q.updateStats()

	return nil
}

// Dequeue removes and returns the next file from the pending queue
func (q *FileQueue) Dequeue() (registry.FileInfo, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return registry.FileInfo{}, false
	}

	// Get first file (FIFO)
	fileInfo := q.pending[0]
	q.pending = q.pending[1:]

	// Move to processing
	q.processing[fileInfo.ID] = fileInfo
	q.stats.TotalDequeued++
	q.updateStats()

	return fileInfo, true
}

// MarkCompleted marks a file as completed and removes it from processing
func (q *FileQueue) MarkCompleted(fileID int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isFileProcessing(fileID) {
		return errors.New(FileQueueNotProcessing, "file is not in processing state").AddContext("file_id", fmt.Sprintf("%d", fileID))
	}

	delete(q.processing, fileID)
	q.stats.TotalCompleted++
	q.updateStats()

	return nil
}

// MarkFailed marks a file as failed and removes it from processing
func (q *FileQueue) MarkFailed(fileID int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.isFileProcessing(fileID) {
		return errors.New(FileQueueNotProcessing, "file is not in processing state").AddContext("file_id", fmt.Sprintf("%d", fileID))
	}

	delete(q.processing, fileID)
	q.stats.TotalFailed++
	q.updateStats()

	return nil
}

// GetPendingFiles returns all files waiting to be processed
func (q *FileQueue) GetPendingFiles() []registry.FileInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files := make([]registry.FileInfo, len(q.pending))
	copy(files, q.pending)
	return files
}

// GetProcessingFiles returns all files currently being processed
func (q *FileQueue) GetProcessingFiles() []registry.FileInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files := make([]registry.FileInfo, 0, len(q.processing))
	for _, file := range q.processing {
		files = append(files, file)
	}
	return files
}

// GetBatch returns a batch of files for processing (up to maxFiles)
func (q *FileQueue) GetBatch(maxFiles int) []registry.FileInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	// Determine batch size
	batchSize := maxFiles
	if len(q.pending) < maxFiles {
		batchSize = len(q.pending)
	}

	// Get batch
	batch := q.pending[:batchSize]
	q.pending = q.pending[batchSize:]

	// Move batch to processing
	for _, file := range batch {
		q.processing[file.ID] = file
	}

	q.stats.TotalDequeued += int64(batchSize)
	q.updateStats()

	return batch
}

// GetStats returns queue statistics
func (q *FileQueue) GetStats() *QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	stats := &QueueStats{
		PendingCount:    q.stats.PendingCount,
		ProcessingCount: q.stats.ProcessingCount,
		TotalEnqueued:   q.stats.TotalEnqueued,
		TotalDequeued:   q.stats.TotalDequeued,
		TotalCompleted:  q.stats.TotalCompleted,
		TotalFailed:     q.stats.TotalFailed,
	}

	return stats
}

// Clear removes all files from the queue
func (q *FileQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending = make([]registry.FileInfo, 0)
	q.processing = make(map[int64]registry.FileInfo)
	q.stats = &QueueStats{}
}

// isFileInQueue checks if a file is in the pending queue
func (q *FileQueue) isFileInQueue(fileID int64) bool {
	for _, file := range q.pending {
		if file.ID == fileID {
			return true
		}
	}
	return false
}

// isFileProcessing checks if a file is currently being processed
func (q *FileQueue) isFileProcessing(fileID int64) bool {
	_, exists := q.processing[fileID]
	return exists
}

// updateStats updates the queue statistics
func (q *FileQueue) updateStats() {
	q.stats.PendingCount = len(q.pending)
	q.stats.ProcessingCount = len(q.processing)
}
