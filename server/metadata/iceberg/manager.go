package iceberg

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/TFMV/icebox/server/paths"
	"github.com/rs/zerolog"
)

// Manager handles Iceberg metadata generation and updates
type Manager struct {
	pathManager       paths.PathManager
	logger            zerolog.Logger
	workerPool        *WorkerPool
	fileQueue         *FileQueue
	metadataGenerator *MetadataGenerator
	mu                sync.RWMutex
	running           bool
	stats             *ManagerStats
}

// ManagerStats tracks manager performance metrics
type ManagerStats struct {
	FilesProcessed      int64         `json:"files_processed"`
	BatchesProcessed    int64         `json:"batches_processed"`
	TotalProcessingTime time.Duration `json:"total_processing_time"`
	LastProcessed       time.Time     `json:"last_processed"`
	Errors              int64         `json:"errors"`
}

// BatchInfo represents a batch of files to process together
type BatchInfo struct {
	ID        string              `json:"id"`
	Files     []registry.FileInfo `json:"files"`
	CreatedAt time.Time           `json:"created_at"`
	Status    string              `json:"status"` // pending, processing, completed, failed
}

// NewManager creates a new Iceberg metadata manager
func NewManager(pathManager paths.PathManager, logger zerolog.Logger) *Manager {
	// Create worker pool with max workers based on GOMAXPROCS
	maxWorkers := runtime.GOMAXPROCS(0)
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	workerPool := NewWorkerPool(maxWorkers, logger)
	fileQueue := NewFileQueue()
	metadataGenerator := NewMetadataGenerator(pathManager, logger)

	return &Manager{
		pathManager:       pathManager,
		logger:            logger,
		workerPool:        workerPool,
		fileQueue:         fileQueue,
		metadataGenerator: metadataGenerator,
		stats:             &ManagerStats{},
	}
}

// Start starts the Iceberg metadata manager
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("manager is already running")
	}

	// Start worker pool
	if err := m.workerPool.Start(); err != nil {
		return fmt.Errorf("failed to start worker pool: %w", err)
	}

	// Load pending files from startup
	if err := m.loadPendingFiles(); err != nil {
		m.logger.Warn().Err(err).Msg("failed to load pending files during startup")
	}

	m.running = true
	m.logger.Info().Msg("Iceberg metadata manager started")
	return nil
}

// Stop stops the Iceberg metadata manager
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("manager is not running")
	}

	// Stop worker pool
	if err := m.workerPool.Stop(); err != nil {
		m.logger.Warn().Err(err).Msg("failed to stop worker pool gracefully")
	}

	m.running = false
	m.logger.Info().Msg("Iceberg metadata manager stopped")
	return nil
}

// ProcessFile adds a file to the processing queue
func (m *Manager) ProcessFile(fileInfo registry.FileInfo) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return fmt.Errorf("manager is not running")
	}

	// Add file to queue
	if err := m.fileQueue.Enqueue(fileInfo); err != nil {
		return fmt.Errorf("failed to enqueue file: %w", err)
	}

	// Submit processing task to worker pool
	task := &ProcessFileTask{
		FileInfo: fileInfo,
		Manager:  m,
	}

	if err := m.workerPool.Submit(task); err != nil {
		return fmt.Errorf("failed to submit file processing task: %w", err)
	}

	m.logger.Debug().
		Int64("file_id", fileInfo.ID).
		Str("file_name", fileInfo.FileName).
		Msg("File queued for Iceberg metadata processing")

	return nil
}

// ProcessBatch processes a batch of files together
func (m *Manager) ProcessBatch(batch BatchInfo) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return fmt.Errorf("manager is not running")
	}

	// Submit batch processing task to worker pool
	task := &ProcessBatchTask{
		Batch:   batch,
		Manager: m,
	}

	if err := m.workerPool.Submit(task); err != nil {
		return fmt.Errorf("failed to submit batch processing task: %w", err)
	}

	m.logger.Debug().
		Str("batch_id", batch.ID).
		Int("file_count", len(batch.Files)).
		Msg("Batch queued for Iceberg metadata processing")

	return nil
}

// GetPendingFiles returns files waiting to be processed
func (m *Manager) GetPendingFiles() []registry.FileInfo {
	return m.fileQueue.GetPendingFiles()
}

// GetStats returns manager statistics
func (m *Manager) GetStats() *ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get queue stats
	queueStats := m.fileQueue.GetStats()

	// Combine stats
	stats := &ManagerStats{
		FilesProcessed:      m.stats.FilesProcessed,
		BatchesProcessed:    m.stats.BatchesProcessed,
		TotalProcessingTime: m.stats.TotalProcessingTime,
		LastProcessed:       m.stats.LastProcessed,
		Errors:              m.stats.Errors,
	}

	// Add queue stats
	stats.FilesProcessed += int64(queueStats.PendingCount + queueStats.ProcessingCount)

	return stats
}

// loadPendingFiles loads files that need Iceberg metadata generation during startup
func (m *Manager) loadPendingFiles() error {
	// This will be implemented to query the Registry for files
	// where iceberg_metadata_state != "completed"
	// For now, we'll just log that this would happen
	m.logger.Info().Msg("Loading pending files for Iceberg metadata generation")
	return nil
}

// generateManifest creates an Iceberg manifest file for a batch of files
func (m *Manager) generateManifest(batch BatchInfo) (string, error) {
	// TODO: Get actual table info from batch or Registry
	tableInfo := &registry.TableInfo{
		ID:   1,               // Placeholder
		Name: "default_table", // Placeholder
	}

	return m.metadataGenerator.GenerateManifest(context.Background(), batch, tableInfo)
}

// updateMetadataFile updates the Iceberg metadata file with new snapshot
func (m *Manager) updateMetadataFile(batch BatchInfo, manifestPath string) error {
	// TODO: Get actual table info from batch or Registry
	tableInfo := &registry.TableInfo{
		ID:   1,               // Placeholder
		Name: "default_table", // Placeholder
	}

	return m.metadataGenerator.UpdateMetadataFile(context.Background(), batch, manifestPath, tableInfo)
}

// generateUUID generates a unique identifier for batches
func (m *Manager) generateUUID() string {
	return fmt.Sprintf("batch-%d", time.Now().UnixNano())
}
