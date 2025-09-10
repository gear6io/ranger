package iceberg

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/gear6io/ranger/server/paths"
	"github.com/rs/zerolog"
)

// Package-specific error codes for iceberg management
var (
	IcebergManagerAlreadyRunning  = errors.MustNewCode("iceberg.already_running")
	IcebergManagerNotRunning      = errors.MustNewCode("iceberg.not_running")
	IcebergManagerStartupFailed   = errors.MustNewCode("iceberg.startup_failed")
	IcebergManagerOperationFailed = errors.MustNewCode("iceberg.operation_failed")
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
	ID        string                `json:"id"`
	Files     []*regtypes.TableFile `json:"files"`
	CreatedAt time.Time             `json:"created_at"`
	Status    string                `json:"status"` // pending, processing, completed, failed
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
		return errors.New(IcebergManagerAlreadyRunning, "manager is already running", nil)
	}

	// Start worker pool
	if err := m.workerPool.Start(); err != nil {
		return err
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
		return errors.New(IcebergManagerNotRunning, "manager is not running", nil)
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
func (m *Manager) ProcessFile(fileInfo *regtypes.TableFile) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return errors.New(IcebergManagerNotRunning, "manager is not running", nil)
	}

	// Add file to queue
	if err := m.fileQueue.Enqueue(fileInfo); err != nil {
		return err
	}

	// Submit processing task to worker pool
	task := &ProcessFileTask{
		FileInfo: fileInfo,
		Manager:  m,
	}

	if err := m.workerPool.Submit(task); err != nil {
		return err
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
		return errors.New(IcebergManagerNotRunning, "manager is not running", nil)
	}

	// Submit batch processing task to worker pool
	task := &ProcessBatchTask{
		Batch:   batch,
		Manager: m,
	}

	if err := m.workerPool.Submit(task); err != nil {
		return err
	}

	m.logger.Debug().
		Str("batch_id", batch.ID).
		Int("file_count", len(batch.Files)).
		Msg("Batch queued for Iceberg metadata processing")

	return nil
}

// GetPendingFiles returns files waiting to be processed
func (m *Manager) GetPendingFiles() []*regtypes.TableFile {
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
func (m *Manager) generateManifest(ctx context.Context, batch BatchInfo, tableInfo *registry.CompleteTableInfo) (string, error) {
	// Validate table info before processing
	if err := m.validateTableInfo(tableInfo); err != nil {
		return "", errors.New(IcebergManagerOperationFailed, "invalid table info", err).AddContext("table_id", tableInfo.ID)
	}

	return m.metadataGenerator.GenerateManifest(ctx, batch, tableInfo)
}

// updateMetadataFile updates the Iceberg metadata file with new snapshot
func (m *Manager) updateMetadataFile(ctx context.Context, batch BatchInfo, manifestPath string, tableInfo *registry.CompleteTableInfo) error {
	// Validate table info before processing
	if err := m.validateTableInfo(tableInfo); err != nil {
		return errors.New(IcebergManagerOperationFailed, "invalid table info", err).AddContext("table_id", tableInfo.ID)
	}

	return m.metadataGenerator.UpdateMetadataFile(ctx, batch, manifestPath, tableInfo)
}

// validateTableInfo validates that table info is complete for Iceberg operations
func (m *Manager) validateTableInfo(tableInfo *registry.CompleteTableInfo) error {
	if tableInfo == nil {
		return errors.New(IcebergManagerOperationFailed, "table info is nil", nil)
	}

	if tableInfo.ID <= 0 {
		return errors.New(IcebergManagerOperationFailed, "invalid table ID", nil).AddContext("table_id", tableInfo.ID)
	}

	if tableInfo.Name == "" {
		return errors.New(IcebergManagerOperationFailed, "table name is empty", nil).AddContext("table_id", tableInfo.ID)
	}

	if tableInfo.Database == "" {
		return errors.New(IcebergManagerOperationFailed, "database name is empty", nil).AddContext("table_id", tableInfo.ID)
	}

	return nil
}

// generateUUID generates a unique identifier for batches
func (m *Manager) generateUUID() string {
	return fmt.Sprintf("batch-%d", time.Now().UnixNano())
}
