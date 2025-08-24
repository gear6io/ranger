package iceberg

import (
	"context"
	"sync"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/rs/zerolog"
)

// Package-specific error codes for worker pool
var (
	WorkerPoolAlreadyRunning = errors.MustNewCode("iceberg.worker_pool.already_running")
	WorkerPoolNotRunning     = errors.MustNewCode("iceberg.worker_pool.not_running")
	WorkerPoolStartupFailed  = errors.MustNewCode("iceberg.worker_pool.startup_failed")
)

// Task interface that all worker pool tasks must implement
type Task interface {
	Execute(ctx context.Context) error
	GetID() string
}

// WorkerPool manages a pool of workers for concurrent task execution
type WorkerPool struct {
	maxWorkers int
	workers    []*Worker
	taskQueue  chan Task
	logger     zerolog.Logger
	mu         sync.RWMutex
	running    bool
	stats      *PoolStats
}

// Worker represents a single worker in the pool
type Worker struct {
	id        int
	pool      *WorkerPool
	taskQueue <-chan Task
	logger    zerolog.Logger
	stats     *WorkerStats
	ctx       context.Context
	cancel    context.CancelFunc
}

// PoolStats tracks worker pool performance metrics
type PoolStats struct {
	TotalWorkers    int           `json:"total_workers"`
	ActiveWorkers   int           `json:"active_workers"`
	TasksQueued     int           `json:"tasks_queued"`
	TasksCompleted  int64         `json:"tasks_completed"`
	TasksFailed     int64         `json:"tasks_failed"`
	TotalWaitTime   time.Duration `json:"total_wait_time"`
	AverageWaitTime time.Duration `json:"average_wait_time"`
}

// WorkerStats tracks individual worker performance
type WorkerStats struct {
	ID             int           `json:"id"`
	TasksProcessed int64         `json:"tasks_processed"`
	TotalWorkTime  time.Duration `json:"total_work_time"`
	LastTaskTime   time.Time     `json:"last_task_time"`
	Status         string        `json:"status"` // idle, busy, stopped
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(maxWorkers int, logger zerolog.Logger) *WorkerPool {
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	pool := &WorkerPool{
		maxWorkers: maxWorkers,
		taskQueue:  make(chan Task, maxWorkers*2), // Buffer for task queue
		logger:     logger,
		stats:      &PoolStats{},
	}

	// Create workers
	pool.workers = make([]*Worker, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		pool.workers[i] = pool.newWorker(i)
	}

	pool.stats.TotalWorkers = maxWorkers
	return pool
}

// Start starts the worker pool
func (wp *WorkerPool) Start() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.running {
		return errors.New(WorkerPoolAlreadyRunning, "worker pool is already running", nil)
	}

	// Start all workers
	for _, worker := range wp.workers {
		if err := worker.start(); err != nil {
			return err
		}
	}

	wp.running = true
	wp.logger.Info().
		Int("max_workers", wp.maxWorkers).
		Msg("Worker pool started")

	return nil
}

// Stop stops the worker pool
func (wp *WorkerPool) Stop() error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if !wp.running {
		return errors.New(WorkerPoolNotRunning, "worker pool is not running", nil)
	}

	// Stop all workers
	for _, worker := range wp.workers {
		worker.stop()
	}

	// Close task queue
	close(wp.taskQueue)

	wp.running = false
	wp.logger.Info().Msg("Worker pool stopped")
	return nil
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task Task) error {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.running {
		return errors.New(WorkerPoolNotRunning, "worker pool is not running", nil)
	}

	select {
	case wp.taskQueue <- task:
		wp.stats.TasksQueued++
		wp.logger.Debug().
			Str("task_id", task.GetID()).
			Msg("Task submitted to worker pool")
		return nil
	default:
		return ErrQueueFull
	}
}

// GetStats returns worker pool statistics
func (wp *WorkerPool) GetStats() *PoolStats {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	stats := &PoolStats{
		TotalWorkers:   wp.stats.TotalWorkers,
		TasksQueued:    len(wp.taskQueue),
		TasksCompleted: wp.stats.TasksCompleted,
		TasksFailed:    wp.stats.TasksFailed,
		TotalWaitTime:  wp.stats.TotalWaitTime,
	}

	// Count active workers
	activeCount := 0
	for _, worker := range wp.workers {
		if worker.stats.Status == "busy" {
			activeCount++
		}
	}
	stats.ActiveWorkers = activeCount

	// Calculate average wait time
	if wp.stats.TasksCompleted > 0 {
		stats.AverageWaitTime = wp.stats.TotalWaitTime / time.Duration(wp.stats.TasksCompleted)
	}

	return stats
}

// newWorker creates a new worker
func (wp *WorkerPool) newWorker(id int) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		id:        id,
		pool:      wp,
		taskQueue: wp.taskQueue,
		logger:    wp.logger.With().Int("worker_id", id).Logger(),
		stats:     &WorkerStats{ID: id, Status: "idle"},
		ctx:       ctx,
		cancel:    cancel,
	}
}

// start starts the worker
func (w *Worker) start() error {
	w.stats.Status = "idle"
	go w.run()
	return nil
}

// run is the main worker loop
func (w *Worker) run() {
	w.logger.Debug().Msg("Worker started")

	for {
		select {
		case task, ok := <-w.taskQueue:
			if !ok {
				// Task queue closed, worker should stop
				w.logger.Debug().Msg("Task queue closed, worker stopping")
				return
			}

			w.processTask(task)

		case <-w.ctx.Done():
			w.logger.Debug().Msg("Worker context cancelled, stopping")
			return
		}
	}
}

// stop stops the worker
func (w *Worker) stop() {
	w.stats.Status = "stopped"
	w.cancel()
}

// processTask processes a single task
func (w *Worker) processTask(task Task) {
	startTime := time.Now()
	w.stats.Status = "busy"

	w.logger.Debug().
		Str("task_id", task.GetID()).
		Msg("Processing task")

	// Execute task
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	if err := task.Execute(ctx); err != nil {
		w.pool.stats.TasksFailed++
		w.logger.Error().
			Err(err).
			Str("task_id", task.GetID()).
			Msg("Task execution failed")
	} else {
		w.pool.stats.TasksCompleted++
		w.logger.Debug().
			Str("task_id", task.GetID()).
			Msg("Task completed successfully")
	}

	// Update statistics
	workTime := time.Since(startTime)
	w.stats.TasksProcessed++
	w.stats.TotalWorkTime += workTime
	w.stats.LastTaskTime = time.Now()
	w.pool.stats.TotalWaitTime += workTime

	w.stats.Status = "idle"
}

// Errors
var (
	ErrQueueFull = &PoolError{Message: "task queue is full"}
)

// PoolError represents a worker pool error
type PoolError struct {
	Message string
}

func (e *PoolError) Error() string {
	return e.Message
}
