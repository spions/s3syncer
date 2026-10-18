package taskmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spions/s3syncer/internal/config"
	"github.com/spions/s3syncer/internal/metrics"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/taskstate"
)

type TaskStatus = taskstate.Status

const (
	// How often the owner of a running task refreshes its heartbeat and looks for a cancel request.
	defaultWatchInterval = 5 * time.Second
	// A running task without a heartbeat for this long has lost its owner (cluster mode).
	StaleAfter = 90 * time.Second
)

type TaskManager struct {
	state    taskstate.Store
	owner    string // identifies this instance in task state
	queue    queue.Store
	backends map[string]storage.StorageBackend
	config   *config.Config
	log      *slog.Logger
	worker   WorkerNotifier

	watchInterval time.Duration

	// Running tasks of THIS instance, for cancellation
	mu           sync.RWMutex
	cancelFuncs  map[string]context.CancelFunc
	runningTasks map[string]bool
}

// WorkerNotifier is an interface for notifying worker about new tasks
type WorkerNotifier interface {
	Notify()
}

func New(state taskstate.Store, owner string, q queue.Store, backends map[string]storage.StorageBackend, cfg *config.Config, log *slog.Logger, worker WorkerNotifier) *TaskManager {
	return &TaskManager{
		state:         state,
		owner:         owner,
		queue:         q,
		backends:      backends,
		config:        cfg,
		log:           log,
		worker:        worker,
		watchInterval: defaultWatchInterval,
		cancelFuncs:   make(map[string]context.CancelFunc),
		runningTasks:  make(map[string]bool),
	}
}

func (tm *TaskManager) bucketExists(bucket string) bool {
	for _, vb := range tm.config.VirtualBuckets {
		if vb.Name == bucket {
			return true
		}
	}
	return false
}

// start registers a task and runs fn in the background.
func (tm *TaskManager) start(taskType, bucket string, fn func(ctx context.Context, taskID, bucket string)) (string, error) {
	if !tm.bucketExists(bucket) {
		return "", fmt.Errorf("bucket %q not found", bucket)
	}
	taskID := uuid.New().String()

	if err := tm.state.Create(taskID, taskType, bucket, tm.owner); err != nil {
		return "", err
	}

	ctx, cancel := context.WithCancel(context.Background())
	tm.mu.Lock()
	tm.cancelFuncs[taskID] = cancel
	tm.runningTasks[taskID] = true
	tm.mu.Unlock()

	go tm.watch(ctx, taskID, cancel)
	go fn(ctx, taskID, bucket)

	metrics.ManagedTasksActive.WithLabelValues(taskType, bucket).Inc()
	metrics.ManagedTasksTotal.WithLabelValues(taskType, "started").Inc()

	return taskID, nil
}

// StartResync starts a resync task for the specified bucket
func (tm *TaskManager) StartResync(bucket string) (string, error) {
	return tm.start("resync", bucket, tm.executeResync)
}

// StartBisync starts a bisync task for the specified bucket
func (tm *TaskManager) StartBisync(bucket string) (string, error) {
	return tm.start("bisync", bucket, tm.executeBisync)
}

// watch is the owner's side of a running task: it keeps the heartbeat fresh (so other
// instances do not reap the task) and turns a cancel requested on any instance into a local cancel.
func (tm *TaskManager) watch(ctx context.Context, taskID string, cancel context.CancelFunc) {
	t := time.NewTicker(tm.watchInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := tm.state.Touch(taskID); err != nil {
				tm.log.Warn("task heartbeat failed", "task_id", taskID, "error", err)
			}
			if on, err := tm.state.CancelRequested(taskID); err == nil && on {
				tm.log.Info("task cancel requested", "task_id", taskID)
				cancel()
				return
			}
		}
	}
}

// GetStatus returns the status of a task
func (tm *TaskManager) GetStatus(taskID string) (*TaskStatus, error) {
	st, err := tm.state.Get(taskID)
	if errors.Is(err, taskstate.ErrNotFound) {
		return nil, fmt.Errorf("task not found")
	}
	return st, err
}

// Cancel cancels a running task: directly when it runs on this instance, otherwise
// by asking its owner through the shared state (cluster mode).
func (tm *TaskManager) Cancel(taskID string) error {
	tm.mu.Lock()
	cancel, exists := tm.cancelFuncs[taskID]
	tm.mu.Unlock()

	if !exists {
		return tm.state.RequestCancel(taskID)
	}

	// Record the status first: the task's own goroutine records "cancelled" as soon as its
	// context ends, and whoever finishes first wins. Losing means it already ended.
	changed, err := tm.state.Finish(taskID, taskstate.StatusCancelled, "")
	cancel()
	if err != nil {
		return err
	}
	if !changed {
		return taskstate.ErrNotRunning
	}
	return nil
}

// ListTasks returns all tasks
func (tm *TaskManager) ListTasks() ([]*TaskStatus, error) {
	return tm.state.List()
}

// updateProgress updates task progress in the state store and metrics
func (tm *TaskManager) updateProgress(taskID, taskType, bucket string, processed, total, chunk int64) {
	if err := tm.state.UpdateProgress(taskID, processed, total, chunk); err != nil {
		tm.log.Error("task state update failed", "task_id", taskID, "error", err)
	}

	if total > 0 {
		progress := float64(processed) / float64(total)
		metrics.ManagedTaskProgress.WithLabelValues(taskID, taskType, bucket).Set(progress)
	}
}

func (tm *TaskManager) markCompleted(taskID, taskType, bucket string) {
	tm.finish(taskID, taskType, bucket, taskstate.StatusCompleted, "")
}

func (tm *TaskManager) markFailed(taskID, taskType, bucket, errMsg string) {
	tm.finish(taskID, taskType, bucket, taskstate.StatusFailed, errMsg)
}

func (tm *TaskManager) markCancelled(taskID, taskType, bucket string) {
	tm.finish(taskID, taskType, bucket, taskstate.StatusCancelled, "")
}

// finish records the terminal status, releases the local bookkeeping and stops the watcher.
func (tm *TaskManager) finish(taskID, taskType, bucket, status, errMsg string) {
	if _, err := tm.state.Finish(taskID, status, errMsg); err != nil {
		tm.log.Error("task state update failed", "task_id", taskID, "error", err)
	}

	tm.mu.Lock()
	cancel := tm.cancelFuncs[taskID]
	delete(tm.cancelFuncs, taskID)
	delete(tm.runningTasks, taskID)
	tm.mu.Unlock()
	if cancel != nil {
		cancel() // ends the watch goroutine
	}

	metrics.ManagedTasksActive.WithLabelValues(taskType, bucket).Dec()
	metrics.ManagedTaskProgress.DeleteLabelValues(taskID, taskType, bucket)
	metrics.ManagedTasksTotal.WithLabelValues(taskType, status).Inc()
}
