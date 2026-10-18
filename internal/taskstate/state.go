// Package taskstate stores the state of managed resync/bisync tasks: in local
// SQLite for a single instance, in Redis when several instances share it so that
// task-status / task-list / task-cancel work on any of them.
package taskstate

import (
	"errors"
	"fmt"
	"time"
)

const (
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

var (
	ErrNotFound = errors.New("task not found")
	// ErrNotRunning: the task is unknown, finished, or (single mode) not running in this process.
	ErrNotRunning = errors.New("task not running or not found")
)

// ConflictError: a task of the same type is already running for that bucket.
type ConflictError struct {
	Type, Bucket, ExistingID string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s for bucket %q is already running (task %s)", e.Type, e.Bucket, e.ExistingID)
}

type Status struct {
	ID               string    `json:"id"`
	Type             string    `json:"type"`
	Bucket           string    `json:"bucket"`
	Status           string    `json:"status"`
	TotalObjects     int64     `json:"total_objects"`
	ProcessedObjects int64     `json:"processed_objects"`
	CurrentChunk     int64     `json:"current_chunk"`
	Error            string    `json:"error,omitempty"`
	Owner            string    `json:"owner,omitempty"` // instance running the task (cluster mode)
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Store keeps task state. A running task must be refreshed (UpdateProgress or
// Touch) by its owner; FailStale reaps running tasks whose owner went silent.
type Store interface {
	// Create registers a running task. It fails with *ConflictError when a task of the
	// same type is already running for the bucket (atomically, also across instances).
	Create(id, taskType, bucket, owner string) error
	Get(id string) (*Status, error)
	// List returns all tasks, newest first.
	List() ([]*Status, error)
	// UpdateProgress records progress and refreshes the owner's heartbeat.
	UpdateProgress(id string, processed, total, chunk int64) error
	// Touch refreshes the owner's heartbeat.
	Touch(id string) error
	// Finish sets a terminal status (completed|failed|cancelled) only if the task
	// is still running. It reports whether the status changed.
	Finish(id, status, errMsg string) (bool, error)
	// RequestCancel asks the owner (possibly another instance) to cancel a running
	// task; ErrNotRunning if it is not running. SQLite always returns ErrNotRunning:
	// in single mode the owner is this process and cancels directly.
	RequestCancel(id string) error
	CancelRequested(id string) (bool, error)
	// FailStale marks running tasks without a heartbeat for olderThan as failed with reason.
	FailStale(olderThan time.Duration, reason string) (int64, error)
	// Prune keeps all running tasks and only the newest `keep` finished ones.
	Prune(keep int) error
}
