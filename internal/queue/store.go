package queue

import (
	"context"
	"strings"
	"time"
)

// A task that keeps failing is retried with exponential backoff: RetryBackoffBase after the
// first failure, doubling per failure up to RetryBackoffCap. Without it a backend that is down
// for hours is hit by every failed task once a minute, which slows its recovery.
const (
	RetryBackoffBase = 60 * time.Second
	RetryBackoffCap  = 30 * time.Minute
)

// DeletePrefix marks a task whose source is "DELETE:<backend>": remove the object from
// that backend instead of copying it. Such tasks are never purged or aged out: if they
// were, an object deleted while a backend was down would come back after the retry window.
const DeletePrefix = "DELETE:"

// IsDeleteTask reports whether source names a delete task.
func IsDeleteTask(source string) bool { return strings.HasPrefix(source, DeletePrefix) }

const (
	PriorityLow  = 0
	PriorityMed  = 1
	PriorityHigh = 2
)

// Task statuses as exposed in Task.Status and used as the "state" metric label.
const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusFailed     = "failed"
)

type Task struct {
	// ID is opaque: a row id for SQLite, a dedup hash for Redis.
	ID                string
	VirtualBucket     string
	ObjectKey         string
	SourceBackendName string
	TargetBackendName string
	Priority          int
	Status            string
	Attempts          int
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Store is the sync-task queue. All implementations must satisfy the
// contract in store_contract_test.go; several processes may share one Store
// backend (Redis), so every method must be safe for concurrent use across processes.
//
// Semantics:
//   - Tasks are deduplicated by (bucket, key, source, target).
//   - Enqueue of an existing pending task only raises its priority.
//   - Enqueue of a task that is being processed marks it dirty: MarkDone then
//     returns it to pending instead of deleting it, so a write that raced with
//     a copy is not lost.
//   - Dequeue returns highest priority first, oldest first within a priority.
type Store interface {
	Enqueue(virtualBucket, objectKey, source, target string, priority int) error
	// HasTask reports whether a task with exactly this identity is waiting in the queue
	// (pending, processing or failed). Finished tasks are removed and report false.
	HasTask(virtualBucket, objectKey, source, target string) (bool, error)
	Dequeue(limit int) ([]Task, error)
	MarkDone(id string) error
	// MarkPending returns a task to pending (e.g. skipped due to a held lock).
	MarkPending(id string) error
	MarkFailed(id string, lastErr string) error
	// Touch refreshes the processing lease of in-flight tasks (heartbeat).
	Touch(ids ...string) error

	// RetryFailed moves failed tasks back to pending when they failed at least
	// retryInterval ago and were created less than maxAge ago.
	RetryFailed(retryInterval, maxAge time.Duration) (int64, error)
	// PurgeExpired deletes failed tasks created more than maxAge ago.
	PurgeExpired(maxAge time.Duration) (int64, error)
	// ResetStuckProcessing returns tasks without a heartbeat for stuckThreshold to pending.
	ResetStuckProcessing(stuckThreshold time.Duration) (int64, error)

	PendingCount() (int64, error)
	ProcessingCount() (int64, error)
	FailedCount() (int64, error)
	// OldestPendingAge is the age of the oldest pending task, 0 if none.
	OldestPendingAge() (time.Duration, error)
}

// KeyLocker serialises work on one object across goroutines and processes.
type KeyLocker interface {
	// TryLock returns ok=false when the key is held elsewhere. The lock is
	// kept alive until the returned release func is called.
	TryLock(ctx context.Context, key string) (release func(), ok bool, err error)
}

// WaitForChunkCompletion polls until the queue is mostly empty or ctx is cancelled.
// Returns true if the chunk completed, false if cancelled.
// Waits until pending + processing tasks are below the threshold (allows the next chunk to be enqueued).
func WaitForChunkCompletion(ctx context.Context, s Store, pollInterval time.Duration) bool {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	const threshold = 50 // Allow next chunk when queue has less than 50 tasks

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			processing, _ := s.ProcessingCount()
			pending, _ := s.PendingCount()
			if processing+pending < threshold {
				return true
			}
		}
	}
}
