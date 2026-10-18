package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/spions/s3syncer/internal/metrics"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
)

const (
	retryInterval = 60 * time.Second // retry failed tasks every 60s
	maxTaskAge    = 12 * time.Hour   // give up after 12 hours

	// A processing task without a heartbeat for this long is considered dead and re-queued.
	stuckThreshold           = 5 * time.Minute
	defaultHeartbeatInterval = time.Minute
)

// filterSymlinkMetadata removes symlink-related metadata to prevent
// target files from inheriting symlink markers during replication.
func filterSymlinkMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}

	filtered := make(map[string]string)
	for k, v := range metadata {
		if k == storage.SymlinkMetaKey || k == "symlink-target" || k == "--symlink-target" {
			continue
		}
		filtered[k] = v
	}

	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// maxConcurrentTransfers bounds simultaneous object copies: each one holds an
// upload buffer in memory (~48 MiB), so this caps worker memory use.
const maxConcurrentTransfers = 4

const (
	// defaultPoolSize bounds the tasks handled at the same time (copies, verifications, deletes).
	defaultPoolSize = 16
	// fillBatch is the most tasks taken from the queue in one go.
	fillBatch = 100
)

type Worker struct {
	transfers chan struct{}
	queue     queue.Store
	backends  map[string]storage.StorageBackend
	interval  time.Duration
	log       *slog.Logger

	// Per-key lock to prevent parallel sync of the same object, across instances in cluster mode.
	locker queue.KeyLocker

	// How often in-flight tasks refresh their lease; must stay well below stuckThreshold.
	heartbeatInterval time.Duration

	// Notification channel to trigger immediate processing
	notify chan struct{}

	// slots bounds the tasks in progress; a slot is taken before a task leaves the queue,
	// so a slow task never blocks others from starting and the queue is not drained faster
	// than it can be worked off.
	slots chan struct{}
	// inflight holds the ids of dequeued, unfinished tasks: their lease is kept fresh.
	inflight sync.Map
	wg       sync.WaitGroup

	// failedDirs: directions (source->target) that failed since the start of the current
	// pass; the rest of their tasks wait for the next pass instead of failing one by one.
	failedMu   sync.Mutex
	failedDirs map[string]bool
}

func New(q queue.Store, locker queue.KeyLocker, backends map[string]storage.StorageBackend, interval time.Duration, log *slog.Logger) *Worker {
	return &Worker{
		queue:             q,
		locker:            locker,
		heartbeatInterval: defaultHeartbeatInterval,
		backends:          backends,
		interval:          interval,
		log:               log,
		notify:            make(chan struct{}, 1),

		transfers:  make(chan struct{}, maxConcurrentTransfers),
		slots:      make(chan struct{}, defaultPoolSize),
		failedDirs: map[string]bool{},
	}
}

// Notify triggers immediate processing of pending tasks
func (w *Worker) Notify() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// Run works the queue until ctx is cancelled and returns only after every task in progress
// has stopped: tasks that were cut short go back to pending, nothing is left "processing".
func (w *Worker) Run(ctx context.Context) {
	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	var hbDone sync.WaitGroup
	hbDone.Add(1)
	go func() { defer hbDone.Done(); w.heartbeat(hbCtx, &w.inflight) }()
	defer func() { w.wg.Wait(); stopHeartbeat(); hbDone.Wait() }()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.process(ctx, false, true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.process(ctx, false, true)
		case <-w.notify:
			// a task finished or new work arrived: refill free slots (maintenance runs on the tick)
			w.process(ctx, false, false)
		}
	}
}

// ProcessOnce runs one cycle: maintenance, then works the queue until it is empty and every
// task has finished.
func (w *Worker) ProcessOnce(ctx context.Context) {
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	var hbDone sync.WaitGroup
	hbDone.Add(1)
	go func() { defer hbDone.Done(); w.heartbeat(hbCtx, &w.inflight) }()
	defer func() { stopHeartbeat(); hbDone.Wait() }()

	w.process(ctx, true, true)
	w.wg.Wait()
}

func (w *Worker) maintenance() {
	// Reset stuck processing tasks (no heartbeat for stuckThreshold)
	if n, err := w.queue.ResetStuckProcessing(stuckThreshold); err != nil {
		w.log.Error("reset stuck processing tasks", "error", err)
	} else if n > 0 {
		w.log.Info("reset stuck processing tasks", "count", n)
	}

	// Retry failed tasks: only those failed > 60s ago and created < 12h ago (delete tasks never age out)
	if n, err := w.queue.RetryFailed(retryInterval, maxTaskAge); err != nil {
		w.log.Error("retry failed tasks", "error", err)
	} else if n > 0 {
		w.log.Info("retried failed tasks", "count", n)
	}

	// Purge copy tasks that have been failing for > 12 hours
	if n, err := w.queue.PurgeExpired(maxTaskAge); err != nil {
		w.log.Error("purge expired tasks", "error", err)
	} else if n > 0 {
		w.log.Info("purged expired tasks", "count", n)
	}

	if count, err := w.queue.PendingCount(); err == nil {
		metrics.SyncQueuePending.Set(float64(count))
	}
}

// process fills the free slots with tasks from the queue. With drain it keeps going until the
// queue is empty (waiting for slots); otherwise it stops when no slot is free.
func (w *Worker) process(ctx context.Context, drain, maintenance bool) {
	if maintenance {
		w.maintenance()
	}

	w.failedMu.Lock()
	w.failedDirs = map[string]bool{}
	w.failedMu.Unlock()

	for {
		if ctx.Err() != nil {
			return
		}
		// reserve one slot (waiting only in drain mode), then as many more as are free
		if drain {
			select {
			case w.slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
		} else {
			select {
			case w.slots <- struct{}{}:
			default:
				return
			}
		}
		got := 1
		for got < fillBatch {
			select {
			case w.slots <- struct{}{}:
				got++
				continue
			default:
			}
			break
		}

		tasks, err := w.queue.Dequeue(got)
		for i := len(tasks); i < got; i++ {
			<-w.slots // unused reservations
		}
		if err != nil {
			w.log.Error("dequeue", "error", err)
			return
		}
		if len(tasks) == 0 {
			return
		}

		skipped := false
		for _, task := range tasks {
			// the lease must be kept fresh from the moment the task leaves the queue
			w.inflight.Store(task.ID, struct{}{})
			if !w.dispatch(ctx, task) {
				skipped = true
			}
		}
		if skipped || len(tasks) < got {
			return // skipped tasks wait for the next pass; otherwise the queue is empty
		}
	}
}

// dispatch starts a task on its reserved slot. It returns false if the task was put back
// (failed direction, key busy elsewhere, shutting down), which the caller uses to stop
// re-reading the same tasks in a tight loop.
func (w *Worker) dispatch(ctx context.Context, task queue.Task) bool {
	finish := func() {
		w.inflight.Delete(task.ID)
		<-w.slots
	}
	direction := task.SourceBackendName + "->" + task.TargetBackendName

	w.failedMu.Lock()
	dirFailed := w.failedDirs[direction]
	w.failedMu.Unlock()
	if dirFailed || ctx.Err() != nil {
		w.markPending(task.ID)
		finish()
		return false
	}

	// Per-key lock (shared between instances in cluster mode)
	release, locked, err := w.locker.TryLock(ctx, task.VirtualBucket+"/"+task.ObjectKey)
	if err != nil {
		w.log.Warn("lock key", "key", task.ObjectKey, "error", err)
	}
	if !locked {
		w.markPending(task.ID)
		finish()
		return false
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer finish()
		defer release()
		if !w.syncTask(ctx, task) && ctx.Err() == nil {
			w.failedMu.Lock()
			w.failedDirs[direction] = true
			w.failedMu.Unlock()
		}
		w.Notify() // a slot is free again
	}()
	return true
}

// fail records a failed attempt, unless the worker is shutting down: then the task was merely
// interrupted and goes back to pending without counting as a failure.
func (w *Worker) fail(ctx context.Context, task queue.Task, reason string) {
	if ctx.Err() != nil {
		w.markPending(task.ID)
		return
	}
	w.markFailed(task.ID, reason)
	metrics.SyncTasksTotal.WithLabelValues("failed").Inc()
}

// syncTask returns true on success, false on failure.
func (w *Worker) syncTask(ctx context.Context, task queue.Task) bool {
	// Check if this is a delete task (source starts with "DELETE:")
	if queue.IsDeleteTask(task.SourceBackendName) {
		return w.syncDeleteTask(ctx, task)
	}

	src, ok := w.backends[task.SourceBackendName]
	if !ok {
		w.log.Error("unknown source backend", "name", task.SourceBackendName)
		w.fail(ctx, task, "unknown source backend")
		return false
	}
	dst, ok := w.backends[task.TargetBackendName]
	if !ok {
		w.log.Error("unknown target backend", "name", task.TargetBackendName)
		w.fail(ctx, task, "unknown target backend")
		return false
	}

	// The client deleted this object while the delete has not reached the source backend yet:
	// copying from it would bring the object back.
	if ok, err := w.queue.HasTask(task.VirtualBucket, task.ObjectKey, queue.DeletePrefix+src.Name(), src.Name()); err == nil && ok {
		w.log.Info("sync dropped: source copy is pending deletion", "key", task.ObjectKey, "source", src.Name())
		if err := w.queue.MarkDone(task.ID); err != nil {
			w.log.Error("mark done", "id", task.ID, "error", err)
		}
		metrics.SyncTasksTotal.WithLabelValues("skipped").Inc()
		return true
	}

	// Check if file already exists on target with same size and ETag
	dstInfo, err := dst.Head(ctx, task.ObjectKey)
	if err == nil {
		// File exists on target, check if it matches source
		srcInfo, err := src.Head(ctx, task.ObjectKey)
		if err == nil {
			// Compare size and ETag
			if dstInfo.Size == srcInfo.Size && dstInfo.ETag == srcInfo.ETag {
				// File already synced, skip
				if err := w.queue.MarkDone(task.ID); err != nil {
					w.log.Error("mark done", "id", task.ID, "error", err)
				}
				metrics.SyncTasksTotal.WithLabelValues("skipped").Inc()
				w.log.Debug("sync skipped (already synced)", "key", task.ObjectKey, "source", src.Name(), "target", dst.Name(), "size", srcInfo.Size, "etag", srcInfo.ETag)
				return true
			}
		}
	}

	select {
	case w.transfers <- struct{}{}:
		defer func() { <-w.transfers }()
	case <-ctx.Done():
		w.markPending(task.ID)
		return false
	}

	body, info, err := src.Get(ctx, task.ObjectKey)
	if errors.Is(err, storage.ErrNotFound) {
		// The source no longer has the object (deleted or overwritten elsewhere): there is
		// nothing to copy, retrying for 12 hours would only burn requests.
		w.log.Info("sync dropped: object no longer on the source", "key", task.ObjectKey, "source", src.Name())
		if err := w.queue.MarkDone(task.ID); err != nil {
			w.log.Error("mark done", "id", task.ID, "error", err)
		}
		metrics.SyncTasksTotal.WithLabelValues("skipped").Inc()
		return true
	}
	if err != nil {
		w.log.Warn("sync get failed", "key", task.ObjectKey, "source", src.Name(), "error", err)
		w.fail(ctx, task, err.Error())
		return false
	}

	defer body.Close()

	// Preserve symlink metadata on symlink objects, filter from regular files
	meta := info.Metadata
	if !info.IsSymlink() {
		meta = filterSymlinkMetadata(meta)
	}

	// Stream src -> dst: bounded memory, no temp files
	if _, err := dst.PutStream(ctx, task.ObjectKey, body, info.ContentType, meta); err != nil {
		w.log.Warn("sync put failed", "key", task.ObjectKey, "target", dst.Name(), "error", err)
		w.fail(ctx, task, err.Error())
		return false
	}

	if err := w.queue.MarkDone(task.ID); err != nil {
		w.log.Error("mark done", "id", task.ID, "error", err)
	}
	metrics.SyncTasksTotal.WithLabelValues("synced").Inc()

	w.log.Info("synced", "key", task.ObjectKey, "source", src.Name(), "target", dst.Name())
	return true
}

// syncDeleteTask handles delete synchronization tasks
func (w *Worker) syncDeleteTask(ctx context.Context, task queue.Task) bool {
	dst, ok := w.backends[task.TargetBackendName]
	if !ok {
		w.log.Error("unknown target backend for delete", "name", task.TargetBackendName)
		w.fail(ctx, task, "unknown target backend")
		return false
	}

	if err := dst.Delete(ctx, task.ObjectKey); err != nil {
		w.log.Warn("sync delete failed", "key", task.ObjectKey, "target", dst.Name(), "error", err)
		w.fail(ctx, task, err.Error())
		return false
	}

	if err := w.queue.MarkDone(task.ID); err != nil {
		w.log.Error("mark done", "id", task.ID, "error", err)
	}
	metrics.SyncTasksTotal.WithLabelValues("synced").Inc()

	w.log.Info("delete synced", "key", task.ObjectKey, "target", dst.Name())
	return true
}

func (w *Worker) markFailed(id string, msg string) {
	if err := w.queue.MarkFailed(id, msg); err != nil {
		w.log.Error("mark failed", "id", id, "error", err)
	}
}

func (w *Worker) markPending(id string) {
	if err := w.queue.MarkPending(id); err != nil {
		w.log.Error("mark pending", "id", id, "error", err)
	}
}

// heartbeat refreshes the lease of all in-flight tasks until ctx is done.
func (w *Worker) heartbeat(ctx context.Context, inflight *sync.Map) {
	t := time.NewTicker(w.heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var ids []string
			inflight.Range(func(k, _ any) bool {
				ids = append(ids, k.(string))
				return true
			})
			if len(ids) == 0 {
				continue
			}
			if err := w.queue.Touch(ids...); err != nil {
				w.log.Warn("heartbeat", "tasks", len(ids), "error", err)
			}
		}
	}
}
