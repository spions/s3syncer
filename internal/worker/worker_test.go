package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
)

// fakeBackend is an in-memory StorageBackend. Only what the worker calls is implemented.
type fakeBackend struct {
	storage.StorageBackend // nil: any other method panics, which flags an unexpected call
	name                   string

	mu        sync.Mutex
	puts      map[string]int
	putDelay  time.Duration
	putBlock  chan struct{} // if set, PutStream waits for it to close
	putCalled chan struct{} // if set, signalled on each PutStream entry

	blockKey string          // if set, only PutStream of this key waits for putBlock
	missing  map[string]bool // Get reports ErrNotFound for these keys
	cur, max atomic.Int64    // concurrent PutStream calls, and the highest value seen
}

func newFake(name string) *fakeBackend {
	return &fakeBackend{name: name, puts: map[string]int{}}
}

func (f *fakeBackend) Name() string { return f.name }

func (f *fakeBackend) Head(_ context.Context, key string) (*storage.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.puts[key] > 0 {
		return &storage.ObjectInfo{Key: key, Size: 4, ETag: "e"}, nil
	}
	return nil, errors.New("not found")
}

func (f *fakeBackend) Get(_ context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	if f.missing[key] {
		return nil, nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader([]byte("data"))), &storage.ObjectInfo{Key: key, Size: 4, ETag: "e"}, nil
}

func (f *fakeBackend) PutStream(ctx context.Context, key string, body io.Reader, _ string, _ map[string]string) (string, error) {
	n := f.cur.Add(1)
	defer f.cur.Add(-1)
	for {
		m := f.max.Load()
		if n <= m || f.max.CompareAndSwap(m, n) {
			break
		}
	}
	if f.putCalled != nil {
		select {
		case f.putCalled <- struct{}{}:
		default:
		}
	}
	if f.putBlock != nil && (f.blockKey == "" || f.blockKey == key) {
		select {
		case <-f.putBlock:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	time.Sleep(f.putDelay)
	_, _ = io.Copy(io.Discard, body)
	f.mu.Lock()
	f.puts[key]++
	f.mu.Unlock()
	return "e", nil
}

func (f *fakeBackend) putCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts[key]
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// instance builds one "s3syncer process": its own Store/Locker clients on a shared Redis.
func instance(t *testing.T, mr *miniredis.Miniredis, backends map[string]storage.StorageBackend) (*Worker, queue.Store) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	store, err := queue.NewRedisStore(client, queue.RedisOptions{})
	if err != nil {
		t.Fatal(err)
	}
	locker := queue.NewRedisLocker(client, queue.RedisLockOptions{TTL: 5 * time.Second})
	t.Cleanup(locker.Close)
	return New(store, locker, backends, time.Hour, quietLog()), store
}

// Two workers on one queue must copy every object exactly once.
func TestTwoWorkersProcessEachTaskOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putDelay = time.Millisecond
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}

	w1, store := instance(t, mr, backends)
	w2, _ := instance(t, mr, backends)

	const n = 60
	for i := 0; i < n; i++ {
		if err := store.Enqueue("b", fmt.Sprintf("k%02d", i), "src", "dst", queue.PriorityLow); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for _, w := range []*Worker{w1, w2} {
		wg.Add(1)
		go func() { defer wg.Done(); w.ProcessOnce(context.Background()) }()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if got := dst.putCount(fmt.Sprintf("k%02d", i)); got != 1 {
			t.Fatalf("k%02d copied %d times, want exactly 1", i, got)
		}
	}
	if p, _ := store.PendingCount(); p != 0 {
		t.Fatalf("pending = %d, want 0", p)
	}
}

// The a->b and b->a tasks of one object must not run at the same time on different instances.
func TestKeyLockedByAnotherInstanceIsSkippedNotLost(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)

	// Another instance holds the lock for b/obj.
	other := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { other.Close() })
	otherLocker := queue.NewRedisLocker(other, queue.RedisLockOptions{TTL: 5 * time.Second})
	t.Cleanup(otherLocker.Close)
	release, ok, err := otherLocker.TryLock(context.Background(), "b/obj")
	if err != nil || !ok {
		t.Fatalf("setup lock: %v %v", ok, err)
	}

	_ = store.Enqueue("b", "obj", "src", "dst", queue.PriorityLow)
	w.ProcessOnce(context.Background())

	if dst.putCount("obj") != 0 {
		t.Fatal("object was copied although another instance holds its lock")
	}
	if p, _ := store.PendingCount(); p != 1 {
		t.Fatalf("task must go back to pending, pending = %d", p)
	}

	release()
	w.ProcessOnce(context.Background())
	if dst.putCount("obj") != 1 {
		t.Fatal("object must be copied once the lock is free")
	}
}

// A long copy must keep refreshing its lease, otherwise another instance
// resets it as "stuck" and copies the same object concurrently.
func TestLongCopySendsHeartbeats(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putBlock = make(chan struct{})
	dst.putCalled = make(chan struct{}, 1)
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)
	w.heartbeatInterval = 10 * time.Millisecond

	counting := &touchCounter{Store: store}
	w.queue = counting

	_ = store.Enqueue("b", "big", "src", "dst", queue.PriorityLow)
	done := make(chan struct{})
	go func() { w.ProcessOnce(context.Background()); close(done) }()

	<-dst.putCalled
	deadline := time.Now().Add(2 * time.Second)
	for counting.touches.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	close(dst.putBlock)
	<-done

	if got := counting.touches.Load(); got < 3 {
		t.Fatalf("expected >= 3 heartbeats during a blocked copy, got %d", got)
	}
	// ...and the heartbeat goroutine must not outlive the task.
	after := counting.touches.Load()
	time.Sleep(50 * time.Millisecond)
	if counting.touches.Load() != after {
		t.Fatal("heartbeats continue after the task finished (goroutine leak)")
	}
}

type touchCounter struct {
	queue.Store
	touches atomic.Int64
}

func (c *touchCounter) Touch(ids ...string) error {
	c.touches.Add(1)
	return c.Store.Touch(ids...)
}

// A write that lands while its object is being copied must trigger another pass.
func TestWriteDuringCopyIsResynced(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putBlock = make(chan struct{})
	dst.putCalled = make(chan struct{}, 1)
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)

	_ = store.Enqueue("b", "obj", "src", "dst", queue.PriorityLow)
	done := make(chan struct{})
	go func() { w.ProcessOnce(context.Background()); close(done) }()
	<-dst.putCalled

	_ = store.Enqueue("b", "obj", "src", "dst", queue.PriorityHigh) // client overwrites the object mid-copy
	close(dst.putBlock)
	<-done

	if p, _ := store.PendingCount(); p != 1 {
		t.Fatalf("task must be re-queued after the racing write, pending = %d", p)
	}
}

// ---------------------------------------------------------------------------
// Scheduling, shutdown and stale tasks
// ---------------------------------------------------------------------------

func counts(t *testing.T, s queue.Store) (pending, processing, failed int64) {
	t.Helper()
	pending, _ = s.PendingCount()
	processing, _ = s.ProcessingCount()
	failed, _ = s.FailedCount()
	return
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// One huge copy must not hold up the small tasks queued behind it.
func TestSlowCopyDoesNotBlockOtherTasks(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putBlock, dst.blockKey = make(chan struct{}), "huge"
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)
	w.interval = 20 * time.Millisecond

	_ = store.Enqueue("b", "huge", "src", "dst", queue.PriorityHigh) // dequeued first
	for i := 0; i < 5; i++ {
		_ = store.Enqueue("b", fmt.Sprintf("small%d", i), "src", "dst", queue.PriorityLow)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "small tasks to finish while the huge copy is still running", func() bool {
		for i := 0; i < 5; i++ {
			if dst.putCount(fmt.Sprintf("small%d", i)) != 1 {
				return false
			}
		}
		return true
	})
	if dst.putCount("huge") != 0 {
		t.Fatal("test setup: the huge copy should still be blocked")
	}
	close(dst.putBlock)
	waitFor(t, "the huge copy to finish", func() bool { return dst.putCount("huge") == 1 })
}

// Stopping the worker mid-copy must leave nothing "processing" and must not count as a failure:
// the interrupted task simply goes back to pending for the next start.
func TestShutdownReturnsInterruptedTaskToPending(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putBlock = make(chan struct{}) // never released
	dst.putCalled = make(chan struct{}, 1)
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)

	_ = store.Enqueue("b", "big", "src", "dst", queue.PriorityLow)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	<-dst.putCalled

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	pending, processing, failed := counts(t, store)
	if pending != 1 || processing != 0 || failed != 0 {
		t.Fatalf("after shutdown pending=%d processing=%d failed=%d; want 1/0/0", pending, processing, failed)
	}
}

// The source lost the object (deleted or replaced): retrying for 12 hours is pointless.
func TestTaskWhoseSourceObjectIsGoneIsCompleted(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	src.missing = map[string]bool{"gone": true}
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)

	_ = store.Enqueue("b", "gone", "src", "dst", queue.PriorityLow)
	w.ProcessOnce(context.Background())

	pending, processing, failed := counts(t, store)
	if pending+processing+failed != 0 {
		t.Fatalf("task not completed: pending=%d processing=%d failed=%d", pending, processing, failed)
	}
	if dst.putCount("gone") != 0 {
		t.Fatal("something was written for a vanished source object")
	}
}

// A copy out of a backend that still waits for the client's DELETE would resurrect the object.
func TestCopyFromBackendWithPendingDeleteIsDropped(t *testing.T) {
	mr := miniredis.RunT(t)
	backup, primary := newFake("backup"), newFake("primary")
	backends := map[string]storage.StorageBackend{"backup": backup, "primary": primary}
	w, store := instance(t, mr, backends)

	_ = store.Enqueue("b", "k", queue.DeletePrefix+"backup", "backup", queue.PriorityHigh) // delete not done yet
	_ = store.Enqueue("b", "k", "backup", "primary", queue.PriorityMed)                    // restore from that backup
	// process only the restore: take the delete task out of the way (it is retried later)
	tasks, _ := store.Dequeue(10)
	for _, task := range tasks {
		if queue.IsDeleteTask(task.SourceBackendName) {
			_ = store.MarkFailed(task.ID, "backup down")
		} else {
			_ = store.MarkPending(task.ID)
		}
	}

	w.ProcessOnce(context.Background())
	if primary.putCount("k") != 0 {
		t.Fatal("the primary was restored from a copy that is waiting to be deleted")
	}
}

// Copies are bounded: a backlog of big objects must not start dozens of uploads at once.
func TestConcurrentCopiesAreBounded(t *testing.T) {
	mr := miniredis.RunT(t)
	src, dst := newFake("src"), newFake("dst")
	dst.putDelay = 15 * time.Millisecond
	backends := map[string]storage.StorageBackend{"src": src, "dst": dst}
	w, store := instance(t, mr, backends)

	const n = 80
	for i := 0; i < n; i++ {
		_ = store.Enqueue("b", fmt.Sprintf("k%02d", i), "src", "dst", queue.PriorityLow)
	}
	w.ProcessOnce(context.Background())

	for i := 0; i < n; i++ {
		if dst.putCount(fmt.Sprintf("k%02d", i)) != 1 {
			t.Fatalf("k%02d not copied exactly once", i)
		}
	}
	if m := dst.max.Load(); m > maxConcurrentTransfers {
		t.Fatalf("%d uploads ran at the same time, limit is %d", m, maxConcurrentTransfers)
	}
}
