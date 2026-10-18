package taskmanager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/spions/s3syncer/internal/config"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/taskstate"
)

// blockingBackend lists forever (until cancelled): a resync that is "running".
type blockingBackend struct {
	storage.StorageBackend
	name string
}

func (b *blockingBackend) Name() string { return b.name }

func (b *blockingBackend) ListObjects(ctx context.Context, _ string) ([]storage.ObjectInfo, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingBackend) ListObjectsPaginatedFull(ctx context.Context, _ string, _ int, _ func([]storage.ObjectInfo) error) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func testConfig() *config.Config {
	return &config.Config{VirtualBuckets: []config.VirtualBucket{{
		Name: "vb", PrimaryBackendName: "a",
		Backends: []config.Backend{{Name: "a"}, {Name: "b"}},
	}}}
}

func newManager(t *testing.T, state taskstate.Store, owner string) *TaskManager {
	t.Helper()
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	backends := map[string]storage.StorageBackend{"a": &blockingBackend{name: "a"}, "b": &blockingBackend{name: "b"}}
	tm := New(state, owner, queue.NewSQLite(db), backends, testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	tm.watchInterval = 20 * time.Millisecond
	return tm
}

func redisManagers(t *testing.T) (a, b *TaskManager, state taskstate.Store) {
	t.Helper()
	mr := miniredis.RunT(t)
	mk := func(owner string) *TaskManager {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })
		st, err := taskstate.NewRedis(c, taskstate.RedisOptions{})
		if err != nil {
			t.Fatal(err)
		}
		state = st
		return newManager(t, st, owner)
	}
	return mk("pod-a"), mk("pod-b"), state
}

func waitStatus(t *testing.T, tm *TaskManager, id, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, err := tm.GetStatus(id); err == nil && s.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s, _ := tm.GetStatus(id)
	t.Fatalf("task %s did not reach %q, last: %+v", id, want, s)
}

// Task started on pod A is visible and listable on pod B.
func TestTaskVisibleFromOtherInstance(t *testing.T) {
	a, b, _ := redisManagers(t)
	id, err := a.StartResync("vb")
	if err != nil {
		t.Fatal(err)
	}
	s, err := b.GetStatus(id)
	if err != nil || s.Owner != "pod-a" || s.Status != taskstate.StatusRunning {
		t.Fatalf("pod B sees %+v, %v", s, err)
	}
	if list, _ := b.ListTasks(); len(list) != 1 {
		t.Fatalf("pod B lists %d tasks, want 1", len(list))
	}
	_ = a.Cancel(id)
}

// task-cancel may land on any pod; the owner must stop the work.
func TestCancelOnOtherInstanceStopsOwner(t *testing.T) {
	a, b, _ := redisManagers(t)
	id, err := a.StartResync("vb")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(id); err != nil {
		t.Fatalf("cancel via pod B: %v", err)
	}
	waitStatus(t, a, id, taskstate.StatusCancelled)

	// The task goroutine drops its bookkeeping just after it recorded the final status, so the
	// owner stops tracking the task shortly after, not at the same instant.
	tracked := func() int {
		a.mu.RLock()
		defer a.mu.RUnlock()
		return len(a.cancelFuncs)
	}
	deadline := time.Now().Add(3 * time.Second)
	for tracked() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if left := tracked(); left != 0 {
		t.Fatalf("owner still tracks %d running tasks after cancel", left)
	}
}

func TestCancelUnknownTask(t *testing.T) {
	a, b, _ := redisManagers(t)
	if err := a.Cancel("nope"); err == nil {
		t.Fatal("cancel of unknown task must fail")
	}
	if err := b.Cancel("nope"); err == nil {
		t.Fatal("cancel of unknown task must fail")
	}
}

// The owner's heartbeat keeps a long task from being reaped as orphaned.
func TestHeartbeatKeepsTaskFromBeingReaped(t *testing.T) {
	a, _, state := redisManagers(t)
	id, _ := a.StartResync("vb")
	time.Sleep(150 * time.Millisecond) // several heartbeats
	if n, _ := state.FailStale(100*time.Millisecond, "owner lost"); n != 0 {
		t.Fatalf("a task with a live owner was reaped (%d)", n)
	}
	_ = a.Cancel(id)
}

func TestSingleModeCancelLocal(t *testing.T) {
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state, err := taskstate.NewSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	tm := newManager(t, state, "solo")

	id, err := tm.StartResync("vb")
	if err != nil {
		t.Fatal(err)
	}
	if err := tm.Cancel(id); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, tm, id, taskstate.StatusCancelled)
	if err := tm.Cancel(id); err == nil {
		t.Fatal("second cancel of a finished task must fail")
	}
}

func TestStartUnknownBucket(t *testing.T) {
	a, _, _ := redisManagers(t)
	if _, err := a.StartResync("nope"); err == nil {
		t.Fatal("unknown bucket must be rejected")
	}
}

func conflictOf(t *testing.T, err error) *taskstate.ConflictError {
	t.Helper()
	var c *taskstate.ConflictError
	if !errors.As(err, &c) {
		t.Fatalf("err = %v, want *taskstate.ConflictError", err)
	}
	return c
}

// A second resync/bisync of the same bucket is refused while one is running, on any instance.
func TestDuplicateStartRejectedWhileRunning(t *testing.T) {
	a, b, _ := redisManagers(t)

	first, err := a.StartResync("vb")
	if err != nil {
		t.Fatal(err)
	}
	if c := conflictOf(t, func() error { _, err := a.StartResync("vb"); return err }()); c.ExistingID != first {
		t.Fatalf("same instance: conflict points at %s, want %s", c.ExistingID, first)
	}
	if c := conflictOf(t, func() error { _, err := b.StartResync("vb"); return err }()); c.ExistingID != first {
		t.Fatalf("other instance: conflict points at %s, want %s", c.ExistingID, first)
	}

	// A different kind of task for the same bucket is not a duplicate.
	other, err := b.StartBisync("vb")
	if err != nil {
		t.Fatalf("bisync next to a running resync must be allowed: %v", err)
	}

	if err := a.Cancel(first); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, a, first, taskstate.StatusCancelled)
	again, err := b.StartResync("vb")
	if err != nil {
		t.Fatalf("resync after the previous one ended must be allowed: %v", err)
	}
	_ = a.Cancel(other)
	_ = b.Cancel(again)
}

func TestDuplicateStartRejectedInSingleMode(t *testing.T) {
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state, err := taskstate.NewSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	tm := newManager(t, state, "solo")

	first, err := tm.StartResync("vb")
	if err != nil {
		t.Fatal(err)
	}
	_ = conflictOf(t, func() error { _, err := tm.StartResync("vb"); return err }())
	_ = tm.Cancel(first)
}
