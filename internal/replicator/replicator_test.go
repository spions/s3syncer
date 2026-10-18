package replicator

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

	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// memBackend is an in-memory storage backend with fault injection. Every method the
// replicator may call is implemented explicitly, so an unexpected call fails loudly.
type memBackend struct {
	storage.StorageBackend // nil: calling anything not overridden below panics

	name string

	mu      sync.Mutex
	objects map[string][]byte
	puts    int // successful Put/PutStream calls
	deletes int

	putErr    error         // returned by Put/PutStream before storing anything
	getErr    error         // returned by Get (HEAD still succeeds)
	headErr   error         // returned by Head
	deleteErr error         // returned by Delete
	headDelay time.Duration // Head and Get block this long (or until ctx ends)

	headCalls, getCalls atomic.Int64

	putDelayFor map[string]time.Duration // Put sleeps this long when the body equals the key
}

func newMem(name string) *memBackend {
	return &memBackend{name: name, objects: map[string][]byte{}}
}

func (b *memBackend) Name() string { return b.name }

func (b *memBackend) set(key, data string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = []byte(data)
}

func (b *memBackend) has(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.objects[key]
	return ok
}

func (b *memBackend) content(key string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.objects[key])
}

func (b *memBackend) Put(_ context.Context, key string, body io.Reader, _ int64, _ string, _ map[string]string) (string, error) {
	if b.putErr != nil {
		return "", b.putErr
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	time.Sleep(b.putDelayFor[string(data)])
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = data
	b.puts++
	return fmt.Sprintf(`"%s-etag"`, b.name), nil
}

func (b *memBackend) PutStream(ctx context.Context, key string, body io.Reader, ct string, md map[string]string) (string, error) {
	return b.Put(ctx, key, body, -1, ct, md)
}

func (b *memBackend) Head(ctx context.Context, key string) (*storage.ObjectInfo, error) {
	b.headCalls.Add(1)
	if b.headDelay > 0 {
		select {
		case <-time.After(b.headDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if b.headErr != nil {
		return nil, b.headErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return &storage.ObjectInfo{Key: key, Size: int64(len(data)), ETag: `"e"`}, nil
}

func (b *memBackend) GetRange(ctx context.Context, key, rng string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return b.Get(ctx, key)
}

func (b *memBackend) Get(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	b.getCalls.Add(1)
	if b.headDelay > 0 {
		select {
		case <-time.After(b.headDelay):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	if b.getErr != nil {
		return nil, nil, b.getErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.ObjectInfo{Key: key, Size: int64(len(data))}, nil
}

func (b *memBackend) CopyObject(_ context.Context, srcKey, dstKey string, _ map[string]string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[srcKey]
	if !ok {
		return "", fmt.Errorf("copy source: %w", storage.ErrNotFound)
	}
	b.objects[dstKey] = append([]byte(nil), data...)
	return `"copy"`, nil
}

func (b *memBackend) Delete(_ context.Context, key string) error {
	if b.deleteErr != nil {
		return b.deleteErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	b.deletes++
	return nil
}

// failingQueue behaves like a queue whose backend (Redis, SQLite) is down.
type failingQueue struct{ queue.Store }

func (failingQueue) Enqueue(string, string, string, string, int) error {
	return errors.New("queue backend down")
}

type env struct {
	rep     *Replicator
	primary *memBackend
	backup  *memBackend
	q       queue.Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	q := queue.NewSQLite(db)

	primary, backup := newMem("primary"), newMem("backup")
	rep := New("vb", primary, []storage.StorageBackend{backup}, q, slog.New(slog.NewJSONHandler(io.Discard, nil)), 300*time.Millisecond)
	return &env{rep: rep, primary: primary, backup: backup, q: q}
}

// tasks drains and returns everything currently queued.
func (e *env) tasks(t *testing.T) []queue.Task {
	t.Helper()
	tasks, err := e.q.Dequeue(1000)
	if err != nil {
		t.Fatal(err)
	}
	return tasks
}

func findTask(tasks []queue.Task, key, source, target string) *queue.Task {
	for i := range tasks {
		if tasks[i].ObjectKey == key && tasks[i].SourceBackendName == source && tasks[i].TargetBackendName == target {
			return &tasks[i]
		}
	}
	return nil
}

func put(e *env, key, data string) (string, error) {
	return e.rep.Put(context.Background(), key, bytes.NewReader([]byte(data)), int64(len(data)), "text/plain", nil)
}

// ---------------------------------------------------------------------------
// PUT
// ---------------------------------------------------------------------------

// The happy path: the object lands on every backend. Only a verification task is queued
// (the worker compares size/ETag and repairs divergence left by concurrent writers).
func TestPutWritesEveryBackendAndQueuesOnlyVerification(t *testing.T) {
	e := newEnv(t)

	etag, err := put(e, "k", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("PUT must return an ETag")
	}
	if e.primary.content("k") != "hello" || e.backup.content("k") != "hello" {
		t.Fatalf("object not on both backends: primary=%q backup=%q", e.primary.content("k"), e.backup.content("k"))
	}
	tasks := e.tasks(t)
	if len(tasks) != 1 || findTask(tasks, "k", "primary", "backup") == nil {
		t.Fatalf("expected exactly one primary→backup verification task, got %+v", tasks)
	}
}

// A dead backup must not fail the client's PUT, but the missing copy must be
// scheduled for repair; otherwise redundancy is silently lost.
func TestPutWithFailedBackupSucceedsAndSchedulesRepair(t *testing.T) {
	e := newEnv(t)
	e.backup.putErr = errors.New("backup down")

	if _, err := put(e, "k", "hello"); err != nil {
		t.Fatalf("PUT must succeed when the primary accepted it: %v", err)
	}
	if e.backup.has("k") {
		t.Fatal("backup unexpectedly has the object")
	}
	task := findTask(e.tasks(t), "k", "primary", "backup")
	if task == nil {
		t.Fatal("no repair task primary→backup queued: the replica is lost without a trace")
	}
	if task.Priority != queue.PriorityHigh {
		t.Fatalf("repair of a failed write must be HIGH priority, got %d", task.Priority)
	}
}

// If the primary is down but a backup took the write, the primary has to be repaired later.
func TestPutWithFailedPrimarySchedulesRepairFromBackup(t *testing.T) {
	e := newEnv(t)
	e.primary.putErr = errors.New("primary down")

	if _, err := put(e, "k", "hello"); err != nil {
		t.Fatalf("PUT must succeed when a backup accepted it: %v", err)
	}
	if findTask(e.tasks(t), "k", "backup", "primary") == nil {
		t.Fatal("no repair task backup→primary queued")
	}
}

func TestPutFailsWhenAllBackendsFail(t *testing.T) {
	e := newEnv(t)
	e.primary.putErr = errors.New("down")
	e.backup.putErr = errors.New("down")

	if _, err := put(e, "k", "hello"); err == nil {
		t.Fatal("PUT must fail when nothing could be stored")
	}
	if n := len(e.tasks(t)); n != 0 {
		t.Fatalf("%d tasks queued although no copy exists to replicate from", n)
	}
}

// A client that sends fewer bytes than announced must not leave a truncated object anywhere.
func TestPutRejectsShortBodyAndStoresNothing(t *testing.T) {
	e := newEnv(t)

	_, err := e.rep.Put(context.Background(), "k", bytes.NewReader([]byte("abc")), 10, "text/plain", nil)
	if err == nil {
		t.Fatal("a body shorter than Content-Length must be rejected")
	}
	if e.primary.has("k") || e.backup.has("k") {
		t.Fatal("a truncated object was stored")
	}
}

// A client that sends MORE bytes than announced must be rejected without the server
// reading the whole excess into memory (memory-exhaustion vector).
func TestPutRejectsOversizedBodyWithoutReadingItAll(t *testing.T) {
	e := newEnv(t)
	const announced = 10
	body := &countingReader{remaining: 1 << 30} // 1 GiB of data behind a 10 byte announcement

	_, err := e.rep.Put(context.Background(), "k", body, announced, "text/plain", nil)
	if err == nil {
		t.Fatal("a body longer than Content-Length must be rejected")
	}
	if body.read > 1<<20 {
		t.Fatalf("server read %d bytes of a body announced as %d: unbounded buffering", body.read, announced)
	}
	if e.primary.has("k") || e.backup.has("k") {
		t.Fatal("an object was stored from an inconsistent request")
	}
}

type countingReader struct {
	remaining int64
	read      int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > c.remaining {
		n = c.remaining
	}
	c.remaining -= n
	c.read += n
	return int(n), nil
}

// If the repair task cannot be recorded (queue backend down) the client must be told:
// answering 200 would acknowledge a write that exists on one backend only, forever.
func TestPutReportsErrorWhenRepairCannotBeQueued(t *testing.T) {
	e := newEnv(t)
	e.backup.putErr = errors.New("backup down")
	e.rep.queue = failingQueue{e.q}

	_, err := put(e, "k", "hello")
	if err == nil {
		t.Fatal("PUT acknowledged although the missing replica could not even be queued")
	}
	if !errors.Is(err, ErrQueueUnavailable) {
		t.Fatalf("error should be ErrQueueUnavailable so the API can answer 503, got %v", err)
	}
}

// Large bodies go to the primary only and the backups are filled in by the worker.
func TestLargePutStreamsToPrimaryAndQueuesBackups(t *testing.T) {
	e := newEnv(t)
	e.rep.SetStreamThreshold(8)
	data := "0123456789abcdef"

	if _, err := e.rep.Put(context.Background(), "big", bytes.NewReader([]byte(data)), int64(len(data)), "x", nil); err != nil {
		t.Fatal(err)
	}
	if e.primary.content("big") != data {
		t.Fatal("primary does not have the full object")
	}
	if e.backup.has("big") {
		t.Fatal("a streamed PUT must not be written to the backup synchronously")
	}
	if findTask(e.tasks(t), "big", "primary", "backup") == nil {
		t.Fatal("backup copy of a streamed PUT was not queued")
	}
}

// A streamed upload that the primary rejects must fail the request and queue nothing.
func TestLargePutPrimaryFailureFailsRequest(t *testing.T) {
	e := newEnv(t)
	e.rep.SetStreamThreshold(8)
	e.primary.putErr = errors.New("primary down")

	_, err := e.rep.Put(context.Background(), "big", bytes.NewReader([]byte("0123456789abcdef")), 16, "x", nil)
	if err == nil {
		t.Fatal("streamed PUT must fail when the primary fails")
	}
	if n := len(e.tasks(t)); n != 0 {
		t.Fatalf("%d tasks queued for an upload that never succeeded", n)
	}
}

// Memory budget: with room for one upload at a time, many concurrent PUTs still all
// complete, none is lost, and the budget is fully returned afterwards.
func TestConcurrentPutsRespectMemoryBudgetAndAllComplete(t *testing.T) {
	e := newEnv(t)
	budget := NewMemoryBudget(48 << 10)
	e.rep.SetMemoryBudget(budget)

	const n = 60
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			data := bytes.Repeat([]byte{byte(i)}, 20<<10)
			if _, err := e.rep.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)), "x", nil); err != nil {
				errs <- fmt.Errorf("%s: %w", key, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		if !e.primary.has(key) || !e.backup.has(key) {
			t.Fatalf("%s missing after concurrent PUTs", key)
		}
	}
	// Everything released: a request for the whole budget must not block.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := budget.acquire(ctx, 48<<10)
	if err != nil {
		t.Fatalf("memory budget leaked: %v", err)
	}
	release()
}

// A client that gives up while waiting for memory must not leak budget or hang.
func TestPutWaitingForMemoryHonoursCancellation(t *testing.T) {
	e := newEnv(t)
	budget := NewMemoryBudget(48 << 10)
	e.rep.SetMemoryBudget(budget)

	hold, err := budget.acquire(context.Background(), 48<<10) // somebody else holds everything
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = e.rep.Put(ctx, "k", bytes.NewReader(make([]byte, 1000)), 1000, "x", nil)
	if err == nil {
		t.Fatal("PUT must fail when its context ends while waiting for memory")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("PUT did not return promptly after cancellation")
	}
	hold()
	if e.primary.has("k") {
		t.Fatal("object stored although the request was cancelled")
	}
	// the cancelled waiter must not have consumed budget
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	rel, err := budget.acquire(ctx2, 48<<10)
	if err != nil {
		t.Fatalf("budget leaked by cancelled waiter: %v", err)
	}
	rel()
}

// ---------------------------------------------------------------------------
// GET / HEAD
// ---------------------------------------------------------------------------

func get(e *env, key string) (string, error) {
	body, _, err := e.rep.Get(context.Background(), key)
	if err != nil {
		return "", err
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	return string(b), err
}

func TestGetServesFromPrimary(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "from-primary")
	e.backup.set("k", "from-backup")

	got, err := get(e, "k")
	if err != nil || got != "from-primary" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// Primary lost the object: serve the backup AND schedule restoring the primary.
func TestGetFallsBackToBackupAndSchedulesRestore(t *testing.T) {
	e := newEnv(t)
	e.backup.set("k", "from-backup")

	got, err := get(e, "k")
	if err != nil || got != "from-backup" {
		t.Fatalf("got %q, %v", got, err)
	}
	task := findTask(e.tasks(t), "k", "backup", "primary")
	if task == nil {
		t.Fatal("primary restore not queued")
	}
	if task.Priority != queue.PriorityMed {
		t.Fatalf("restore priority = %d, want MED", task.Priority)
	}
}

func TestGetMissingEverywhereIsNotFound(t *testing.T) {
	e := newEnv(t)
	if _, err := get(e, "nope"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

// A hanging primary must not hang the client: after the operation timeout the backup answers.
func TestGetPrimaryHangFallsBackWithinTimeout(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "from-primary")
	e.primary.headDelay = 5 * time.Second // opTimeout in the test env is 300ms
	e.backup.set("k", "from-backup")

	start := time.Now()
	got, err := get(e, "k")
	if err != nil || got != "from-backup" {
		t.Fatalf("got %q, %v", got, err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v: the hanging primary was not abandoned at the operation timeout", d)
	}
}

// A transient backend error is not "object does not exist". If the primary answers HEAD
// but then fails to serve the body, the backup copy must be used, and if nothing can serve
// it the error must not masquerade as 404 (clients would drop the file instead of retrying).
func TestGetBackendErrorFallsBackAndIsNotReportedAsNotFound(t *testing.T) {
	t.Run("backup serves when primary body fails", func(t *testing.T) {
		e := newEnv(t)
		e.primary.set("k", "from-primary")
		e.primary.getErr = errors.New("503 slow down")
		e.backup.set("k", "from-backup")

		got, err := get(e, "k")
		if err != nil || got != "from-backup" {
			t.Fatalf("got %q, %v; the healthy backup should have served the object", got, err)
		}
	})
	t.Run("error is not NotFound when nobody can serve", func(t *testing.T) {
		e := newEnv(t)
		e.primary.set("k", "from-primary")
		e.primary.getErr = errors.New("503 slow down")

		_, err := get(e, "k")
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, storage.ErrNotFound) {
			t.Fatal("a 5xx from the backend was reported as NoSuchKey")
		}
	})
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

func TestDeleteRemovesFromAllBackends(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "x")
	e.backup.set("k", "x")

	if err := e.rep.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if e.primary.has("k") || e.backup.has("k") {
		t.Fatal("object survived DELETE")
	}
	if n := len(e.tasks(t)); n != 0 {
		t.Fatalf("%d tasks queued after a clean DELETE", n)
	}
}

// If one backend could not delete, the delete must be retried later; otherwise the object
// would be resurrected from that backend by the next sync.
func TestDeleteWithFailedBackendSchedulesDeleteRetry(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "x")
	e.backup.set("k", "x")
	e.backup.deleteErr = errors.New("backup down")

	if err := e.rep.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("DELETE should succeed when one backend did it: %v", err)
	}
	if findTask(e.tasks(t), "k", "DELETE:backup", "backup") == nil {
		t.Fatal("delete retry for the failed backend was not queued")
	}
}

func TestDeleteFailsWhenNoBackendCouldDelete(t *testing.T) {
	e := newEnv(t)
	e.primary.deleteErr = errors.New("down")
	e.backup.deleteErr = errors.New("down")

	if err := e.rep.Delete(context.Background(), "k"); err == nil {
		t.Fatal("DELETE must fail when nothing was deleted")
	}
}

func TestDeleteReportsErrorWhenRetryCannotBeQueued(t *testing.T) {
	e := newEnv(t)
	e.backup.deleteErr = errors.New("backup down")
	e.rep.queue = failingQueue{e.q}

	err := e.rep.Delete(context.Background(), "k")
	if !errors.Is(err, ErrQueueUnavailable) {
		t.Fatalf("got %v; an un-queued delete retry means a deleted object comes back", err)
	}
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

// The background restore check on GET must not leak goroutines or panic when a request
// finishes immediately (smoke test under -race with many concurrent readers).
func TestConcurrentGetsAreRaceFree(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "x")

	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := get(e, "k"); err == nil && got == "x" {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 50 {
		t.Fatalf("only %d/50 reads succeeded", ok.Load())
	}
}

// ---------------------------------------------------------------------------
// Deleted objects must not come back
// ---------------------------------------------------------------------------

// The client deleted an object, but the backup was down and still holds a copy. Until the
// delete reaches the backup, that copy must neither be served nor copied back to the primary.
func TestDeletedObjectIsNotResurrectedFromBackupWithPendingDelete(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "x")
	e.backup.set("k", "x")
	e.backup.deleteErr = errors.New("backup down")

	if err := e.rep.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if e.primary.has("k") || !e.backup.has("k") {
		t.Fatal("test setup: expected the object gone from the primary but still on the backup")
	}
	_ = e.tasks(t) // moves the delete task to "processing": it must still protect the object

	if _, err := get(e, "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GET of a deleted object returned %v, want ErrNotFound", err)
	}
	if _, err := e.rep.Head(context.Background(), "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("HEAD of a deleted object returned %v, want ErrNotFound", err)
	}
	if findTask(e.tasks(t), "k", "backup", "primary") != nil {
		t.Fatal("a restore backup→primary was queued for a deleted object")
	}
}

// The delete protection must not hide objects the primary lost by accident.
func TestObjectLostOnPrimaryIsStillServedFromBackup(t *testing.T) {
	e := newEnv(t)
	e.backup.set("k", "from-backup") // no delete task exists

	got, err := get(e, "k")
	if err != nil || got != "from-backup" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// When uploads cannot get memory within the wait limit, the request is shed with
// ErrOverloaded (HTTP 503) instead of queueing without bound behind a stuck upload.
func TestPutIsShedWhenMemoryStaysUnavailable(t *testing.T) {
	e := newEnv(t)
	budget := NewMemoryBudget(48 << 10)
	budget.SetMaxWait(100 * time.Millisecond)
	e.rep.SetMemoryBudget(budget)

	hold, err := budget.acquire(context.Background(), 48<<10) // a slow upload holds everything
	if err != nil {
		t.Fatal(err)
	}
	defer hold()

	start := time.Now()
	_, err = e.rep.Put(context.Background(), "k", bytes.NewReader(make([]byte, 1000)), 1000, "x", nil)
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("got %v, want ErrOverloaded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shedding took %v", d)
	}
}

// ---------------------------------------------------------------------------
// Concurrent writers of one key
// ---------------------------------------------------------------------------

// Two overlapping PUTs of the same key must not leave the primary and the backup
// holding different versions (the primary finishing last for A, the backup for B).
func TestConcurrentPutsOfOneKeyLeaveBackendsConsistent(t *testing.T) {
	e := newEnv(t)
	e.rep.SetLocker(queue.NewMemoryLocker())
	// request A is slow on the primary only, so without serialisation it lands there last
	e.primary.putDelayFor = map[string]time.Duration{"version-A": 250 * time.Millisecond}

	var wg sync.WaitGroup
	for _, v := range []string{"version-A", "version-B"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := put(e, "k", v); err != nil {
				t.Errorf("PUT %s: %v", v, err)
			}
		}()
		time.Sleep(30 * time.Millisecond) // A starts first
	}
	wg.Wait()

	p, b := e.primary.content("k"), e.backup.content("k")
	if p != b {
		t.Fatalf("backends diverged: primary=%q backup=%q", p, b)
	}
}

// DELETE racing a PUT of the same key must also be serialised: the outcome is either
// "deleted everywhere" or "stored everywhere", never a mix.
func TestPutAndDeleteOfOneKeyDoNotInterleave(t *testing.T) {
	e := newEnv(t)
	e.rep.SetLocker(queue.NewMemoryLocker())
	e.primary.putDelayFor = map[string]time.Duration{"data": 200 * time.Millisecond}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = put(e, "k", "data") }()
	go func() {
		defer wg.Done()
		time.Sleep(40 * time.Millisecond)
		_ = e.rep.Delete(context.Background(), "k")
	}()
	wg.Wait()

	if e.primary.has("k") != e.backup.has("k") {
		t.Fatalf("PUT/DELETE interleaved: primary has=%v backup has=%v", e.primary.has("k"), e.backup.has("k"))
	}
}

// A writer that cannot get the key within the wait limit must fail with a retryable error.
func TestWriterGivesUpWhenKeyStaysLocked(t *testing.T) {
	e := newEnv(t)
	locker := queue.NewMemoryLocker()
	e.rep.SetLocker(locker)

	release, ok, err := locker.TryLock(context.Background(), "put/vb/k")
	if err != nil || !ok {
		t.Fatal("setup: could not take the lock")
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = e.rep.Put(ctx, "k", bytes.NewReader([]byte("x")), 1, "x", nil)
	if err == nil {
		t.Fatal("PUT must not proceed while the key is locked by another writer")
	}
	if e.primary.has("k") {
		t.Fatal("object stored without holding the write lock")
	}
}

// ---------------------------------------------------------------------------
// Request amplification on the hot path
// ---------------------------------------------------------------------------

// A read of a healthy object is one request to one backend, not a HEAD + GET pair plus a
// probe of every backup.
func TestGetOfHealthyObjectCostsOneBackendRequest(t *testing.T) {
	e := newEnv(t)
	e.primary.set("k", "x")

	if _, err := get(e, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let any background check run
	if h, g := e.primary.headCalls.Load(), e.primary.getCalls.Load(); h != 0 || g != 1 {
		t.Fatalf("primary saw %d HEAD and %d GET; want 0 and 1", h, g)
	}
}

// Many reads of one key must not each probe the backups: the repair check is rate-limited.
func TestRepeatedReadsDoNotProbeBackupsEveryTime(t *testing.T) {
	e := newEnv(t)
	e.primary.set("hot", "x")
	e.backup.set("hot", "x")

	for i := 0; i < 200; i++ {
		if _, err := get(e, "hot"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if n := e.backup.headCalls.Load(); n > 1 {
		t.Fatalf("backup received %d HEAD probes for 200 reads of one object, want at most 1", n)
	}
}

// Even for many different keys the number of simultaneous probes is capped, so a read burst
// cannot turn into a burst of backend HEADs.
func TestBackupProbesAreConcurrencyBounded(t *testing.T) {
	e := newEnv(t)
	e.backup.headDelay = 200 * time.Millisecond
	for i := 0; i < 100; i++ {
		e.primary.set(fmt.Sprintf("k%d", i), "x")
	}
	for i := 0; i < 100; i++ {
		_, _ = get(e, fmt.Sprintf("k%d", i))
	}
	time.Sleep(50 * time.Millisecond)
	if n := e.backup.headCalls.Load(); n > maxBackupChecks {
		t.Fatalf("%d probes in flight/started within 50ms, limit is %d", n, maxBackupChecks)
	}
}

// Writing a brand-new key costs one HEAD (symlink check on the primary), not one per backend.
func TestPutOfNewKeyDoesNotProbeBackups(t *testing.T) {
	e := newEnv(t)
	if _, err := put(e, "new", "x"); err != nil {
		t.Fatal(err)
	}
	if n := e.backup.headCalls.Load(); n != 0 {
		t.Fatalf("backup received %d HEAD requests for a PUT of a new key", n)
	}
}

// A body that needs seconds to transfer must not be cut off by the short timeout that suits
// HEAD and DELETE: PUT timeouts grow with the body size.
func TestPutTimeoutGrowsWithBodySize(t *testing.T) {
	e := newEnv(t) // operation timeout is 300ms
	size := 4 << 20
	e.primary.putDelayFor = map[string]time.Duration{string(bytes.Repeat([]byte("a"), size)): 600 * time.Millisecond}

	if _, err := e.rep.Put(context.Background(), "big", bytes.NewReader(bytes.Repeat([]byte("a"), size)), int64(size), "x", nil); err != nil {
		t.Fatalf("a 4 MiB PUT taking 600ms failed although it is far below the size-based limit: %v", err)
	}
	if !e.primary.has("big") {
		t.Fatal("object not stored")
	}
}

func TestPutTimeoutStaysShortForTinyBodies(t *testing.T) {
	e := newEnv(t)
	if d := e.rep.putTimeout(1); d != e.rep.opTimeout {
		t.Fatalf("tiny PUT timeout = %v, want exactly opTimeout %v", d, e.rep.opTimeout)
	}
}

// ---------------------------------------------------------------------------
// CopyObject
// ---------------------------------------------------------------------------

func TestCopyObjectCopiesOnPrimaryAndSchedulesBackups(t *testing.T) {
	e := newEnv(t)
	e.primary.set("src", "data")
	e.backup.set("src", "data")

	if _, err := e.rep.CopyObject(context.Background(), "src", "dst", nil); err != nil {
		t.Fatal(err)
	}
	if e.primary.content("dst") != "data" {
		t.Fatal("copy not made on the primary")
	}
	if findTask(e.tasks(t), "dst", "primary", "backup") == nil {
		t.Fatal("copy was not scheduled for the backup")
	}
}

// The source may exist only on a backup (the primary lost it): the copy must still succeed.
func TestCopyObjectWorksWhenSourceExistsOnlyOnBackup(t *testing.T) {
	e := newEnv(t)
	e.backup.set("src", "only-on-backup")

	if _, err := e.rep.CopyObject(context.Background(), "src", "dst", nil); err != nil {
		t.Fatalf("copy of an object that only the backup has failed: %v", err)
	}
	if e.primary.content("dst") != "only-on-backup" || e.backup.content("dst") != "only-on-backup" {
		t.Fatalf("copy missing: primary=%q backup=%q", e.primary.content("dst"), e.backup.content("dst"))
	}
}

func TestCopyObjectOfMissingSourceIsNotFound(t *testing.T) {
	e := newEnv(t)
	_, err := e.rep.CopyObject(context.Background(), "nope", "dst", nil)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if e.primary.has("dst") || e.backup.has("dst") {
		t.Fatal("a destination was created from a missing source")
	}
}

// A server-side copy of a big object takes far longer than a HEAD: it must not be limited to
// the short operation timeout.
func TestCopyObjectIsNotCutOffByOperationTimeout(t *testing.T) {
	if maxCopyTime <= 10*time.Minute {
		t.Fatalf("maxCopyTime = %v is too short for server-side copies of large objects", maxCopyTime)
	}
}
