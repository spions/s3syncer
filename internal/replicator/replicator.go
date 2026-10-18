package replicator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/spions/s3syncer/internal/metrics"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/worker"
)

const maxSymlinkDepth = 10

// DefaultStreamThreshold is the PUT body size from which the body is streamed
// to the primary backend (bounded memory) instead of buffered for parallel
// writes to all backends. Backups of streamed objects are synced by the worker.
const DefaultStreamThreshold int64 = 32 << 20

// streamMemory is the worst-case memory held by one streaming upload
// (multipart part buffers, see storage.streamPartSize/streamConcurrency).
const streamMemory int64 = 48 << 20

// MemoryBudget caps the memory used by in-flight PUT bodies across all
// replicators, so concurrent uploads queue up instead of exhausting RAM.
type MemoryBudget struct {
	total   int64
	sem     *semaphore.Weighted
	maxWait time.Duration
}

// DefaultMemoryWait is how long an upload waits for memory before it is rejected with 503.
const DefaultMemoryWait = 30 * time.Second

func NewMemoryBudget(total int64) *MemoryBudget {
	return &MemoryBudget{total: total, sem: semaphore.NewWeighted(total), maxWait: DefaultMemoryWait}
}

// SetMaxWait changes how long acquire waits before returning ErrOverloaded.
func (m *MemoryBudget) SetMaxWait(d time.Duration) { m.maxWait = d }

// acquire blocks until n bytes are available (clamped to the total) or ctx ends.
// A nil budget is unlimited.
func (m *MemoryBudget) acquire(ctx context.Context, n int64) (release func(), err error) {
	if m == nil {
		return func() {}, nil
	}
	n = min(n, m.total)
	waitCtx, cancel := context.WithTimeout(ctx, m.maxWait)
	defer cancel()
	if err := m.sem.Acquire(waitCtx, n); err != nil {
		if ctx.Err() == nil {
			// our own wait limit expired while the client is still there: shed load
			metrics.OverloadRejections.WithLabelValues("upload_memory").Inc()
			return nil, fmt.Errorf("%w: no upload memory available after %v", ErrOverloaded, m.maxWait)
		}
		return nil, fmt.Errorf("waiting for upload memory: %w", err)
	}
	metrics.UploadMemoryInUse.Add(float64(n))
	return func() {
		m.sem.Release(n)
		metrics.UploadMemoryInUse.Add(-float64(n))
	}, nil
}

type Replicator struct {
	memBudget     *MemoryBudget
	virtualBucket string
	primary       storage.StorageBackend
	backups       []storage.StorageBackend
	queue         queue.Store
	locker        queue.KeyLocker // serialises writers of one key; nil = no serialisation
	checkSem      chan struct{}   // bounds concurrent backup presence checks
	checked       checkedKeys     // keys checked recently
	worker        *worker.Worker
	log           *slog.Logger
	opTimeout     time.Duration
	// streamThreshold: bodies of at least this size (or of unknown size) are streamed.
	streamThreshold int64
}

func New(virtualBucket string, primary storage.StorageBackend, backups []storage.StorageBackend, q queue.Store, log *slog.Logger, opTimeout time.Duration) *Replicator {
	return &Replicator{
		virtualBucket: virtualBucket,
		primary:       primary,
		backups:       backups,
		queue:         q,
		log:           log,
		opTimeout:     opTimeout,

		streamThreshold: DefaultStreamThreshold,
		checkSem:        make(chan struct{}, maxBackupChecks),
		checked:         checkedKeys{m: map[string]time.Time{}},
	}
}

// SetMemoryBudget limits memory used by concurrent PUT bodies (nil = unlimited).
// SetLocker makes concurrent writes (PUT, DELETE, COPY) of one key run one after another,
// across goroutines and, with a shared locker, across instances. Without it two overlapping
// PUTs of a key can leave the primary and a backup holding different versions.
func (r *Replicator) SetLocker(l queue.KeyLocker) { r.locker = l }

func (r *Replicator) SetMemoryBudget(m *MemoryBudget) { r.memBudget = m }

// SetStreamThreshold sets the PUT body size from which the body is streamed to
// the primary only (<= 0 restores the default).
func (r *Replicator) SetStreamThreshold(n int64) {
	if n <= 0 {
		n = DefaultStreamThreshold
	}
	r.streamThreshold = n
}

// exactReader fails with io.ErrUnexpectedEOF if the body ends before size
// bytes, so an uploader aborts instead of committing a truncated object.
type exactReader struct {
	r    io.Reader
	left int64
}

func (e *exactReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	e.left -= int64(n)
	if err == io.EOF && e.left != 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// putStream sends a large body to the primary only; backups are synced by the worker.
func (r *Replicator) putStream(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error) {
	start := time.Now()
	release, err := r.memBudget.acquire(ctx, streamMemory)
	if err != nil {
		return "", err
	}
	defer release()
	if size >= 0 {
		body = &exactReader{r: body, left: size}
	}
	// no operation timeout: bounded by the request context only
	etag, err := r.primary.PutStream(ctx, key, body, contentType, metadata)
	if err != nil {
		return "", fmt.Errorf("primary stream put %s: %w", key, err)
	}
	metrics.RequestDuration.WithLabelValues(r.primary.Name(), "put").Observe(time.Since(start).Seconds())
	for _, b := range r.backups {
		if err := r.enqueue(key, r.primary.Name(), b.Name(), queue.PriorityHigh); err != nil {
			return "", err
		}
	}
	return etag, nil
}

// Put writes to all backends in parallel for maximum speed.
// Returns success if at least one backend succeeds.
// Enqueues HIGH priority sync for any failed backends.
func (r *Replicator) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error) {
	start := time.Now()

	writeKey := key
	if !isSymlinkCreate(metadata) {
		resolved, err := r.resolveSymlinkVia(ctx, key, false)
		if err == nil && resolved != key {
			r.log.Info("symlink resolved for PUT", "key", key, "target", resolved)
			writeKey = resolved
		}
	} else {
		contentType = "binary/octet-stream"
	}

	unlock, err := r.lockWrite(ctx, writeKey)
	if err != nil {
		return "", err
	}
	defer unlock()

	return r.putLocked(ctx, start, writeKey, body, size, contentType, metadata)
}

// putLocked stores the object; the caller holds the write lock of key.
func (r *Replicator) putLocked(ctx context.Context, start time.Time, writeKey string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error) {
	if size < 0 || size >= r.streamThreshold {
		return r.putStream(ctx, writeKey, body, size, contentType, metadata)
	}

	release, err := r.memBudget.acquire(ctx, size)
	if err != nil {
		return "", err
	}
	defer release()

	// Buffer body fully so we can send to multiple backends in parallel
	// Read at most size+1 bytes: the budget was reserved for `size`, and a client must not
	// be able to make us buffer more than it announced.
	bodyBytes, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return "", fmt.Errorf("read request body: %w", err)
	}
	if int64(len(bodyBytes)) != size {
		return "", fmt.Errorf("incomplete request body: got %d bytes, expected %d", len(bodyBytes), size)
	}

	// Write to all backends in parallel
	allBackends := append([]storage.StorageBackend{r.primary}, r.backups...)

	type putResult struct {
		backend storage.StorageBackend
		etag    string
		err     error
	}

	results := make([]putResult, len(allBackends))
	var wg sync.WaitGroup

	for i, backend := range allBackends {
		wg.Add(1)
		go func(idx int, b storage.StorageBackend) {
			defer wg.Done()
			pCtx, cancel := context.WithTimeout(ctx, r.putTimeout(size))
			defer cancel()
			etag, err := b.Put(pCtx, writeKey, bytes.NewReader(bodyBytes), size, contentType, metadata)
			results[idx] = putResult{backend: b, etag: etag, err: err}
		}(i, backend)
	}
	wg.Wait()

	metrics.RequestDuration.WithLabelValues(r.primary.Name(), "put").Observe(time.Since(start).Seconds())

	var successEtag string
	var successBackend storage.StorageBackend
	var failedBackends []storage.StorageBackend

	for _, res := range results {
		if res.err == nil {
			if successBackend == nil {
				successEtag = res.etag
				successBackend = res.backend
			}
		} else {
			metrics.BackendErrors.WithLabelValues(res.backend.Name(), "put").Inc()
			r.log.Error("backend put failed", "backend", res.backend.Name(), "key", writeKey, "error", res.err)
			failedBackends = append(failedBackends, res.backend)
		}
	}

	if successBackend == nil {
		return "", fmt.Errorf("all backends failed for PUT %s", writeKey)
	}

	for _, failed := range failedBackends {
		if err := r.enqueue(writeKey, successBackend.Name(), failed.Name(), queue.PriorityHigh); err != nil {
			return "", err
		}
	}

	// Verification task for the copies that succeeded: the worker compares size/ETag and
	// repairs a divergence left by concurrent writers. Best effort, nothing is lost if it fails.
	for _, res := range results {
		if res.err == nil && res.backend.Name() != successBackend.Name() {
			_ = r.enqueue(writeKey, successBackend.Name(), res.backend.Name(), queue.PriorityHigh)
		}
	}

	return successEtag, nil
}

// Get reads from the primary first and falls back to the backups. The common case costs one
// request to one backend: the object is fetched directly, not probed with HEAD first. Only
// symlinks (rare) take the slower path that follows the chain.
func (r *Replicator) Get(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return r.GetRange(ctx, key, "")
}

// GetRange is Get for a byte range (empty = whole object). A range the object cannot satisfy
// returns storage.ErrInvalidRange straight away: every copy would answer the same.
func (r *Replicator) GetRange(ctx context.Context, key, rng string) (io.ReadCloser, *storage.ObjectInfo, error) {
	var lastErr error
	for i, b := range append([]storage.StorageBackend{r.primary}, r.backups...) {
		if i > 0 && r.pendingDelete(key, b) {
			continue // deleted by the client; this copy only waits for its delete to succeed
		}
		body, info, err := r.getBounded(ctx, b, key, rng)
		if err != nil {
			if errors.Is(err, storage.ErrInvalidRange) {
				return nil, nil, err
			}
			if !errors.Is(err, storage.ErrNotFound) {
				r.log.Warn("GET from backend failed, trying the next one", "key", key, "backend", b.Name(), "error", err)
				lastErr = err
			}
			continue
		}
		if info.IsSymlink() {
			_ = body.Close()
			return r.getResolving(ctx, key)
		}
		if b == r.primary {
			r.scheduleBackupCheck(key)
		} else {
			_ = r.enqueue(key, b.Name(), r.primary.Name(), queue.PriorityMed) // restore the primary
		}
		return body, info, nil
	}
	if lastErr != nil {
		// not "NoSuchKey": the object may exist but cannot be served right now
		return nil, nil, fmt.Errorf("get %s: %w", key, lastErr)
	}
	return nil, nil, storage.ErrNotFound
}

// cancelOnClose releases the request context when the response body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// getBounded fetches an object, giving the backend opTimeout to START answering (connect and
// response headers). Once the response is there the body streams without a time limit.
func (r *Replicator) getBounded(ctx context.Context, b storage.StorageBackend, key, rng string) (io.ReadCloser, *storage.ObjectInfo, error) {
	gctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(r.opTimeout, cancel)
	start := time.Now()
	body, info, err := b.GetRange(gctx, key, rng)
	metrics.RequestDuration.WithLabelValues(b.Name(), "get").Observe(time.Since(start).Seconds())
	if !timer.Stop() { // the deadline fired first: whatever came back is unusable
		if err == nil {
			_ = body.Close()
		}
		cancel()
		return nil, nil, fmt.Errorf("backend %s did not answer within %v", b.Name(), r.opTimeout)
	}
	if err != nil {
		cancel()
		if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrInvalidRange) {
			metrics.BackendErrors.WithLabelValues(b.Name(), "get").Inc()
		}
		return nil, nil, err
	}
	return cancelOnClose{ReadCloser: body, cancel: cancel}, info, nil
}

// getResolving is the slow GET path used for symlinks: probe with HEAD, follow the chain to
// its target, then fetch the target.
func (r *Replicator) getResolving(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	headWithTimeout := func(backend storage.StorageBackend, k string) (*storage.ObjectInfo, error) {
		hCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
		defer cancel()
		return backend.Head(hCtx, k)
	}

	// Check which backend has the object (with timeout on each HEAD)
	var foundBackend storage.StorageBackend
	info, err := headWithTimeout(r.primary, key)
	if err == nil {
		foundBackend = r.primary
	} else {
		if !errors.Is(err, storage.ErrNotFound) {
			r.log.Warn("primary HEAD failed/timeout", "key", key, "error", err)
		}
		for _, backup := range r.backups {
			if r.pendingDelete(key, backup) {
				continue // deleted by the client; the backup copy is only waiting for its delete to succeed
			}
			info, err = headWithTimeout(backup, key)
			if err == nil {
				foundBackend = backup
				break
			}
		}
	}

	if foundBackend == nil {
		return nil, nil, storage.ErrNotFound
	}

	// Resolve symlink if needed
	resolved := key
	isSymlink := info.IsSymlink()
	if isSymlink {
		var resolveErr error
		resolved, resolveErr = r.resolveSymlink(ctx, key)
		if resolveErr != nil && !errors.Is(resolveErr, storage.ErrNotFound) {
			r.log.Warn("symlink resolve error", "key", key, "error", resolveErr)
		}
		if resolveErr == nil && resolved != key {
			r.log.Debug("symlink resolved for GET", "key", key, "target", resolved)
		} else {
			resolved = key
			isSymlink = false
		}
	}

	// GET from the backend that had it (no timeout on GET — body streams to client)
	start := time.Now()
	body, targetInfo, err := foundBackend.Get(ctx, resolved)
	metrics.RequestDuration.WithLabelValues(foundBackend.Name(), "get").Observe(time.Since(start).Seconds())

	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		// HEAD succeeded but the body could not be served (5xx, reset): try the other copies
		r.log.Warn("GET failed after successful HEAD, trying other backends", "key", resolved, "backend", foundBackend.Name(), "error", err)
		for _, b := range append([]storage.StorageBackend{r.primary}, r.backups...) {
			if b == foundBackend || r.pendingDelete(resolved, b) {
				continue
			}
			if altBody, altInfo, altErr := b.Get(ctx, resolved); altErr == nil {
				body, targetInfo, err, foundBackend = altBody, altInfo, nil, b
				break
			}
		}
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil, storage.ErrNotFound
		}
		// not "NoSuchKey": the object exists but cannot be served right now
		return nil, nil, fmt.Errorf("get %s from %s: %w", resolved, foundBackend.Name(), err)
	}

	// Enqueue sync based on which backend served the file
	if foundBackend == r.primary {
		// Primary found — LOW sync to backups
		r.scheduleBackupCheck(resolved)
		if isSymlink && resolved != key {
			r.scheduleBackupCheck(key)
		}
	} else {
		// Backup found — MED sync to primary
		_ = r.enqueue(resolved, foundBackend.Name(), r.primary.Name(), queue.PriorityMed)
		if isSymlink && resolved != key {
			_ = r.enqueue(key, foundBackend.Name(), r.primary.Name(), queue.PriorityMed)
		}
	}

	return body, targetInfo, nil
}

// Head returns info for the object.
func (r *Replicator) Head(ctx context.Context, key string) (*storage.ObjectInfo, error) {
	headWithTimeout := func(backend storage.StorageBackend) (*storage.ObjectInfo, error) {
		hCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
		defer cancel()
		return backend.Head(hCtx, key)
	}

	info, err := headWithTimeout(r.primary)
	if err == nil {
		r.scheduleBackupCheck(key)
		return info, nil
	}

	if !errors.Is(err, storage.ErrNotFound) {
		r.log.Warn("primary head failed, trying backups", "key", key, "error", err)
	}
	for _, backup := range r.backups {
		if r.pendingDelete(key, backup) {
			continue
		}
		info, err = headWithTimeout(backup)
		if err == nil {
			_ = r.enqueue(key, backup.Name(), r.primary.Name(), queue.PriorityMed)
			return info, nil
		}
	}
	return nil, storage.ErrNotFound
}

// Delete removes the object from all backends.
// Returns success if at least one backend succeeds.
// Enqueues delete tasks for failed backends.
func (r *Replicator) Delete(ctx context.Context, key string) error {
	unlock, err := r.lockWrite(ctx, key)
	if err != nil {
		return err
	}
	defer unlock()

	allBackends := append([]storage.StorageBackend{r.primary}, r.backups...)

	type deleteResult struct {
		backend storage.StorageBackend
		err     error
	}

	results := make([]deleteResult, len(allBackends))
	var wg sync.WaitGroup

	for i, backend := range allBackends {
		wg.Add(1)
		go func(idx int, b storage.StorageBackend) {
			defer wg.Done()
			delCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
			defer cancel()
			err := b.Delete(delCtx, key)
			results[idx] = deleteResult{backend: b, err: err}
		}(i, backend)
	}
	wg.Wait()

	var successCount int
	var lastErr, queueErr error

	for _, res := range results {
		if res.err == nil {
			successCount++
		} else {
			metrics.BackendErrors.WithLabelValues(res.backend.Name(), "delete").Inc()
			r.log.Warn("backend delete failed", "backend", res.backend.Name(), "key", key, "error", res.err)
			lastErr = res.err
			// Enqueue delete task with special marker "DELETE:" in source
			if err := r.enqueue(key, queue.DeletePrefix+res.backend.Name(), res.backend.Name(), queue.PriorityHigh); err != nil {
				queueErr = err
			}
		}
	}
	if queueErr != nil && successCount > 0 {
		// Some backend still has the object and the retry could not be recorded: it would come back.
		return queueErr
	}

	if successCount == 0 {
		return fmt.Errorf("all backends failed for DELETE %s: %w", key, lastErr)
	}

	return nil
}

// maxCopyTime bounds a server-side copy. It scales with object size, so it is far above
// opTimeout (which would cut off any copy of a big object); the client's context still applies.
const maxCopyTime = 30 * time.Minute

// CopyObject copies srcKey to dstKey. The common case is a server-side copy on the primary.
// If the primary does not have the source (but a backup does), the object is read through
// Get and written with Put, which also fills every backend.
func (r *Replicator) CopyObject(ctx context.Context, srcKey, dstKey string, metadata map[string]string) (string, error) {
	unlock, err := r.lockWrite(ctx, dstKey)
	if err != nil {
		return "", err
	}
	defer unlock()

	copyCtx, cancel := context.WithTimeout(ctx, maxCopyTime)
	defer cancel()

	etag, err := r.primary.CopyObject(copyCtx, srcKey, dstKey, metadata)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || storage.IsNotFound(err) {
			return r.copyViaGet(ctx, srcKey, dstKey, metadata)
		}
		metrics.BackendErrors.WithLabelValues(r.primary.Name(), "copy").Inc()
		return "", fmt.Errorf("primary copy failed: %w", err)
	}

	// Enqueue replication for backups
	for _, backup := range r.backups {
		if err := r.enqueue(dstKey, r.primary.Name(), backup.Name(), queue.PriorityHigh); err != nil {
			return "", err
		}
	}

	return etag, nil
}

// copyViaGet handles a copy whose source is missing on the primary: read it from wherever it
// is and write it as a new object. The caller holds the write lock of dstKey.
func (r *Replicator) copyViaGet(ctx context.Context, srcKey, dstKey string, metadata map[string]string) (string, error) {
	body, info, err := r.Get(ctx, srcKey)
	if err != nil {
		return "", err // ErrNotFound if it exists nowhere
	}
	defer body.Close()

	md := info.Metadata
	if len(metadata) > 0 {
		md = metadata // the caller asked to replace the metadata
	}
	if !info.IsSymlink() {
		md = withoutSymlinkMetadata(md)
	}
	return r.putLocked(ctx, time.Now(), dstKey, body, info.Size, info.ContentType, md)
}

// withoutSymlinkMetadata drops symlink markers so a copy of a regular file never becomes a symlink.
func withoutSymlinkMetadata(md map[string]string) map[string]string {
	if md == nil {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if k == storage.SymlinkMetaKey || k == "symlink-target" {
			continue
		}
		out[k] = v
	}
	return out
}

func (r *Replicator) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	listCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
	defer cancel()

	objects, err := r.primary.ListObjects(listCtx, prefix)
	if err == nil {
		return objects, nil
	}

	r.log.Warn("primary list failed, trying backups", "prefix", prefix, "error", err)
	for _, backup := range r.backups {
		bCtx, bCancel := context.WithTimeout(ctx, r.opTimeout)
		objects, err = backup.ListObjects(bCtx, prefix)
		bCancel()
		if err == nil {
			return objects, nil
		}
	}

	return nil, err
}

func (r *Replicator) ListObjectsHierarchical(ctx context.Context, prefix, delimiter string) ([]storage.ObjectInfo, error) {
	listCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
	defer cancel()

	objects, err := r.primary.ListObjectsHierarchical(listCtx, prefix, delimiter)
	if err == nil {
		return objects, nil
	}

	r.log.Warn("primary hierarchical list failed, trying backups", "prefix", prefix, "delimiter", delimiter, "error", err)
	for _, backup := range r.backups {
		bCtx, bCancel := context.WithTimeout(ctx, r.opTimeout)
		objects, err = backup.ListObjectsHierarchical(bCtx, prefix, delimiter)
		bCancel()
		if err == nil {
			return objects, nil
		}
	}

	return nil, err
}

func (r *Replicator) ListObjectsV2(ctx context.Context, prefix, delimiter, continuationToken, startAfter string, maxKeys int32) (*storage.ListObjectsV2Result, error) {
	listCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
	defer cancel()

	result, err := r.primary.ListObjectsV2(listCtx, prefix, delimiter, continuationToken, startAfter, maxKeys)
	if err == nil {
		return result, nil
	}

	r.log.Warn("primary list v2 failed, trying backups", "prefix", prefix, "delimiter", delimiter, "error", err)
	for _, backup := range r.backups {
		bCtx, bCancel := context.WithTimeout(ctx, r.opTimeout)
		result, err = backup.ListObjectsV2(bCtx, prefix, delimiter, continuationToken, startAfter, maxKeys)
		bCancel()
		if err == nil {
			return result, nil
		}
	}

	return nil, err
}

func (r *Replicator) resolveSymlink(ctx context.Context, key string) (string, error) {
	return r.resolveSymlinkVia(ctx, key, true)
}

// resolveSymlinkVia follows a symlink chain. With probeBackups=false a definite "not found"
// from the primary ends the lookup without asking the backups: for a PUT of a new key that
// saves a HEAD per backup on every upload.
func (r *Replicator) resolveSymlinkVia(ctx context.Context, key string, probeBackups bool) (string, error) {
	current := key
	seen := make(map[string]bool)

	headWithTimeout := func(backend storage.StorageBackend, k string) (*storage.ObjectInfo, error) {
		hCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
		defer cancel()
		return backend.Head(hCtx, k)
	}

	for i := 0; i < maxSymlinkDepth; i++ {
		if seen[current] {
			return "", fmt.Errorf("symlink loop detected at %s", current)
		}
		seen[current] = true

		info, err := headWithTimeout(r.primary, current)
		if err != nil && !probeBackups && errors.Is(err, storage.ErrNotFound) {
			return current, err
		}
		if err != nil {
			for _, backup := range r.backups {
				info, err = headWithTimeout(backup, current)
				if err == nil {
					break
				}
			}
			if err != nil {
				return current, err
			}
		}

		target := info.SymlinkTarget()
		if target == "" {
			return current, nil
		}

		r.log.Debug("following symlink", "from", current, "to", target)
		current = target
	}

	return "", fmt.Errorf("symlink depth exceeded at %s (max %d)", key, maxSymlinkDepth)
}

// minUploadRate is the slowest upload speed a backend is expected to sustain; it stretches the
// PUT timeout for bigger bodies so a 30 MiB upload is not cut off by a limit meant for HEAD.
const minUploadRate = 1 << 20 // bytes per second

// putTimeout is opTimeout plus the time size bytes need at minUploadRate.
func (r *Replicator) putTimeout(size int64) time.Duration {
	if size <= 0 {
		return r.opTimeout
	}
	return r.opTimeout + time.Duration(size/minUploadRate)*time.Second
}

// Backup presence checks run behind reads. Without limits every GET would add a HEAD per
// backup, doubling backend traffic. A key is checked at most once per backupCheckTTL and at
// most maxBackupChecks run at a time; when saturated the check is simply skipped (a later
// read of the object will trigger it), so reads never queue up behind repairs.
const (
	backupCheckTTL  = 10 * time.Minute
	maxBackupChecks = 8
	maxCheckedKeys  = 100_000
)

type checkedKeys struct {
	mu sync.Mutex
	m  map[string]time.Time
}

// due reports whether key should be checked now and records it.
func (c *checkedKeys) due(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.m[key]; ok && now.Sub(t) < backupCheckTTL {
		return false
	}
	if len(c.m) >= maxCheckedKeys {
		c.m = make(map[string]time.Time) // cheap bounded eviction: forget everything
	}
	c.m[key] = now
	return true
}

func (r *Replicator) scheduleBackupCheck(key string) {
	if len(r.backups) == 0 {
		return
	}
	select {
	case r.checkSem <- struct{}{}:
	default:
		return // too many checks in flight
	}
	if !r.checked.due(key, time.Now()) {
		<-r.checkSem
		return
	}
	go func() {
		defer func() { <-r.checkSem }()
		r.checkBackupsExist(key)
	}()
}

func (r *Replicator) checkBackupsExist(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, backup := range r.backups {
		_, err := backup.Head(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			_ = r.enqueue(key, r.primary.Name(), backup.Name(), queue.PriorityLow)
		}
	}
}

// pendingDelete reports whether the object was deleted but the delete has not yet reached
// this backend. Serving or "restoring" such a copy would resurrect a deleted object.
// If the queue cannot be asked, the copy is served (availability over strictness).
func (r *Replicator) pendingDelete(key string, b storage.StorageBackend) bool {
	ok, err := r.queue.HasTask(r.virtualBucket, key, queue.DeletePrefix+b.Name(), b.Name())
	if err != nil {
		r.log.Warn("cannot check pending deletes", "key", key, "backend", b.Name(), "error", err)
		return false
	}
	return ok
}

func (r *Replicator) SetWorker(w *worker.Worker) {
	r.worker = w
}

// enqueue records a sync task. Callers that must not lose the task (writes, deletes)
// return the error (wrapped in ErrQueueUnavailable); best-effort callers ignore it.
func (r *Replicator) enqueue(key, source, target string, priority int) error {
	if err := r.queue.Enqueue(r.virtualBucket, key, source, target, priority); err != nil {
		r.log.Error("enqueue failed", "key", key, "source", source, "target", target, "error", err)
		return fmt.Errorf("%w: %v", ErrQueueUnavailable, err)
	}
	if r.worker != nil {
		r.worker.Notify()
	}
	return nil
}

func isSymlinkCreate(metadata map[string]string) bool {
	if metadata == nil {
		return false
	}
	_, ok := metadata[storage.SymlinkMetaKey]
	return ok
}

// ErrOverloaded means the request waited too long for upload memory (see MemoryBudget).
var ErrOverloaded = errors.New("server is overloaded")

// ErrObjectBusy means another write to the same key did not finish within writeLockWait.
var ErrObjectBusy = errors.New("object is being written by another request")

// writeLockWait bounds how long a write waits for a concurrent write of the same key.
const writeLockWait = 30 * time.Second

// ErrQueueUnavailable means a sync task could not be recorded (queue backend down).
// The write itself may already be stored on some backends; the caller should fail the
// request so the client retries, instead of acknowledging a write whose replica is lost.
var ErrQueueUnavailable = errors.New("sync queue unavailable")

// lockWrite serialises writers of one key. Writers use their own lock namespace, so they
// never wait for a background copy of the same object (that conflict is resolved by the
// "dirty" re-enqueue of the sync task instead). The returned func releases the lock.
func (r *Replicator) lockWrite(ctx context.Context, key string) (func(), error) {
	if r.locker == nil {
		return func() {}, nil
	}
	lockKey := "put/" + r.virtualBucket + "/" + key
	deadline := time.Now().Add(writeLockWait)
	for {
		release, ok, err := r.locker.TryLock(ctx, lockKey)
		if err != nil {
			return nil, fmt.Errorf("%w: write lock: %v", ErrQueueUnavailable, err)
		}
		if ok {
			return release, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrObjectBusy
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}
}
