package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/replicator"
	"github.com/spions/s3syncer/internal/storage"
)

// fakeBackend is a minimal in-memory backend for handler-level tests.
type fakeBackend struct {
	storage.StorageBackend
	name string
	mu   sync.Mutex
	objs map[string][]byte
	err  error // returned by every write
}

func (f *fakeBackend) Name() string { return f.name }

func (f *fakeBackend) Put(_ context.Context, key string, body io.Reader, _ int64, _ string, _ map[string]string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = data
	return `"etag"`, nil
}

func (f *fakeBackend) PutStream(ctx context.Context, key string, body io.Reader, ct string, md map[string]string) (string, error) {
	return f.Put(ctx, key, body, -1, ct, md)
}

var fakeModTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// GetRange serves "bytes=a-b" and "bytes=a-" from the stored object, like a real backend.
func (f *fakeBackend) GetRange(_ context.Context, key, rng string) (io.ReadCloser, *storage.ObjectInfo, error) {
	f.mu.Lock()
	data, ok := f.objs[key]
	f.mu.Unlock()
	if !ok {
		return nil, nil, storage.ErrNotFound
	}
	info := &storage.ObjectInfo{Key: key, Size: int64(len(data)), ETag: `"etag-1"`, LastModified: fakeModTime, ContentType: "text/plain"}
	if rng == "" {
		return io.NopCloser(bytes.NewReader(data)), info, nil
	}
	var from, to int
	if n, _ := fmt.Sscanf(rng, "bytes=%d-%d", &from, &to); n == 1 {
		to = len(data) - 1
	} else if n != 2 {
		return nil, nil, storage.ErrInvalidRange
	}
	if from >= len(data) || from > to {
		return nil, nil, storage.ErrInvalidRange
	}
	to = min(to, len(data)-1)
	info.Size = int64(to - from + 1)
	info.ContentRange = fmt.Sprintf("bytes %d-%d/%d", from, to, len(data))
	return io.NopCloser(bytes.NewReader(data[from : to+1])), info, nil
}

func (f *fakeBackend) Get(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return f.GetRange(ctx, key, "")
}

func (f *fakeBackend) Head(_ context.Context, key string) (*storage.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.objs[key]; ok {
		return &storage.ObjectInfo{Key: key, Size: int64(len(d)), ETag: `"etag-1"`, LastModified: fakeModTime}, nil
	}
	return nil, storage.ErrNotFound
}

type downQueue struct{ queue.Store }

func (downQueue) Enqueue(string, string, string, string, int) error { return errors.New("redis down") }

func newTestHandler(t *testing.T, q func(queue.Store) queue.Store) (*Handler, *fakeBackend, *fakeBackend) {
	t.Helper()
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	primary := &fakeBackend{name: "secret-primary-host", objs: map[string][]byte{}}
	backup := &fakeBackend{name: "secret-backup-host", objs: map[string][]byte{}}
	store := queue.Store(queue.NewSQLite(db))
	if q != nil {
		store = q(store)
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	rep := replicator.New("vb", primary, []storage.StorageBackend{backup}, store, log, time.Second)
	return NewHandler(map[string]*replicator.Replicator{"vb": rep}, log), primary, backup
}

func doPut(h http.Handler, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPutStoresObjectOnBothBackends(t *testing.T) {
	h, primary, backup := newTestHandler(t, nil)

	rec := doPut(h, "/vb/dir/file.txt", "hello")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if string(primary.objs["dir/file.txt"]) != "hello" || string(backup.objs["dir/file.txt"]) != "hello" {
		t.Fatal("object not stored on both backends")
	}
	if rec.Header().Get("ETag") == "" {
		t.Fatal("PUT response has no ETag")
	}
}

// When the missing replica cannot even be queued the client must get a retryable 503,
// not a 200 for a write that exists on one backend only.
func TestPutAnswers503WhenReplicationCannotBeQueued(t *testing.T) {
	h, _, backup := newTestHandler(t, func(s queue.Store) queue.Store { return downQueue{s} })
	backup.err = errors.New("backup down")

	rec := doPut(h, "/vb/k", "hello")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("503 must carry Retry-After so clients back off")
	}
	if !strings.Contains(rec.Body.String(), "<Code>ServiceUnavailable</Code>") {
		t.Fatalf("not an S3 error document: %s", rec.Body)
	}
}

// Internal details (backend names, endpoints, driver messages) must not reach clients.
func TestPutErrorDoesNotLeakInternals(t *testing.T) {
	h, primary, backup := newTestHandler(t, nil)
	primary.err = errors.New("dial tcp 10.1.2.3:9000: connect: refused (secret-primary-host)")
	backup.err = errors.New("dial tcp 10.1.2.4:9000: connect: refused (secret-backup-host)")

	rec := doPut(h, "/vb/k", "hello")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	for _, leak := range []string{"secret-", "10.1.2", "dial tcp", "refused"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Fatalf("error response leaks %q: %s", leak, rec.Body)
		}
	}
}

// A client that stops sending its body must be cut off by the idle timeout instead of
// holding the connection (and the upload memory reserved for it) forever.
func TestStalledUploadIsAbortedByIdleTimeout(t *testing.T) {
	h, primary, _ := newTestHandler(t, nil)
	h.SetBodyIdleTimeout(300 * time.Millisecond)

	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// announce 1000 bytes, send 10, then go silent
	fmt.Fprintf(conn, "PUT /vb/stalled HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n0123456789")

	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 512)
	n, _ := conn.Read(buf)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("server held the stalled upload for %v", elapsed)
	}
	if n == 0 {
		t.Fatal("server closed the connection without answering")
	}
	if bytes.HasPrefix(buf[:n], []byte("HTTP/1.1 200")) {
		t.Fatal("a truncated upload was acknowledged")
	}
	if _, stored := primary.objs["stalled"]; stored {
		t.Fatal("a truncated object was stored")
	}
}
