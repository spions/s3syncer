package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spions/s3syncer/internal/config"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/taskmanager"
	"github.com/spions/s3syncer/internal/taskstate"
)

type hangingBackend struct {
	storage.StorageBackend
	name string
}

func (b *hangingBackend) Name() string { return b.name }

func (b *hangingBackend) ListObjects(ctx context.Context, _ string) ([]storage.ObjectInfo, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *hangingBackend) ListObjectsPaginatedFull(ctx context.Context, _ string, _ int, _ func([]storage.ObjectInfo) error) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	state, err := taskstate.NewSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{VirtualBuckets: []config.VirtualBucket{{
		Name: "vb", PrimaryBackendName: "a",
		Backends: []config.Backend{{Name: "a"}},
	}}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	tm := taskmanager.New(state, "test", queue.NewSQLite(db),
		map[string]storage.StorageBackend{"a": &hangingBackend{name: "a"}}, cfg, log, nil)
	return NewHandler(tm, log)
}

func post(h http.HandlerFunc, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, target, nil))
	return rec
}

func TestStartResyncTwiceReturns409WithRunningTaskID(t *testing.T) {
	h := newTestHandler(t)

	first := post(h.StartResync, "/api/tasks/resync?bucket=vb")
	if first.Code != http.StatusOK {
		t.Fatalf("first start: %d %s", first.Code, first.Body)
	}
	var started map[string]string
	if err := json.Unmarshal(first.Body.Bytes(), &started); err != nil || started["task_id"] == "" {
		t.Fatalf("bad body %q: %v", first.Body, err)
	}

	second := post(h.StartResync, "/api/tasks/resync?bucket=vb")
	if second.Code != http.StatusConflict {
		t.Fatalf("duplicate start: status %d, want 409; body %s", second.Code, second.Body)
	}
	var conflict map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &conflict); err != nil {
		t.Fatalf("409 body is not JSON: %q", second.Body)
	}
	if conflict["task_id"] != started["task_id"] || conflict["error"] == "" {
		t.Fatalf("409 body %v must name the running task %s", conflict, started["task_id"])
	}

	// bisync of the same bucket is a different task and must not be blocked
	if rec := post(h.StartBisync, "/api/tasks/bisync?bucket=vb"); rec.Code != http.StatusOK {
		t.Fatalf("bisync next to resync: %d %s", rec.Code, rec.Body)
	}

	// once cancelled, a new resync is accepted again
	del := httptest.NewRecorder()
	h.CancelTask(del, httptest.NewRequest(http.MethodDelete, "/api/tasks/"+started["task_id"], nil))
	if del.Code != http.StatusNoContent {
		t.Fatalf("cancel: %d", del.Code)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if rec := post(h.StartResync, "/api/tasks/resync?bucket=vb"); rec.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resync not accepted after the previous one was cancelled")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
