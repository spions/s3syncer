package taskmanager

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/spions/s3syncer/internal/config"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/taskstate"
)

// listBackend is a backend whose listing is a sorted slice, served page by page like S3.
type listBackend struct {
	storage.StorageBackend
	name string
	objs []storage.ObjectInfo
}

func newListBackend(name string, keys ...string) *listBackend {
	sort.Strings(keys)
	b := &listBackend{name: name}
	for _, k := range keys {
		b.objs = append(b.objs, storage.ObjectInfo{Key: k, Size: int64(len(k))})
	}
	return b
}

func (b *listBackend) Name() string { return b.name }

func (b *listBackend) ListObjectsPaginatedFull(ctx context.Context, _ string, pageSize int, fn func([]storage.ObjectInfo) error) (int64, error) {
	var n int64
	for i := 0; i < len(b.objs); i += pageSize {
		end := min(i+pageSize, len(b.objs))
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if err := fn(b.objs[i:end]); err != nil {
			return n, err
		}
		n += int64(end - i)
	}
	return n, nil
}

func bisyncManager(t *testing.T, primary, backup *listBackend) (*TaskManager, queue.Store) {
	t.Helper()
	db, err := queue.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	q := queue.NewSQLite(db)
	state, err := taskstate.NewSQLite(db)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{VirtualBuckets: []config.VirtualBucket{{
		Name: "vb", PrimaryBackendName: "primary",
		Backends: []config.Backend{{Name: "primary"}, {Name: "backup"}},
	}}}
	backends := map[string]storage.StorageBackend{"primary": primary, "backup": backup}
	tm := New(state, "test", q, backends, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	tm.watchInterval = 20 * time.Millisecond
	return tm, q
}

func drain(t *testing.T, q queue.Store) map[string]string {
	t.Helper()
	got := map[string]string{}
	for {
		tasks, err := q.Dequeue(1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) == 0 {
			return got
		}
		for _, task := range tasks {
			got[task.ObjectKey] = task.SourceBackendName + "->" + task.TargetBackendName
			_ = q.MarkDone(task.ID)
		}
	}
}

// bisync must queue exactly the differences: what only one side has goes to the other.
func TestBisyncQueuesExactlyTheDifferences(t *testing.T) {
	primary := newListBackend("primary", "a", "b", "only-primary", "z")
	backup := newListBackend("backup", "a", "b", "only-backup", "z")
	tm, q := bisyncManager(t, primary, backup)

	id, err := tm.StartBisync("vb")
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, tm, id, taskstate.StatusCompleted)

	got := drain(t, q)
	want := map[string]string{"only-primary": "primary->backup", "only-backup": "backup->primary"}
	if len(got) != len(want) {
		t.Fatalf("queued %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: queued %q, want %q", k, got[k], v)
		}
	}
}

// Many pages on both sides, offsets between them: the walk must not skip or duplicate keys.
func TestBisyncAcrossManyPages(t *testing.T) {
	var pk, bk []string
	want := map[string]string{}
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key-%05d", i)
		switch {
		case i%7 == 0:
			pk = append(pk, key)
			want[key] = "primary->backup"
		case i%11 == 0:
			bk = append(bk, key)
			want[key] = "backup->primary"
		default:
			pk, bk = append(pk, key), append(bk, key)
		}
	}
	tm, q := bisyncManager(t, newListBackend("primary", pk...), newListBackend("backup", bk...))

	id, _ := tm.StartBisync("vb")
	// the queue is drained concurrently, otherwise the chunk wait would hold the task back
	got := map[string]string{}
	deadline := time.After(20 * time.Second)
loop:
	for {
		select {
		case <-deadline:
			t.Fatal("bisync did not finish")
		default:
		}
		for k, v := range drain(t, q) {
			got[k] = v
		}
		if s, err := tm.GetStatus(id); err == nil && s.Status == taskstate.StatusCompleted {
			for k, v := range drain(t, q) {
				got[k] = v
			}
			break loop
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(got) != len(want) {
		t.Fatalf("queued %d tasks, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: queued %q, want %q", k, got[k], v)
		}
	}
}

// A key the client deleted, whose delete has not reached the backup yet, must not be copied back.
func TestBisyncDoesNotResurrectDeletedObjects(t *testing.T) {
	primary := newListBackend("primary", "keep")
	backup := newListBackend("backup", "keep", "deleted")
	tm, q := bisyncManager(t, primary, backup)
	_ = q.Enqueue("vb", "deleted", queue.DeletePrefix+"backup", "backup", queue.PriorityHigh)

	id, _ := tm.StartBisync("vb")
	waitStatus(t, tm, id, taskstate.StatusCompleted)

	// the delete task itself is in the queue; what must not appear is a copy backup->primary
	if dir := drain(t, q)["deleted"]; dir == "backup->primary" {
		t.Fatal("bisync queued a copy of an object that is waiting to be deleted")
	}
}

func TestMergeKeysHandlesEmptySides(t *testing.T) {
	for name, tc := range map[string]struct{ a, b []string }{
		"both empty":   {nil, nil},
		"left empty":   {nil, []string{"x", "y"}},
		"right empty":  {[]string{"x", "y"}, nil},
		"disjoint":     {[]string{"a", "c"}, []string{"b", "d"}},
		"prefix trick": {[]string{"a", "a/b"}, []string{"a/b", "ab"}}, // "/" sorts before "b"
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			as, bs := newKeyStream(ctx, newListBackend("a", tc.a...), 2), newKeyStream(ctx, newListBackend("b", tc.b...), 2)
			defer as.Close()
			defer bs.Close()
			var onlyA, onlyB, both []string
			err := mergeKeys(as, bs,
				func(o storage.ObjectInfo) error { onlyA = append(onlyA, o.Key); return nil },
				func(o storage.ObjectInfo) error { onlyB = append(onlyB, o.Key); return nil },
				func(a, _ storage.ObjectInfo) error { both = append(both, a.Key); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if len(onlyA)+len(both) != len(tc.a) || len(onlyB)+len(both) != len(tc.b) {
				t.Fatalf("onlyA=%v onlyB=%v both=%v for a=%v b=%v", onlyA, onlyB, both, tc.a, tc.b)
			}
		})
	}
}
