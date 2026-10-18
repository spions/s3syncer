package taskstate

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type harness struct {
	Store
	Advance func(d time.Duration)
	// CrossInstanceCancel: RequestCancel works from any instance (Redis).
	CrossInstanceCancel bool
}

func runContract(t *testing.T, newHarness func(t *testing.T) harness) {
	t.Helper()
	run := func(name string, fn func(t *testing.T, h harness)) {
		t.Run(name, func(t *testing.T) { fn(t, newHarness(t)) })
	}

	run("CreateGet", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "bkt", "pod-a"))
		s, err := h.Get("id1")
		must(t, err)
		if s.ID != "id1" || s.Type != "resync" || s.Bucket != "bkt" || s.Status != StatusRunning || s.Owner != "pod-a" {
			t.Fatalf("unexpected status %+v", s)
		}
		if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
			t.Fatalf("timestamps must be set: %+v", s)
		}
	})

	run("CreateRejectedWhileSameTaskRuns", func(t *testing.T, h harness) {
		must(t, h.Create("first", "resync", "bkt", "pod-a"))

		err := h.Create("second", "resync", "bkt", "pod-b")
		var conflict *ConflictError
		if !errors.As(err, &conflict) || conflict.ExistingID != "first" {
			t.Fatalf("err = %v, want ConflictError pointing at the running task", err)
		}
		if _, err := h.Get("second"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("a rejected task must not be stored, Get err = %v", err)
		}
		if list, _ := h.List(); len(list) != 1 {
			t.Fatalf("list has %d tasks, want 1", len(list))
		}
	})

	run("CreateAllowedForOtherTypeOrBucket", func(t *testing.T, h harness) {
		must(t, h.Create("r1", "resync", "bkt", "o"))
		must(t, h.Create("b1", "bisync", "bkt", "o"))       // other type
		must(t, h.Create("r2", "resync", "other-bkt", "o")) // other bucket
	})

	run("CreateAllowedAfterFinish", func(t *testing.T, h harness) {
		for _, status := range []string{StatusCompleted, StatusFailed, StatusCancelled} {
			id := "t-" + status
			must(t, h.Create(id, "resync", "bkt", "o"))
			if err := h.Create(id+"-dup", "resync", "bkt", "o"); err == nil {
				t.Fatalf("%s: duplicate must be rejected while running", status)
			}
			if changed, err := h.Finish(id, status, ""); err != nil || !changed {
				t.Fatalf("Finish = %v, %v", changed, err)
			}
		}
		must(t, h.Create("again", "resync", "bkt", "o"))
	})

	run("CreateAllowedAfterOwnerLost", func(t *testing.T, h harness) {
		must(t, h.Create("dead", "resync", "bkt", "pod-dead"))
		h.Advance(5 * time.Minute)
		if n, _ := h.FailStale(time.Minute, "owner lost"); n != 1 {
			t.Fatalf("setup: reaped %d", n)
		}
		must(t, h.Create("again", "resync", "bkt", "pod-b"))
	})

	run("ConcurrentCreateHasOneWinner", func(t *testing.T, h harness) {
		const n = 12
		var wg sync.WaitGroup
		results := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results <- h.Create(fmt.Sprintf("c%02d", i), "resync", "bkt", "o")
			}(i)
		}
		wg.Wait()
		close(results)
		won, rejected := 0, 0
		for err := range results {
			var c *ConflictError
			switch {
			case err == nil:
				won++
			case errors.As(err, &c):
				rejected++
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if won != 1 || rejected != n-1 {
			t.Fatalf("won=%d rejected=%d, want exactly 1 winner and %d rejected", won, rejected, n-1)
		}
	})

	run("GetUnknown", func(t *testing.T, h harness) {
		if _, err := h.Get("nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	run("CreateDuplicateFails", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		if err := h.Create("id1", "resync", "b", "o"); err == nil {
			t.Fatal("duplicate id must fail")
		}
	})

	run("ListNewestFirst", func(t *testing.T, h harness) {
		for _, id := range []string{"a", "b", "c"} {
			must(t, h.Create(id, "bisync", "bkt-"+id, "o"))
			h.Advance(2 * time.Second)
		}
		list, err := h.List()
		must(t, err)
		if len(list) != 3 || list[0].ID != "c" || list[1].ID != "b" || list[2].ID != "a" {
			t.Fatalf("order = %v", ids(list))
		}
	})

	run("ListEmpty", func(t *testing.T, h harness) {
		list, err := h.List()
		if err != nil || len(list) != 0 {
			t.Fatalf("List = %v, %v", list, err)
		}
	})

	run("UpdateProgress", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		must(t, h.UpdateProgress("id1", 500, 1500, 3))
		s, _ := h.Get("id1")
		if s.ProcessedObjects != 500 || s.TotalObjects != 1500 || s.CurrentChunk != 3 {
			t.Fatalf("progress not stored: %+v", s)
		}
	})

	run("FinishOnlyOnce", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		changed, err := h.Finish("id1", StatusFailed, "boom")
		if err != nil || !changed {
			t.Fatalf("Finish = %v, %v", changed, err)
		}
		changed, err = h.Finish("id1", StatusCompleted, "")
		if err != nil || changed {
			t.Fatalf("second Finish must not change anything, got %v, %v", changed, err)
		}
		s, _ := h.Get("id1")
		if s.Status != StatusFailed || s.Error != "boom" {
			t.Fatalf("finished status overwritten: %+v", s)
		}
	})

	run("FinishUnknownIsNoop", func(t *testing.T, h harness) {
		if changed, err := h.Finish("nope", StatusCompleted, ""); err != nil || changed {
			t.Fatalf("Finish(unknown) = %v, %v", changed, err)
		}
	})

	run("ProgressAfterFinishIsIgnored", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		_, _ = h.Finish("id1", StatusCompleted, "")
		must(t, h.UpdateProgress("id1", 9, 9, 9))
		if s, _ := h.Get("id1"); s.Status != StatusCompleted {
			t.Fatalf("status = %s", s.Status)
		}
	})

	run("RequestCancel", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		err := h.RequestCancel("id1")
		if !h.CrossInstanceCancel {
			if !errors.Is(err, ErrNotRunning) {
				t.Fatalf("single-process store: err = %v, want ErrNotRunning", err)
			}
			return
		}
		must(t, err)
		if on, err := h.CancelRequested("id1"); err != nil || !on {
			t.Fatalf("CancelRequested = %v, %v", on, err)
		}
		_, _ = h.Finish("id1", StatusCancelled, "")
		if err := h.RequestCancel("id1"); !errors.Is(err, ErrNotRunning) {
			t.Fatalf("cancel of finished task: err = %v, want ErrNotRunning", err)
		}
		if err := h.RequestCancel("nope"); !errors.Is(err, ErrNotRunning) {
			t.Fatalf("cancel of unknown task: err = %v, want ErrNotRunning", err)
		}
	})

	run("CancelRequestedFalseByDefault", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "o"))
		if on, err := h.CancelRequested("id1"); err != nil || on {
			t.Fatalf("CancelRequested = %v, %v", on, err)
		}
	})

	run("FailStaleReapsSilentOwner", func(t *testing.T, h harness) {
		must(t, h.Create("id1", "resync", "b", "pod-dead"))
		if n, _ := h.FailStale(time.Minute, "owner lost"); n != 0 {
			t.Fatalf("fresh task reaped: %d", n)
		}
		h.Advance(2 * time.Minute)
		n, err := h.FailStale(time.Minute, "owner lost")
		if err != nil || n != 1 {
			t.Fatalf("FailStale = %d, %v; want 1", n, err)
		}
		if s, _ := h.Get("id1"); s.Status != StatusFailed || s.Error != "owner lost" {
			t.Fatalf("status = %+v", s)
		}
	})

	run("HeartbeatKeepsTaskAlive", func(t *testing.T, h harness) {
		must(t, h.Create("touch", "resync", "b1", "o"))
		must(t, h.Create("progress", "resync", "b2", "o"))
		must(t, h.Create("silent", "resync", "b3", "o"))
		h.Advance(50 * time.Second)
		must(t, h.Touch("touch"))
		must(t, h.UpdateProgress("progress", 1, 2, 1))
		h.Advance(50 * time.Second)
		n, _ := h.FailStale(time.Minute, "owner lost")
		if n != 1 {
			t.Fatalf("only the silent task may be reaped, reaped %d", n)
		}
		if s, _ := h.Get("silent"); s.Status != StatusFailed {
			t.Fatalf("silent task status = %s", s.Status)
		}
		for _, id := range []string{"touch", "progress"} {
			if s, _ := h.Get(id); s.Status != StatusRunning {
				t.Fatalf("%s reaped despite heartbeat", id)
			}
		}
	})

	run("FailStaleZeroFailsAllRunning", func(t *testing.T, h harness) {
		// Single-mode startup: every task left running by the previous process is dead.
		must(t, h.Create("r1", "resync", "b", "o"))
		must(t, h.Create("r2", "bisync", "b", "o"))
		must(t, h.Create("done", "resync", "other", "o"))
		_, _ = h.Finish("done", StatusCompleted, "")
		h.Advance(2 * time.Second)
		n, err := h.FailStale(0, "server restarted")
		if err != nil || n != 2 {
			t.Fatalf("FailStale(0) = %d, %v; want 2", n, err)
		}
		if s, _ := h.Get("done"); s.Status != StatusCompleted {
			t.Fatalf("finished task touched: %s", s.Status)
		}
	})

	run("PruneKeepsRunningAndNewestFinished", func(t *testing.T, h harness) {
		for i := 0; i < 105; i++ {
			id := fmt.Sprintf("old%03d", i)
			must(t, h.Create(id, "resync", "b", "o"))
			_, _ = h.Finish(id, StatusCompleted, "")
			h.Advance(2 * time.Second)
		}
		must(t, h.Create("run1", "resync", "b", "o"))
		must(t, h.Prune(100))
		list, err := h.List()
		must(t, err)
		if len(list) != 101 {
			t.Fatalf("after prune: %d tasks, want 101 (100 finished + 1 running)", len(list))
		}
		if _, err := h.Get("old000"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("oldest finished task must be pruned, err = %v", err)
		}
		if _, err := h.Get("old104"); err != nil {
			t.Fatalf("newest finished task must stay: %v", err)
		}
		if s, err := h.Get("run1"); err != nil || s.Status != StatusRunning {
			t.Fatalf("running task must stay: %v", err)
		}
	})
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func ids(l []*Status) []string {
	out := make([]string, len(l))
	for i, s := range l {
		out[i] = s.ID
	}
	return out
}
