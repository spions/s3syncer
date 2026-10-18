package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// harness wraps a Store with a way to move time forward, so timing-based
// behaviour (retry, stuck reset, purge) is tested without long sleeps.
type harness struct {
	Store
	Advance func(d time.Duration)
}

type harnessFactory func(t *testing.T) harness

// runStoreContract is the behavioural contract every Store implementation must pass.
func runStoreContract(t *testing.T, newHarness harnessFactory) {
	t.Helper()

	run := func(name string, fn func(t *testing.T, h harness)) {
		t.Run(name, func(t *testing.T) { fn(t, newHarness(t)) })
	}

	run("EnqueueDequeue", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "bucket1", "file.txt", "src", "dst", PriorityLow)
		tasks := mustDequeue(t, h, 10)
		if len(tasks) != 1 || tasks[0].ObjectKey != "file.txt" || tasks[0].VirtualBucket != "bucket1" ||
			tasks[0].SourceBackendName != "src" || tasks[0].TargetBackendName != "dst" {
			t.Fatalf("unexpected tasks: %+v", tasks)
		}
		if tasks[0].ID == "" {
			t.Fatal("task ID must not be empty")
		}
	})

	run("DedupWhilePending", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		assertCounts(t, h, 1, 0, 0)
	})

	run("DifferentDirectionsAreDistinct", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "a", "b", PriorityLow)
		mustEnqueue(t, h, "b", "key", "b", "a", PriorityLow)
		assertCounts(t, h, 2, 0, 0)
	})

	run("DedupUpgradesPriority", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityHigh)
		tasks := mustDequeue(t, h, 1)
		if len(tasks) != 1 || tasks[0].Priority != PriorityHigh {
			t.Fatalf("expected one HIGH task, got %+v", tasks)
		}
	})

	run("DedupDoesNotDowngradePriority", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityHigh)
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		if len(tasks) != 1 || tasks[0].Priority != PriorityHigh {
			t.Fatalf("expected one HIGH task, got %+v", tasks)
		}
	})

	run("UpgradedPriorityChangesOrder", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "first", "s", "d", PriorityLow)
		mustEnqueue(t, h, "b", "second", "s", "d", PriorityLow)
		mustEnqueue(t, h, "b", "second", "s", "d", PriorityHigh)
		tasks := mustDequeue(t, h, 2)
		if len(tasks) != 2 || tasks[0].ObjectKey != "second" {
			t.Fatalf("upgraded task must come first, got %+v", tasks)
		}
	})

	run("DequeueMarksProcessing", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		if got := mustDequeue(t, h, 10); len(got) != 1 {
			t.Fatalf("expected 1 task, got %d", len(got))
		}
		if got := mustDequeue(t, h, 10); len(got) != 0 {
			t.Fatalf("second dequeue must be empty, got %d", len(got))
		}
		assertCounts(t, h, 0, 1, 0)
	})

	run("DequeueLimit", func(t *testing.T, h harness) {
		for i := 0; i < 5; i++ {
			mustEnqueue(t, h, "b", fmt.Sprintf("k%d", i), "s", "d", PriorityLow)
		}
		if got := mustDequeue(t, h, 3); len(got) != 3 {
			t.Fatalf("expected 3, got %d", len(got))
		}
		assertCounts(t, h, 2, 3, 0)
	})

	run("PriorityOrdering", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "low-key", "s", "d", PriorityLow)
		mustEnqueue(t, h, "b", "high-key", "s", "d", PriorityHigh)
		mustEnqueue(t, h, "b", "med-key", "s", "d", PriorityMed)
		assertKeys(t, mustDequeue(t, h, 10), "high-key", "med-key", "low-key")
	})

	run("FIFOWithinPriority", func(t *testing.T, h harness) {
		for _, k := range []string{"a", "b", "c"} {
			mustEnqueue(t, h, "bk", k, "s", "d", PriorityLow)
			h.Advance(10 * time.Millisecond)
			time.Sleep(2 * time.Millisecond)
		}
		assertKeys(t, mustDequeue(t, h, 10), "a", "b", "c")
	})

	run("MarkDoneRemoves", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		if err := h.MarkDone(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, h, 0, 0, 0)
	})

	run("MarkDoneUnknownIDIsNotAnError", func(t *testing.T, h harness) {
		if err := h.MarkDone("does-not-exist"); err != nil {
			t.Fatalf("MarkDone of unknown id must be a no-op, got %v", err)
		}
	})

	run("DedupAllowsAfterDone", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		if err := h.MarkDone(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		assertCounts(t, h, 1, 0, 0)
	})

	run("EnqueueDuringProcessingIsNotLost", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		// The object changes while it is being copied.
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityHigh)
		assertCounts(t, h, 0, 1, 0) // still a single processing task, no duplicate
		if err := h.MarkDone(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, h, 1, 0, 0) // came back for another pass
		again := mustDequeue(t, h, 1)
		if len(again) != 1 || again[0].Priority != PriorityHigh {
			t.Fatalf("expected re-queued HIGH task, got %+v", again)
		}
		if err := h.MarkDone(again[0].ID); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, h, 0, 0, 0) // dirty flag is cleared: no endless loop
	})

	run("MarkPendingReturnsTask", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		if err := h.MarkPending(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, h, 1, 0, 0)
		if got := mustDequeue(t, h, 1); len(got) != 1 || got[0].Attempts != 0 {
			t.Fatalf("MarkPending must not count as an attempt, got %+v", got)
		}
	})

	run("MarkFailedAndRetry", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		if err := h.MarkFailed(tasks[0].ID, "connection refused"); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, h, 0, 0, 1)

		h.Advance(2 * time.Second)
		n, err := h.RetryFailed(time.Second, time.Hour)
		if err != nil || n != 1 {
			t.Fatalf("RetryFailed = %d, %v; want 1", n, err)
		}
		assertCounts(t, h, 1, 0, 0)

		got := mustDequeue(t, h, 1)
		if got[0].Attempts != 1 || got[0].LastError != "connection refused" {
			t.Fatalf("attempts/last_error not preserved: %+v", got[0])
		}
	})

	run("RetryRespectsInterval", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		_ = h.MarkFailed(tasks[0].ID, "boom")
		if n, _ := h.RetryFailed(time.Minute, time.Hour); n != 0 {
			t.Fatalf("task failed just now must not be retried yet, got %d", n)
		}
		h.Advance(2 * time.Minute)
		if n, _ := h.RetryFailed(time.Minute, time.Hour); n != 1 {
			t.Fatalf("expected 1 retried after interval, got %d", n)
		}
	})

	run("RetrySkipsTooOld", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		_ = h.MarkFailed(tasks[0].ID, "boom")
		h.Advance(2 * time.Hour)
		if n, _ := h.RetryFailed(time.Second, time.Hour); n != 0 {
			t.Fatalf("task older than maxAge must not be retried, got %d", n)
		}
	})

	run("EnqueueAfterFailedKeepsItFailedAndRaisesPriority", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		_ = h.MarkFailed(tasks[0].ID, "test error")

		mustEnqueue(t, h, "b", "key", "s", "d", PriorityHigh)
		assertCounts(t, h, 0, 0, 1)

		h.Advance(2 * time.Second)
		_, _ = h.RetryFailed(time.Second, time.Hour)
		got := mustDequeue(t, h, 1)
		if len(got) != 1 || got[0].Priority != PriorityHigh {
			t.Fatalf("expected HIGH task after retry, got %+v", got)
		}
	})

	run("ResetStuckProcessing", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		mustDequeue(t, h, 1)
		if n, _ := h.ResetStuckProcessing(5 * time.Minute); n != 0 {
			t.Fatalf("fresh processing task must not be reset, got %d", n)
		}
		h.Advance(6 * time.Minute)
		n, err := h.ResetStuckProcessing(5 * time.Minute)
		if err != nil || n != 1 {
			t.Fatalf("ResetStuckProcessing = %d, %v; want 1", n, err)
		}
		assertCounts(t, h, 1, 0, 0)
	})

	run("TouchPreventsStuckReset", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		tasks := mustDequeue(t, h, 1)
		h.Advance(4 * time.Minute)
		if err := h.Touch(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		h.Advance(4 * time.Minute) // 8 min since dequeue, 4 min since heartbeat
		if n, _ := h.ResetStuckProcessing(5 * time.Minute); n != 0 {
			t.Fatalf("heartbeating task must not be reset, got %d", n)
		}
		assertCounts(t, h, 0, 1, 0)
	})

	run("TouchIgnoresUnknownAndNonProcessing", func(t *testing.T, h harness) {
		if err := h.Touch("does-not-exist"); err != nil {
			t.Fatalf("Touch of unknown id must be a no-op, got %v", err)
		}
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		assertCounts(t, h, 1, 0, 0)
	})

	run("PurgeExpired", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "old", "s", "d", PriorityLow)
		old := mustDequeue(t, h, 1)
		_ = h.MarkFailed(old[0].ID, "boom")
		h.Advance(13 * time.Hour)
		mustEnqueue(t, h, "b", "fresh-pending", "s", "d", PriorityLow)

		n, err := h.PurgeExpired(12 * time.Hour)
		if err != nil || n != 1 {
			t.Fatalf("PurgeExpired = %d, %v; want 1", n, err)
		}
		assertCounts(t, h, 1, 0, 0) // pending is never purged, failed is gone
	})

	// A task that keeps failing must be retried less and less often, up to a cap, so a dead
	// backend is not hammered by every failed task once a minute for hours.
	run("RetryBackoffGrowsWithEachFailure", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "k", "s", "d", PriorityLow)
		fail := func() {
			tasks := mustDequeue(t, h, 1)
			if len(tasks) != 1 {
				t.Fatalf("expected the task to be pending, got %d tasks", len(tasks))
			}
			_ = h.MarkFailed(tasks[0].ID, "backend down")
		}
		retried := func(after time.Duration) int64 {
			h.Advance(after)
			n, err := h.RetryFailed(time.Minute, 12*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}

		fail() // 1st failure: retry after 1 minute
		if n := retried(30 * time.Second); n != 0 {
			t.Fatal("retried after 30s, before the base interval")
		}
		if n := retried(31 * time.Second); n != 1 {
			t.Fatal("not retried after the base interval")
		}
		fail() // 2nd failure: 2 minutes
		if n := retried(90 * time.Second); n != 0 {
			t.Fatal("2nd failure retried after 90s, backoff should be 2 minutes")
		}
		if n := retried(31 * time.Second); n != 1 {
			t.Fatal("2nd failure not retried after 2 minutes")
		}
		fail() // 3rd failure: 4 minutes
		if n := retried(3 * time.Minute); n != 0 {
			t.Fatal("3rd failure retried after 3 minutes, backoff should be 4 minutes")
		}
		if n := retried(61 * time.Second); n != 1 {
			t.Fatal("3rd failure not retried after 4 minutes")
		}
	})

	run("RetryBackoffIsCapped", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "k", "s", "d", PriorityLow)
		for i := 0; i < 12; i++ { // far past the point where doubling would exceed the cap
			tasks := mustDequeue(t, h, 1)
			if len(tasks) != 1 {
				// wait out whatever backoff is pending, then retry
				h.Advance(RetryBackoffCap + time.Minute)
				_, _ = h.RetryFailed(time.Minute, 24*time.Hour)
				tasks = mustDequeue(t, h, 1)
			}
			_ = h.MarkFailed(tasks[0].ID, "still down")
		}
		h.Advance(RetryBackoffCap + time.Minute)
		if n, err := h.RetryFailed(time.Minute, 24*time.Hour); err != nil || n != 1 {
			t.Fatalf("after %v the task must be retryable whatever its attempt count; got %d, %v", RetryBackoffCap, n, err)
		}
	})

	run("HasTaskTracksQueueMembership", func(t *testing.T, h harness) {
		has := func(bucket, key, src, dst string) bool {
			ok, err := h.HasTask(bucket, key, src, dst)
			if err != nil {
				t.Fatal(err)
			}
			return ok
		}
		if has("b", "k", "s", "d") {
			t.Fatal("empty queue reports a task")
		}
		mustEnqueue(t, h, "b", "k", "s", "d", PriorityLow)
		if !has("b", "k", "s", "d") {
			t.Fatal("pending task not found")
		}
		for name, other := range map[string][4]string{
			"other bucket": {"x", "k", "s", "d"}, "other key": {"b", "x", "s", "d"},
			"other source": {"b", "k", "x", "d"}, "other target": {"b", "k", "s", "x"},
		} {
			if has(other[0], other[1], other[2], other[3]) {
				t.Fatalf("%s: a different task was reported as present", name)
			}
		}
		tasks := mustDequeue(t, h, 1) // processing
		if !has("b", "k", "s", "d") {
			t.Fatal("processing task not found")
		}
		_ = h.MarkFailed(tasks[0].ID, "boom")
		if !has("b", "k", "s", "d") {
			t.Fatal("failed task not found")
		}
		if err := h.MarkDone(tasks[0].ID); err != nil {
			t.Fatal(err)
		}
		if has("b", "k", "s", "d") {
			t.Fatal("finished task still reported")
		}
	})

	// A delete that could not be executed must survive any outage: if the task were
	// aged out or purged, the object would be resurrected from the backend that kept it.
	run("DeleteTasksAreNeverPurgedOrAgedOut", func(t *testing.T, h harness) {
		mustEnqueue(t, h, "b", "gone", DeletePrefix+"backup", "backup", PriorityHigh)
		mustEnqueue(t, h, "b", "copy", "primary", "backup", PriorityHigh)
		for _, task := range mustDequeue(t, h, 10) {
			_ = h.MarkFailed(task.ID, "backend down")
		}
		h.Advance(13 * time.Hour)

		if n, err := h.PurgeExpired(12 * time.Hour); err != nil || n != 1 {
			t.Fatalf("PurgeExpired = %d, %v; want 1 (only the copy task)", n, err)
		}
		if ok, _ := h.HasTask("b", "gone", DeletePrefix+"backup", "backup"); !ok {
			t.Fatal("delete task was purged")
		}
		if ok, _ := h.HasTask("b", "copy", "primary", "backup"); ok {
			t.Fatal("expired copy task was not purged")
		}

		if n, err := h.RetryFailed(time.Minute, 12*time.Hour); err != nil || n != 1 {
			t.Fatalf("RetryFailed = %d, %v; want the old delete task retried", n, err)
		}
		assertCounts(t, h, 1, 0, 0)
	})

	run("OldestPendingAge", func(t *testing.T, h harness) {
		if age, err := h.OldestPendingAge(); err != nil || age != 0 {
			t.Fatalf("empty queue: age=%v err=%v; want 0", age, err)
		}
		mustEnqueue(t, h, "b", "key", "s", "d", PriorityLow)
		h.Advance(90 * time.Second)
		age, err := h.OldestPendingAge()
		if err != nil {
			t.Fatal(err)
		}
		if age < 89*time.Second || age > 95*time.Second {
			t.Fatalf("age = %v; want ~90s", age)
		}
	})

	run("ConcurrentDequeueNoDuplicates", func(t *testing.T, h harness) {
		const total = 200
		for i := 0; i < total; i++ {
			mustEnqueue(t, h, "b", fmt.Sprintf("k%03d", i), "s", "d", PriorityLow)
		}
		var (
			mu   sync.Mutex
			seen = map[string]int{}
			wg   sync.WaitGroup
		)
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					tasks, err := h.Dequeue(10)
					if err != nil {
						t.Error(err)
						return
					}
					if len(tasks) == 0 {
						return
					}
					mu.Lock()
					for _, tk := range tasks {
						seen[tk.ID]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if len(seen) != total {
			t.Fatalf("dequeued %d distinct tasks, want %d", len(seen), total)
		}
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("task %s dequeued %d times", id, n)
			}
		}
	})

	run("ConcurrentEnqueueDedup", func(t *testing.T, h harness) {
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := h.Enqueue("b", "same", "s", "d", i%3); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		assertCounts(t, h, 1, 0, 0)
		if got := mustDequeue(t, h, 1); got[0].Priority != PriorityHigh {
			t.Fatalf("expected max priority to win, got %d", got[0].Priority)
		}
	})
}

func mustEnqueue(t *testing.T, s Store, b, k, src, dst string, prio int) {
	t.Helper()
	if err := s.Enqueue(b, k, src, dst, prio); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

func mustDequeue(t *testing.T, s Store, limit int) []Task {
	t.Helper()
	tasks, err := s.Dequeue(limit)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return tasks
}

func assertCounts(t *testing.T, s Store, pending, processing, failed int64) {
	t.Helper()
	p, err1 := s.PendingCount()
	pr, err2 := s.ProcessingCount()
	f, err3 := s.FailedCount()
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("counts: %v %v %v", err1, err2, err3)
	}
	if p != pending || pr != processing || f != failed {
		t.Fatalf("counts pending/processing/failed = %d/%d/%d; want %d/%d/%d", p, pr, f, pending, processing, failed)
	}
}

func assertKeys(t *testing.T, tasks []Task, want ...string) {
	t.Helper()
	if len(tasks) != len(want) {
		t.Fatalf("got %d tasks, want %d: %+v", len(tasks), len(want), tasks)
	}
	for i, k := range want {
		if tasks[i].ObjectKey != k {
			t.Fatalf("position %d: got %q, want %q", i, tasks[i].ObjectKey, k)
		}
	}
}
