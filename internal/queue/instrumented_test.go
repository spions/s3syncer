package queue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/spions/s3syncer/internal/metrics"
)

// failingStore makes every call fail, to exercise error metrics and queue_up=0.
type failingStore struct{ Store }

var errBoom = errors.New("boom")

func (failingStore) Enqueue(_, _, _, _ string, _ int) error { return errBoom }
func (failingStore) PendingCount() (int64, error)           { return 0, errBoom }
func (failingStore) ProcessingCount() (int64, error)        { return 0, errBoom }
func (failingStore) FailedCount() (int64, error)            { return 0, errBoom }
func (failingStore) OldestPendingAge() (time.Duration, error) {
	return 0, errBoom
}

// histCount returns how many observations the queue-op duration histogram has recorded.
func histCount(t *testing.T, backend, op string) uint64 {
	t.Helper()
	var m dto.Metric
	obs := metrics.QueueOpDuration.WithLabelValues(backend, op)
	if err := obs.(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestInstrumentedRecordsDurationAndErrors(t *testing.T) {
	h := newSQLiteHarness(t)
	s := Instrument(h.Store, "sqlite")

	// The registry is process-global, so compare sample counts before and after instead of
	// assuming the series do not exist yet (this test must pass with -count=N and -shuffle).
	enqBefore, deqBefore := histCount(t, "sqlite", "enqueue"), histCount(t, "sqlite", "dequeue")
	mustEnqueue(t, s, "b", "k", "s", "d", PriorityLow)
	mustDequeue(t, s, 1)
	if got := histCount(t, "sqlite", "enqueue"); got != enqBefore+1 {
		t.Fatalf("enqueue duration observations = %d, want %d", got, enqBefore+1)
	}
	if got := histCount(t, "sqlite", "dequeue"); got != deqBefore+1 {
		t.Fatalf("dequeue duration observations = %d, want %d", got, deqBefore+1)
	}

	bad := Instrument(failingStore{h.Store}, "sqlite")
	before := testutil.ToFloat64(metrics.QueueOpErrors.WithLabelValues("sqlite", "enqueue"))
	if err := bad.Enqueue("b", "k", "s", "d", 0); !errors.Is(err, errBoom) {
		t.Fatalf("error must pass through unchanged, got %v", err)
	}
	if got := testutil.ToFloat64(metrics.QueueOpErrors.WithLabelValues("sqlite", "enqueue")); got != before+1 {
		t.Fatalf("errors counter = %v, want %v", got, before+1)
	}
}

func TestInstrumentedCountsMaintenanceTransitions(t *testing.T) {
	h := newSQLiteHarness(t)
	s := Instrument(h.Store, "sqlite")

	mustEnqueue(t, s, "b", "k", "s", "d", PriorityLow)
	tasks := mustDequeue(t, s, 1)
	h.Advance(10 * time.Minute)
	if n, _ := s.ResetStuckProcessing(5 * time.Minute); n != 1 {
		t.Fatalf("setup: reset %d", n)
	}
	if got := testutil.ToFloat64(metrics.QueueTransitions.WithLabelValues("stuck_reset")); got < 1 {
		t.Fatalf("stuck_reset counter = %v, want >= 1", got)
	}
	_ = tasks

	tasks = mustDequeue(t, s, 1)
	_ = s.MarkFailed(tasks[0].ID, "x")
	h.Advance(2 * time.Second)
	_, _ = s.RetryFailed(time.Second, time.Hour)
	if got := testutil.ToFloat64(metrics.QueueTransitions.WithLabelValues("retried")); got < 1 {
		t.Fatalf("retried counter = %v, want >= 1", got)
	}
	_ = s.MarkFailed(mustDequeue(t, s, 1)[0].ID, "x")
	h.Advance(13 * time.Hour)
	_, _ = s.PurgeExpired(12 * time.Hour)
	if got := testutil.ToFloat64(metrics.QueueTransitions.WithLabelValues("purged")); got < 1 {
		t.Fatalf("purged counter = %v, want >= 1", got)
	}
}

func TestCollectorExposesQueueState(t *testing.T) {
	h := newSQLiteHarness(t)
	mustEnqueue(t, h, "b", "a", "s", "d", PriorityLow)
	mustEnqueue(t, h, "b", "b", "s", "d", PriorityLow)
	mustEnqueue(t, h, "b", "c", "s", "d", PriorityLow)
	tasks := mustDequeue(t, h, 2)
	_ = h.MarkFailed(tasks[0].ID, "x")

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(h.Store, "sqlite"))

	want := `
# HELP s3syncer_queue_tasks Sync tasks in the queue by state (shared queue: every instance reports the same value, aggregate with max/avg, not sum)
# TYPE s3syncer_queue_tasks gauge
s3syncer_queue_tasks{state="failed"} 1
s3syncer_queue_tasks{state="pending"} 1
s3syncer_queue_tasks{state="processing"} 1
# HELP s3syncer_queue_up 1 if the queue backend answered the last scrape
# TYPE s3syncer_queue_up gauge
s3syncer_queue_up{backend="sqlite"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "s3syncer_queue_tasks", "s3syncer_queue_up"); err != nil {
		t.Fatal(err)
	}
	if n, err := testutil.GatherAndCount(reg, "s3syncer_queue_oldest_pending_age_seconds"); err != nil || n != 1 {
		t.Fatalf("oldest pending age series = %d, %v; want 1", n, err)
	}
}

func TestCollectorReportsBackendDown(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(failingStore{}, "redis"))

	want := `
# HELP s3syncer_queue_up 1 if the queue backend answered the last scrape
# TYPE s3syncer_queue_up gauge
s3syncer_queue_up{backend="redis"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "s3syncer_queue_up"); err != nil {
		t.Fatal(err)
	}
	// Stale or zero task gauges would hide the outage on dashboards: they must be absent.
	if n, _ := testutil.GatherAndCount(reg, "s3syncer_queue_tasks"); n != 0 {
		t.Fatalf("task gauges must not be exported while the backend is down, got %d series", n)
	}
}

func TestInstrumentedLockerCountsResults(t *testing.T) {
	l := InstrumentLocker(NewMemoryLocker())
	ctx := t.Context()

	acq := func(result string) float64 { return testutil.ToFloat64(metrics.LockAttempts.WithLabelValues(result)) }
	a0, c0 := acq("acquired"), acq("contended")

	release, ok, _ := l.TryLock(ctx, "b/k")
	if !ok {
		t.Fatal("first lock must succeed")
	}
	if _, ok, _ := l.TryLock(ctx, "b/k"); ok {
		t.Fatal("second lock must be contended")
	}
	release()

	if acq("acquired") != a0+1 || acq("contended") != c0+1 {
		t.Fatalf("lock counters acquired=%v contended=%v", acq("acquired")-a0, acq("contended")-c0)
	}
}
