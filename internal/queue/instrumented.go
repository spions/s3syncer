package queue

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/spions/s3syncer/internal/metrics"
)

type instrumented struct {
	Store
	backend string
}

// Instrument wraps a Store with latency and error metrics (backend = "sqlite" | "redis").
func Instrument(s Store, backend string) Store {
	return &instrumented{Store: s, backend: backend}
}

func (i *instrumented) observe(op string, start time.Time, err error) {
	metrics.QueueOpDuration.WithLabelValues(i.backend, op).Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.QueueOpErrors.WithLabelValues(i.backend, op).Inc()
	}
}

func (i *instrumented) Enqueue(vb, key, src, dst string, prio int) error {
	start := time.Now()
	err := i.Store.Enqueue(vb, key, src, dst, prio)
	i.observe("enqueue", start, err)
	return err
}

func (i *instrumented) HasTask(vb, key, src, dst string) (bool, error) {
	start := time.Now()
	ok, err := i.Store.HasTask(vb, key, src, dst)
	i.observe("has_task", start, err)
	return ok, err
}

func (i *instrumented) Dequeue(limit int) (tasks []Task, err error) {
	start := time.Now()
	tasks, err = i.Store.Dequeue(limit)
	i.observe("dequeue", start, err)
	return tasks, err
}

func (i *instrumented) MarkDone(id string) error {
	start := time.Now()
	err := i.Store.MarkDone(id)
	i.observe("mark_done", start, err)
	return err
}

func (i *instrumented) MarkPending(id string) error {
	start := time.Now()
	err := i.Store.MarkPending(id)
	i.observe("mark_pending", start, err)
	return err
}

func (i *instrumented) MarkFailed(id, lastErr string) error {
	start := time.Now()
	err := i.Store.MarkFailed(id, lastErr)
	i.observe("mark_failed", start, err)
	return err
}

func (i *instrumented) Touch(ids ...string) error {
	start := time.Now()
	err := i.Store.Touch(ids...)
	i.observe("touch", start, err)
	return err
}

func (i *instrumented) transition(kind, op string, fn func() (int64, error)) (int64, error) {
	start := time.Now()
	n, err := fn()
	i.observe(op, start, err)
	if err == nil && n > 0 {
		metrics.QueueTransitions.WithLabelValues(kind).Add(float64(n))
	}
	return n, err
}

func (i *instrumented) RetryFailed(retryInterval, maxAge time.Duration) (int64, error) {
	return i.transition("retried", "retry_failed", func() (int64, error) { return i.Store.RetryFailed(retryInterval, maxAge) })
}

func (i *instrumented) PurgeExpired(maxAge time.Duration) (int64, error) {
	return i.transition("purged", "purge_expired", func() (int64, error) { return i.Store.PurgeExpired(maxAge) })
}

func (i *instrumented) ResetStuckProcessing(threshold time.Duration) (int64, error) {
	return i.transition("stuck_reset", "reset_stuck", func() (int64, error) { return i.Store.ResetStuckProcessing(threshold) })
}

type instrumentedLocker struct{ KeyLocker }

// InstrumentLocker counts lock attempts by result.
func InstrumentLocker(l KeyLocker) KeyLocker { return &instrumentedLocker{KeyLocker: l} }

func (i *instrumentedLocker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	release, ok, err := i.KeyLocker.TryLock(ctx, key)
	switch {
	case err != nil:
		metrics.LockAttempts.WithLabelValues("error").Inc()
	case ok:
		metrics.LockAttempts.WithLabelValues("acquired").Inc()
	default:
		metrics.LockAttempts.WithLabelValues("contended").Inc()
	}
	return release, ok, err
}

// Collector exports queue depth on every scrape, so the numbers are always
// current and come straight from the (possibly shared) backend.
type Collector struct {
	s       Store
	backend string

	tasks  *prometheus.Desc
	oldest *prometheus.Desc
	up     *prometheus.Desc
}

func NewCollector(s Store, backend string) *Collector {
	return &Collector{
		s:       s,
		backend: backend,
		tasks: prometheus.NewDesc("s3syncer_queue_tasks",
			"Sync tasks in the queue by state (shared queue: every instance reports the same value, aggregate with max/avg, not sum)",
			[]string{"state"}, nil),
		oldest: prometheus.NewDesc("s3syncer_queue_oldest_pending_age_seconds",
			"Age of the oldest pending sync task; grows when workers cannot keep up", nil, nil),
		up: prometheus.NewDesc("s3syncer_queue_up",
			"1 if the queue backend answered the last scrape", []string{"backend"}, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.oldest
	ch <- c.up
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	pending, e1 := c.s.PendingCount()
	processing, e2 := c.s.ProcessingCount()
	failed, e3 := c.s.FailedCount()
	age, e4 := c.s.OldestPendingAge()

	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		// Export nothing but up=0: stale or zero gauges would hide the outage.
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0, c.backend)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1, c.backend)
	ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(pending), StatusPending)
	ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(processing), StatusProcessing)
	ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(failed), StatusFailed)
	ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, age.Seconds())
}
