package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	SyncTasksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_sync_tasks_total",
		Help: "Total sync tasks processed",
	}, []string{"status"})

	SyncQueuePending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "s3syncer_sync_queue_pending",
		Help: "Number of pending sync tasks",
	})

	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3syncer_request_duration_seconds",
		Help:    "Request latency per backend and operation",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend", "operation"})

	HTTPErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_http_errors_total",
		Help: "HTTP error responses",
	}, []string{"code"})

	ManagedTasksActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3syncer_managed_tasks_active",
		Help: "Currently running managed tasks",
	}, []string{"type", "bucket"})

	ManagedTaskProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3syncer_managed_task_progress",
		Help: "Progress of managed tasks (processed/total)",
	}, []string{"task_id", "type", "bucket"})

	ManagedTasksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_managed_tasks_total",
		Help: "Total managed tasks by status",
	}, []string{"type", "status"})
)

// Client-facing HTTP and overload metrics. Labels are bounded on purpose (method and status
// code, never paths or keys), so cardinality stays constant however many objects exist.
var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_http_requests_total",
		Help: "Client requests by method and response status",
	}, []string{"method", "code"})

	HTTPRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3syncer_http_request_duration_seconds",
		Help:    "Time to serve a client request, including streaming the body",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"method"})

	HTTPInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "s3syncer_http_inflight_requests",
		Help: "Client requests currently being served",
	})

	BackendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_backend_errors_total",
		Help: "Failed requests to storage backends (not counting 'object not found')",
	}, []string{"backend", "operation"})

	UploadMemoryInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "s3syncer_upload_memory_bytes",
		Help: "Memory reserved by in-flight uploads (see put_memory_budget_bytes)",
	})

	OverloadRejections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_overload_rejections_total",
		Help: "Requests shed with 503 because the server was overloaded or a dependency was unavailable",
	}, []string{"reason"})
)

var (
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3syncer_info",
		Help: "Build version and run mode (single or cluster); always 1",
	}, []string{"version", "mode"})

	QueueOpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3syncer_queue_op_duration_seconds",
		Help:    "Latency of queue backend operations",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"backend", "op"})

	QueueOpErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_queue_op_errors_total",
		Help: "Failed queue backend operations",
	}, []string{"backend", "op"})

	QueueTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_queue_transitions_total",
		Help: "Tasks moved by queue maintenance: retried (failed->pending), stuck_reset (processing->pending), purged (failed->deleted)",
	}, []string{"kind"})

	LockAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "s3syncer_key_lock_attempts_total",
		Help: "Per-object lock attempts by result (acquired, contended, error)",
	}, []string{"result"})
)

func Register() {
	prometheus.MustRegister(
		SyncTasksTotal,
		SyncQueuePending,
		RequestDuration,
		HTTPErrors,
		ManagedTasksActive,
		ManagedTaskProgress,
		ManagedTasksTotal,
		HTTPRequests,
		HTTPRequestDuration,
		HTTPInflight,
		BackendErrors,
		UploadMemoryInUse,
		OverloadRejections,
		BuildInfo,
		QueueOpDuration,
		QueueOpErrors,
		QueueTransitions,
		LockAttempts,
	)
}
