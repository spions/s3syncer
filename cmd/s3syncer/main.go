package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/spions/s3syncer/internal/api"
	"github.com/spions/s3syncer/internal/config"
	"github.com/spions/s3syncer/internal/metrics"
	"github.com/spions/s3syncer/internal/proxy"
	"github.com/spions/s3syncer/internal/queue"
	"github.com/spions/s3syncer/internal/replicator"
	"github.com/spions/s3syncer/internal/storage"
	"github.com/spions/s3syncer/internal/taskmanager"
	"github.com/spions/s3syncer/internal/taskstate"
	"github.com/spions/s3syncer/internal/worker"
)

// version is set at build time: -ldflags "-X main.version=v1.2.3"
var version = "dev"

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  s3syncer -config <path>                      Start the proxy server
  s3syncer -config <path> resync [bucket]      Trigger resync task (requires server running)
  s3syncer -config <path> bisync [bucket]      Trigger bisync task (requires server running)
  s3syncer -config <path> task-status <id>     Get task status
  s3syncer -config <path> task-cancel <id>     Cancel running task
  s3syncer -config <path> task-list            List all tasks
  s3syncer -version                            Print version
`)
	os.Exit(1)
}

// queueRuntime is everything that differs between single and cluster mode.
type queueRuntime struct {
	store     queue.Store
	locker    queue.KeyLocker
	tasks     taskstate.Store
	backend   string
	clustered bool
	close     func()
}

// buildQueue selects the backends from cfg.Mode: SQLite + in-process locks (single)
// or a shared Redis for queue, locks and task state (cluster). Queue and locker are instrumented.
func buildQueue(cfg *config.Config, db *sql.DB) (*queueRuntime, error) {
	if cfg.Mode != config.ModeCluster {
		tasks, err := taskstate.NewSQLite(db)
		if err != nil {
			return nil, err
		}
		return &queueRuntime{
			store:   queue.Instrument(queue.NewSQLite(db), "sqlite"),
			locker:  queue.InstrumentLocker(queue.NewMemoryLocker()),
			tasks:   tasks,
			backend: "sqlite",
			close:   func() {},
		}, nil
	}

	rdb, err := queue.NewRedisClient(queue.RedisConfig{
		Addr: cfg.Redis.Addr, Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		DB: cfg.Redis.DB, TLS: cfg.Redis.TLS,
	})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*queueRuntime, error) { rdb.Close(); return nil, err }

	store, err := queue.NewRedisStore(rdb, queue.RedisOptions{KeyPrefix: cfg.Redis.KeyPrefix})
	if err != nil {
		return fail(err)
	}
	tasks, err := taskstate.NewRedis(rdb, taskstate.RedisOptions{KeyPrefix: cfg.Redis.KeyPrefix})
	if err != nil {
		return fail(err)
	}
	locker := queue.NewRedisLocker(rdb, queue.RedisLockOptions{KeyPrefix: cfg.Redis.KeyPrefix})
	return &queueRuntime{
		store:     queue.Instrument(store, "redis"),
		locker:    queue.InstrumentLocker(locker),
		tasks:     tasks,
		backend:   "redis",
		clustered: true,
		close:     func() { locker.Close(); rdb.Close() },
	}, nil
}

// instanceID names this process in task state ("pod-name-1a2b3c4d"), so a task shows which instance runs it.
func instanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "s3syncer"
	}
	return host + "-" + uuid.NewString()[:8]
}

func main() {
	// Simple arg parsing: find -config flag, then optional command
	configPath := "config.yaml"
	var args []string

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-config" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++ // skip value
		} else if os.Args[i] == "-version" || os.Args[i] == "--version" {
			fmt.Println("s3syncer", version)
			return
		} else if os.Args[i] == "-h" || os.Args[i] == "--help" {
			usage()
		} else {
			args = append(args, os.Args[i])
		}
	}

	var logLevel slog.LevelVar // info until the config says otherwise
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &logLevel}))

	log.Info("s3syncer", "version", version)

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	apiToken = cfg.APIToken
	if cfg.LogLevel != "" {
		if err := logLevel.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
			log.Error("invalid log_level", "error", err)
			os.Exit(1)
		}
	}

	// Handle CLI commands
	if len(args) > 0 {
		// All CLI commands now require the server to be running
		serverAddr := cfg.ListenAddr
		if serverAddr[0] == ':' {
			serverAddr = "localhost" + serverAddr
		}

		switch args[0] {
		case "resync":
			bucket := ""
			if len(args) > 1 {
				bucket = args[1]
			}
			if err := triggerResync(serverAddr, bucket); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				fmt.Fprintf(os.Stderr, "Make sure the server is running: s3syncer -config %s\n", configPath)
				os.Exit(1)
			}
			return

		case "bisync":
			bucket := ""
			if len(args) > 1 {
				bucket = args[1]
			}
			if err := triggerBisync(serverAddr, bucket); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				fmt.Fprintf(os.Stderr, "Make sure the server is running: s3syncer -config %s\n", configPath)
				os.Exit(1)
			}
			return

		case "task-status":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Error: task-status requires task ID\n")
				os.Exit(1)
			}
			if err := getTaskStatus(serverAddr, args[1]); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return

		case "task-cancel":
			if len(args) < 2 {
				fmt.Fprintf(os.Stderr, "Error: task-cancel requires task ID\n")
				os.Exit(1)
			}
			if err := cancelTask(serverAddr, args[1]); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return

		case "task-list":
			if err := listTasks(serverAddr); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			return

		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
			usage()
		}
	}

	// SQLite always holds the local state of resync/bisync tasks; in single mode it is also the queue.
	db, err := queue.OpenDB(cfg.DBPath)
	if err != nil {
		log.Error("open db", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rt, err := buildQueue(cfg, db)
	if err != nil {
		log.Error("init queue", "mode", cfg.Mode, "error", err)
		os.Exit(1)
	}
	defer rt.close()
	q, locker, queueBackend := rt.store, rt.locker, rt.backend
	log.Info("queue ready", "mode", cfg.Mode, "backend", queueBackend)

	// Build backends and replicators
	allBackends := make(map[string]storage.StorageBackend)
	replicators := make(map[string]*replicator.Replicator)
	putMemory := replicator.NewMemoryBudget(cfg.PutMemoryBudgetBytes)
	putMemory.SetMaxWait(time.Duration(cfg.PutMemoryWaitSeconds) * time.Second)

	for _, vb := range cfg.VirtualBuckets {
		var primary storage.StorageBackend
		var backups []storage.StorageBackend

		for _, bcfg := range vb.Backends {
			backend := storage.NewS3Backend(
				bcfg.Name, bcfg.Region, bcfg.Bucket, bcfg.Endpoint,
				bcfg.AccessKeyID, bcfg.SecretAccessKey, bcfg.ForcePathStyle,
			)
			allBackends[bcfg.Name] = backend

			if bcfg.Name == vb.PrimaryBackendName {
				primary = backend
			} else {
				backups = append(backups, backend)
			}
		}

		if primary == nil {
			log.Error("primary backend not found", "bucket", vb.Name, "primary", vb.PrimaryBackendName)
			os.Exit(1)
		}

		replicators[vb.Name] = replicator.New(vb.Name, primary, backups, q, log, time.Duration(cfg.OperationTimeoutSeconds)*time.Second)
		replicators[vb.Name].SetStreamThreshold(cfg.PutStreamThresholdBytes)
		replicators[vb.Name].SetMemoryBudget(putMemory)
		replicators[vb.Name].SetLocker(locker)
		log.Info("configured virtual bucket", "name", vb.Name, "primary", vb.PrimaryBackendName, "backups", len(backups))
	}

	// Server mode
	metrics.Register()
	metrics.BuildInfo.WithLabelValues(version, cfg.Mode).Set(1)
	prometheus.MustRegister(queue.NewCollector(q, queueBackend))

	if rt.clustered {
		// Other instances may be running tasks right now: never fail them on startup.
		// Tasks of a dead instance are reaped when their heartbeat goes stale (see below).
		log.Info("cluster mode: resync/bisync state is shared through Redis")
	} else {
		// Single instance: whatever was still running belongs to the previous process.
		if _, err := rt.tasks.FailStale(0, "server restarted"); err != nil {
			log.Error("cleanup on startup", "error", err)
		}
	}
	if err := rt.tasks.Prune(100); err != nil {
		log.Error("prune old tasks", "error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker
	w := worker.New(q, locker, allBackends, time.Duration(cfg.SyncInterval)*time.Second, log)
	workerDone := make(chan struct{})
	go func() { w.Run(ctx); close(workerDone) }()

	// Update replicators with worker for immediate notification
	for _, rep := range replicators {
		rep.SetWorker(w)
	}

	// Initialize task manager
	tm := taskmanager.New(rt.tasks, instanceID(), q, allBackends, cfg, log, w)
	if rt.clustered {
		go reapStaleTasks(ctx, rt.tasks, log)
	}

	// S3 handler with auth
	creds := proxy.BuildCredentials(cfg.VirtualBuckets)
	publicBuckets := proxy.PublicReadBuckets(cfg.VirtualBuckets)
	proxyHandler := proxy.NewHandler(replicators, log)
	proxyHandler.SetMaxPutSize(cfg.MaxPutSizeBytes)
	proxyHandler.SetBodyIdleTimeout(time.Duration(cfg.RequestBodyIdleTimeoutSeconds) * time.Second)
	s3Handler := proxy.Observe(proxy.AuthMiddleware(creds, publicBuckets, log, proxyHandler))

	// API handler
	apiHandler := api.NewHandler(tm, log)

	mux := http.NewServeMux()

	// Probes (no auth): liveness only says the process serves HTTP, readiness needs the queue.
	mux.Handle("/healthz", api.Healthz())
	mux.Handle("/readyz", api.Readyz(
		func() error { _, err := q.PendingCount(); return err },
		func(err error) { log.Warn("not ready", "error", err) },
	))

	// Prometheus metrics (no auth, as is usual for exporters; restrict at network level)
	mux.Handle("/metrics", promhttp.Handler())

	// Management API: Bearer token, or loopback only when no token is configured
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/tasks/resync", apiHandler.StartResync)
	apiMux.HandleFunc("/api/tasks/bisync", apiHandler.StartBisync)
	apiMux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		// /api/tasks/:id or /api/tasks
		if r.URL.Path == "/api/tasks" || r.URL.Path == "/api/tasks/" {
			apiHandler.ListTasks(w, r)
		} else {
			// /api/tasks/:id
			switch r.Method {
			case http.MethodGet:
				apiHandler.GetTaskStatus(w, r)
			case http.MethodDelete:
				apiHandler.CancelTask(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		}
	})
	mux.Handle("/api/", api.Protect(cfg.APIToken, apiMux))

	// S3 endpoints (with auth) are routed around the mux, see proxy.Route

	// No WriteTimeout/ReadTimeout: GET streams and large PUT bodies can take arbitrarily long.
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           proxy.Route(mux, s3Handler),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("starting server", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Shutdown order matters: stop taking requests and let uploads in flight finish first,
	// only then stop the worker (its interrupted tasks go back to the queue) and close the
	// queue connections.
	log.Info("shutting down")
	grace := time.Duration(cfg.ShutdownGraceSeconds) * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), grace)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown incomplete, closing remaining connections", "error", err)
		_ = srv.Close()
	}
	cancel()
	select {
	case <-workerDone:
	case <-time.After(grace):
		log.Error("worker did not stop in time; unfinished tasks will be reset by the lease timeout")
	}
	log.Info("stopped")
}

// reapStaleTasks fails running tasks whose owner stopped sending heartbeats (cluster mode).
// Every instance runs it; the operation is idempotent.
func reapStaleTasks(ctx context.Context, tasks taskstate.Store, log *slog.Logger) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c, err := tasks.FailStale(taskmanager.StaleAfter, "owner lost: no heartbeat"); err != nil {
				log.Warn("reap stale tasks", "error", err)
			} else if c > 0 {
				log.Warn("failed tasks of a lost instance", "count", c)
			}
			if n%120 == 119 { // about hourly
				if err := tasks.Prune(100); err != nil {
					log.Warn("prune old tasks", "error", err)
				}
			}
		}
	}
}

// HTTP client functions for CLI commands

// apiToken is sent as a Bearer token to the management API (config api_token / S3SYNCER_API_TOKEN).
var apiToken string

var apiClient = &http.Client{Timeout: 30 * time.Second}

// startError describes a rejected resync/bisync start; 409 means the same task is already running.
func startError(resp *http.Response) error {
	if resp.StatusCode == http.StatusConflict {
		var c map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&c); err == nil && c["task_id"] != "" {
			return fmt.Errorf("%s\ncheck it with: s3syncer task-status %s (or cancel it with task-cancel)", c["error"], c["task_id"])
		}
		return fmt.Errorf("the same task is already running")
	}
	return statusError(resp)
}

func statusError(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("server returned status %d: check api_token / S3SYNCER_API_TOKEN", resp.StatusCode)
	}
	return fmt.Errorf("server returned status %d", resp.StatusCode)
}

func doAPI(method, url string) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+apiToken)
	}
	return apiClient.Do(req)
}

func triggerResync(serverAddr, bucket string) error {
	url := fmt.Sprintf("http://%s/api/tasks/resync", serverAddr)
	if bucket != "" {
		url += "?bucket=" + neturl.QueryEscape(bucket)
	}

	resp, err := doAPI(http.MethodPost, url)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return startError(resp)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("Resync task started: %s\n", result["task_id"])
	fmt.Printf("Check status with: s3syncer task-status %s\n", result["task_id"])
	return nil
}

func triggerBisync(serverAddr, bucket string) error {
	url := fmt.Sprintf("http://%s/api/tasks/bisync", serverAddr)
	if bucket != "" {
		url += "?bucket=" + neturl.QueryEscape(bucket)
	}

	resp, err := doAPI(http.MethodPost, url)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return startError(resp)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("Bisync task started: %s\n", result["task_id"])
	fmt.Printf("Check status with: s3syncer task-status %s\n", result["task_id"])
	return nil
}

func getTaskStatus(serverAddr, taskID string) error {
	url := fmt.Sprintf("http://%s/api/tasks/%s", serverAddr, taskID)

	resp, err := doAPI(http.MethodGet, url)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}

	var status taskmanager.TaskStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Printf("Task ID:     %s\n", status.ID)
	fmt.Printf("Type:        %s\n", status.Type)
	fmt.Printf("Bucket:      %s\n", status.Bucket)
	fmt.Printf("Status:      %s\n", status.Status)
	fmt.Printf("Progress:    %d/%d objects\n", status.ProcessedObjects, status.TotalObjects)
	if status.TotalObjects > 0 {
		pct := float64(status.ProcessedObjects) / float64(status.TotalObjects) * 100
		fmt.Printf("             %.1f%%\n", pct)
	}
	fmt.Printf("Chunk:       %d\n", status.CurrentChunk)
	if status.Error != "" {
		fmt.Printf("Error:       %s\n", status.Error)
	}
	fmt.Printf("Created:     %s\n", status.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Updated:     %s\n", status.UpdatedAt.Format(time.RFC3339))
	return nil
}

func cancelTask(serverAddr, taskID string) error {
	url := fmt.Sprintf("http://%s/api/tasks/%s", serverAddr, taskID)

	resp, err := doAPI(http.MethodDelete, url)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return statusError(resp)
	}

	fmt.Printf("Task %s cancelled\n", taskID)
	return nil
}

func listTasks(serverAddr string) error {
	url := fmt.Sprintf("http://%s/api/tasks", serverAddr)

	resp, err := doAPI(http.MethodGet, url)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}

	var tasks []*taskmanager.TaskStatus
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks found")
		return nil
	}

	fmt.Printf("%-36s %-8s %-20s %-12s %s\n", "TASK ID", "TYPE", "BUCKET", "STATUS", "PROGRESS")
	fmt.Println("---------------------------------------------------------------------------------------------------")
	for _, t := range tasks {
		progress := "-"
		if t.TotalObjects > 0 {
			pct := float64(t.ProcessedObjects) / float64(t.TotalObjects) * 100
			progress = fmt.Sprintf("%d/%d (%.1f%%)", t.ProcessedObjects, t.TotalObjects, pct)
		}
		fmt.Printf("%-36s %-8s %-20s %-12s %s\n", t.ID, t.Type, t.Bucket, t.Status, progress)
	}
	return nil
}
