package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/spions/s3syncer/internal/taskmanager"
	"github.com/spions/s3syncer/internal/taskstate"
)

type Handler struct {
	tm  *taskmanager.TaskManager
	log *slog.Logger
}

func NewHandler(tm *taskmanager.TaskManager, log *slog.Logger) *Handler {
	return &Handler{tm: tm, log: log}
}

// startFailed answers a failed start. A task of the same type already running for the
// bucket is a client-visible conflict (409 with the id of the running task), not a server error.
func (h *Handler) startFailed(w http.ResponseWriter, kind, bucket string, err error) {
	var conflict *taskstate.ConflictError
	if errors.As(err, &conflict) {
		h.log.Info("start rejected: already running", "type", kind, "bucket", bucket, "task_id", conflict.ExistingID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   err.Error(),
			"task_id": conflict.ExistingID,
			"type":    kind,
			"bucket":  bucket,
		})
		return
	}
	h.log.Error("start "+kind+" failed", "bucket", bucket, "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// POST /api/tasks/resync?bucket=X
func (h *Handler) StartResync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		http.Error(w, "bucket parameter required", http.StatusBadRequest)
		return
	}

	taskID, err := h.tm.StartResync(bucket)
	if err != nil {
		h.startFailed(w, "resync", bucket, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"task_id": taskID,
		"type":    "resync",
		"bucket":  bucket,
	})
}

// POST /api/tasks/bisync?bucket=X
func (h *Handler) StartBisync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		http.Error(w, "bucket parameter required", http.StatusBadRequest)
		return
	}

	taskID, err := h.tm.StartBisync(bucket)
	if err != nil {
		h.startFailed(w, "bisync", bucket, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"task_id": taskID,
		"type":    "bisync",
		"bucket":  bucket,
	})
}

// GET /api/tasks/:id
func (h *Handler) GetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract task ID from path
	taskID := r.URL.Path[len("/api/tasks/"):]
	if taskID == "" {
		http.Error(w, "task_id required", http.StatusBadRequest)
		return
	}

	status, err := h.tm.GetStatus(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// DELETE /api/tasks/:id
func (h *Handler) CancelTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract task ID from path
	taskID := r.URL.Path[len("/api/tasks/"):]
	if taskID == "" {
		http.Error(w, "task_id required", http.StatusBadRequest)
		return
	}

	if err := h.tm.Cancel(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/tasks
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks, err := h.tm.ListTasks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}
