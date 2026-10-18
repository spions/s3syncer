package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SQLiteStore is the single-node Store backed by a local SQLite file.
type SQLiteStore struct {
	db *sql.DB
}

func NewSQLite(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db}
}

// Enqueue creates a sync task. Duplicate tasks for the same object+direction
// only raise the priority; a task enqueued while being processed is marked dirty
// so MarkDone sends it around again.
func (q *SQLiteStore) Enqueue(virtualBucket, objectKey, source, target string, priority int) error {
	_, err := q.db.Exec(`
		INSERT INTO sync_tasks (virtual_bucket, object_key, source_backend_name, target_backend_name, status, priority)
		VALUES (?, ?, ?, ?, 'pending', ?)
		ON CONFLICT (virtual_bucket, object_key, source_backend_name, target_backend_name)
		DO UPDATE SET
			priority = MAX(excluded.priority, sync_tasks.priority),
			dirty = CASE WHEN sync_tasks.status = 'processing' THEN 1 ELSE sync_tasks.dirty END,
			updated_at = CASE WHEN sync_tasks.status = 'pending' THEN datetime('now') ELSE sync_tasks.updated_at END`,
		virtualBucket, objectKey, source, target, priority,
	)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// Dequeue atomically picks up to limit pending tasks and marks them processing.
func (q *SQLiteStore) Dequeue(limit int) ([]Task, error) {
	tx, err := q.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(`
		SELECT id, virtual_bucket, object_key, source_backend_name, target_backend_name, priority, attempts, last_error
		FROM sync_tasks
		WHERE status = 'pending'
		ORDER BY priority DESC, created_at ASC, id ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select tasks: %w", err)
	}

	var tasks []Task
	for rows.Next() {
		var t Task
		var id int64
		if err := rows.Scan(&id, &t.VirtualBucket, &t.ObjectKey, &t.SourceBackendName, &t.TargetBackendName, &t.Priority, &t.Attempts, &t.LastError); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan: %w", err)
		}
		t.ID = strconv.FormatInt(id, 10)
		t.Status = StatusProcessing
		tasks = append(tasks, t)
	}
	rows.Close()

	if len(tasks) == 0 {
		return nil, nil
	}

	for _, t := range tasks {
		if _, err := tx.Exec(`UPDATE sync_tasks SET status = 'processing', updated_at = datetime('now') WHERE id = ?`, t.ID); err != nil {
			return nil, fmt.Errorf("mark processing: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return tasks, nil
}

// MarkDone deletes a finished task, or sends it around again when it was
// re-enqueued (dirty) while being processed.
func (q *SQLiteStore) MarkDone(id string) error {
	if _, err := q.db.Exec(`DELETE FROM sync_tasks WHERE id = ? AND dirty = 0`, id); err != nil {
		return err
	}
	_, err := q.db.Exec(`UPDATE sync_tasks SET status = 'pending', dirty = 0, updated_at = datetime('now') WHERE id = ? AND dirty = 1`, id)
	return err
}

// MarkPending resets a task back to pending status (e.g. when skipping due to direction failure).
func (q *SQLiteStore) MarkPending(id string) error {
	_, err := q.db.Exec(`UPDATE sync_tasks SET status = 'pending', dirty = 0, updated_at = datetime('now') WHERE id = ?`, id)
	return err
}

func (q *SQLiteStore) MarkFailed(id string, lastErr string) error {
	_, err := q.db.Exec(`
		UPDATE sync_tasks
		SET status = 'failed', dirty = 0, attempts = attempts + 1, last_error = ?, updated_at = datetime('now')
		WHERE id = ?`, lastErr, id)
	return err
}

// Touch refreshes updated_at of in-flight tasks so they are not reset as stuck.
func (q *SQLiteStore) Touch(ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	_, err := q.db.Exec(`
		UPDATE sync_tasks SET updated_at = datetime('now')
		WHERE status = 'processing' AND id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+`)`, args...)
	return err
}

// HasTask reports whether the task is still in the queue.
func (q *SQLiteStore) HasTask(virtualBucket, objectKey, source, target string) (bool, error) {
	var one int
	err := q.db.QueryRow(`
		SELECT 1 FROM sync_tasks
		WHERE virtual_bucket = ? AND object_key = ? AND source_backend_name = ? AND target_backend_name = ?
		LIMIT 1`, virtualBucket, objectKey, source, target).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// RetryFailed resets failed tasks back to pending for retry.
// Only retries tasks that have been failed for at least retryInterval (doubling with every
// failure of the same task, capped at RetryBackoffCap),
// and that were created less than maxAge ago.
func (q *SQLiteStore) RetryFailed(retryInterval, maxAge time.Duration) (int64, error) {
	retrySeconds := int(retryInterval.Seconds())
	if retrySeconds < 1 {
		retrySeconds = 0
	}
	res, err := q.db.Exec(`
		UPDATE sync_tasks
		SET status = 'pending', updated_at = datetime('now')
		WHERE status = 'failed'
		AND datetime(updated_at) <= datetime('now', '-' || MIN(? * (1 << MIN(MAX(attempts - 1, 0), 20)), ?) || ' seconds')
		AND (datetime(created_at) > datetime('now', ?) OR source_backend_name LIKE 'DELETE:%')`,
		retrySeconds,
		int(RetryBackoffCap.Seconds()),
		fmt.Sprintf("-%d seconds", int(maxAge.Seconds())),
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PurgeExpired deletes failed tasks older than maxAge.
func (q *SQLiteStore) PurgeExpired(maxAge time.Duration) (int64, error) {
	res, err := q.db.Exec(`
		DELETE FROM sync_tasks
		WHERE status = 'failed'
		AND source_backend_name NOT LIKE 'DELETE:%'
		AND datetime(created_at) < datetime('now', ?)`,
		fmt.Sprintf("-%d seconds", int(maxAge.Seconds())),
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ResetStuckProcessing resets tasks stuck in 'processing' state back to 'pending'
// This handles tasks that were being processed when the server crashed
func (q *SQLiteStore) ResetStuckProcessing(stuckThreshold time.Duration) (int64, error) {
	res, err := q.db.Exec(`
		UPDATE sync_tasks
		SET status = 'pending', dirty = 0, updated_at = datetime('now')
		WHERE status = 'processing'
		AND datetime(updated_at) < datetime('now', ?)`,
		fmt.Sprintf("-%d seconds", int(stuckThreshold.Seconds())))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (q *SQLiteStore) count(status string) (int64, error) {
	var n int64
	err := q.db.QueryRow(`SELECT COUNT(*) FROM sync_tasks WHERE status = ?`, status).Scan(&n)
	return n, err
}

func (q *SQLiteStore) PendingCount() (int64, error)    { return q.count(StatusPending) }
func (q *SQLiteStore) ProcessingCount() (int64, error) { return q.count(StatusProcessing) }
func (q *SQLiteStore) FailedCount() (int64, error)     { return q.count(StatusFailed) }

func (q *SQLiteStore) OldestPendingAge() (time.Duration, error) {
	var secs int64
	err := q.db.QueryRow(`
		SELECT COALESCE(CAST(strftime('%s','now') AS INTEGER) - CAST(strftime('%s', MIN(created_at)) AS INTEGER), 0)
		FROM sync_tasks WHERE status = 'pending'`).Scan(&secs)
	if err != nil {
		return 0, err
	}
	return time.Duration(secs) * time.Second, nil
}
