package taskstate

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS managed_tasks (
	id TEXT PRIMARY KEY,
	type TEXT NOT NULL,
	bucket TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'running',
	total_objects INTEGER NOT NULL DEFAULT 0,
	processed_objects INTEGER NOT NULL DEFAULT 0,
	current_chunk INTEGER NOT NULL DEFAULT 0,
	error TEXT NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT (datetime('now')),
	updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_managed_tasks_status ON managed_tasks (status);
CREATE INDEX IF NOT EXISTS idx_managed_tasks_created ON managed_tasks (created_at DESC);
`

const selectCols = `id, type, bucket, status, total_objects, processed_objects, current_chunk, error, owner, created_at, updated_at`

// SQLiteStore is the single-instance Store.
type SQLiteStore struct{ db *sql.DB }

// NewSQLite creates the table if needed and migrates tables from older versions.
func NewSQLite(db *sql.DB) (*SQLiteStore, error) {
	if _, err := db.Exec(sqliteSchema); err != nil {
		return nil, fmt.Errorf("init managed_tasks schema: %w", err)
	}
	// Error ignored on purpose: fails with "duplicate column" once migrated
	_, _ = db.Exec(`ALTER TABLE managed_tasks ADD COLUMN owner TEXT NOT NULL DEFAULT ''`)
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Create(id, taskType, bucket, owner string) error {
	// One statement, so the check and the insert cannot interleave with another Create.
	res, err := s.db.Exec(`INSERT INTO managed_tasks (id, type, bucket, status, owner)
		SELECT ?, ?, ?, 'running', ?
		WHERE NOT EXISTS (SELECT 1 FROM managed_tasks WHERE type = ? AND bucket = ? AND status = 'running')`,
		id, taskType, bucket, owner, taskType, bucket)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var existing string
		if err := s.db.QueryRow(`SELECT id FROM managed_tasks WHERE type = ? AND bucket = ? AND status = 'running' LIMIT 1`,
			taskType, bucket).Scan(&existing); err != nil {
			return fmt.Errorf("create task: rejected but no running task found: %w", err)
		}
		return &ConflictError{Type: taskType, Bucket: bucket, ExistingID: existing}
	}
	return nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanStatus(r rowScanner) (*Status, error) {
	var st Status
	err := r.Scan(&st.ID, &st.Type, &st.Bucket, &st.Status, &st.TotalObjects, &st.ProcessedObjects,
		&st.CurrentChunk, &st.Error, &st.Owner, &st.CreatedAt, &st.UpdatedAt)
	return &st, err
}

func (s *SQLiteStore) Get(id string) (*Status, error) {
	st, err := scanStatus(s.db.QueryRow(`SELECT `+selectCols+` FROM managed_tasks WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query task: %w", err)
	}
	return st, nil
}

func (s *SQLiteStore) List() ([]*Status, error) {
	rows, err := s.db.Query(`SELECT ` + selectCols + ` FROM managed_tasks ORDER BY created_at DESC, rowid DESC`)
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()
	var out []*Status
	for rows.Next() {
		st, err := scanStatus(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) UpdateProgress(id string, processed, total, chunk int64) error {
	_, err := s.db.Exec(`UPDATE managed_tasks
		SET processed_objects = ?, total_objects = ?, current_chunk = ?, updated_at = datetime('now')
		WHERE id = ? AND status = 'running'`, processed, total, chunk, id)
	return err
}

func (s *SQLiteStore) Touch(id string) error {
	_, err := s.db.Exec(`UPDATE managed_tasks SET updated_at = datetime('now') WHERE id = ? AND status = 'running'`, id)
	return err
}

func (s *SQLiteStore) Finish(id, status, errMsg string) (bool, error) {
	res, err := s.db.Exec(`UPDATE managed_tasks SET status = ?, error = ?, updated_at = datetime('now')
		WHERE id = ? AND status = 'running'`, status, errMsg, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// RequestCancel: in single mode the task runs in this process, which cancels it directly.
func (s *SQLiteStore) RequestCancel(string) error { return ErrNotRunning }

func (s *SQLiteStore) CancelRequested(string) (bool, error) { return false, nil }

func (s *SQLiteStore) FailStale(olderThan time.Duration, reason string) (int64, error) {
	res, err := s.db.Exec(`UPDATE managed_tasks
		SET status = 'failed', error = ?, updated_at = datetime('now')
		WHERE status = 'running' AND datetime(updated_at) <= datetime('now', ?)`,
		reason, fmt.Sprintf("-%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLiteStore) Prune(keep int) error {
	_, err := s.db.Exec(`
		DELETE FROM managed_tasks
		WHERE status IN ('completed', 'failed', 'cancelled')
		AND id NOT IN (
			SELECT id FROM managed_tasks
			WHERE status IN ('completed', 'failed', 'cancelled')
			ORDER BY created_at DESC, rowid DESC
			LIMIT ?
		)`, keep)
	return err
}
