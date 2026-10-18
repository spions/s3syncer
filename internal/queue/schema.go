package queue

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS sync_tasks (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	virtual_bucket TEXT NOT NULL,
	object_key TEXT NOT NULL,
	source_backend_name TEXT NOT NULL,
	target_backend_name TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'pending',
	attempts INTEGER NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	created_at DATETIME NOT NULL DEFAULT (datetime('now')),
	updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
	UNIQUE(virtual_bucket, object_key, source_backend_name, target_backend_name)
);

CREATE INDEX IF NOT EXISTS idx_sync_tasks_status ON sync_tasks (status);
`

func OpenDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	// Migration: add priority column if not exists
	// Error ignored on purpose: fails with "duplicate column" once migrated
	_, _ = db.Exec("ALTER TABLE sync_tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
	// dirty: task was re-enqueued while being processed, MarkDone must requeue it
	_, _ = db.Exec("ALTER TABLE sync_tasks ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0")
	// Dequeue picks pending tasks ordered by priority and age; without this index SQLite
	// sorts the whole pending set on every cycle, which hurts once the queue holds millions of rows.
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_sync_tasks_dequeue ON sync_tasks (status, priority DESC, created_at ASC, id ASC)"); err != nil {
		db.Close()
		return nil, fmt.Errorf("create dequeue index: %w", err)
	}
	return db, nil
}
