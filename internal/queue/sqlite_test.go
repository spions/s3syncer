package queue

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

func newSQLiteHarness(t *testing.T) harness {
	t.Helper()
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return harness{
		Store: NewSQLite(db),
		// SQLite timestamps have 1s resolution: moving time forward = moving rows into the past.
		Advance: func(d time.Duration) { shiftSQLiteTime(t, db, d) },
	}
}

func shiftSQLiteTime(t *testing.T, db *sql.DB, d time.Duration) {
	t.Helper()
	secs := int(d.Seconds())
	if secs == 0 {
		return
	}
	mod := fmt.Sprintf("-%d seconds", secs)
	if _, err := db.Exec(`UPDATE sync_tasks SET created_at = datetime(created_at, ?), updated_at = datetime(updated_at, ?)`, mod, mod); err != nil {
		t.Fatalf("advance: %v", err)
	}
}

func TestSQLiteStoreContract(t *testing.T) {
	runStoreContract(t, newSQLiteHarness)
}

func TestSQLiteMigratesLegacyDB(t *testing.T) {
	// A database created by an older version (no priority / dirty columns) must open and work.
	path := t.TempDir() + "/legacy.db"
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE sync_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		virtual_bucket TEXT NOT NULL, object_key TEXT NOT NULL,
		source_backend_name TEXT NOT NULL, target_backend_name TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
		last_error TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
		UNIQUE(virtual_bucket, object_key, source_backend_name, target_backend_name))`)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = legacy.Exec(`INSERT INTO sync_tasks (virtual_bucket, object_key, source_backend_name, target_backend_name) VALUES ('b','old','s','d')`)
	legacy.Close()

	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer db.Close()
	s := NewSQLite(db)
	mustEnqueue(t, s, "b", "new", "s", "d", PriorityHigh)
	assertKeys(t, mustDequeue(t, s, 10), "new", "old")
}

// Dequeue runs every worker cycle; its ORDER BY must be served by an index, otherwise
// SQLite sorts the entire pending set each time (a problem with a long backlog).
func TestSQLiteDequeueUsesIndexOrder(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`EXPLAIN QUERY PLAN
		SELECT id FROM sync_tasks WHERE status = 'pending'
		ORDER BY priority DESC, created_at ASC, id ASC LIMIT 500`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	joined := strings.Join(plan, " | ")
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("dequeue sorts in a temp b-tree instead of using an index: %s", joined)
	}
	if !strings.Contains(joined, "idx_sync_tasks_dequeue") {
		t.Fatalf("dequeue does not use the dequeue index: %s", joined)
	}
}
