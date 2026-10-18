package taskstate

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteContract(t *testing.T) {
	runContract(t, func(t *testing.T) harness {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		s, err := NewSQLite(db)
		if err != nil {
			t.Fatal(err)
		}
		return harness{Store: s, Advance: func(d time.Duration) {
			secs := int(d.Seconds())
			if secs == 0 {
				return
			}
			mod := fmt.Sprintf("-%d seconds", secs)
			if _, err := db.Exec(`UPDATE managed_tasks SET created_at = datetime(created_at, ?), updated_at = datetime(updated_at, ?)`, mod, mod); err != nil {
				t.Fatal(err)
			}
		}}
	})
}

// A database written by the pre-taskstate version must keep working (no owner column).
func TestSQLiteOpensLegacyTable(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE managed_tasks (
		id TEXT PRIMARY KEY, type TEXT NOT NULL, bucket TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'running', total_objects INTEGER NOT NULL DEFAULT 0,
		processed_objects INTEGER NOT NULL DEFAULT 0, current_chunk INTEGER NOT NULL DEFAULT 0,
		error TEXT NOT NULL DEFAULT '', created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		updated_at DATETIME NOT NULL DEFAULT (datetime('now')))`)
	must(t, err)
	_, err = db.Exec(`INSERT INTO managed_tasks (id, type, bucket, status) VALUES ('old', 'resync', 'b', 'completed')`)
	must(t, err)
	s, err := NewSQLite(db)
	must(t, err)
	if st, err := s.Get("old"); err != nil || st.Status != StatusCompleted {
		t.Fatalf("legacy row: %v %+v", err, st)
	}
	must(t, s.Create("new", "bisync", "b", "pod"))
}
