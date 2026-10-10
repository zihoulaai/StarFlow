package auth

import (
	"database/sql"
	"path/filepath"
	"testing"

	// Registers the "sqlite" driver used by the control-plane tests.
	_ "modernc.org/sqlite"
)

// openDB returns a migrated, file-backed control-plane database for one test.
//
// It is file-backed rather than ":memory:" because these tests reopen the
// database through the control-plane loader and would otherwise get a pool of
// separate empty in-memory databases.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "auth.db") + "?_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
