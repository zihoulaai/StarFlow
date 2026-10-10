package controlplane

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	// A file-backed database in the test's temp directory, rather than
	// ":memory:", so that migration re-runs open a genuinely separate
	// connection and are not served from a shared in-memory pool.
	dsn := filepath.Join(t.TempDir(), "controlplane.db") + "?_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// A single connection keeps SQLite's writer lock uncontended and makes the
	// migration re-run test deterministic.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrateAppliesCleanlyToEmptyDatabase(t *testing.T) {
	db := openTestDB(t)
	now := time.UnixMilli(1_700_000_000_000)

	if err := Migrate(context.Background(), db, now); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	got, err := AppliedVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("AppliedVersion: %v", err)
	}
	if got != CurrentVersion() {
		t.Errorf("applied version = %d, want %d", got, CurrentVersion())
	}

	// Every table the architecture depends on must exist.
	want := []string{
		"tenants", "applications", "channel_config", "tenant_quotas",
		"moderation_rules", "subject_restrictions", "network_blocks",
		"admin_principals", "audit_entries",
	}
	for _, table := range want {
		var name string
		err := db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Errorf("table %q missing after migrate: %v", table, err)
		}
	}
}

// Re-running migration on every process start must be a no-op, not an error.
func TestMigrateRerunIsNoOp(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)

	if err := Migrate(ctx, db, now); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := Migrate(ctx, db, now.Add(time.Hour)); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if err := Migrate(ctx, db, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("third Migrate: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count != len(migrations) {
		t.Errorf("recorded %d migrations, want %d", count, len(migrations))
	}

	// The recorded apply time must still be the first run's, proving the
	// second run wrote nothing.
	var appliedAt int64
	if err := db.QueryRow(
		`SELECT applied_at_ms FROM schema_migrations WHERE version = ?`, migrations[0].Version,
	).Scan(&appliedAt); err != nil {
		t.Fatalf("read applied_at: %v", err)
	}
	if appliedAt != now.UnixMilli() {
		t.Errorf("applied_at_ms = %d, want %d (first run's timestamp)", appliedAt, now.UnixMilli())
	}
}

// A second migration version must be applied incrementally rather than skipped,
// so a fresh database and a previously-migrated one converge.
func TestMigrateIsForwardOnlyAcrossVersions(t *testing.T) {
	original := migrations
	defer func() { migrations = original }()

	migrations = []Migration{
		{Version: 1, Name: "initial", Statements: original[0].Statements},
		{Version: 2, Name: "add-test-table", Statements: []string{
			`CREATE TABLE migration_probe (id TEXT PRIMARY KEY)`,
		}},
	}

	db := openTestDB(t)
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)

	if err := Migrate(ctx, db, now); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := AppliedVersion(ctx, db); err != nil {
		t.Fatalf("AppliedVersion: %v", err)
	}
	got, _ := AppliedVersion(ctx, db)
	if got != 2 {
		t.Fatalf("version = %d, want 2", got)
	}

	var name string
	if err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='migration_probe'`).Scan(&name); err != nil {
		t.Fatalf("second migration did not apply: %v", err)
	}
}

// A failing statement must roll back that migration entirely rather than leave
// a half-built schema behind.
func TestMigrateRollsBackFailedMigration(t *testing.T) {
	original := migrations
	defer func() { migrations = original }()

	migrations = []Migration{{
		Version: 1,
		Name:    "broken",
		Statements: []string{
			`CREATE TABLE first_ok (id TEXT PRIMARY KEY)`,
			`THIS IS NOT SQL`,
		},
	}}

	db := openTestDB(t)
	if err := Migrate(context.Background(), db, time.UnixMilli(0)); err == nil {
		t.Fatal("Migrate succeeded despite invalid statement")
	}

	var name string
	err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='first_ok'`).Scan(&name)
	if err == nil {
		t.Error("partial migration was left behind; expected rollback")
	}
}

func TestForeignKeysCascadeFromTenant(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, time.UnixMilli(0)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO tenants (id, name, created_at_ms) VALUES ('t1', 'Tenant One', 0)`); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO applications (id, tenant_id, name, app_key, secret_hash, created_at_ms)
		 VALUES ('a1', 't1', 'App', 'key-1', 'hash-1', 0)`); err != nil {
		t.Fatalf("insert application: %v", err)
	}

	// A row referencing an unknown tenant must be refused, so a typo cannot
	// create an orphaned configuration that no tenant's gateway will read.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO applications (id, tenant_id, name, app_key, secret_hash, created_at_ms)
		 VALUES ('a2', 'nope', 'App', 'key-2', 'hash-2', 0)`); err == nil {
		t.Error("application for a nonexistent tenant was accepted")
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM tenants WHERE id = 't1'`); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM applications WHERE tenant_id = 't1'`).Scan(&count); err != nil {
		t.Fatalf("count applications: %v", err)
	}
	if count != 0 {
		t.Errorf("applications survived tenant deletion: %d", count)
	}
}
