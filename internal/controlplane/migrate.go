// Package controlplane owns the durable state of the service: tenants and their
// credentials, quotas, per-channel configuration, moderation rules, subject
// restrictions, network blocks, and the audit trail.
//
// None of this state is read on the message delivery path. The gateway reads
// configuration at startup and again on change notification; the publish and
// delivery paths hold their own in-memory copies precisely so that admitting a
// message never waits on this database.
package controlplane

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Migration is a single forward-only schema step.
type Migration struct {
	Version int
	Name    string
	// Statements are executed in order inside one transaction per migration.
	Statements []string
}

// migrations is the ordered schema history. Entries are never edited once
// applied; a change to the schema is a new entry with a higher version.
var migrations = []Migration{
	{
		Version: 1,
		Name:    "initial",
		Statements: []string{
			`CREATE TABLE tenants (
				id                     TEXT PRIMARY KEY,
				name                   TEXT NOT NULL,
				audit_retention_days   INTEGER NOT NULL DEFAULT 30,
				redact_audit_bodies    INTEGER NOT NULL DEFAULT 1,
				created_at_ms          INTEGER NOT NULL
			)`,

			// secret_hash stores a keyed digest, never the secret itself. A
			// rotation leaves the previous digest in place only for the length
			// of secret_prev_valid_until_ms, which is zero by default, so a
			// rotated-out secret is rejected immediately.
			`CREATE TABLE applications (
				id                        TEXT PRIMARY KEY,
				tenant_id                 TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				name                      TEXT NOT NULL,
				app_key                   TEXT NOT NULL UNIQUE,
				secret_hash               TEXT NOT NULL,
				secret_prev_hash          TEXT,
				secret_prev_valid_until_ms INTEGER NOT NULL DEFAULT 0,
				can_publish               INTEGER NOT NULL DEFAULT 1,
				can_subscribe             INTEGER NOT NULL DEFAULT 1,
				enabled                   INTEGER NOT NULL DEFAULT 1,
				created_at_ms             INTEGER NOT NULL
			)`,
			`CREATE INDEX idx_applications_tenant ON applications(tenant_id)`,

			// A NULL in an override column means "derive it", not "zero". The
			// derived publish ceiling comes from subscriber count, which is
			// runtime state the gateway owns, so the store must express absence
			// rather than a value.
			`CREATE TABLE channel_config (
				tenant_id                TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				channel                  TEXT NOT NULL,
				publish_ceiling_override_qps REAL,
				receive_rate_target       REAL,
				merge_ratio_override      INTEGER,
				coalescing_enabled        INTEGER NOT NULL DEFAULT 1,
				max_subscribers           INTEGER NOT NULL DEFAULT 0,
				updated_at_ms             INTEGER NOT NULL,
				PRIMARY KEY (tenant_id, channel)
			)`,

			`CREATE TABLE tenant_quotas (
				tenant_id                    TEXT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
				max_connections              INTEGER NOT NULL,
				max_publish_qps              INTEGER NOT NULL,
				max_channels                 INTEGER NOT NULL,
				publish_failure_policy       TEXT NOT NULL DEFAULT 'fail_fast',
				per_user_publish_qps         INTEGER NOT NULL DEFAULT 10,
				duplicate_window_ms          INTEGER NOT NULL DEFAULT 3000,
				duplicate_content_enabled    INTEGER NOT NULL DEFAULT 1,
				promo_pattern_enabled        INTEGER NOT NULL DEFAULT 1,
				updated_at_ms                INTEGER NOT NULL
			)`,

			`CREATE TABLE moderation_rules (
				id            TEXT PRIMARY KEY,
				tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				kind          TEXT NOT NULL,
				pattern       TEXT NOT NULL,
				action        TEXT NOT NULL,
				replacement   TEXT NOT NULL DEFAULT '',
				tag           TEXT NOT NULL DEFAULT '',
				enabled       INTEGER NOT NULL DEFAULT 1,
				updated_at_ms INTEGER NOT NULL
			)`,
			`CREATE INDEX idx_moderation_rules_tenant ON moderation_rules(tenant_id)`,

			// expires_at_ms = 0 means the restriction does not expire, which is
			// how a permanent ban differs from a mute. The column default
			// matches the convention so an omitted value can never silently
			// become a mute that expires at the epoch.
			`CREATE TABLE subject_restrictions (
				id            TEXT PRIMARY KEY,
				tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				subject_kind  TEXT NOT NULL,
				subject_id    TEXT NOT NULL,
				action        TEXT NOT NULL,
				reason        TEXT NOT NULL DEFAULT '',
				expires_at_ms INTEGER NOT NULL DEFAULT 0,
				created_at_ms INTEGER NOT NULL
			)`,
			`CREATE INDEX idx_restrictions_subject
				ON subject_restrictions(tenant_id, subject_kind, subject_id)`,

			`CREATE TABLE network_blocks (
				id            TEXT PRIMARY KEY,
				tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				cidr          TEXT NOT NULL,
				reason        TEXT NOT NULL DEFAULT '',
				enabled       INTEGER NOT NULL DEFAULT 1,
				created_at_ms INTEGER NOT NULL
			)`,
			`CREATE INDEX idx_network_blocks_tenant ON network_blocks(tenant_id)`,

			`CREATE TABLE admin_principals (
				id            TEXT PRIMARY KEY,
				tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
				subject       TEXT NOT NULL,
				role          TEXT NOT NULL,
				enabled       INTEGER NOT NULL DEFAULT 1,
				created_at_ms INTEGER NOT NULL
			)`,
			`CREATE UNIQUE INDEX idx_admin_principal ON admin_principals(tenant_id, subject)`,

			// body is stored already redacted, decided by the writer using the
			// tenant's redact_audit_bodies setting. Redacting at rest rather than
			// at read time means a mis-scoped query cannot surface content.
			`CREATE TABLE audit_entries (
				id         INTEGER PRIMARY KEY AUTOINCREMENT,
				tenant_id  TEXT NOT NULL,
				at_ms      INTEGER NOT NULL,
				kind       TEXT NOT NULL,
				subject    TEXT NOT NULL DEFAULT '',
				channel    TEXT NOT NULL DEFAULT '',
				message_id TEXT NOT NULL DEFAULT '',
				rule_id    TEXT NOT NULL DEFAULT '',
				decision   TEXT NOT NULL,
				body       TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX idx_audit_tenant_time ON audit_entries(tenant_id, at_ms)`,
		},
	},
}

// Migrate brings db up to the latest schema version.
//
// It is safe to call on every process start: already-applied versions are
// skipped, so a second call makes no changes.
func Migrate(ctx context.Context, db *sql.DB, now time.Time) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version       INTEGER PRIMARY KEY,
		name          TEXT NOT NULL,
		applied_at_ms INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("controlplane: create migration table: %w", err)
	}

	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("controlplane: begin migration %d: %w", m.Version, err)
		}
		for _, stmt := range m.Statements {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("controlplane: migration %d (%s): %w\n%s",
					m.Version, m.Name, err, stmt)
			}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, name, applied_at_ms) VALUES (?, ?, ?)`,
			m.Version, m.Name, now.UnixMilli()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("controlplane: record migration %d: %w", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("controlplane: commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// CurrentVersion reports the highest schema version this build expects.
func CurrentVersion() int {
	highest := 0
	for _, m := range migrations {
		if m.Version > highest {
			highest = m.Version
		}
	}
	return highest
}

// AppliedVersion reports the highest version actually present in db, or zero.
func AppliedVersion(ctx context.Context, db *sql.DB) (int, error) {
	versions, err := appliedVersions(ctx, db)
	if err != nil {
		return 0, err
	}
	highest := 0
	for v := range versions {
		if v > highest {
			highest = v
		}
	}
	return highest, nil
}

func appliedVersions(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("controlplane: read applied migrations: %w", err)
	}
	defer rows.Close()

	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("controlplane: scan applied migration: %w", err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}
