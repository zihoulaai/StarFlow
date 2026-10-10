package controlplane

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	docEntityHeading = regexp.MustCompile("(?m)^### `([a-z_]+)`\\s*$")
	docTableRow      = regexp.MustCompile("(?m)^\\| `([^|]+)` \\|")
)

// TestDocumentedEntitiesAndColumnsExistInSchema keeps docs/control-plane.md
// honest.
//
// A control-plane document that drifts from the schema is worse than no
// document: an operator reading it will write queries against columns that do
// not exist. Every entity heading and every first-column field named in the
// document is checked against the live schema, and the reverse is checked too —
// a column added to the schema but not documented is also drift.
func TestDocumentedEntitiesAndColumnsExistInSchema(t *testing.T) {
	db := openTestDB(t)
	if err := Migrate(context.Background(), db, zeroTime()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	doc := readDoc(t, filepath.Join("..", "..", "docs", "control-plane.md"))

	documented := map[string]map[string]bool{}
	current := ""
	seenRow := false
	for _, line := range strings.Split(doc, "\n") {
		if m := docEntityHeading.FindStringSubmatch(line); m != nil {
			current = m[1]
			documented[current] = map[string]bool{}
			seenRow = false
			continue
		}
		if current == "" {
			continue
		}
		m := docTableRow.FindStringSubmatch(line)
		if m == nil {
			// A blank line closes the entity's table, but only once the table
			// has actually started: an entity heading is followed by prose
			// before its column table, and a blank line there must not end the
			// section before any row is seen.
			if strings.TrimSpace(line) == "" && seenRow {
				current = ""
			}
			continue
		}
		seenRow = true
		for _, field := range strings.Split(m[1], ",") {
			field = strings.Trim(strings.TrimSpace(field), "`")
			if field == "" {
				continue
			}
			documented[current][field] = true
		}
	}

	if len(documented) == 0 {
		t.Fatal("no entities parsed from the document; the parser and the document disagree")
	}

	for table, fields := range documented {
		actual := schemaColumns(t, db, table)
		if len(actual) == 0 {
			t.Errorf("documented entity %q does not exist in the schema", table)
			continue
		}
		for field := range fields {
			if !actual[field] {
				t.Errorf("%s.%s is documented but not in the schema", table, field)
			}
		}
		for column := range actual {
			if !fields[column] {
				t.Errorf("%s.%s exists in the schema but is not documented", table, column)
			}
		}
	}
}

// Every table the architecture depends on must be documented. This is the
// converse check: a new entity added to the migration without a section in the
// document is drift in the other direction.
func TestEverySchemaEntityIsDocumented(t *testing.T) {
	db := openTestDB(t)
	if err := Migrate(context.Background(), db, zeroTime()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	doc := readDoc(t, filepath.Join("..", "..", "docs", "control-plane.md"))

	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()

	var undocumented []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		// schema_migrations is bookkeeping, not part of the data model.
		if name == "schema_migrations" {
			continue
		}
		if !strings.Contains(doc, "`"+name+"`") {
			undocumented = append(undocumented, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
	if len(undocumented) > 0 {
		t.Errorf("schema entities with no documentation: %v", undocumented)
	}
}

// The document makes a specific claim about the column defaults: enabled and
// redact columns default to 1. If that changes in the schema, the repository
// defaults must change with it, or a struct-literal caller will silently store
// the opposite of what the schema says.
func TestDocumentedColumnDefaultsMatchSchema(t *testing.T) {
	db := openTestDB(t)
	if err := Migrate(context.Background(), db, zeroTime()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	wantDefaultOne := map[string][]string{
		"applications":     {"can_publish", "can_subscribe", "enabled"},
		"channel_config":   {"coalescing_enabled"},
		"tenants":          {"redact_audit_bodies"},
		"moderation_rules": {"enabled"},
		"network_blocks":   {"enabled"},
		"admin_principals": {"enabled"},
		"tenant_quotas":    {"duplicate_content_enabled", "promo_pattern_enabled"},
	}

	for table, columns := range wantDefaultOne {
		for _, column := range columns {
			def := columnDefault(t, db, table, column)
			if def != "1" {
				t.Errorf("%s.%s default = %q, want 1", table, column, def)
			}
		}
	}

	// And the zero-means-infinity columns the document describes.
	for table, column := range map[string]string{
		"subject_restrictions": "expires_at_ms",
		"channel_config":       "max_subscribers",
		"applications":         "secret_prev_valid_until_ms",
	} {
		def := columnDefault(t, db, table, column)
		if def != "0" {
			t.Errorf("%s.%s default = %q, want 0", table, column, def)
		}
	}
}

func zeroTime() time.Time { return time.UnixMilli(0) }

func readDoc(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func schemaColumns(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()

	cols := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name, typ  string
			notNull    int
			dflt       sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &primaryKey); err != nil {
			t.Fatalf("scan PRAGMA table_info(%s): %v", table, err)
		}
		cols[name] = true
	}
	return cols
}

func columnDefault(t *testing.T, db *sql.DB, table, column string) string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name, typ  string
			notNull    int
			dflt       sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &primaryKey); err != nil {
			t.Fatalf("scan PRAGMA table_info(%s): %v", table, err)
		}
		if name == column {
			return dflt.String
		}
	}
	return ""
}
