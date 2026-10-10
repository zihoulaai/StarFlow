package controlplane

import (
	"context"
	"fmt"
	"time"
)

// AppendAudit records one decision.
//
// body is redacted by the caller before it reaches this method, because the
// tenant's retention and redaction preference lives alongside the tenant and
// resolving it here would mean a second read on a path that writes one row per
// moderated message.
func (s *Store) AppendAudit(ctx context.Context, e AuditEntry) error {
	if e.TenantID == "" {
		return fmt.Errorf("controlplane: audit entry requires a tenant id")
	}
	if e.Kind == "" || e.Decision == "" {
		return fmt.Errorf("controlplane: audit entry requires a kind and a decision")
	}
	if e.At.IsZero() {
		e.At = s.now()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_entries
		   (tenant_id, at_ms, kind, subject, channel, message_id, rule_id, decision, body)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.TenantID, e.At.UnixMilli(), e.Kind, e.Subject, e.Channel, e.MessageID,
		e.RuleID, e.Decision, e.Body)
	if err != nil {
		return fmt.Errorf("controlplane: append audit: %w", err)
	}
	return nil
}

// RedactBody returns the body to store for an entry, applying the tenant's
// redaction preference. A redacted entry records that content was withheld
// rather than storing an empty string, so an auditor can tell the two apart.
func RedactBody(tenant Tenant, body string) string {
	if tenant.RedactAuditBodies {
		return "[redacted]"
	}
	return body
}

// AuditQuery narrows an audit search.
type AuditQuery struct {
	TenantID string
	Kind     string
	Channel  string
	Subject  string
	Since    time.Time
	Until    time.Time
	Limit    int
}

// QueryAudit returns entries for one tenant only.
//
// Every predicate is optional except the tenant, which is required: an audit
// query that could reach another tenant's decisions would be a disclosure bug
// regardless of the filters supplied.
func (s *Store) QueryAudit(ctx context.Context, q AuditQuery) ([]AuditEntry, error) {
	if q.TenantID == "" {
		return nil, fmt.Errorf("controlplane: audit query requires a tenant id")
	}

	query := `SELECT id, tenant_id, at_ms, kind, subject, channel, message_id, rule_id, decision, body
	          FROM audit_entries WHERE tenant_id = ?`
	args := []any{q.TenantID}

	if q.Kind != "" {
		query += ` AND kind = ?`
		args = append(args, q.Kind)
	}
	if q.Channel != "" {
		query += ` AND channel = ?`
		args = append(args, q.Channel)
	}
	if q.Subject != "" {
		query += ` AND subject = ?`
		args = append(args, q.Subject)
	}
	if !q.Since.IsZero() {
		query += ` AND at_ms >= ?`
		args = append(args, q.Since.UnixMilli())
	}
	if !q.Until.IsZero() {
		query += ` AND at_ms < ?`
		args = append(args, q.Until.UnixMilli())
	}

	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		// An unbounded audit scan would be a denial-of-service vector against
		// the control plane.
		limit = 200
	}
	query += ` ORDER BY at_ms DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("controlplane: query audit: %w", err)
	}
	defer rows.Close()

	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var atMs int64
		if err := rows.Scan(&e.ID, &e.TenantID, &atMs, &e.Kind, &e.Subject, &e.Channel,
			&e.MessageID, &e.RuleID, &e.Decision, &e.Body); err != nil {
			return nil, fmt.Errorf("controlplane: scan audit: %w", err)
		}
		e.At = time.UnixMilli(atMs)
		out = append(out, e)
	}
	return out, rows.Err()
}

// PurgeAuditBefore deletes entries older than cutoff for one tenant and
// returns how many were removed, honouring the tenant's retention setting.
func (s *Store) PurgeAuditBefore(ctx context.Context, tenantID string, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM audit_entries WHERE tenant_id = ? AND at_ms < ?`,
		tenantID, cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("controlplane: purge audit: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("controlplane: purge audit rows affected: %w", err)
	}
	return n, nil
}

// RetentionCutoff returns the time before which a tenant's audit entries may
// be purged.
func RetentionCutoff(tenant Tenant, now time.Time) time.Time {
	days := tenant.AuditRetentionDays
	if days <= 0 {
		days = 30
	}
	return now.AddDate(0, 0, -days)
}
