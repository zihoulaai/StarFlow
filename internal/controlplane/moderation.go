package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// UpsertRule writes a moderation rule.
//
// A rule supplied without an id is being created, and is enabled by default to
// match the column default. A caller that builds a rule with a struct literal
// and omits Enabled would otherwise store a rule that no gateway compiles into
// its automaton, which looks like a broken word list rather than a disabled
// one.
func (s *Store) UpsertRule(ctx context.Context, r ModerationRule) (ModerationRule, error) {
	if r.TenantID == "" || r.Pattern == "" {
		return ModerationRule{}, fmt.Errorf("controlplane: rule requires a tenant id and a pattern")
	}
	creating := r.ID == ""
	if creating {
		id, err := NewID()
		if err != nil {
			return ModerationRule{}, err
		}
		r.ID = id
		r.Enabled = true
	}
	if r.Action == "" {
		r.Action = ActionBlock
	}
	if r.Kind == "" {
		r.Kind = RuleKindSensitiveWord
	}
	r.UpdatedAt = s.now()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO moderation_rules
		   (id, tenant_id, kind, pattern, action, replacement, tag, enabled, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   kind = excluded.kind,
		   pattern = excluded.pattern,
		   action = excluded.action,
		   replacement = excluded.replacement,
		   tag = excluded.tag,
		   enabled = excluded.enabled,
		   updated_at_ms = excluded.updated_at_ms`,
		r.ID, r.TenantID, r.Kind, r.Pattern, r.Action, r.Replacement, r.Tag,
		boolToInt(r.Enabled), r.UpdatedAt.UnixMilli())
	if err != nil {
		return ModerationRule{}, fmt.Errorf("controlplane: upsert rule: %w", err)
	}
	return r, nil
}

// ListRules returns the rules of one tenant. Sensitive word rules are the ones
// the gateway compiles into its automaton, so ordering matters twice over:
// replacement rules are evaluated before block rules, because a block rule for a
// shorter string would otherwise shadow a replacement for a longer one; and
// same-action rules are ordered by pattern so that rebuilding the automaton
// from the same rule set yields an identical machine rather than one whose
// match precedence depends on random primary keys.
func (s *Store) ListRules(ctx context.Context, tenantID string) ([]ModerationRule, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, kind, pattern, action, replacement, tag, enabled, updated_at_ms
		 FROM moderation_rules
		 WHERE tenant_id = ?
		 ORDER BY CASE action WHEN 'replace' THEN 0 WHEN 'tag' THEN 1 ELSE 2 END, pattern, id`,
		tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list rules: %w", err)
	}
	defer rows.Close()

	var out []ModerationRule
	for rows.Next() {
		var r ModerationRule
		var enabled int
		var updatedMs int64
		if err := rows.Scan(&r.ID, &r.TenantID, &r.Kind, &r.Pattern, &r.Action,
			&r.Replacement, &r.Tag, &enabled, &updatedMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan rule: %w", err)
		}
		r.Enabled = enabled != 0
		r.UpdatedAt = time.UnixMilli(updatedMs)
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteRule removes a rule from one tenant.
func (s *Store) DeleteRule(ctx context.Context, tenantID, ruleID string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM moderation_rules WHERE tenant_id = ? AND id = ?`, tenantID, ruleID)
	if err != nil {
		return fmt.Errorf("controlplane: delete rule: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// CreateRestriction records a mute or ban. A zero expiresAt records a
// restriction that does not lapse.
func (s *Store) CreateRestriction(ctx context.Context, r Restriction) (Restriction, error) {
	if r.TenantID == "" || r.SubjectID == "" || r.SubjectKind == "" || r.Action == "" {
		return Restriction{}, fmt.Errorf("controlplane: restriction requires tenant, subject kind, subject id and action")
	}
	if r.ID == "" {
		id, err := NewID()
		if err != nil {
			return Restriction{}, err
		}
		r.ID = id
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = s.now()
	}
	var expiresMs int64
	if !r.ExpiresAt.IsZero() {
		expiresMs = r.ExpiresAt.UnixMilli()
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO subject_restrictions
		   (id, tenant_id, subject_kind, subject_id, action, reason, expires_at_ms, created_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.TenantID, r.SubjectKind, r.SubjectID, r.Action, r.Reason,
		expiresMs, r.CreatedAt.UnixMilli())
	if err != nil {
		return Restriction{}, fmt.Errorf("controlplane: create restriction: %w", err)
	}
	return r, nil
}

// ListRestrictions returns every restriction recorded for a tenant, including
// lapsed ones. Callers filter by Active; keeping the history lets an operator
// see that a mute was applied even after it expired.
func (s *Store) ListRestrictions(ctx context.Context, tenantID string) ([]Restriction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, subject_kind, subject_id, action, reason, expires_at_ms, created_at_ms
		 FROM subject_restrictions WHERE tenant_id = ? ORDER BY created_at_ms, id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list restrictions: %w", err)
	}
	defer rows.Close()

	var out []Restriction
	for rows.Next() {
		var r Restriction
		var expiresMs, createdMs int64
		if err := rows.Scan(&r.ID, &r.TenantID, &r.SubjectKind, &r.SubjectID,
			&r.Action, &r.Reason, &expiresMs, &createdMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan restriction: %w", err)
		}
		if expiresMs != 0 {
			r.ExpiresAt = time.UnixMilli(expiresMs)
		}
		r.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActiveRestrictionsFor returns the restrictions in force for one subject at
// now. It is the query the publish path's ban check resolves from, so it must
// exclude lapsed rows rather than filtering them in Go.
func (s *Store) ActiveRestrictionsFor(ctx context.Context, tenantID, kind, subjectID string, now time.Time) ([]Restriction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, subject_kind, subject_id, action, reason, expires_at_ms, created_at_ms
		 FROM subject_restrictions
		 WHERE tenant_id = ? AND subject_kind = ? AND subject_id = ?
		   AND (expires_at_ms = 0 OR expires_at_ms > ?)`,
		tenantID, kind, subjectID, now.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("controlplane: active restrictions: %w", err)
	}
	defer rows.Close()

	var out []Restriction
	for rows.Next() {
		var r Restriction
		var expiresMs, createdMs int64
		if err := rows.Scan(&r.ID, &r.TenantID, &r.SubjectKind, &r.SubjectID,
			&r.Action, &r.Reason, &expiresMs, &createdMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan restriction: %w", err)
		}
		if expiresMs != 0 {
			r.ExpiresAt = time.UnixMilli(expiresMs)
		}
		r.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteRestriction lifts a restriction.
func (s *Store) DeleteRestriction(ctx context.Context, tenantID, id string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM subject_restrictions WHERE tenant_id = ? AND id = ?`, tenantID, id)
	if err != nil {
		return fmt.Errorf("controlplane: delete restriction: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// CreateNetworkBlock records a blocked address range.
//
// Enabled is defaulted to true to match the column default: a block that is
// stored disabled is a block that never takes effect while still appearing in
// the operator's list.
func (s *Store) CreateNetworkBlock(ctx context.Context, b NetworkBlock) (NetworkBlock, error) {
	if b.TenantID == "" || b.CIDR == "" {
		return NetworkBlock{}, fmt.Errorf("controlplane: network block requires a tenant id and a cidr")
	}
	if b.ID == "" {
		id, err := NewID()
		if err != nil {
			return NetworkBlock{}, err
		}
		b.ID = id
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = s.now()
	}
	b.Enabled = true
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO network_blocks (id, tenant_id, cidr, reason, enabled, created_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		b.ID, b.TenantID, b.CIDR, b.Reason, boolToInt(b.Enabled), b.CreatedAt.UnixMilli())
	if err != nil {
		return NetworkBlock{}, fmt.Errorf("controlplane: create network block: %w", err)
	}
	return b, nil
}

// ListNetworkBlocks returns the enabled blocks of one tenant.
func (s *Store) ListNetworkBlocks(ctx context.Context, tenantID string) ([]NetworkBlock, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, cidr, reason, enabled, created_at_ms
		 FROM network_blocks WHERE tenant_id = ? AND enabled = 1 ORDER BY id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list network blocks: %w", err)
	}
	defer rows.Close()

	var out []NetworkBlock
	for rows.Next() {
		var b NetworkBlock
		var enabled int
		var createdMs int64
		if err := rows.Scan(&b.ID, &b.TenantID, &b.CIDR, &b.Reason, &enabled, &createdMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan network block: %w", err)
		}
		b.Enabled = enabled != 0
		b.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, b)
	}
	return out, rows.Err()
}

// DeleteNetworkBlock lifts a network block.
func (s *Store) DeleteNetworkBlock(ctx context.Context, tenantID, id string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM network_blocks WHERE tenant_id = ? AND id = ?`, tenantID, id)
	if err != nil {
		return fmt.Errorf("controlplane: delete network block: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpsertAdminPrincipal grants or updates an administrative principal.
//
// Enabled is defaulted to true for a principal that does not exist yet. Go's
// zero value for the field is false, so without this a caller that fills in a
// principal's identity and role but omits Enabled would silently create a
// grant with no authority, and the failure would only surface as a refused
// administrative call. Use SetAdminPrincipalEnabled to withdraw access.
func (s *Store) UpsertAdminPrincipal(ctx context.Context, p AdminPrincipal) (AdminPrincipal, error) {
	if p.TenantID == "" || p.Subject == "" {
		return AdminPrincipal{}, fmt.Errorf("controlplane: admin principal requires a tenant id and a subject")
	}
	if p.Role == "" {
		p.Role = RoleViewer
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = s.now()
	}

	_, exists, err := s.adminPrincipalExists(ctx, p.TenantID, p.Subject)
	if err != nil {
		return AdminPrincipal{}, err
	}
	if !exists {
		id, err := NewID()
		if err != nil {
			return AdminPrincipal{}, err
		}
		p.ID = id
		p.Enabled = true
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO admin_principals (id, tenant_id, subject, role, enabled, created_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, subject) DO UPDATE SET
		   role = excluded.role,
		   enabled = excluded.enabled`,
		p.ID, p.TenantID, p.Subject, p.Role, boolToInt(p.Enabled), p.CreatedAt.UnixMilli())
	if err != nil {
		return AdminPrincipal{}, fmt.Errorf("controlplane: upsert admin principal: %w", err)
	}
	return p, nil
}

// SetAdminPrincipalEnabled grants or withdraws administrative access without
// disturbing the principal's role.
func (s *Store) SetAdminPrincipalEnabled(ctx context.Context, tenantID, subject string, enabled bool) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE admin_principals SET enabled = ? WHERE tenant_id = ? AND subject = ?`,
		boolToInt(enabled), tenantID, subject)
	if err != nil {
		return fmt.Errorf("controlplane: set admin principal enabled: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) adminPrincipalExists(ctx context.Context, tenantID, subject string) (string, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx,
		`SELECT id FROM admin_principals WHERE tenant_id = ? AND subject = ?`, tenantID, subject).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("controlplane: check admin principal: %w", err)
	}
	return id, true, nil
}

// GetAdminPrincipal resolves a caller within a tenant.
func (s *Store) GetAdminPrincipal(ctx context.Context, tenantID, subject string) (AdminPrincipal, error) {
	var p AdminPrincipal
	var enabled int
	var createdMs int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, subject, role, enabled, created_at_ms
		 FROM admin_principals WHERE tenant_id = ? AND subject = ?`, tenantID, subject).
		Scan(&p.ID, &p.TenantID, &p.Subject, &p.Role, &enabled, &createdMs)
	if errors.Is(err, sql.ErrNoRows) {
		return AdminPrincipal{}, ErrNotFound
	}
	if err != nil {
		return AdminPrincipal{}, fmt.Errorf("controlplane: get admin principal: %w", err)
	}
	p.Enabled = enabled != 0
	p.CreatedAt = time.UnixMilli(createdMs)
	return p, nil
}

// MayAdminister reports whether the principal may perform mutating
// administrative operations.
func (s *Store) MayAdminister(ctx context.Context, tenantID, subject string) (bool, error) {
	p, err := s.GetAdminPrincipal(ctx, tenantID, subject)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return p.Enabled && p.Role == RoleAdmin, nil
}
