package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CreateTenant inserts a tenant and returns it.
//
// RedactAuditBodies is defaulted to true to match the column default. Go's zero
// value is false, so without this a tenant created with a literal struct would
// be stored with audit bodies retained while every other tenant's are not,
// purely because the caller did not think to set the field.
func (s *Store) CreateTenant(ctx context.Context, t Tenant) (Tenant, error) {
	if t.ID == "" {
		id, err := NewID()
		if err != nil {
			return Tenant{}, err
		}
		t.ID = id
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = s.now()
	}
	if t.AuditRetentionDays == 0 {
		t.AuditRetentionDays = 30
	}
	t.RedactAuditBodies = true

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenants (id, name, audit_retention_days, redact_audit_bodies, created_at_ms)
		 VALUES (?, ?, ?, ?, ?)`,
		t.ID, t.Name, t.AuditRetentionDays, boolToInt(t.RedactAuditBodies), t.CreatedAt.UnixMilli())
	if err != nil {
		return Tenant{}, fmt.Errorf("controlplane: create tenant: %w", err)
	}
	return t, nil
}

// GetTenant returns a tenant by id.
func (s *Store) GetTenant(ctx context.Context, id string) (Tenant, error) {
	var t Tenant
	var redact int
	var createdMs int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, audit_retention_days, redact_audit_bodies, created_at_ms
		 FROM tenants WHERE id = ?`, id).
		Scan(&t.ID, &t.Name, &t.AuditRetentionDays, &redact, &createdMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("controlplane: get tenant: %w", err)
	}
	t.RedactAuditBodies = redact != 0
	t.CreatedAt = time.UnixMilli(createdMs)
	return t, nil
}

// ListTenants returns every tenant, ordered by id.
func (s *Store) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, audit_retention_days, redact_audit_bodies, created_at_ms
		 FROM tenants ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list tenants: %w", err)
	}
	defer rows.Close()

	var out []Tenant
	for rows.Next() {
		var t Tenant
		var redact int
		var createdMs int64
		if err := rows.Scan(&t.ID, &t.Name, &t.AuditRetentionDays, &redact, &createdMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan tenant: %w", err)
		}
		t.RedactAuditBodies = redact != 0
		t.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTenant applies fn to a copy of the tenant and persists the result.
func (s *Store) UpdateTenant(ctx context.Context, id string, fn func(*Tenant)) (Tenant, error) {
	t, err := s.GetTenant(ctx, id)
	if err != nil {
		return Tenant{}, err
	}
	fn(&t)
	_, err = s.db.ExecContext(ctx,
		`UPDATE tenants SET name = ?, audit_retention_days = ?, redact_audit_bodies = ?
		 WHERE id = ?`,
		t.Name, t.AuditRetentionDays, boolToInt(t.RedactAuditBodies), id)
	if err != nil {
		return Tenant{}, fmt.Errorf("controlplane: update tenant: %w", err)
	}
	return t, nil
}

// CreateApplication inserts an application. The plaintext secret is returned
// once; only its digest is stored.
//
// The privilege and enabled flags are defaulted on to match the column
// defaults. Go's zero value is false, so a caller that fills in a tenant and a
// name would otherwise create a credential that is disabled and holds neither
// privilege — which looks like a misconfigured tenant rather than an unset
// flag.
func (s *Store) CreateApplication(ctx context.Context, a Application, plaintextSecret string) (Application, string, error) {
	if a.ID == "" {
		id, err := NewID()
		if err != nil {
			return Application{}, "", err
		}
		a.ID = id
	}
	if a.AppKey == "" {
		var err error
		a.AppKey, err = NewAppKey()
		if err != nil {
			return Application{}, "", err
		}
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = s.now()
	}
	a.Enabled = true
	if !a.CanPublish && !a.CanSubscribe {
		// An application with neither privilege could never do anything. The
		// least-privilege default that is still useful is subscribe-only, which
		// is also the common case for a browser client.
		a.CanSubscribe = true
	}

	// A secret is generated when the caller does not supply one.
	//
	// Hashing an empty string instead would store a credential that exists and
	// can never authenticate, and the caller would be handed an empty secret as
	// though that were a value to keep. The application would look configured
	// and fail every publish with a credential error, which reads like a network
	// fault rather than like a missing secret.
	if plaintextSecret == "" {
		var err error
		plaintextSecret, err = NewSecret()
		if err != nil {
			// NewSecret already says what failed; re-wrapping would repeat it.
			return Application{}, "", err
		}
	}
	a.SecretHash = HashSecret(plaintextSecret)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO applications
		   (id, tenant_id, name, app_key, secret_hash, secret_prev_hash, secret_prev_valid_until_ms,
		    can_publish, can_subscribe, enabled, created_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.TenantID, a.Name, a.AppKey, a.SecretHash, nil, int64(0),
		boolToInt(a.CanPublish), boolToInt(a.CanSubscribe), boolToInt(a.Enabled), a.CreatedAt.UnixMilli())
	if err != nil {
		return Application{}, "", fmt.Errorf("controlplane: create application: %w", err)
	}
	return a, plaintextSecret, nil
}

// NewAppKey returns a random application key.
func NewAppKey() (string, error) {
	var b [12]byte
	if _, err := randRead(b[:]); err != nil {
		return "", fmt.Errorf("controlplane: generate app key: %w", err)
	}
	return "ak_" + hexEncode(b[:]), nil
}

// NewSecret returns a random application secret.
func NewSecret() (string, error) {
	var b [32]byte
	if _, err := randRead(b[:]); err != nil {
		return "", fmt.Errorf("controlplane: generate secret: %w", err)
	}
	return hexEncode(b[:]), nil
}

// GetApplicationByAppKey resolves a credential. App keys are globally unique,
// so this lookup is deliberately not tenant-scoped: a client presents the key
// and the tenant is discovered from it.
func (s *Store) GetApplicationByAppKey(ctx context.Context, appKey string) (Application, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, name, app_key, secret_hash,
		        COALESCE(secret_prev_hash, ''), secret_prev_valid_until_ms,
		        can_publish, can_subscribe, enabled, created_at_ms
		 FROM applications WHERE app_key = ?`, appKey)

	var a Application
	var prevValid int64
	var canPub, canSub, enabled int
	var createdMs int64
	err := row.Scan(&a.ID, &a.TenantID, &a.Name, &a.AppKey, &a.SecretHash,
		&a.SecretPrevHash, &prevValid, &canPub, &canSub, &enabled, &createdMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Application{}, ErrNotFound
	}
	if err != nil {
		return Application{}, fmt.Errorf("controlplane: get application by key: %w", err)
	}
	a.SecretPrevValidUntil = time.UnixMilli(prevValid)
	a.CanPublish = canPub != 0
	a.CanSubscribe = canSub != 0
	a.Enabled = enabled != 0
	a.CreatedAt = time.UnixMilli(createdMs)
	return a, nil
}

// GetApplication returns an application scoped to its tenant.
func (s *Store) GetApplication(ctx context.Context, tenantID, appID string) (Application, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, name, app_key, secret_hash,
		        COALESCE(secret_prev_hash, ''), secret_prev_valid_until_ms,
		        can_publish, can_subscribe, enabled, created_at_ms
		 FROM applications WHERE tenant_id = ? AND id = ?`, tenantID, appID)

	var a Application
	var prevValid int64
	var canPub, canSub, enabled int
	var createdMs int64
	err := row.Scan(&a.ID, &a.TenantID, &a.Name, &a.AppKey, &a.SecretHash,
		&a.SecretPrevHash, &prevValid, &canPub, &canSub, &enabled, &createdMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Application{}, ErrNotFound
	}
	if err != nil {
		return Application{}, fmt.Errorf("controlplane: get application: %w", err)
	}
	a.SecretPrevValidUntil = time.UnixMilli(prevValid)
	a.CanPublish = canPub != 0
	a.CanSubscribe = canSub != 0
	a.Enabled = enabled != 0
	a.CreatedAt = time.UnixMilli(createdMs)
	return a, nil
}

// ListApplications returns the applications of one tenant only.
func (s *Store) ListApplications(ctx context.Context, tenantID string) ([]Application, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, name, app_key, secret_hash,
		        COALESCE(secret_prev_hash, ''), secret_prev_valid_until_ms,
		        can_publish, can_subscribe, enabled, created_at_ms
		 FROM applications WHERE tenant_id = ? ORDER BY id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list applications: %w", err)
	}
	defer rows.Close()

	var out []Application
	for rows.Next() {
		var a Application
		var prevValid int64
		var canPub, canSub, enabled int
		var createdMs int64
		if err := rows.Scan(&a.ID, &a.TenantID, &a.Name, &a.AppKey, &a.SecretHash,
			&a.SecretPrevHash, &prevValid, &canPub, &canSub, &enabled, &createdMs); err != nil {
			return nil, fmt.Errorf("controlplane: scan application: %w", err)
		}
		a.SecretPrevValidUntil = time.UnixMilli(prevValid)
		a.CanPublish = canPub != 0
		a.CanSubscribe = canSub != 0
		a.Enabled = enabled != 0
		a.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, a)
	}
	return out, rows.Err()
}

// UpdateApplication applies fn to a copy of the application and persists it.
func (s *Store) UpdateApplication(ctx context.Context, tenantID, appID string, fn func(*Application)) (Application, error) {
	a, err := s.GetApplication(ctx, tenantID, appID)
	if err != nil {
		return Application{}, err
	}
	fn(&a)
	_, err = s.db.ExecContext(ctx,
		`UPDATE applications
		 SET name = ?, can_publish = ?, can_subscribe = ?, enabled = ?
		 WHERE tenant_id = ? AND id = ?`,
		a.Name, boolToInt(a.CanPublish), boolToInt(a.CanSubscribe), boolToInt(a.Enabled),
		tenantID, appID)
	if err != nil {
		return Application{}, fmt.Errorf("controlplane: update application: %w", err)
	}
	return a, nil
}

// RotateApplicationSecret installs a new secret and returns its plaintext
// once.
//
// The previous digest is retained only for grace. A zero grace means the
// rotated-out secret stops verifying at the instant rotation is recorded,
// which is the default because a rotation is usually a response to a leak.
func (s *Store) RotateApplicationSecret(ctx context.Context, tenantID, appID string, grace time.Duration) (Application, string, error) {
	a, err := s.GetApplication(ctx, tenantID, appID)
	if err != nil {
		return Application{}, "", err
	}
	secret, err := NewSecret()
	if err != nil {
		return Application{}, "", err
	}

	a.SecretPrevHash = a.SecretHash
	a.SecretHash = HashSecret(secret)
	a.SecretPrevValidUntil = time.Time{}
	if grace > 0 {
		a.SecretPrevValidUntil = s.now().Add(grace)
	}

	var prevHash any
	if grace > 0 {
		prevHash = a.SecretPrevHash
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE applications SET secret_hash = ?, secret_prev_hash = ?, secret_prev_valid_until_ms = ?
		 WHERE tenant_id = ? AND id = ?`,
		a.SecretHash, prevHash, a.SecretPrevValidUntil.UnixMilli(), tenantID, appID)
	if err != nil {
		return Application{}, "", fmt.Errorf("controlplane: rotate secret: %w", err)
	}
	return a, secret, nil
}

// SecretAccepted reports whether secret is currently valid for the
// application, at now.
//
// Both the current digest and a still-in-grace previous digest are accepted.
// A rotated-out secret with no grace is rejected.
func SecretAccepted(a Application, secret string, now time.Time) bool {
	candidate := HashSecret(secret)
	if subtleEqual(candidate, a.SecretHash) {
		return true
	}
	if a.SecretPrevHash != "" && subtleEqual(candidate, a.SecretPrevHash) {
		return a.SecretPrevValidUntil.After(now)
	}
	return false
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
