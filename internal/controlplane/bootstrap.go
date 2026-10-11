package controlplane

import (
	"context"
	"errors"
	"fmt"
)

// Bootstrap configures the first administrator.
//
// The administrative surface authorizes a caller against a stored grant, so an
// instance with no grant has no reachable administrator: every admin call would
// be refused, and there would be no way to create the grant that would unblock
// it. That is a lockout rather than a security property, so the first grant is
// created out of band, from configuration, before the API is used.
//
// This is deliberately not an endpoint. An endpoint that can mint an
// administrator is an unauthenticated privilege escalation, and the fact that it
// only works once is a property of timing rather than of access control.
//
// Both settings are required. Bootstrapping an instance with a default subject
// would create an administrator whose credentials are public knowledge, which is
// worse than an instance that refuses to start.
type Bootstrap struct {
	// TenantID is the tenant the principal administers. It is created if absent.
	TenantID string
	// Subject is the principal identifier, matched against the admin API's
	// X-Admin-Subject header.
	Subject string
	// Role is admin or viewer. Anything else grants nothing.
	Role string
}

// BootstrapStore seeds the first administrative grant.
//
// It is separate from the admin HTTP surface on purpose: the surface is
// authorized by the rows this writes, so it cannot also be the thing that
// creates them.
func BootstrapStore(ctx context.Context, s *Store, b Bootstrap) error {
	if b.TenantID == "" || b.Subject == "" {
		return errors.New("controlplane: bootstrap requires a tenant and a subject")
	}
	if b.Role != RoleAdmin && b.Role != RoleViewer {
		return fmt.Errorf("controlplane: bootstrap role must be %q or %q, got %q",
			RoleAdmin, RoleViewer, b.Role)
	}

	tenant, err := s.Tenant(ctx, b.TenantID)
	if errors.Is(err, ErrNotFound) {
		tenant, err = s.CreateTenant(ctx, Tenant{ID: b.TenantID, Name: b.TenantID})
	}
	if err != nil {
		return fmt.Errorf("controlplane: bootstrap tenant: %w", err)
	}

	_, err = s.UpsertAdminPrincipal(ctx, AdminPrincipal{
		TenantID: tenant.ID,
		Subject:  b.Subject,
		Role:     b.Role,
		Enabled:  true,
	})
	if err != nil {
		return fmt.Errorf("controlplane: bootstrap principal: %w", err)
	}
	return nil
}
