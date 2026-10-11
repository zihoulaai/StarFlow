package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newBootstrapStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := Migrate(ctx, db, time.Now()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewStore(db, time.Now), ctx
}

// Without a grant the admin surface authorizes nobody, so an instance needs one
// seeded before its API can be used at all.
func TestBootstrapCreatesTheFirstAdministrator(t *testing.T) {
	s, ctx := newBootstrapStore(t)

	if err := BootstrapStore(ctx, s, Bootstrap{TenantID: "t1", Subject: "root", Role: RoleAdmin}); err != nil {
		t.Fatalf("BootstrapStore: %v", err)
	}

	p, err := s.AdminPrincipal(ctx, "t1", "root")
	if err != nil {
		t.Fatalf("AdminPrincipal: %v", err)
	}
	if p.Role != RoleAdmin || !p.Enabled {
		t.Errorf("principal = %+v, want an enabled admin", p)
	}
	ok, err := s.MayAdminister(ctx, "t1", "root")
	if err != nil || !ok {
		t.Errorf("MayAdminister = %v, %v; want true, nil", ok, err)
	}
}

// Bootstrapping is idempotent: a restart must not fail on an existing grant,
// which would turn a routine deployment into an outage.
func TestBootstrapIsIdempotent(t *testing.T) {
	s, ctx := newBootstrapStore(t)
	b := Bootstrap{TenantID: "t1", Subject: "root", Role: RoleAdmin}

	if err := BootstrapStore(ctx, s, b); err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if err := BootstrapStore(ctx, s, b); err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}

	tenants, err := s.ListTenants(ctx)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if len(tenants) != 1 {
		t.Errorf("ListTenants returned %d, want 1", len(tenants))
	}
}

// An unrecognised role must be refused. Granting nothing silently would look
// like a working configuration that authorizes no one.
func TestBootstrapRefusesAnUnknownRole(t *testing.T) {
	s, ctx := newBootstrapStore(t)

	err := BootstrapStore(ctx, s, Bootstrap{TenantID: "t1", Subject: "root", Role: "superuser"})
	if err == nil {
		t.Fatal("an unknown role was accepted")
	}
	if _, gerr := s.AdminPrincipal(ctx, "t1", "root"); !errors.Is(gerr, ErrNotFound) {
		t.Error("a principal was created despite the role being refused")
	}
}

// A half-configured bootstrap is refused: a tenant without a subject names an
// administrator with no identity.
func TestBootstrapRequiresBothTenantAndSubject(t *testing.T) {
	s, ctx := newBootstrapStore(t)

	for _, b := range []Bootstrap{
		{TenantID: "t1", Role: RoleAdmin},
		{Subject: "root", Role: RoleAdmin},
	} {
		if err := BootstrapStore(ctx, s, b); err == nil {
			t.Errorf("BootstrapStore accepted %+v", b)
		}
	}
}

// A viewer bootstrap must grant read access but not the admin role, so the
// split between read and write survives the seeding path.
func TestBootstrapViewerIsNotAnAdministrator(t *testing.T) {
	s, ctx := newBootstrapStore(t)

	if err := BootstrapStore(ctx, s, Bootstrap{TenantID: "t1", Subject: "watcher", Role: RoleViewer}); err != nil {
		t.Fatalf("BootstrapStore: %v", err)
	}
	ok, err := s.MayAdminister(ctx, "t1", "watcher")
	if err != nil {
		t.Fatalf("MayAdminister: %v", err)
	}
	if ok {
		t.Error("a viewer bootstrap may administer")
	}
	p, err := s.AdminPrincipal(ctx, "t1", "watcher")
	if err != nil {
		t.Fatalf("AdminPrincipal: %v", err)
	}
	if p.Role != RoleViewer {
		t.Errorf("role = %q, want viewer", p.Role)
	}
}

// An application must be issued a usable secret. Hashing an empty string would
// store a credential that exists and can never authenticate, while the caller
// is handed an empty value as though it were one to keep.
func TestCreateApplicationIssuesAUsableSecret(t *testing.T) {
	s, ctx := newBootstrapStore(t)
	if _, err := s.CreateTenant(ctx, Tenant{ID: "t1", Name: "t1"}); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	app, secret, err := s.CreateApplication(ctx, Application{
		TenantID: "t1", Name: "app", CanPublish: true, CanSubscribe: true,
	}, "")
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if secret == "" {
		t.Fatal("an empty secret was issued")
	}
	if !SecretAccepted(app, secret, time.Now()) {
		t.Error("the issued secret does not authenticate its own application")
	}
	if SecretAccepted(app, "", time.Now()) {
		t.Error("an empty secret authenticates")
	}
	if SecretAccepted(app, secret+"x", time.Now()) {
		t.Error("a wrong secret authenticates")
	}
}
