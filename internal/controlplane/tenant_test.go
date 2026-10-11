package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newFixture returns a migrated store with a controllable clock, plus two
// tenants whose data must never be visible to each other.
type fixture struct {
	store *Store
	now   time.Time
	t1    Tenant
	t2    Tenant
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db := openTestDB(t)
	now := time.UnixMilli(1_700_000_000_000)
	if err := Migrate(context.Background(), db, now); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// The clock is a closure so tests can advance time to prove expiry
	// behaviour without sleeping.
	clock := now
	store := NewStore(db, func() time.Time { return clock })

	f := &fixture{store: store, now: now}
	var err error
	f.t1, err = store.CreateTenant(context.Background(), Tenant{Name: "Tenant One"})
	if err != nil {
		t.Fatalf("create t1: %v", err)
	}
	f.t2, err = store.CreateTenant(context.Background(), Tenant{Name: "Tenant Two"})
	if err != nil {
		t.Fatalf("create t2: %v", err)
	}
	return f
}

func (f *fixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func (f *fixture) seedApplication(t *testing.T, tenant Tenant, name string) Application {
	t.Helper()
	app, _, err := f.store.CreateApplication(context.Background(),
		Application{TenantID: tenant.ID, Name: name, CanPublish: true, CanSubscribe: true}, "secret-"+name)
	if err != nil {
		t.Fatalf("create application for %s: %v", tenant.ID, err)
	}
	return app
}

func TestTenantRoundTrip(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	got, err := f.store.Tenant(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if got.Name != f.t1.Name {
		t.Errorf("name = %q, want %q", got.Name, f.t1.Name)
	}
	if _, err := f.store.Tenant(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}

	updated, err := f.store.UpdateTenant(ctx, f.t1.ID, func(tn *Tenant) {
		tn.Name = "Renamed"
		tn.AuditRetentionDays = 7
		tn.RedactAuditBodies = false
	})
	if err != nil {
		t.Fatalf("UpdateTenant: %v", err)
	}
	if updated.Name != "Renamed" || updated.AuditRetentionDays != 7 || updated.RedactAuditBodies {
		t.Errorf("update did not apply: %+v", updated)
	}
}

func TestCreateApplicationReturnsSecretOnceAndStoresOnlyDigest(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	app, secret, err := f.store.CreateApplication(ctx,
		Application{TenantID: f.t1.ID, Name: "App"}, "the-secret")
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if secret != "the-secret" {
		t.Errorf("returned secret = %q, want the plaintext", secret)
	}
	if app.AppKey == "" {
		t.Error("AppKey was not generated")
	}

	stored, err := f.store.Application(ctx, f.t1.ID, app.ID)
	if err != nil {
		t.Fatalf("Application: %v", err)
	}
	if stored.SecretHash == "the-secret" {
		t.Fatal("the plaintext secret was persisted")
	}
	if stored.SecretHash != HashSecret("the-secret") {
		t.Error("stored digest does not match the plaintext")
	}
}

// App keys are globally unique by design, so this lookup is the one read path
// that is not tenant-filtered. It must resolve the tenant, not just the key.
func TestApplicationByAppKeyResolvesTenant(t *testing.T) {
	f := newFixture(t)
	app := f.seedApplication(t, f.t2, "Other")

	got, err := f.store.ApplicationByAppKey(context.Background(), app.AppKey)
	if err != nil {
		t.Fatalf("ApplicationByAppKey: %v", err)
	}
	if got.TenantID != f.t2.ID {
		t.Errorf("tenant = %q, want %q", got.TenantID, f.t2.ID)
	}

	if _, err := f.store.ApplicationByAppKey(context.Background(), "ak_unknown"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// Every tenant-scoped read must refuse to cross the tenant boundary. This is
// checked by proving that a row created by one tenant is invisible to the
// other's read path, and that a mutation scoped to the wrong tenant affects
// nothing.
func TestTenantScopedReadsNeverCrossTenantBoundary(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	other := f.t2.ID

	app := f.seedApplication(t, f.t1, "Owned")
	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 111, MaxPublishQPS: 222, MaxChannels: 333,
	}); err != nil {
		t.Fatalf("UpsertQuota: %v", err)
	}
	ceiling := 500.0
	if _, err := f.store.UpsertChannelConfig(ctx, ChannelConfig{
		TenantID: f.t1.ID, Channel: "room:1", PublishCeilingOverrideQPS: &ceiling,
	}); err != nil {
		t.Fatalf("UpsertChannelConfig: %v", err)
	}
	rule, err := f.store.UpsertRule(ctx, ModerationRule{
		TenantID: f.t1.ID, Pattern: "secretword", Action: ActionBlock,
	})
	if err != nil {
		t.Fatalf("UpsertRule: %v", err)
	}
	restriction, err := f.store.CreateRestriction(ctx, Restriction{
		TenantID: f.t1.ID, SubjectKind: SubjectUser, SubjectID: "u1", Action: RestrictionMute,
	})
	if err != nil {
		t.Fatalf("CreateRestriction: %v", err)
	}
	block, err := f.store.CreateNetworkBlock(ctx, NetworkBlock{
		TenantID: f.t1.ID, CIDR: "10.0.0.0/8",
	})
	if err != nil {
		t.Fatalf("CreateNetworkBlock: %v", err)
	}
	principal, err := f.store.UpsertAdminPrincipal(ctx, AdminPrincipal{
		TenantID: f.t1.ID, Subject: "ops@example.com", Role: RoleAdmin,
	})
	if err != nil {
		t.Fatalf("UpsertAdminPrincipal: %v", err)
	}
	if err := f.store.AppendAudit(ctx, AuditEntry{
		TenantID: f.t1.ID, Kind: AuditAdmin, Subject: "ops", Decision: "granted",
	}); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	t.Run("Application", func(t *testing.T) {
		if _, err := f.store.Application(ctx, other, app.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("ListApplications", func(t *testing.T) {
		apps, err := f.store.ListApplications(ctx, other)
		if err != nil {
			t.Fatalf("ListApplications: %v", err)
		}
		if len(apps) != 0 {
			t.Errorf("other tenant sees %d applications, want 0", len(apps))
		}
	})

	t.Run("Quota", func(t *testing.T) {
		q, err := f.store.Quota(ctx, other)
		if err != nil {
			t.Fatalf("Quota: %v", err)
		}
		if q.MaxConnections == 111 || q.MaxPublishQPS == 222 || q.MaxChannels == 333 {
			t.Errorf("other tenant sees t1's quota: %+v", q)
		}
	})

	t.Run("ChannelConfig", func(t *testing.T) {
		c, err := f.store.ChannelConfig(ctx, other, "room:1")
		if err != nil {
			t.Fatalf("ChannelConfig: %v", err)
		}
		if c.PublishCeilingOverrideQPS != nil {
			t.Errorf("other tenant sees t1's channel override: %+v", c.PublishCeilingOverrideQPS)
		}
	})

	t.Run("ListChannelConfigs", func(t *testing.T) {
		cfgs, err := f.store.ListChannelConfigs(ctx, other)
		if err != nil {
			t.Fatalf("ListChannelConfigs: %v", err)
		}
		if len(cfgs) != 0 {
			t.Errorf("other tenant sees %d channel configs, want 0", len(cfgs))
		}
	})

	t.Run("ListRules", func(t *testing.T) {
		rules, err := f.store.ListRules(ctx, other)
		if err != nil {
			t.Fatalf("ListRules: %v", err)
		}
		if len(rules) != 0 {
			t.Errorf("other tenant sees %d rules, want 0", len(rules))
		}
	})

	t.Run("ListRestrictions", func(t *testing.T) {
		rs, err := f.store.ListRestrictions(ctx, other)
		if err != nil {
			t.Fatalf("ListRestrictions: %v", err)
		}
		if len(rs) != 0 {
			t.Errorf("other tenant sees %d restrictions, want 0", len(rs))
		}
	})

	t.Run("ActiveRestrictionsFor", func(t *testing.T) {
		rs, err := f.store.ActiveRestrictionsFor(ctx, other, SubjectUser, "u1", f.now)
		if err != nil {
			t.Fatalf("ActiveRestrictionsFor: %v", err)
		}
		if len(rs) != 0 {
			t.Errorf("other tenant sees %d active restrictions, want 0", len(rs))
		}
	})

	t.Run("ListNetworkBlocks", func(t *testing.T) {
		blocks, err := f.store.ListNetworkBlocks(ctx, other)
		if err != nil {
			t.Fatalf("ListNetworkBlocks: %v", err)
		}
		if len(blocks) != 0 {
			t.Errorf("other tenant sees %d network blocks, want 0", len(blocks))
		}
	})

	t.Run("AdminPrincipal", func(t *testing.T) {
		if _, err := f.store.AdminPrincipal(ctx, other, principal.Subject); !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("MayAdminister", func(t *testing.T) {
		ok, err := f.store.MayAdminister(ctx, other, principal.Subject)
		if err != nil {
			t.Fatalf("MayAdminister: %v", err)
		}
		if ok {
			t.Error("other tenant's principal administers the wrong tenant")
		}
	})

	t.Run("QueryAudit", func(t *testing.T) {
		entries, err := f.store.QueryAudit(ctx, AuditQuery{TenantID: other})
		if err != nil {
			t.Fatalf("QueryAudit: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("other tenant sees %d audit entries, want 0", len(entries))
		}
	})

	// Mutations scoped to the wrong tenant must report not-found and leave the
	// owner's data intact.
	t.Run("mutations are scoped", func(t *testing.T) {
		if err := f.store.DeleteRule(ctx, other, rule.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("DeleteRule err = %v, want ErrNotFound", err)
		}
		if err := f.store.DeleteRestriction(ctx, other, restriction.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("DeleteRestriction err = %v, want ErrNotFound", err)
		}
		if err := f.store.DeleteNetworkBlock(ctx, other, block.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("DeleteNetworkBlock err = %v, want ErrNotFound", err)
		}
		if _, err := f.store.UpdateApplication(ctx, other, app.ID, func(*Application) {}); !errors.Is(err, ErrNotFound) {
			t.Errorf("UpdateApplication err = %v, want ErrNotFound", err)
		}
		if _, _, err := f.store.RotateApplicationSecret(ctx, other, app.ID, 0); !errors.Is(err, ErrNotFound) {
			t.Errorf("RotateApplicationSecret err = %v, want ErrNotFound", err)
		}
		if err := f.store.DeleteChannelConfig(ctx, other, "room:1"); err != nil {
			t.Errorf("DeleteChannelConfig: %v", err)
		}

		// The owner's rows must have survived every rejected mutation.
		rules, err := f.store.ListRules(ctx, f.t1.ID)
		if err != nil || len(rules) != 1 {
			t.Errorf("owner's rules after cross-tenant deletes = %d (err %v), want 1", len(rules), err)
		}
		cfg, err := f.store.ChannelConfig(ctx, f.t1.ID, "room:1")
		if err != nil || cfg.PublishCeilingOverrideQPS == nil {
			t.Errorf("owner's channel config was removed by a cross-tenant delete (err %v)", err)
		}
	})
}

func TestQuotaUpsertOverwrites(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 10, MaxPublishQPS: 20, MaxChannels: 30,
	}); err != nil {
		t.Fatalf("UpsertQuota: %v", err)
	}
	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 100, MaxPublishQPS: 200, MaxChannels: 300,
	}); err != nil {
		t.Fatalf("second UpsertQuota: %v", err)
	}

	q, err := f.store.Quota(ctx, f.t1.ID)
	if err != nil {
		t.Fatalf("Quota: %v", err)
	}
	if q.MaxConnections != 100 || q.MaxPublishQPS != 200 || q.MaxChannels != 300 {
		t.Errorf("quota = %+v, want the second write", q)
	}
}

func TestChannelConfigRoundTripsAbsentOverrides(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// A channel with no stored config is the normal case, not an error, and
	// must come back with coalescing on and no override anywhere.
	c, err := f.store.ChannelConfig(ctx, f.t1.ID, "room:never-configured")
	if err != nil {
		t.Fatalf("ChannelConfig: %v", err)
	}
	if c.PublishCeilingOverrideQPS != nil || c.ReceiveRateTarget != nil || c.MergeRatioOverride != nil {
		t.Errorf("absent overrides should stay nil, got %+v", c)
	}
	if !c.CoalescingEnabled {
		t.Error("coalescing should default to enabled")
	}

	// Zero is a meaningful override for the subscriber ceiling; nil is not,
	// so the two must not be conflated on the way back out.
	if _, err := f.store.UpsertChannelConfig(ctx, ChannelConfig{
		TenantID: f.t1.ID, Channel: "room:2", CoalescingEnabled: false, MaxSubscribers: 0,
	}); err != nil {
		t.Fatalf("UpsertChannelConfig: %v", err)
	}
	got, err := f.store.ChannelConfig(ctx, f.t1.ID, "room:2")
	if err != nil {
		t.Fatalf("ChannelConfig: %v", err)
	}
	if got.CoalescingEnabled {
		t.Error("coalescing override was not persisted")
	}
	if got.PublishCeilingOverrideQPS != nil {
		t.Errorf("nil override came back as %v", *got.PublishCeilingOverrideQPS)
	}
}

func TestDeleteChannelConfigRestoresDerived(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rate := 12.5
	if _, err := f.store.UpsertChannelConfig(ctx, ChannelConfig{
		TenantID: f.t1.ID, Channel: "room:3", ReceiveRateTarget: &rate,
	}); err != nil {
		t.Fatalf("UpsertChannelConfig: %v", err)
	}
	if err := f.store.DeleteChannelConfig(ctx, f.t1.ID, "room:3"); err != nil {
		t.Fatalf("DeleteChannelConfig: %v", err)
	}
	got, err := f.store.ChannelConfig(ctx, f.t1.ID, "room:3")
	if err != nil {
		t.Fatalf("ChannelConfig: %v", err)
	}
	if got.ReceiveRateTarget != nil {
		t.Errorf("override survived deletion: %v", *got.ReceiveRateTarget)
	}
}

func TestAdminPrincipalRoles(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if ok, err := f.store.MayAdminister(ctx, f.t1.ID, "nobody@example.com"); err != nil || ok {
		t.Errorf("unknown principal: ok=%v err=%v, want false/nil", ok, err)
	}

	if _, err := f.store.UpsertAdminPrincipal(ctx, AdminPrincipal{
		TenantID: f.t1.ID, Subject: "viewer@example.com", Role: RoleViewer,
	}); err != nil {
		t.Fatalf("UpsertAdminPrincipal: %v", err)
	}
	if ok, err := f.store.MayAdminister(ctx, f.t1.ID, "viewer@example.com"); err != nil || ok {
		t.Errorf("viewer: ok=%v err=%v, want false/nil", ok, err)
	}

	if _, err := f.store.UpsertAdminPrincipal(ctx, AdminPrincipal{
		TenantID: f.t1.ID, Subject: "admin@example.com", Role: RoleAdmin,
	}); err != nil {
		t.Fatalf("UpsertAdminPrincipal: %v", err)
	}
	if ok, err := f.store.MayAdminister(ctx, f.t1.ID, "admin@example.com"); err != nil || !ok {
		t.Errorf("admin: ok=%v err=%v, want true/nil", ok, err)
	}

	// Disabling a principal withdraws the grant without deleting the record.
	if err := f.store.SetAdminPrincipalEnabled(ctx, f.t1.ID, "admin@example.com", false); err != nil {
		t.Fatalf("SetAdminPrincipalEnabled: %v", err)
	}
	if ok, err := f.store.MayAdminister(ctx, f.t1.ID, "admin@example.com"); err != nil || ok {
		t.Errorf("disabled admin: ok=%v err=%v, want false/nil", ok, err)
	}
}

// A grant created from a struct that omits Enabled must still carry authority.
// Go's zero value is false, and a grant with no authority would otherwise look
// like it succeeded until the first administrative call was refused.
func TestNewAdminPrincipalDefaultsToEnabled(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	p, err := f.store.UpsertAdminPrincipal(ctx, AdminPrincipal{
		TenantID: f.t1.ID, Subject: "ops@example.com", Role: RoleAdmin,
	})
	if err != nil {
		t.Fatalf("UpsertAdminPrincipal: %v", err)
	}
	if !p.Enabled {
		t.Error("new principal came back disabled")
	}
	ok, err := f.store.MayAdminister(ctx, f.t1.ID, "ops@example.com")
	if err != nil || !ok {
		t.Errorf("MayAdminister = %v (err %v), want true/nil", ok, err)
	}
}

func TestSetAdminPrincipalEnabledOnUnknownSubject(t *testing.T) {
	f := newFixture(t)
	if err := f.store.SetAdminPrincipalEnabled(context.Background(), f.t1.ID, "nobody@example.com", true); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
