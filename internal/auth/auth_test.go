package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/starflow/push/internal/controlplane"
)

func testSigner(t *testing.T, now func() time.Time) *Signer {
	t.Helper()
	return NewSigner([]byte("test-signing-key"), 5*time.Minute, 2*time.Minute, now)
}

func TestTokenRoundTrip(t *testing.T) {
	clock := time.UnixMilli(1_700_000_000_000)
	s := testSigner(t, func() time.Time { return clock })

	token, err := s.Issue("ak_1", "user-42", "tenant-1")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	claims, err := s.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.AppKey != "ak_1" || claims.Subject != "user-42" || claims.TenantID != "tenant-1" {
		t.Errorf("claims = %+v, want the issued values", claims)
	}
	if claims.ExpiresAtMs <= claims.IssuedAtMs {
		t.Error("expiry is not after issue time")
	}
}

// A token signed with a different key must not verify, and must be reported as
// a signature failure rather than as an expiry problem.
func TestVerifyRejectsForeignSignature(t *testing.T) {
	clock := time.UnixMilli(1_700_000_000_000)
	a := testSigner(t, func() time.Time { return clock })
	b := NewSigner([]byte("a-completely-different-key"), 5*time.Minute, 2*time.Minute,
		func() time.Time { return clock })

	token, err := a.Issue("ak_1", "u", "t")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := b.Verify(token); !errors.Is(err, ErrTokenSignature) {
		t.Errorf("err = %v, want ErrTokenSignature", err)
	}
}

func TestVerifyRejectsTamperedClaims(t *testing.T) {
	clock := time.UnixMilli(1_700_000_000_000)
	s := testSigner(t, func() time.Time { return clock })

	token, err := s.Issue("ak_1", "user-1", "tenant-1")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	parts := strings.Split(token, ".")

	// Swapping the tenant in the payload must invalidate the signature, or a
	// client could replay a token issued for one tenant against another.
	forged, err := s.Issue("ak_1", "user-1", "tenant-2")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	forgedParts := strings.Split(forged, ".")
	tampered := parts[0] + "." + forgedParts[1] + "." + parts[2]

	if _, err := s.Verify(tampered); !errors.Is(err, ErrTokenSignature) {
		t.Errorf("tenant-swapped token err = %v, want ErrTokenSignature", err)
	}
}

func TestVerifyRejectsMalformedTokens(t *testing.T) {
	s := testSigner(t, time.Now)

	for _, token := range []string{
		"", "not-a-token", "v1.only-two", "v2.a.b.c",
		"v1.~.~.0.0.c2ln",
	} {
		if _, err := s.Verify(token); err == nil {
			t.Errorf("token %q verified without error", token)
		}
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	clock := time.UnixMilli(1_700_000_000_000)
	s := testSigner(t, func() time.Time { return clock })

	token, err := s.Issue("ak_1", "u", "t")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := s.Verify(token); err != nil {
		t.Fatalf("token rejected before expiry: %v", err)
	}

	clock = clock.Add(6 * time.Minute)
	_, err = s.Verify(token)
	if !errors.Is(err, ErrTokenExpired) {
		t.Errorf("err = %v, want ErrTokenExpired", err)
	}
}

// A token dated too far in the future must be refused rather than given an
// extended life, and must be distinguishable from ordinary expiry so an
// operator can tell a clock problem from a forgery attempt.
func TestVerifyEnforcesSkewWindow(t *testing.T) {
	clock := time.UnixMilli(1_700_000_000_000)
	s := testSigner(t, func() time.Time { return clock })

	token, err := s.Issue("ak_1", "u", "t")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Just inside the window: accepted.
	clock = clock.Add(-90 * time.Second)
	if _, err := s.Verify(token); err != nil {
		t.Errorf("token rejected inside the skew window: %v", err)
	}

	// Beyond the window in the past.
	clock = time.UnixMilli(1_700_000_000_000).Add(-10 * time.Minute)
	if _, err := s.Verify(token); !errors.Is(err, ErrTokenSkew) {
		t.Errorf("far-past token err = %v, want ErrTokenSkew", err)
	}
}

// A token dated too far in the future must be refused rather than silently
// granted an extended life. Issuing it from a clock set ahead isolates that case
// from ordinary expiry, which would otherwise mask it.
func TestVerifyRejectsFarFutureToken(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	clock := base.Add(30 * time.Minute)
	futureSigner := testSigner(t, func() time.Time { return clock })

	token, err := futureSigner.Issue("ak_1", "u", "t")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Verify against the real clock: the token is not yet expired, but its
	// issue time is half an hour ahead of a two-minute skew window.
	present := testSigner(t, func() time.Time { return base })
	_, err = present.Verify(token)
	if !errors.Is(err, ErrTokenSkew) {
		t.Errorf("far-future token err = %v, want ErrTokenSkew", err)
	}
}

func TestIssueRequiresAppKey(t *testing.T) {
	s := testSigner(t, time.Now)
	if _, err := s.Issue("", "u", "t"); err == nil {
		t.Error("token issued without an app key")
	}
}

func TestPrivilegeSeparation(t *testing.T) {
	t.Run("subscribeOnlyCannotPublish", func(t *testing.T) {
		g := Privileges(false, true)
		if err := Check(g, PrivilegePublish); !errors.Is(err, ErrPrivilegeDenied) {
			t.Errorf("err = %v, want ErrPrivilegeDenied", err)
		}
		if err := Check(g, PrivilegeSubscribe); err != nil {
			t.Errorf("subscribe refused for a subscribe-only grant: %v", err)
		}
	})

	t.Run("publishOnlyCannotSubscribe", func(t *testing.T) {
		g := Privileges(true, false)
		if err := Check(g, PrivilegeSubscribe); !errors.Is(err, ErrPrivilegeDenied) {
			t.Errorf("err = %v, want ErrPrivilegeDenied", err)
		}
		if err := Check(g, PrivilegePublish); err != nil {
			t.Errorf("publish refused for a publish-only grant: %v", err)
		}
	})

	t.Run("neitherPrivilegeRefusesBoth", func(t *testing.T) {
		g := Privileges(false, false)
		if err := Check(g, PrivilegePublish); err == nil {
			t.Error("publish allowed with no privileges")
		}
		if err := Check(g, PrivilegeSubscribe); err == nil {
			t.Error("subscribe allowed with no privileges")
		}
	})

	t.Run("bothPrivilegesAllowBoth", func(t *testing.T) {
		g := Privileges(true, true)
		if err := Check(g, PrivilegePublish); err != nil {
			t.Errorf("publish refused: %v", err)
		}
		if err := Check(g, PrivilegeSubscribe); err != nil {
			t.Errorf("subscribe refused: %v", err)
		}
	})
}

// --- Resolver tests ---

type recordingAuditor struct {
	failures []string
}

func (r *recordingAuditor) RecordAuthFailure(_ context.Context, _, _, _, reason string) {
	r.failures = append(r.failures, reason)
}

type resolverFixture struct {
	resolver *Resolver
	cache    *controlplane.ConfigCache
	store    *controlplane.Store
	auditor  *recordingAuditor
	signer   *Signer
	// clock is a pointer so a test can advance the shared clock that the
	// store, signer and resolver closures all read. A by-value field would be
	// invisible to those closures.
	clock   *time.Time
	tenantA controlplane.Tenant
	tenantB controlplane.Tenant
}

func newResolverFixture(t *testing.T) *resolverFixture {
	t.Helper()
	db := openDB(t)
	now := time.UnixMilli(1_700_000_000_000)
	if err := controlplane.Migrate(context.Background(), db, now); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	clock := new(time.Time)
	*clock = now
	store := controlplane.NewStore(db, func() time.Time { return *clock })
	ctx := context.Background()

	tenantA, err := store.CreateTenant(ctx, controlplane.Tenant{Name: "A"})
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	tenantB, err := store.CreateTenant(ctx, controlplane.Tenant{Name: "B"})
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	cache := controlplane.NewConfigCache(controlplane.NewStoreLoader(store))
	for _, tenant := range []controlplane.Tenant{tenantA, tenantB} {
		if _, err := store.UpsertQuota(ctx, controlplane.Quota{
			TenantID: tenant.ID, MaxConnections: 500, MaxPublishQPS: 2000, MaxChannels: 50,
		}); err != nil {
			t.Fatalf("UpsertQuota: %v", err)
		}
		if err := cache.Reload(ctx, tenant.ID); err != nil {
			t.Fatalf("Reload: %v", err)
		}
	}

	auditor := &recordingAuditor{}
	signer := NewSigner([]byte("test-signing-key"), 5*time.Minute, 2*time.Minute,
		func() time.Time { return *clock })

	return &resolverFixture{
		resolver: NewResolver(signer, cache, auditor, func() time.Time { return *clock }),
		cache:    cache,
		store:    store,
		auditor:  auditor,
		signer:   signer,
		clock:    clock,
		tenantA:  tenantA,
		tenantB:  tenantB,
	}
}

func (f *resolverFixture) app(t *testing.T, tenant controlplane.Tenant, name string, pub, sub bool) controlplane.Application {
	t.Helper()
	app, _, err := f.store.CreateApplication(context.Background(), controlplane.Application{
		TenantID: tenant.ID, Name: name, CanPublish: pub, CanSubscribe: sub,
	}, "secret-"+name)
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if err := f.cache.Reload(context.Background(), tenant.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	return app
}

func TestAuthenticateResolvesIdentity(t *testing.T) {
	f := newResolverFixture(t)
	app := f.app(t, f.tenantA, "svc", true, false)

	token, err := f.signer.Issue(app.AppKey, "user-1", f.tenantA.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	id, err := f.resolver.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.TenantID != f.tenantA.ID || id.Subject != "user-1" || id.AppID != app.ID {
		t.Errorf("identity = %+v", id)
	}
	if !id.Privileges.Has(PrivilegePublish) {
		t.Error("publish privilege missing")
	}
	if id.Privileges.Has(PrivilegeSubscribe) {
		t.Error("subscribe privilege granted to a publish-only application")
	}
	if id.ConnLimit != 500 || id.PublishLimit != 2000 || id.ChannelLimit != 50 {
		t.Errorf("quotas not carried onto the identity: %+v", id)
	}
}

// A token issued for one tenant must not authenticate against another, even
// with a key that exists in both.
func TestTokenIsBoundToIssuingTenant(t *testing.T) {
	f := newResolverFixture(t)
	appA := f.app(t, f.tenantA, "a-app", true, true)

	// A token claiming tenant B, signed correctly, naming a real key.
	token, err := f.signer.Issue(appA.AppKey, "u", f.tenantB.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := f.resolver.Authenticate(context.Background(), token); !errors.Is(err, ErrTokenSignature) {
		t.Errorf("err = %v, want the cross-tenant token rejected", err)
	}
}

func TestAuthenticateRejectsDisabledApplication(t *testing.T) {
	f := newResolverFixture(t)
	app := f.app(t, f.tenantA, "svc", true, true)
	token, err := f.signer.Issue(app.AppKey, "u", f.tenantA.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := f.store.UpdateApplication(context.Background(), f.tenantA.ID, app.ID,
		func(a *controlplane.Application) { a.Enabled = false }); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if err := f.cache.Reload(context.Background(), f.tenantA.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, err := f.resolver.Authenticate(context.Background(), token); !errors.Is(err, ErrCredentialDisabled) {
		t.Errorf("err = %v, want ErrCredentialDisabled", err)
	}
}

// Every rejection must be audited with its reason: "token invalid" alone cannot
// distinguish a client bug from an attack, and the two warrant different
// responses.
func TestEveryAuthenticationRejectionIsAudited(t *testing.T) {
	f := newResolverFixture(t)
	app := f.app(t, f.tenantA, "svc", true, true)

	good, err := f.signer.Issue(app.AppKey, "u", f.tenantA.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	cases := []struct {
		name  string
		token string
	}{
		{"malformed", "garbage"},
		{"bad signature", "v1.aaaa~bbbb~cccc~0~0.zzzz"},
		{"unknown key", mustIssue(t, f.signer, "ak_unknown", "u", f.tenantA.ID)},
	}
	for _, c := range cases {
		before := len(f.auditor.failures)
		if _, err := f.resolver.Authenticate(context.Background(), c.token); err == nil {
			t.Errorf("%s: authenticated without error", c.name)
		}
		if len(f.auditor.failures) != before+1 {
			t.Errorf("%s: audit entries %d, want one more than %d",
				c.name, len(f.auditor.failures), before)
		}
	}

	// A valid token must not be audited as a failure.
	before := len(f.auditor.failures)
	if _, err := f.resolver.Authenticate(context.Background(), good); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if len(f.auditor.failures) != before {
		t.Error("a successful authentication was audited as a failure")
	}
}

// Rotating an application's secret must invalidate tokens signed with the old
// one, and the old plaintext secret must stop working immediately.
func TestRotatedOutSecretIsRejected(t *testing.T) {
	f := newResolverFixture(t)
	app := f.app(t, f.tenantA, "svc", true, true)
	ctx := context.Background()

	oldSecret := "secret-svc"
	before, err := f.resolver.AuthenticateSecret(ctx, app.AppKey, oldSecret)
	if err != nil {
		t.Fatalf("AuthenticateSecret before rotation: %v", err)
	}
	if before.AppID != app.ID {
		t.Errorf("identity = %+v", before)
	}

	rotated, newSecret, err := f.store.RotateApplicationSecret(ctx, f.tenantA.ID, app.ID, 0)
	if err != nil {
		t.Fatalf("RotateApplicationSecret: %v", err)
	}
	if newSecret == oldSecret {
		t.Fatal("rotation returned the same secret")
	}
	if err := f.cache.Reload(ctx, f.tenantA.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// The old secret must stop working at the instant of rotation.
	if _, err := f.resolver.AuthenticateSecret(ctx, rotated.AppKey, oldSecret); err == nil {
		t.Error("the rotated-out secret still authenticates")
	}
	if _, err := f.resolver.AuthenticateSecret(ctx, rotated.AppKey, newSecret); err != nil {
		t.Errorf("the new secret does not authenticate: %v", err)
	}
}

// A rotation grace window is the opposite case: the previous secret keeps
// working until the window closes, so a fleet can be rolled without an outage.
func TestRotationGraceKeepsPreviousSecretValidUntilItExpires(t *testing.T) {
	f := newResolverFixture(t)
	app := f.app(t, f.tenantA, "svc", true, true)
	ctx := context.Background()

	oldSecret := "secret-svc"
	rotated, newSecret, err := f.store.RotateApplicationSecret(ctx, f.tenantA.ID, app.ID, time.Hour)
	if err != nil {
		t.Fatalf("RotateApplicationSecret: %v", err)
	}
	if err := f.cache.Reload(ctx, f.tenantA.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, err := f.resolver.AuthenticateSecret(ctx, rotated.AppKey, oldSecret); err != nil {
		t.Errorf("previous secret refused inside the grace window: %v", err)
	}

	// Past the window, the previous secret must stop working.
	*f.clock = f.clock.Add(2 * time.Hour)
	if err := f.cache.Reload(ctx, f.tenantA.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if _, err := f.resolver.AuthenticateSecret(ctx, rotated.AppKey, oldSecret); err == nil {
		t.Error("previous secret still valid after the grace window closed")
	}
	if _, err := f.resolver.AuthenticateSecret(ctx, rotated.AppKey, newSecret); err != nil {
		t.Errorf("current secret refused after the grace window: %v", err)
	}
}

func mustIssue(t *testing.T, s *Signer, appKey, subject, tenantID string) string {
	t.Helper()
	token, err := s.Issue(appKey, subject, tenantID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return token
}
