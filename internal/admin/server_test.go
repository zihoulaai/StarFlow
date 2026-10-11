package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu sync.Mutex

	tenants  map[string]TenantView
	apps     map[string]AppView
	quotas   map[string]QuotaView
	channels map[string]ChannelView

	// failReads, when set, is returned by the tenant and application reads
	// instead of consulting the maps. It stands in for a store that is present
	// but unreachable, which is a different condition from an absent row.
	failReads error

	nextID    int
	rotations map[string]int
}

// The four aggregates are separate interfaces, and a double serving all of them
// is one type per aggregate rather than one for the whole surface: the point of
// the split is that each is separately substitutable.
type (
	fakeTenants      = fakeStore
	fakeApplications = fakeStore
	fakeQuotas       = fakeStore
	fakeChannels     = fakeStore
)

// Each aggregate is satisfied by the one shared fake, and the assertions below
// fail to compile if a method moves between interfaces.
var (
	_ TenantStore      = (*fakeTenants)(nil)
	_ ApplicationStore = (*fakeApplications)(nil)
	_ QuotaStore       = (*fakeQuotas)(nil)
	_ ChannelStore     = (*fakeChannels)(nil)
)

func newFakeStore() *fakeStore {
	return &fakeStore{
		tenants:   map[string]TenantView{},
		apps:      map[string]AppView{},
		quotas:    map[string]QuotaView{},
		channels:  map[string]ChannelView{},
		rotations: map[string]int{},
	}
}

func (f *fakeStore) ListTenants(context.Context) ([]TenantView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []TenantView{}
	for _, t := range f.tenants {
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeStore) Tenant(_ context.Context, id string) (TenantView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads != nil {
		return TenantView{}, f.failReads
	}
	t, ok := f.tenants[id]
	if !ok {
		return TenantView{}, ErrNotFound
	}
	return t, nil
}

func (f *fakeStore) CreateTenant(_ context.Context, name string) (TenantView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	t := TenantView{ID: "t" + itoa(f.nextID), Name: name, CreatedAt: time.Now()}
	f.tenants[t.ID] = t
	return t, nil
}

func (f *fakeStore) UpdateTenant(_ context.Context, id, name string) (TenantView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tenants[id]
	if !ok {
		return TenantView{}, ErrNotFound
	}
	t.Name = name
	f.tenants[id] = t
	return t, nil
}

func (f *fakeStore) ListApplications(_ context.Context, tenantID string) ([]AppView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []AppView{}
	for _, a := range f.apps {
		if a.TenantID == tenantID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (f *fakeStore) Application(_ context.Context, tenantID, id string) (AppView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads != nil {
		return AppView{}, f.failReads
	}
	a, ok := f.apps[id]
	if !ok || a.TenantID != tenantID {
		return AppView{}, ErrNotFound
	}
	return a, nil
}

func (f *fakeStore) CreateApplication(_ context.Context, tenantID, name string, pub, sub bool) (AppView, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := "a" + itoa(f.nextID)
	a := AppView{
		ID: id, TenantID: tenantID, Name: name,
		CanPublish: pub, CanSubscribe: sub, Enabled: true, CreatedAt: time.Now(),
		AppKey: "ak_" + id,
	}
	f.apps[a.ID] = a
	return a, "secret-" + id, nil
}

func (f *fakeStore) SetApplicationEnabled(_ context.Context, tenantID, id string, enabled bool) (AppView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.apps[id]
	if !ok || a.TenantID != tenantID {
		return AppView{}, ErrNotFound
	}
	a.Enabled = enabled
	f.apps[id] = a
	return a, nil
}

func (f *fakeStore) RotateSecret(_ context.Context, tenantID, id string) (AppView, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.apps[id]
	if !ok || a.TenantID != tenantID {
		return AppView{}, "", ErrNotFound
	}
	f.rotations[id]++
	return a, "secret-" + a.ID + "-v" + itoa(f.rotations[id]), nil
}

func (f *fakeStore) Quota(_ context.Context, tenantID string) (QuotaView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if q, ok := f.quotas[tenantID]; ok {
		return q, nil
	}
	return QuotaView{MaxConnections: 100, MaxPublishQPS: 100, MaxChannels: 10}, nil
}

func (f *fakeStore) SetQuota(_ context.Context, tenantID string, q QuotaView) (QuotaView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quotas[tenantID] = q
	return q, nil
}

func (f *fakeStore) Channel(_ context.Context, tenantID, channel string) (ChannelView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := tenantID + "/" + channel
	if c, ok := f.channels[key]; ok {
		return c, nil
	}
	return ChannelView{TenantID: tenantID, Channel: channel, CoalescingEnabled: true}, nil
}

func (f *fakeStore) SetChannelLimit(_ context.Context, tenantID, channel string, maxSubs int, ceiling float64, hasCeiling bool) (ChannelView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := tenantID + "/" + channel
	c := f.channels[key]
	c.TenantID, c.Channel = tenantID, channel
	c.MaxSubscribers = maxSubs
	if hasCeiling {
		c.PublishCeiling = ceiling
		c.CeilingOverride = true
	} else {
		c.CeilingOverride = false
	}
	f.channels[key] = c
	return c, nil
}

type fakeAuthorizer struct {
	roles map[string]Role
}

func (f *fakeAuthorizer) RoleFor(_ context.Context, tenantID, subject string) (Role, error) {
	return f.roles[tenantID+"/"+subject], nil
}

type fakeAuditor struct {
	mu      sync.Mutex
	actions []Action
}

func (f *fakeAuditor) RecordAdminAction(_ context.Context, a Action) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions = append(f.actions, a)
}

func (f *fakeAuditor) last() Action {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.actions) == 0 {
		return Action{}
	}
	return f.actions[len(f.actions)-1]
}

type fakeConns struct {
	views []ConnectionView
}

func (f *fakeConns) List(tenantID string) []ConnectionView {
	var out []ConnectionView
	for _, v := range f.views {
		if v.TenantID == tenantID {
			out = append(out, v)
		}
	}
	return out
}

func (f *fakeConns) Terminate(tenantID string, id uint64) bool {
	for i, v := range f.views {
		if v.TenantID == tenantID && v.ID == id {
			f.views = append(f.views[:i], f.views[i+1:]...)
			return true
		}
	}
	return false
}

type fakeDegrade struct {
	manual bool
	level  int
}

func (f *fakeDegrade) SetManual(on bool, level int) { f.manual, f.level = on, level }
func (f *fakeDegrade) Manual() bool                 { return f.manual }
func (f *fakeDegrade) Level() int                   { return f.level }

type adminFixture struct {
	server  *Server
	store   *fakeStore
	audit   *fakeAuditor
	auth    *fakeAuthorizer
	conns   *fakeConns
	degrade *fakeDegrade
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	store := newFakeStore()
	audit := &fakeAuditor{}
	auth := &fakeAuthorizer{roles: map[string]Role{
		"t1/admin@example.com":  RoleAdmin,
		"t1/viewer@example.com": RoleViewer,
		"t2/admin@example.com":  RoleAdmin,
	}}
	conns := &fakeConns{views: []ConnectionView{
		{ID: 1, TenantID: "t1", Subject: "u1", Channels: []string{"room:1"}, State: "open"},
		{ID: 2, TenantID: "t2", Subject: "u2", Channels: []string{"room:2"}, State: "open"},
	}}
	degrade := &fakeDegrade{}

	return &adminFixture{
		server: New(ServerConfig{
			Tenants:      store,
			Applications: store,
			Quotas:       store,
			Channels:     store,
			Authorizer:   auth,
			Auditor:      audit,
			Connections:  conns,
			Degradation:  degrade,
			Now:          func() time.Time { return time.UnixMilli(1_700_000_000_000) },
		}),
		store:   store,
		audit:   audit,
		auth:    auth,
		conns:   conns,
		degrade: degrade,
	}
}

func (f *adminFixture) do(method, path, subject string, body any) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("X-Tenant-Id", "t1")
	req.Header.Set("X-Admin-Subject", subject)
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	return rec
}

// An unknown principal must be refused, and the attempt recorded.
func TestUnauthorizedCallIsRefused(t *testing.T) {
	f := newAdminFixture(t)
	rec := f.do(http.MethodGet, "/admin/tenants", "nobody@example.com", nil)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// A viewer may read but not change. Splitting read from write is what makes it
// safe to give an operator visibility without giving them the ability to
// disable a tenant's authentication.
func TestViewerCannotMutate(t *testing.T) {
	f := newAdminFixture(t)

	if rec := f.do(http.MethodGet, "/admin/tenants", "viewer@example.com", nil); rec.Code != http.StatusOK {
		t.Errorf("viewer read status = %d, want 200", rec.Code)
	}
	rec := f.do(http.MethodPost, "/admin/tenants", "viewer@example.com",
		map[string]string{"name": "new"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("viewer write status = %d, want 403", rec.Code)
	}
}

// A principal for one tenant must not act on another, even naming it.
func TestTenantIsScopedToThePrincipal(t *testing.T) {
	f := newAdminFixture(t)

	// t2's admin is not granted anything in t1.
	f.auth.roles["t2/outsider@example.com"] = RoleAdmin
	rec := f.do(http.MethodGet, "/admin/tenants", "outsider@example.com", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d for a principal not granted in this tenant, want 401", rec.Code)
	}
}

// A store that is unreachable is not evidence that the addressed tenant is
// absent. Answering 404 to a database outage sends an operator to look for a
// tenant that does not have a problem while the real fault goes unreported.
func TestLookupDistinguishesAbsentFromUnreachable(t *testing.T) {
	seed := func(t *testing.T) *adminFixture {
		t.Helper()
		f := newAdminFixture(t)
		if rec := f.do(http.MethodPost, "/admin/tenants", "admin@example.com",
			map[string]any{"name": "acme"}); rec.Code != http.StatusCreated {
			t.Fatalf("seeding tenant: status %d", rec.Code)
		}
		return f
	}

	t.Run("an absent tenant is a 404", func(t *testing.T) {
		f := seed(t)
		rec := f.do(http.MethodGet, "/admin/tenants/nosuch", "admin@example.com", nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d for an absent tenant, want 404", rec.Code)
		}
	})

	t.Run("an unreachable store is a 500", func(t *testing.T) {
		f := seed(t)
		f.store.failReads = errors.New("dial tcp 10.0.0.4:5432: connection refused")
		rec := f.do(http.MethodGet, "/admin/tenants/nosuch", "admin@example.com", nil)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d for an unreachable store, want 500", rec.Code)
		}
		if strings.Contains(rec.Body.String(), "connection refused") {
			t.Error("the handler echoed the store's internal error text to the caller")
		}
	})

	t.Run("an unreachable store reading an application is a 500", func(t *testing.T) {
		f := seed(t)
		f.store.failReads = errors.New("dial tcp 10.0.0.4:5432: connection refused")
		rec := f.do(http.MethodGet, "/admin/applications/t1/nosuch", "admin@example.com", nil)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d for an unreachable store, want 500", rec.Code)
		}
	})
}

// Every mutation must be attributable: who, when, against what.
func TestEveryMutationIsAttributed(t *testing.T) {
	f := newAdminFixture(t)

	cases := []struct {
		method    string
		path      string
		subject   string
		body      any
		operation string
	}{
		{method: http.MethodPost, path: "/admin/applications", subject: "admin@example.com",
			body: map[string]any{"name": "app"}, operation: "application.create"},
		{method: http.MethodPut, path: "/admin/quotas/", subject: "admin@example.com",
			body: QuotaView{MaxConnections: 10}, operation: "quota.set"},
		{method: http.MethodPatch, path: "/admin/channels/?channel=room:1", subject: "admin@example.com",
			body: map[string]any{"max_subscribers": 5}, operation: "channel.set_limits"},
		{method: http.MethodPut, path: "/admin/degradation", subject: "admin@example.com",
			body: map[string]any{"manual": true, "level": 2}, operation: "degradation.set"},
	}

	for _, c := range cases {
		f.audit.mu.Lock()
		before := len(f.audit.actions)
		f.audit.mu.Unlock()

		rec := f.do(c.method, c.path, c.subject, c.body)
		if rec.Code >= 300 {
			t.Errorf("%s %s: status = %d, body %s", c.method, c.path, rec.Code, rec.Body)
			continue
		}
		f.audit.mu.Lock()
		after := len(f.audit.actions)
		var last Action
		if after > 0 {
			last = f.audit.actions[after-1]
		}
		f.audit.mu.Unlock()

		if after != before+1 {
			t.Errorf("%s %s: audit entries %d, want one more than %d", c.method, c.path, after, before)
			continue
		}
		if last.Operation != c.operation {
			t.Errorf("%s: operation = %q, want %q", c.path, last.Operation, c.operation)
		}
		if last.Principal != "admin@example.com" {
			t.Errorf("%s: principal = %q", c.path, last.Principal)
		}
		if last.TenantID != "t1" {
			t.Errorf("%s: tenant = %q", c.path, last.TenantID)
		}
		if last.At.IsZero() {
			t.Errorf("%s: audit entry has no timestamp", c.path)
		}
	}
}

// Reads must not be audited as changes; otherwise the audit trail is useless for
// answering what changed.
func TestReadsAreNotAttributedAsChanges(t *testing.T) {
	f := newAdminFixture(t)
	f.do(http.MethodGet, "/admin/tenants", "admin@example.com", nil)
	f.do(http.MethodGet, "/admin/connections", "admin@example.com", nil)
	f.do(http.MethodGet, "/admin/quotas/", "admin@example.com", nil)

	f.audit.mu.Lock()
	defer f.audit.mu.Unlock()
	if len(f.audit.actions) != 0 {
		t.Errorf("reads produced %d audit entries, want 0", len(f.audit.actions))
	}
}

// An application's secret is returned exactly once, at creation, and never on a
// read.
func TestSecretIsReturnedOnceAndNeverExposed(t *testing.T) {
	f := newAdminFixture(t)

	rec := f.do(http.MethodPost, "/admin/applications", "admin@example.com",
		map[string]any{"name": "app", "can_publish": true, "can_subscribe": true})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "secret-") {
		t.Fatalf("creation did not return the secret: %s", rec.Body)
	}

	var created struct {
		Application AppView `json:"application"`
		Secret      string  `json:"secret"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	id := created.Application.ID

	list := f.do(http.MethodGet, "/admin/applications", "admin@example.com", nil)
	if strings.Contains(list.Body.String(), created.Secret) {
		t.Error("the secret appeared in a list response")
	}
	get := f.do(http.MethodGet, "/admin/applications/"+id, "admin@example.com", nil)
	if strings.Contains(get.Body.String(), created.Secret) {
		t.Error("the secret appeared in a get response")
	}
}

// Disabling an application must say so in the audit trail, because it ends live
// connections and an operator later needs to know why.
func TestDisablingApplicationIsRecorded(t *testing.T) {
	f := newAdminFixture(t)
	rec := f.do(http.MethodPost, "/admin/applications", "admin@example.com",
		map[string]any{"name": "app"})
	var created struct {
		Application AppView `json:"application"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	rec = f.do(http.MethodPatch, "/admin/applications/"+created.Application.ID,
		"admin@example.com", map[string]any{"enabled": false})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}

	last := f.audit.last()
	if last.Operation != "application.set_enabled" {
		t.Errorf("operation = %q", last.Operation)
	}
	if !strings.Contains(last.Detail, "live connections ended") {
		t.Errorf("detail = %q, want the user-visible consequence recorded", last.Detail)
	}
}

func TestRotationIsAttributed(t *testing.T) {
	f := newAdminFixture(t)
	rec := f.do(http.MethodPost, "/admin/applications", "admin@example.com",
		map[string]any{"name": "app"})
	var created struct {
		Application AppView `json:"application"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)

	rec = f.do(http.MethodPost, "/admin/applications/"+created.Application.ID+"/rotate",
		"admin@example.com", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if f.store.rotations[created.Application.ID] != 1 {
		t.Error("rotation was not applied")
	}
	if last := f.audit.last(); last.Operation != "application.rotate_secret" {
		t.Errorf("operation = %q, want application.rotate_secret", last.Operation)
	}
}

// Lowering a connection ceiling below current usage is allowed, and must be
// recorded, because existing connections are not ended by it.
func TestLoweredConnectionCeilingIsRecorded(t *testing.T) {
	f := newAdminFixture(t)
	rec := f.do(http.MethodPut, "/admin/quotas/", "admin@example.com",
		QuotaView{MaxConnections: 0, MaxPublishQPS: 50})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	last := f.audit.last()
	if last.Operation != "quota.set" {
		t.Errorf("operation = %q", last.Operation)
	}
	if !strings.Contains(last.Detail, "connections=0") {
		t.Errorf("detail = %q, want the new ceiling recorded", last.Detail)
	}
}

func TestChannelViewReportsDerivedVersusOverridden(t *testing.T) {
	f := newAdminFixture(t)

	// An override must be distinguishable from a derived ceiling, or an operator
	// cannot tell why a channel is behaving as it is.
	rec := f.do(http.MethodPatch, "/admin/channels/?channel=room:1", "admin@example.com",
		map[string]any{"publish_ceiling": 0})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	var view ChannelView
	_ = json.Unmarshal(rec.Body.Bytes(), &view)
	if !view.CeilingOverride {
		t.Error("an explicit zero ceiling was not recorded as an override")
	}
	if last := f.audit.last(); !strings.Contains(last.Detail, "override") {
		t.Errorf("detail = %q, want it to say the ceiling is overridden", last.Detail)
	}
}

func TestConnectionListingIsTenantScoped(t *testing.T) {
	f := newAdminFixture(t)
	rec := f.do(http.MethodGet, "/admin/connections", "admin@example.com", nil)

	var views []ConnectionView
	if err := json.Unmarshal(rec.Body.Bytes(), &views); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, v := range views {
		if v.TenantID != "t1" {
			t.Errorf("connection %d from tenant %q appeared in t1's listing", v.ID, v.TenantID)
		}
	}
	if len(views) != 1 {
		t.Errorf("listed %d connections, want 1", len(views))
	}
}

func TestTerminatingOneConnectionLeavesOthers(t *testing.T) {
	f := newAdminFixture(t)

	rec := f.do(http.MethodDelete, "/admin/connections/?connection_id=1", "admin@example.com", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	remaining := f.conns.List("t2")
	if len(remaining) != 1 {
		t.Errorf("terminating a connection in t1 changed t2's connections")
	}

	// The entry must say the client may reconnect, because termination and a
	// ban are routinely confused.
	if last := f.audit.last(); !strings.Contains(last.Detail, "may reconnect") {
		t.Errorf("detail = %q, want it to distinguish termination from a ban", last.Detail)
	}
}

func TestDegradationControl(t *testing.T) {
	f := newAdminFixture(t)

	rec := f.do(http.MethodPut, "/admin/degradation", "admin@example.com",
		map[string]any{"manual": true, "level": 3})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if !f.degrade.Manual() || f.degrade.Level() != 3 {
		t.Errorf("ladder = manual %v level %d", f.degrade.Manual(), f.degrade.Level())
	}

	// An out-of-range level must be refused rather than clamped, so a typo does
	// not silently set the wrong rung.
	rec = f.do(http.MethodPut, "/admin/degradation", "admin@example.com",
		map[string]any{"manual": true, "level": 9})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d for level 9, want 400", rec.Code)
	}
	if f.degrade.Level() != 3 {
		t.Errorf("a refused request still changed the ladder to %d", f.degrade.Level())
	}
}

func TestMissingTenantOrPrincipalIsRejected(t *testing.T) {
	f := newAdminFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d with no tenant header, want 400", rec.Code)
	}
}

func TestUnsupportedMethodsAreRejected(t *testing.T) {
	f := newAdminFixture(t)
	if rec := f.do(http.MethodDelete, "/admin/tenants", "admin@example.com", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

// TestDocumentedEndpointsAreRouted keeps docs/admin-api.md honest.
//
// A documented endpoint that is not routed is an integration that silently 404s,
// and a routed endpoint that is undocumented is a surface nobody reviewed.
func TestDocumentedEndpointsAreRouted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "admin-api.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	// Every documented path must be routable. The paths carry a trailing
	// segment that names a resource, which is matched by prefix.
	documented := regexp.MustCompile("`(GET|POST|PUT|PATCH|DELETE)` \\| `(/admin/[^`]*)` \\|")
	seen := 0
	for _, m := range documented.FindAllStringSubmatch(doc, -1) {
		method, path := m[1], m[2]
		seen++

		req := httptest.NewRequest(method, path, strings.NewReader(""))
		req.Header.Set("X-Tenant-Id", "t1")
		req.Header.Set("X-Admin-Subject", "admin@example.com")
		rec := httptest.NewRecorder()

		s := New(ServerConfig{
			Tenants:      newFakeStore(),
			Applications: newFakeStore(),
			Quotas:       newFakeStore(),
			Channels:     newFakeStore(),
			Authorizer:   &fakeAuthorizer{roles: map[string]Role{}},
			Auditor:      &fakeAuditor{},
			Connections:  &fakeConns{},
			Degradation:  &fakeDegrade{},
			Now:          time.Now,
		})
		s.ServeHTTP(rec, req)

		// 404 from the mux means the path is not routed at all. Anything else,
		// including 401 and 400, means the route exists and handled it.
		if rec.Code == http.StatusNotFound {
			t.Errorf("documented endpoint %s %s is not routed (404)", method, path)
		}
	}
	if seen == 0 {
		t.Fatal("no endpoints parsed from the document")
	}
}

// Every audit operation the document lists must be one the surface can emit.
func TestDocumentedAuditOperationsAreEmitted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "admin-api.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	emitted := regexp.MustCompile("s\\.record\\(ctx, subject, \"([a-z_.]+)\"")
	names := map[string]bool{}
	for _, m := range emitted.FindAllStringSubmatch(mustReadSource(t), -1) {
		names[m[1]] = true
	}

	listed := regexp.MustCompile("`([a-z_]+\\.[a-z_]+)` \\|")
	listedCount := 0
	for _, m := range listed.FindAllStringSubmatch(doc, -1) {
		op := m[1]
		listedCount++
		if !names[op] {
			t.Errorf("audit operation %q is documented but never emitted", op)
		}
	}
	if listedCount == 0 {
		t.Fatal("no audit operations parsed from the document")
	}
	t.Logf("%d audit operations documented, %d emitted", listedCount, len(names))
}

func mustReadSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	return string(b)
}
