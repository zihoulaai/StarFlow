// Package adminadapters adapts the control plane and the data plane to the
// interfaces the admin HTTP surface is written against.
//
// The adapters exist because the admin surface was specified against narrow
// interfaces while the subsystems it drives own richer types. Each adapter is a
// projection: it translates, and it does not cache, because a cached copy of an
// administrative view would let the admin API report state the data plane has
// already changed.
package adminadapters

import (
	"context"
	"errors"
	"sync"

	"github.com/starflow/push/internal/admin"
	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/gateway"
)

// Store adapts controlplane.Store to admin.Store.
type Store struct {
	CP *controlplane.Store
	// Cache is consulted for the live subscriber count on a channel view, since
	// that number lives in the data plane and not in the database.
	Cache *gateway.Store
	// Refresh reloads a tenant into the in-process config cache after a write.
	//
	// Without it the admin API and the data plane disagree: a credential created
	// through the API is durable in the database but absent from the cache, so
	// every publish and subscribe using it is refused as unknown while the
	// operator is looking at a successful creation. A single-instance deployment
	// never receives a change notification, so this is the only path that keeps
	// the cache current there.
	Refresh func(tenantID string)
}

// NewStore returns a store adapter.
func NewStore(cp *controlplane.Store, cache *gateway.Store, refresh func(string)) *Store {
	return &Store{CP: cp, Cache: cache, Refresh: refresh}
}

// refresh reloads the affected tenant's configuration.
//
// A reload failure is not propagated: the write already committed, and refusing
// to report success would leave the operator retrying a change that is in fact
// durable. The staleness surfaces on the next publish as an unknown credential.
func (s *Store) refresh(tenantID string) {
	if s.Refresh != nil && tenantID != "" {
		s.Refresh(tenantID)
	}
}

// ListTenants returns every tenant. It is not tenant-scoped: the surface it
// serves is the operator's, who is granted a tenant per administrative call and
// sees the whole set from the tenant list.
func (s *Store) ListTenants(ctx context.Context) ([]admin.TenantView, error) {
	tenants, err := s.CP.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]admin.TenantView, 0, len(tenants))
	for _, t := range tenants {
		out = append(out, admin.TenantView{ID: t.ID, Name: t.Name, CreatedAt: t.CreatedAt})
	}
	return out, nil
}

// GetTenant returns one tenant, or admin.ErrNotFound if there is no such row.
func (s *Store) GetTenant(ctx context.Context, id string) (admin.TenantView, error) {
	t, err := s.CP.GetTenant(ctx, id)
	if err != nil {
		return admin.TenantView{}, translate(err)
	}
	return admin.TenantView{ID: t.ID, Name: t.Name, CreatedAt: t.CreatedAt}, nil
}

// CreateTenant creates a tenant and reloads this instance's configuration for
// it, so the new tenant can publish without a restart.
func (s *Store) CreateTenant(ctx context.Context, name string) (admin.TenantView, error) {
	t, err := s.CP.CreateTenant(ctx, controlplane.Tenant{Name: name})
	if err != nil {
		return admin.TenantView{}, err
	}
	s.refresh(t.ID)
	return admin.TenantView{ID: t.ID, Name: t.Name, CreatedAt: t.CreatedAt}, nil
}

// UpdateTenant renames a tenant. No reload follows: the name is not consulted
// on the admission path, so the change cannot affect this instance's behaviour.
func (s *Store) UpdateTenant(ctx context.Context, id, name string) (admin.TenantView, error) {
	t, err := s.CP.UpdateTenant(ctx, id, func(dst *controlplane.Tenant) { dst.Name = name })
	if err != nil {
		return admin.TenantView{}, translate(err)
	}
	return admin.TenantView{ID: t.ID, Name: t.Name, CreatedAt: t.CreatedAt}, nil
}

// ListApplications returns the tenant's credentials.
func (s *Store) ListApplications(ctx context.Context, tenantID string) ([]admin.AppView, error) {
	apps, err := s.CP.ListApplications(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]admin.AppView, 0, len(apps))
	for _, a := range apps {
		out = append(out, appView(a))
	}
	return out, nil
}

// GetApplication returns one credential, or admin.ErrNotFound.
//
// The view never carries the secret digest, so an operator cannot read one back
// out of the API even with full administrative authority.
func (s *Store) GetApplication(ctx context.Context, tenantID, id string) (admin.AppView, error) {
	a, err := s.CP.GetApplication(ctx, tenantID, id)
	if err != nil {
		return admin.AppView{}, translate(err)
	}
	return appView(a), nil
}

// CreateApplication issues a credential and returns its plaintext secret.
//
// The secret is returned once, here, and is not recoverable afterwards: only a
// digest is stored, so an operator who loses it must rotate. It is created
// enabled, because a credential that exists but refuses every call is not a
// state anyone asked for.
func (s *Store) CreateApplication(ctx context.Context, tenantID, name string, canPublish, canSubscribe bool) (admin.AppView, string, error) {
	a, secret, err := s.CP.CreateApplication(ctx, controlplane.Application{
		TenantID:     tenantID,
		Name:         name,
		CanPublish:   canPublish,
		CanSubscribe: canSubscribe,
		Enabled:      true,
	}, "")
	if err != nil {
		return admin.AppView{}, "", err
	}
	s.refresh(tenantID)
	return appView(a), secret, nil
}

// SetApplicationEnabled enables or disables a credential.
//
// Disabling ends the connections the credential already holds, not only future
// authentications. A credential switched off while its subscribers are live
// would otherwise keep delivering to them indefinitely.
func (s *Store) SetApplicationEnabled(ctx context.Context, tenantID, id string, enabled bool) (admin.AppView, error) {
	a, err := s.CP.UpdateApplication(ctx, tenantID, id, func(dst *controlplane.Application) {
		dst.Enabled = enabled
	})
	if err != nil {
		return admin.AppView{}, translate(err)
	}
	s.refresh(tenantID)
	return appView(a), nil
}

// RotateSecret issues a new secret and returns it once, as CreateApplication
// does. The previous secret keeps working until its own validity window closes,
// so a client that has not yet picked up the new one is not cut off mid-deploy.
func (s *Store) RotateSecret(ctx context.Context, tenantID, id string) (admin.AppView, string, error) {
	a, secret, err := s.CP.RotateApplicationSecret(ctx, tenantID, id, 0)
	if err != nil {
		return admin.AppView{}, "", err
	}
	s.refresh(tenantID)
	return appView(a), secret, nil
}

// GetQuota returns the tenant's ceilings, or admin.ErrNotFound.
func (s *Store) GetQuota(ctx context.Context, tenantID string) (admin.QuotaView, error) {
	q, err := s.CP.GetQuota(ctx, tenantID)
	if err != nil {
		return admin.QuotaView{}, translate(err)
	}
	return quotaView(q), nil
}

// SetQuota replaces the tenant's ceilings.
//
// Every field is taken from v, so a caller that omits one lowers it rather than
// leaving it as it was. This is deliberate: a partial update against a quota
// would read as clearing the limits the caller did not mention.
func (s *Store) SetQuota(ctx context.Context, tenantID string, v admin.QuotaView) (admin.QuotaView, error) {
	q, err := s.CP.UpsertQuota(ctx, controlplane.Quota{
		TenantID:             tenantID,
		MaxConnections:       v.MaxConnections,
		MaxPublishQPS:        v.MaxPublishQPS,
		MaxChannels:          v.MaxChannels,
		PublishFailurePolicy: v.PublishFailurePolicy,
		PerUserPublishQPS:    v.PerUserPublishQPS,
	})
	if err != nil {
		return admin.QuotaView{}, err
	}
	s.refresh(tenantID)
	return quotaView(q), nil
}

// GetChannel returns a channel's configured limits joined with its live
// subscriber count, which lives in this instance's data plane rather than in
// the database and so is absent for every other instance.
func (s *Store) GetChannel(ctx context.Context, tenantID, channel string) (admin.ChannelView, error) {
	c, err := s.CP.GetChannelConfig(ctx, tenantID, channel)
	if err != nil {
		return admin.ChannelView{}, translate(err)
	}
	return s.channelView(tenantID, c), nil
}

// SetChannelLimit writes a channel's limits.
//
// hasCeiling is separate from ceiling because clearing an override and setting
// it to zero are different operations: a zero ceiling would refuse every
// publish, which is a valid thing for an operator to want and cannot be
// expressed by the number alone. This costs two writes, because the control
// plane models the override as a nullable column that a single upsert cannot
// set and clear at once.
func (s *Store) SetChannelLimit(ctx context.Context, tenantID, channel string, maxSubscribers int, ceiling float64, hasCeiling bool) (admin.ChannelView, error) {
	c, err := s.CP.UpsertChannelConfig(ctx, controlplane.ChannelConfig{
		TenantID:       tenantID,
		Channel:        channel,
		MaxSubscribers: maxSubscribers,
	})
	if err != nil {
		return admin.ChannelView{}, err
	}
	if hasCeiling {
		v := ceiling
		c.PublishCeilingOverrideQPS = &v
	} else {
		c.PublishCeilingOverrideQPS = nil
	}
	c, err = s.CP.UpsertChannelConfig(ctx, c)
	if err != nil {
		return admin.ChannelView{}, err
	}
	s.refresh(tenantID)
	return s.channelView(tenantID, c), nil
}

func (s *Store) channelView(tenantID string, c controlplane.ChannelConfig) admin.ChannelView {
	v := admin.ChannelView{
		TenantID:          tenantID,
		Channel:           c.Channel,
		MaxSubscribers:    c.MaxSubscribers,
		CeilingOverride:   c.PublishCeilingOverrideQPS != nil,
		CoalescingEnabled: c.CoalescingEnabled,
	}
	if c.PublishCeilingOverrideQPS != nil {
		v.PublishCeiling = *c.PublishCeilingOverrideQPS
	}
	if s.Cache != nil {
		v.Subscribers = s.Cache.SubscriberCountFor(tenantID, c.Channel)
	}
	return v
}

func appView(a controlplane.Application) admin.AppView {
	return admin.AppView{
		ID: a.ID, TenantID: a.TenantID, Name: a.Name, AppKey: a.AppKey,
		CanPublish: a.CanPublish, CanSubscribe: a.CanSubscribe,
		Enabled: a.Enabled, CreatedAt: a.CreatedAt,
	}
}

func quotaView(q controlplane.Quota) admin.QuotaView {
	return admin.QuotaView{
		MaxConnections:       q.MaxConnections,
		MaxPublishQPS:        q.MaxPublishQPS,
		MaxChannels:          q.MaxChannels,
		PublishFailurePolicy: q.PublishFailurePolicy,
		PerUserPublishQPS:    q.PerUserPublishQPS,
	}
}

// translate maps a control-plane miss onto the admin surface's error.
//
// The admin sentinel is exported so the handler can branch on it. Translating
// onto a package-private value would leave every caller outside this package
// unable to tell a miss from a fault without matching on the error text.
func translate(err error) error {
	if errors.Is(err, controlplane.ErrNotFound) {
		return admin.ErrNotFound
	}
	return err
}

// Authorizer adapts control-plane admin grants to admin.Authorizer.
//
// The subject arrives in a request header and is therefore self-asserted. This
// adapter is the only thing standing between that string and the administrative
// surface, so it resolves the grant from the database rather than trusting the
// header: an absent row yields an empty role, which the admin server maps to
// 401.
type Authorizer struct {
	CP *controlplane.Store
}

// RoleFor resolves the grant held by a principal, or an empty role if there is
// none.
//
// An empty role and a nil error is the normal outcome for a caller with no
// grant, so the admin server maps it to 401 rather than treating it as a
// lookup failure. A stored role this build does not recognise also grants
// nothing rather than defaulting.
func (a *Authorizer) RoleFor(ctx context.Context, tenantID, subject string) (admin.Role, error) {
	if tenantID == "" || subject == "" {
		return "", nil
	}
	p, err := a.CP.GetAdminPrincipal(ctx, tenantID, subject)
	if errors.Is(err, controlplane.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !p.Enabled {
		return "", nil
	}
	switch p.Role {
	case controlplane.RoleAdmin:
		return admin.RoleAdmin, nil
	case controlplane.RoleViewer:
		return admin.RoleViewer, nil
	default:
		// An unrecognised stored role grants nothing. Defaulting it to admin
		// would turn a data-entry mistake into a privilege escalation.
		return "", nil
	}
}

// Auditor adapts admin actions onto the control-plane audit log.
//
// Audit writes must not fail the operation they describe: an administrative
// change that succeeded but could not be recorded is still a change, and
// refusing it would leave the operator unsure whether it applied. The error is
// therefore surfaced for the caller to log rather than propagated.
type Auditor struct {
	CP  *controlplane.Store
	Now func() (int64, int64)

	mu    sync.Mutex
	fails int
}

func (a *Auditor) RecordAdminAction(ctx context.Context, entry admin.Action) {
	e := controlplane.AuditEntry{
		TenantID: entry.TenantID,
		Kind:     controlplane.AuditAdmin,
		Subject:  entry.Principal,
		Channel:  entry.Target,
		Decision: entry.Operation,
		Body:     entry.Detail,
		At:       entry.At,
	}
	if err := a.CP.AppendAudit(ctx, e); err != nil {
		a.mu.Lock()
		a.fails++
		a.mu.Unlock()
	}
}

// Failures reports how many audit writes failed.
func (a *Auditor) Failures() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.fails
}

// Connections adapts the data plane's live connections to admin.Connections.
type Connections struct {
	Store *gateway.Store
	// Close terminates a connection by id, scoped to a tenant. It is wired to
	// the transport; a nil Close means no connection can be terminated.
	Close func(tenantID string, id uint64) bool
}

// List returns the instance's live connections, optionally narrowed to a tenant.
//
// A connection is listed once even when it holds several channels, so the count
// here matches the instance's connection count rather than its subscription
// count.
func (c *Connections) List(tenantID string) []admin.ConnectionView {
	if c.Store == nil {
		return nil
	}
	conns := c.Store.Connections()
	out := make([]admin.ConnectionView, 0, len(conns))
	for _, conn := range conns {
		if tenantID != "" && conn.TenantID != tenantID {
			continue
		}
		out = append(out, c.view(conn))
	}
	return out
}

// view projects a connection into the admin representation.
func (c *Connections) view(conn *gateway.Conn) admin.ConnectionView {
	channels := c.Store.ChannelsFor(conn)
	if channels == nil {
		channels = []string{}
	}
	buf := conn.Buffer()
	return admin.ConnectionView{
		ID:           conn.ID,
		TenantID:     conn.TenantID,
		Subject:      conn.Subject,
		Channels:     channels,
		State:        connStateName(conn.State()),
		PendingBytes: buf.Pending(),
		Dropped:      buf.Dropped(),
	}
}

// Terminate closes a connection and reports whether it was found.
//
// The tenant is passed through to the lookup rather than only being used to
// filter the listing: an administrator granted one tenant must not be able to
// close another's connection by naming its id.
func (c *Connections) Terminate(tenantID string, id uint64) bool {
	if c.Close == nil {
		return false
	}
	return c.Close(tenantID, id)
}

// Degradation adapts the ladder to admin.Degradation.
//
// The ladder has no Manual getter, so the flag is tracked here alongside the
// call that sets it. Mirroring it is only safe because SetManual is the only
// writer; a second path would make the mirror a lie.
type Degradation struct {
	Ladder *gateway.Degradation

	mu     sync.Mutex
	manual bool
}

func (d *Degradation) SetManual(on bool, level int) {
	d.mu.Lock()
	d.manual = on
	d.mu.Unlock()
	d.Ladder.SetManual(on, gateway.Level(level))
}

func (d *Degradation) Manual() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.manual
}

func (d *Degradation) Level() int {
	return int(d.Ladder.Level())
}

// connStateName renders a connection state for the admin surface.
func connStateName(s gateway.ConnState) string {
	switch s {
	case gateway.ConnOpen:
		return "open"
	case gateway.ConnDraining:
		return "draining"
	case gateway.ConnClosed:
		return "closed"
	default:
		return "unknown"
	}
}
