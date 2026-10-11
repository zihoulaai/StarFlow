package controlplane

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/starflow/push/internal/bus"
)

// TenantConfig is the snapshot a gateway serves one tenant from.
//
// It is a value, not a pointer to live state: swapping a snapshot is what makes
// "applies without a restart" true, and it also means a request in flight keeps
// serving the configuration it started with rather than observing a half-applied
// change.
type TenantConfig struct {
	TenantID string
	Quota    Quota
	Channels map[string]ChannelConfig
	Rules    []ModerationRule
	// Blocks are the tenant's network blocks as CIDRs.
	Blocks []NetworkBlock
	// Applications is keyed by application id.
	Applications map[string]Application
	// ApplicationsByKey is keyed by app key. Clients present a key and the
	// tenant is discovered from it, so the cache needs this index to resolve a
	// credential without a scan across every loaded tenant.
	ApplicationsByKey map[string]Application
	// LoadedAt records when this snapshot was read from the store.
	LoadedAt time.Time
}

// Loader reads a tenant's configuration from the durable store.
type Loader interface {
	Load(ctx context.Context, tenantID string) (TenantConfig, error)
}

// StoreLoader reads snapshots from a control-plane Store.
type StoreLoader struct{ store *Store }

// NewStoreLoader returns a Loader backed by store.
func NewStoreLoader(store *Store) StoreLoader { return StoreLoader{store: store} }

// Load assembles a tenant's configuration.
//
// Every query is tenant-scoped, so a snapshot can only ever contain that
// tenant's state.
func (l StoreLoader) Load(ctx context.Context, tenantID string) (TenantConfig, error) {
	s := l.store

	quota, err := s.Quota(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	channels, err := s.ListChannelConfigs(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	rules, err := s.ListRules(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	blocks, err := s.ListNetworkBlocks(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}
	apps, err := s.ListApplications(ctx, tenantID)
	if err != nil {
		return TenantConfig{}, err
	}

	cfg := TenantConfig{
		TenantID:          tenantID,
		Quota:             quota,
		Channels:          make(map[string]ChannelConfig, len(channels)),
		Rules:             rules,
		Blocks:            blocks,
		Applications:      make(map[string]Application, len(apps)),
		ApplicationsByKey: make(map[string]Application, len(apps)),
		LoadedAt:          s.Now(),
	}
	for _, c := range channels {
		cfg.Channels[c.Channel] = c
	}
	for _, a := range apps {
		cfg.Applications[a.ID] = a
		cfg.ApplicationsByKey[a.AppKey] = a
	}
	return cfg, nil
}

// ConfigCache holds the current snapshot per tenant and swaps it on change.
//
// A gateway reads configuration from this cache on every publish admission.
// It never queries the store there, which is what keeps the data plane free of
// network calls.
type ConfigCache struct {
	loader Loader
	bus    bus.ConfigBus

	mu     sync.RWMutex
	byName map[string]*TenantConfig

	reloads atomic.Int64
}

// NewConfigCache returns a cache backed by loader.
func NewConfigCache(loader Loader) *ConfigCache {
	return &ConfigCache{loader: loader, byName: map[string]*TenantConfig{}}
}

// Tenant returns the current snapshot for a tenant.
//
// A tenant that has never been loaded gets an empty snapshot rather than an
// error: refusing to serve is the control plane's decision, not the cache's.
func (c *ConfigCache) Tenant(tenantID string) TenantConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cfg, ok := c.byName[tenantID]; ok {
		return *cfg
	}
	return TenantConfig{
		TenantID:          tenantID,
		Channels:          map[string]ChannelConfig{},
		Applications:      map[string]Application{},
		ApplicationsByKey: map[string]Application{},
	}
}

// FindApplication resolves an app key across every loaded tenant.
//
// App keys are globally unique, so this is the only lookup that is not
// pre-scoped by tenant. It returns false when no loaded tenant holds the key,
// which for an unknown key and for a real key belonging to a tenant this
// instance has not loaded are indistinguishable — deliberately, since telling
// them apart would confirm whether a key exists.
func (c *ConfigCache) FindApplication(appKey string) (Application, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cfg := range c.byName {
		if app, ok := cfg.ApplicationsByKey[appKey]; ok {
			return app, true
		}
	}
	return Application{}, false
}

// Loaded reports whether a tenant's configuration has been read at least once.
func (c *ConfigCache) Loaded(tenantID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.byName[tenantID]
	return ok
}

// Reload reads a tenant's configuration and installs it.
func (c *ConfigCache) Reload(ctx context.Context, tenantID string) error {
	cfg, err := c.loader.Load(ctx, tenantID)
	if err != nil {
		return err
	}
	snapshot := cfg
	c.mu.Lock()
	c.byName[tenantID] = &snapshot
	c.mu.Unlock()
	c.reloads.Add(1)
	return nil
}

// ReloadCount reports how many reloads have completed. Tests use it to prove a
// change was applied by reloading rather than by restarting.
func (c *ConfigCache) ReloadCount() int64 { return c.reloads.Load() }

// Reloader applies configuration changes to a cache as they are notified.
//
// The notification names what changed and the tenant it belongs to; the new
// values always come from the store. A notification can therefore never leave a
// gateway holding values that were never persisted.
type Reloader struct {
	cache *ConfigCache
	bus   bus.ConfigBus

	// OnChange is invoked after a successful reload, for audit and metrics.
	OnChange func(bus.ConfigChange)

	mu        sync.Mutex
	failures  atomic.Int64
	reloadErr error
}

// NewReloader returns a Reloader that applies notifications to cache.
func NewReloader(cache *ConfigCache, cfgBus bus.ConfigBus) *Reloader {
	return &Reloader{cache: cache, bus: cfgBus}
}

// Run consumes change notifications until ctx is cancelled.
func (r *Reloader) Run(ctx context.Context) error {
	if r.bus == nil {
		<-ctx.Done()
		return ctx.Err()
	}

	handler := func(change bus.ConfigChange) {
		// Reload failures are recorded rather than propagated: a transient
		// store outage must not stop this instance from applying later
		// changes, and the failure count is what the readiness and alerting
		// paths consult.
		if err := r.cache.Reload(context.WithoutCancel(ctx), change.TenantID); err != nil {
			r.failures.Add(1)
			r.mu.Lock()
			r.reloadErr = err
			r.mu.Unlock()
			return
		}
		if r.OnChange != nil {
			r.OnChange(change)
		}
	}

	if err := r.bus.SubscribeChanges(ctx, handler); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// Failures reports how many reloads have failed.
func (r *Reloader) Failures() int64 { return r.failures.Load() }

// LastError returns the most recent reload failure, if any.
func (r *Reloader) LastError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reloadErr
}

// Tenants lists the tenants whose configuration is currently cached.
func (c *ConfigCache) Tenants() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.byName))
	for id := range c.byName {
		out = append(out, id)
	}
	return out
}
