package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
)

// A quota change must reach a live gateway without the process restarting. The
// gateway holds a snapshot, so the test asserts both that the new value is
// visible and that the cache was reloaded rather than rebuilt.
func TestConfigCacheAppliesChangeWithoutRestart(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 10, MaxPublishQPS: 100, MaxChannels: 5,
	}); err != nil {
		t.Fatalf("UpsertQuota: %v", err)
	}

	cache := NewConfigCache(NewStoreLoader(f.store))
	if err := cache.Reload(ctx, f.t1.ID); err != nil {
		t.Fatalf("initial Reload: %v", err)
	}
	if got := cache.Get(f.t1.ID).Quota.MaxConnections; got != 10 {
		t.Fatalf("initial MaxConnections = %d, want 10", got)
	}

	// The same cache instance, with no restart between the two reads.
	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 99, MaxPublishQPS: 100, MaxChannels: 5,
	}); err != nil {
		t.Fatalf("UpsertQuota (raise): %v", err)
	}

	before := cache.ReloadCount()
	if err := cache.Reload(ctx, f.t1.ID); err != nil {
		t.Fatalf("Reload after change: %v", err)
	}
	if got := cache.Get(f.t1.ID).Quota.MaxConnections; got != 99 {
		t.Errorf("MaxConnections = %d after reload, want 99", got)
	}
	if cache.ReloadCount() != before+1 {
		t.Errorf("reload count = %d, want %d", cache.ReloadCount(), before+1)
	}
}

// A snapshot is a value: a request that read configuration keeps serving it even
// after a reload swaps in a new one. A pointer to live state would let an
// in-flight request observe a half-applied change.
func TestConfigSnapshotIsImmutableAcrossReload(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 10, MaxPublishQPS: 1, MaxChannels: 1,
	}); err != nil {
		t.Fatalf("UpsertQuota: %v", err)
	}
	cache := NewConfigCache(NewStoreLoader(f.store))
	if err := cache.Reload(ctx, f.t1.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	inFlight := cache.Get(f.t1.ID)

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 77, MaxPublishQPS: 1, MaxChannels: 1,
	}); err != nil {
		t.Fatalf("UpsertQuota (raise): %v", err)
	}
	if err := cache.Reload(ctx, f.t1.ID); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if inFlight.Quota.MaxConnections != 10 {
		t.Errorf("in-flight snapshot changed to %d, want the value it read (10)", inFlight.Quota.MaxConnections)
	}
	if got := cache.Get(f.t1.ID).Quota.MaxConnections; got != 77 {
		t.Errorf("current snapshot = %d, want 77", got)
	}
}

func TestConfigCacheUnknownTenantYieldsEmptySnapshot(t *testing.T) {
	cache := NewConfigCache(NewStoreLoader(newFixture(t).store))
	cfg := cache.Get("never-seen")
	if cfg.TenantID != "never-seen" {
		t.Errorf("TenantID = %q", cfg.TenantID)
	}
	if cfg.Quota.MaxConnections != 0 {
		t.Errorf("unloaded tenant reported a ceiling: %+v", cfg.Quota)
	}
	if cache.Loaded("never-seen") {
		t.Error("Loaded reported an unloaded tenant as loaded")
	}
}

// The end-to-end path: an operator writes to the store, publishes a change, and
// the gateway's cache picks it up.
func TestReloaderAppliesPublishedChange(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shared := bus.NewMemoryBus()
	defer shared.Close()

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 5, MaxPublishQPS: 5, MaxChannels: 5,
	}); err != nil {
		t.Fatalf("UpsertQuota: %v", err)
	}

	cache := NewConfigCache(NewStoreLoader(f.store))
	reloader := NewReloader(cache, shared)

	applied := make(chan bus.ConfigChange, 4)
	reloader.OnChange = func(c bus.ConfigChange) { applied <- c }

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() { done <- reloader.Run(runCtx) }()

	// Wait for the subscriber to attach rather than sleeping: publishing into
	// a bus with no listeners would make this test pass for the wrong reason.
	waitForCond(t, time.Second, func() bool { return shared.ConfigSubscribers() > 0 })

	if _, err := f.store.UpsertQuota(ctx, Quota{
		TenantID: f.t1.ID, MaxConnections: 500, MaxPublishQPS: 5, MaxChannels: 5,
	}); err != nil {
		t.Fatalf("UpsertQuota (raise): %v", err)
	}
	if err := shared.PublishChange(ctx, bus.ConfigChange{
		Kind: bus.ConfigQuotaChanged, TenantID: f.t1.ID,
	}); err != nil {
		t.Fatalf("PublishChange: %v", err)
	}

	select {
	case change := <-applied:
		if change.Kind != bus.ConfigQuotaChanged || change.TenantID != f.t1.ID {
			t.Errorf("applied change = %+v", change)
		}
	case <-time.After(bus.PropagationInterval):
		t.Fatalf("change was not applied within %v", bus.PropagationInterval)
	}

	if got := cache.Get(f.t1.ID).Quota.MaxConnections; got != 500 {
		t.Errorf("MaxConnections = %d after reload, want 500", got)
	}

	stopRun()
	<-done
}

// A failing reload must be counted and must not stop the reloader from applying
// later changes, or one transient store outage would freeze this instance's view
// of every tenant indefinitely.
func TestReloaderSurvivesAFailedReload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shared := bus.NewMemoryBus()
	defer shared.Close()

	// A loader that fails the first time and succeeds after.
	calls := 0
	loader := loaderFunc(func(_ context.Context, tenantID string) (TenantConfig, error) {
		calls++
		if calls == 1 {
			return TenantConfig{}, context.DeadlineExceeded
		}
		return TenantConfig{TenantID: tenantID, LoadedAt: time.Now()}, nil
	})

	cache := NewConfigCache(loader)
	reloader := NewReloader(cache, shared)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- reloader.Run(runCtx) }()
	waitForCond(t, time.Second, func() bool { return shared.ConfigSubscribers() > 0 })

	if err := shared.PublishChange(ctx, bus.ConfigChange{Kind: bus.ConfigQuotaChanged, TenantID: "t1"}); err != nil {
		t.Fatalf("PublishChange: %v", err)
	}
	waitForCond(t, time.Second, func() bool { return reloader.Failures() == 1 })

	if err := shared.PublishChange(ctx, bus.ConfigChange{Kind: bus.ConfigQuotaChanged, TenantID: "t1"}); err != nil {
		t.Fatalf("PublishChange: %v", err)
	}
	waitForCond(t, time.Second, func() bool { return !cache.Get("t1").LoadedAt.IsZero() })

	if reloader.Failures() != 1 {
		t.Errorf("failures = %d, want 1", reloader.Failures())
	}
	if reloader.LastError() == nil {
		t.Error("LastError is nil after a failed reload")
	}

	stop()
	<-done
}

// A snapshot must contain only its own tenant's data, because the cache is what
// the publish path consults.
func TestConfigSnapshotIsTenantScoped(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	for _, tenant := range []Tenant{f.t1, f.t2} {
		if _, err := f.store.UpsertRule(ctx, ModerationRule{
			TenantID: tenant.ID, Pattern: "only-" + tenant.ID, Action: ActionBlock,
		}); err != nil {
			t.Fatalf("UpsertRule: %v", err)
		}
		if _, err := f.store.UpsertQuota(ctx, Quota{
			TenantID: tenant.ID, MaxConnections: len(tenant.ID), MaxPublishQPS: 1, MaxChannels: 1,
		}); err != nil {
			t.Fatalf("UpsertQuota: %v", err)
		}
	}

	cache := NewConfigCache(NewStoreLoader(f.store))
	for _, tenant := range []Tenant{f.t1, f.t2} {
		if err := cache.Reload(ctx, tenant.ID); err != nil {
			t.Fatalf("Reload: %v", err)
		}
	}

	for _, tenant := range []Tenant{f.t1, f.t2} {
		cfg := cache.Get(tenant.ID)
		if len(cfg.Rules) != 1 {
			t.Fatalf("tenant %s got %d rules, want 1", tenant.ID, len(cfg.Rules))
		}
		want := "only-" + tenant.ID
		if cfg.Rules[0].Pattern != want {
			t.Errorf("tenant %s got rule %q, want %q", tenant.ID, cfg.Rules[0].Pattern, want)
		}
	}
}

func TestConfigCacheTenants(t *testing.T) {
	cache := NewConfigCache(NewStoreLoader(newFixture(t).store))
	if got := cache.Tenants(); len(got) != 0 {
		t.Errorf("Tenants on an empty cache = %v, want none", got)
	}
	ctx := context.Background()
	if err := cache.Reload(ctx, "a"); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if err := cache.Reload(ctx, "b"); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := cache.Tenants(); len(got) != 2 {
		t.Errorf("Tenants = %v, want 2", got)
	}
}

type loaderFunc func(context.Context, string) (TenantConfig, error)

func (f loaderFunc) Load(ctx context.Context, tenantID string) (TenantConfig, error) {
	return f(ctx, tenantID)
}

// waitForCond polls until cond holds or the timeout expires, failing the test
// if it never does. Used instead of a fixed sleep wherever the condition itself
// is observable, so tests are neither slow nor flaky.
func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition did not hold within %v", timeout)
}
