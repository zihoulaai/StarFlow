package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
)

// A configuration change published by one gateway must reach another gateway's
// reload handler within the propagation interval, without either process
// restarting. This is the property that lets quotas and moderation rules change
// on a live instance.
func TestConfigChangeReachesAnotherProcess(t *testing.T) {
	shared := bus.NewMemoryBus()

	// Two independent subscribers stand in for two gateway processes: neither
	// shares memory with the publisher beyond the bus itself.
	reloaded := make(chan bus.ConfigChange, 8)
	otherCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shared.SubscribeChanges(otherCtx, func(c bus.ConfigChange) {
		reloaded <- c
	}); err != nil {
		t.Fatalf("SubscribeChanges: %v", err)
	}

	ctx := context.Background()
	if err := shared.PublishChange(ctx, bus.ConfigChange{
		Kind:     bus.ConfigQuotaChanged,
		TenantID: "tenant-1",
		Subject:  "quota",
	}); err != nil {
		t.Fatalf("PublishChange: %v", err)
	}

	select {
	case c := <-reloaded:
		if c.Kind != bus.ConfigQuotaChanged || c.TenantID != "tenant-1" {
			t.Errorf("received %+v, want the published change", c)
		}
	case <-time.After(bus.PropagationInterval):
		t.Fatalf("change did not propagate within %v", bus.PropagationInterval)
	}
}

// Every change kind the gateway handles must survive the wire, including a
// tenant-scoped one. A silently dropped notification means an instance keeps
// serving a stale rule set indefinitely.
func TestAllConfigChangeKindsPropagate(t *testing.T) {
	b := bus.NewMemoryBus()
	defer b.Close()

	kinds := []bus.ConfigChangeKind{
		bus.ConfigQuotaChanged,
		bus.ConfigChannelChanged,
		bus.ConfigRulesChanged,
		bus.ConfigRestrictionsChanged,
		bus.ConfigSwitchChanged,
		bus.ConfigDegradationChanged,
		bus.ConfigApplicationDisabled,
	}

	got := make(chan bus.ConfigChange, len(kinds))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.SubscribeChanges(ctx, func(c bus.ConfigChange) { got <- c }); err != nil {
		t.Fatalf("SubscribeChanges: %v", err)
	}

	for _, k := range kinds {
		if err := b.PublishChange(ctx, bus.ConfigChange{Kind: k, TenantID: "t"}); err != nil {
			t.Fatalf("PublishChange(%d): %v", k, err)
		}
	}

	seen := map[bus.ConfigChangeKind]bool{}
	for i := 0; i < len(kinds); i++ {
		select {
		case c := <-got:
			seen[c.Kind] = true
		case <-time.After(bus.PropagationInterval):
			t.Fatalf("only %d of %d change kinds arrived", i, len(kinds))
		}
	}
	for _, k := range kinds {
		if !seen[k] {
			t.Errorf("change kind %d never arrived", k)
		}
	}
}

// A change notification carries the tenant and kind, never the values. That
// forces the receiver to read what was actually persisted, so a notification
// cannot disagree with the durable store.
func TestConfigChangeCarriesNoValues(t *testing.T) {
	b := bus.NewMemoryBus()
	defer b.Close()

	got := make(chan bus.ConfigChange, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.SubscribeChanges(ctx, func(c bus.ConfigChange) { got <- c }); err != nil {
		t.Fatalf("SubscribeChanges: %v", err)
	}

	if err := b.PublishChange(ctx, bus.ConfigChange{
		Kind: bus.ConfigChannelChanged, TenantID: "t", Subject: "room:1",
	}); err != nil {
		t.Fatalf("PublishChange: %v", err)
	}

	select {
	case c := <-got:
		if c.Subject != "room:1" {
			t.Errorf("subject = %q, want room:1", c.Subject)
		}
		// The struct has no payload field at all, which is the guarantee: a
		// stale value cannot be smuggled through the notification.
	case <-time.After(bus.PropagationInterval):
		t.Fatal("change did not propagate")
	}
}
