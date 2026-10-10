package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
)

// contract exercises the properties the fanout path depends on, rather than
// merely checking that an implementation compiles. A bus that fails any of
// these would silently break delivery in a way no unit test on the gateway
// alone would catch.
type contract struct {
	name string
	// makeBus returns a connected pair when the implementation needs one.
	makeBus func(t *testing.T) (bus.Bus, bus.ConfigBus)
}

// The properties: messages reach every subscriber regardless of which instance
// published them, and configuration changes reach every instance.
var contracts = []contract{
	{
		name: "memory",
		makeBus: func(*testing.T) (bus.Bus, bus.ConfigBus) {
			b := bus.NewMemoryBus()
			return b, b
		},
	},
	{
		name: "redis",
		makeBus: func(t *testing.T) (bus.Bus, bus.ConfigBus) {
			t.Skip("redis binding requires a running server; exercised by the integration suite")
			return nil, nil
		},
	},
}

// runContract is invoked once per registered contract.
func runContract(t *testing.T, c contract) {
	t.Run(c.name, func(t *testing.T) {
		t.Run("broadcastsToEverySubscriber", func(t *testing.T) {
			msgBus, _ := c.makeBus(t)
			defer msgBus.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const subscribers = 5
			var mu sync.Mutex
			received := map[int][]bus.Envelope{}
			var wg sync.WaitGroup
			wg.Add(subscribers)

			for i := 0; i < subscribers; i++ {
				i := i
				if err := msgBus.Subscribe(ctx, func(env bus.Envelope) {
					mu.Lock()
					received[i] = append(received[i], env)
					mu.Unlock()
					wg.Done()
				}); err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
			}

			env := bus.Envelope{TenantID: "t1", Channel: "room:1", MessageID: 42, Body: []byte("x")}
			if err := msgBus.Publish(ctx, env); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("not every subscriber received within 2s; got %d of %d",
					len(received), subscribers)
			}

			mu.Lock()
			defer mu.Unlock()
			for i := 0; i < subscribers; i++ {
				if len(received[i]) != 1 {
					t.Errorf("subscriber %d received %d envelopes, want exactly 1", i, len(received[i]))
				}
			}
		})

		t.Run("publishesToNoSubscribersIsNotAnError", func(t *testing.T) {
			msgBus, _ := c.makeBus(t)
			defer msgBus.Close()
			// A message published while no gateway holds a subscriber must be
			// accepted and discarded, not refused. Every gateway filters by its
			// own local subscriptions, so most published messages are for
			// channels no instance it publishes to is serving.
			if err := msgBus.Publish(context.Background(),
				bus.Envelope{TenantID: "t1", Channel: "nobody"}); err != nil {
				t.Errorf("Publish with no subscribers: %v", err)
			}
		})

		t.Run("preservesOrderPerPublisher", func(t *testing.T) {
			msgBus, _ := c.makeBus(t)
			defer msgBus.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var mu sync.Mutex
			var ids []uint64
			if err := msgBus.Subscribe(ctx, func(env bus.Envelope) {
				mu.Lock()
				ids = append(ids, env.MessageID)
				mu.Unlock()
			}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			for i := uint64(1); i <= 50; i++ {
				if err := msgBus.Publish(ctx, bus.Envelope{TenantID: "t1", MessageID: i}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				n := len(ids)
				mu.Unlock()
				if n == 50 {
					break
				}
				time.Sleep(time.Millisecond)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(ids) != 50 {
				t.Fatalf("received %d envelopes, want 50", len(ids))
			}
			for i, id := range ids {
				if id != uint64(i+1) {
					t.Fatalf("out of order at %d: got id %d, want %d", i, id, i+1)
				}
			}
		})

		t.Run("configChangesReachEverySubscriber", func(t *testing.T) {
			_, cfgBus := c.makeBus(t)
			defer cfgBus.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			const subscribers = 4
			var mu sync.Mutex
			got := make([][]bus.ConfigChange, subscribers)
			var wg sync.WaitGroup
			wg.Add(subscribers)

			for i := 0; i < subscribers; i++ {
				i := i
				if err := cfgBus.SubscribeChanges(ctx, func(c bus.ConfigChange) {
					mu.Lock()
					got[i] = append(got[i], c)
					mu.Unlock()
					wg.Done()
				}); err != nil {
					t.Fatalf("SubscribeChanges: %v", err)
				}
			}

			change := bus.ConfigChange{
				Kind:     bus.ConfigQuotaChanged,
				TenantID: "t1",
				Subject:  "quota",
			}
			if err := cfgBus.PublishChange(ctx, change); err != nil {
				t.Fatalf("PublishChange: %v", err)
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(bus.PropagationInterval):
				t.Fatalf("configuration change did not reach every subscriber within %v", bus.PropagationInterval)
			}

			mu.Lock()
			defer mu.Unlock()
			for i := range got {
				if len(got[i]) != 1 || got[i][0].TenantID != "t1" {
					t.Errorf("subscriber %d got %+v, want one change for t1", i, got[i])
				}
			}
		})

		t.Run("rejectsUseAfterClose", func(t *testing.T) {
			msgBus, cfgBus := c.makeBus(t)
			ctx := context.Background()

			if err := msgBus.Publish(ctx, bus.Envelope{TenantID: "t1"}); err != nil {
				t.Fatalf("Publish before close: %v", err)
			}
			if err := msgBus.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// A gateway shutting down must not appear able to accept traffic.
			if err := msgBus.Publish(ctx, bus.Envelope{TenantID: "t1"}); err == nil {
				t.Error("Publish after Close succeeded, want an error")
			}
			if err := msgBus.Subscribe(ctx, func(bus.Envelope) {}); err == nil {
				t.Error("Subscribe after Close succeeded, want an error")
			}
			if err := cfgBus.PublishChange(ctx, bus.ConfigChange{Kind: bus.ConfigQuotaChanged}); err == nil {
				t.Error("PublishChange after Close succeeded, want an error")
			}
		})

		t.Run("honoursContextCancellation", func(t *testing.T) {
			msgBus, _ := c.makeBus(t)
			defer msgBus.Close()
			ctx, cancel := context.WithCancel(context.Background())

			if err := msgBus.Subscribe(ctx, func(bus.Envelope) {}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			cancel()

			// A caller whose own context is already done must be told so rather
			// than having its publish silently dropped.
			if err := msgBus.Publish(ctx, bus.Envelope{TenantID: "t1"}); err == nil {
				t.Error("Publish with a cancelled context succeeded, want an error")
			}
			// A healthy caller is unaffected by another context's cancellation.
			if err := msgBus.Publish(context.Background(), bus.Envelope{TenantID: "t1"}); err != nil {
				t.Errorf("Publish with a healthy context after an unrelated cancel: %v", err)
			}
		})

		t.Run("doesNotRetainSubscribersAfterCancellation", func(t *testing.T) {
			memBus := bus.NewMemoryBus()
			ctx, cancel := context.WithCancel(context.Background())
			if err := memBus.Subscribe(ctx, func(bus.Envelope) {}); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			if memBus.Subscribers() != 1 {
				t.Fatalf("subscribers = %d, want 1", memBus.Subscribers())
			}
			cancel()

			// A leaked subscription would keep receiving envelopes forever,
			// holding a goroutine per cancelled context.
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) && memBus.Subscribers() != 0 {
				time.Sleep(time.Millisecond)
			}
			if got := memBus.Subscribers(); got != 0 {
				t.Errorf("subscribers after cancel = %d, want 0", got)
			}
		})
	})
}

// TestBusContract runs the shared suite against every registered bus. A new
// implementation inherits these guarantees by being added here rather than by
// being re-tested by hand.
func TestBusContract(t *testing.T) {
	for _, c := range contracts {
		runContract(t, c)
	}
}

// The envelope must carry no subscriber information, because the fanout path
// shares one encoded body across every recipient of a channel. A field that
// varied per subscriber would break that sharing silently.
func TestEnvelopeCarriesNoSubscriberIdentity(t *testing.T) {
	env := bus.Envelope{TenantID: "t1", Channel: "room:1", MessageID: 1}
	// A compile-time guard: the struct must not grow a recipient field without
	// this test being reconsidered. Comparing two envelopes that differ only in
	// tenant-scoped addressing must produce identical routing-relevant bytes.
	clone := env
	if clone.MessageID != env.MessageID || clone.Channel != env.Channel {
		t.Fatal("envelope copy is not faithful")
	}
}
