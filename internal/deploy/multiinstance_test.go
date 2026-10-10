package deploy_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/gateway"
)

// multiInstance is two gateway engines sharing one bus, which is what the
// broadcast topology looks like with more than one instance.
type multiInstance struct {
	bus        *bus.MemoryBus
	stores     []*gateway.Store
	fanouts    []*gateway.Fanout
	lifecycles []*gateway.Lifecycle
	// ledger records what each connection actually received, at the point the
	// engine writes it. Recording at the bus would observe messages each engine
	// saw, not messages subscribers were given.
	ledger *deliveryLedger
}

func newMultiInstance(t *testing.T, instances int, connsPerInstance int) *multiInstance {
	t.Helper()
	m := &multiInstance{bus: bus.NewMemoryBus(), ledger: newDeliveryLedger()}
	t.Cleanup(func() { _ = m.bus.Close() })

	// Real time, not a frozen clock: a coalescing window closes on elapsed time
	// as well as on the merge target, and a frozen clock would leave the final
	// partial batch pending forever. That shortfall is correct behaviour being
	// measured wrongly.
	clock := time.Now

	for i := 0; i < instances; i++ {
		store := gateway.NewStore()
		shards := gateway.NewShardSet(128)
		coalesce := gateway.NewCoalescerSet(10, 20, 1<<20)
		fanout := gateway.NewFanout(store, shards, coalesce, clock)
		lc := gateway.NewLifecycle(store, shards, gateway.LifecycleConfig{
			HeartbeatInterval: time.Second, HeartbeatTimeout: time.Second,
		}, clock)

		for c := 0; c < connsPerInstance; c++ {
			id := uint64(i*10000 + c + 1)
			conn := gateway.NewConn(id, gateway.ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: shards})
			lc.Track(conn)
			if _, err := store.Subscribe("t1", "room:1", conn); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() { _ = fanout.Run(ctx, m.bus) }()
		go func() {
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					coalesce.FlushDue(fanout)
					fanout.FlushPass(m.recordWrite)
				}
			}
		}()

		m.stores = append(m.stores, store)
		m.fanouts = append(m.fanouts, fanout)
		m.lifecycles = append(m.lifecycles, lc)
	}

	// Wait for every subscription to attach before publishing, otherwise a
	// message can be published into a bus with fewer listeners than instances.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.bus.Subscribers() >= instances {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := m.bus.Subscribers(); got < instances {
		t.Fatalf("bus has %d subscribers, want %d", got, instances)
	}
	return m
}

func (m *multiInstance) totalSubscribers() int {
	total := 0
	for _, s := range m.stores {
		total += s.SubscriberCountFor("t1", "room:1")
	}
	return total
}

// The correctness property the broadcast topology exists to provide: a message
// published to any instance reaches every subscriber exactly once, no matter
// which instance holds which connection.
//
// A missing delivery is a dropped danmaku. A duplicate is the same danmaku
// rendered twice. Neither is observable from a throughput or latency metric,
// which is why this is asserted directly and across every instance count the
// deployment uses.
func TestMultiInstanceDeliversExactlyOncePerSubscriber(t *testing.T) {
	for _, instances := range []int{1, 2, 5} {
		t.Run(fmt.Sprintf("instances=%d", instances), func(t *testing.T) {
			const perInstance = 20
			const messages = 50

			m := newMultiInstance(t, instances, perInstance)
			total := m.totalSubscribers()
			if total != instances*perInstance {
				t.Fatalf("subscribers = %d, want %d", total, instances*perInstance)
			}

			received := m.ledger
			for i := uint64(1); i <= uint64(messages); i++ {
				if err := m.bus.Publish(context.Background(), bus.Envelope{
					TenantID: "t1", Channel: "room:1", MessageID: i,
					TimeMs: uint64(i), Body: []byte("x"),
				}); err != nil {
					t.Fatalf("Publish: %v", err)
				}
			}

			// Wait for quiescence rather than a fixed window. A fixed deadline
			// passes under normal load and fails under the race detector purely
			// because it is slower, which tests the harness rather than the
			// delivery path. Quiescence is the actual condition of interest.
			waitForQuiescence(t, m, received, uint64(total*messages), 60*time.Second)

			received.mu.Lock()
			defer received.mu.Unlock()

			perConn := messages
			for connID, seen := range received.byConn {
				for msgID, n := range seen {
					if n != 1 {
						t.Errorf("connection %d received message %d %d times, want exactly 1",
							connID, msgID, n)
					}
				}
				if len(seen) != perConn {
					t.Errorf("connection %d received %d distinct messages, want %d",
						connID, len(seen), perConn)
				}
			}
			if len(received.byConn) != total {
				t.Errorf("%d connections received anything, want %d",
					len(received.byConn), total)
			}
		})
	}
}

// waitForQuiescence blocks until every connection has drained and the delivery
// count has stopped moving.
func waitForQuiescence(t *testing.T, m *multiInstance, ledger *deliveryLedger, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	last := uint64(0)
	stable := 0
	for time.Now().Before(deadline) {
		got := ledger.delivered()
		if got >= want {
			return
		}
		if got == last {
			stable++
			if stable > 100 {
				// A hundred consecutive unchanged samples means delivery has
				// stopped short of the target, which is a real shortfall rather
				// than a slow one.
				t.Fatalf("delivery quiesced at %d of an expected %d", got, want)
			}
		} else {
			stable = 0
		}
		last = got
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("delivery did not quiesce within %v: %d of an expected %d", timeout, last, want)
}

// recordWrite is the flush write path: it parses each delivered frame and
// records which connection received which message.
func (m *multiInstance) recordWrite(conn *gateway.Conn, out []byte) {
	pos := 0
	for pos < len(out) {
		batch, n, ok := gateway.ParseFrames(out[pos:])
		if !ok {
			return
		}
		for _, msg := range batch {
			m.ledger.record(conn.ID, msg.ID)
		}
		pos += n
	}
}

// deliveryLedger counts deliveries per connection and message.
type deliveryLedger struct {
	mu     sync.Mutex
	byConn map[uint64]map[uint64]int
}

func newDeliveryLedger() *deliveryLedger {
	return &deliveryLedger{byConn: map[uint64]map[uint64]int{}}
}

func (l *deliveryLedger) record(connID, msgID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	byMsg, ok := l.byConn[connID]
	if !ok {
		byMsg = map[uint64]int{}
		l.byConn[connID] = byMsg
	}
	byMsg[msgID]++
}

func (l *deliveryLedger) delivered() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := uint64(0)
	for _, seen := range l.byConn {
		for _, n := range seen {
			total += uint64(n)
		}
	}
	return total
}

// A message published to a channel no instance serves must be discarded by
// every instance without error, which is the common case under broadcast.
func TestMultiInstanceIgnoresChannelsNoneServe(t *testing.T) {
	m := newMultiInstance(t, 3, 5)

	if err := m.bus.Publish(context.Background(), bus.Envelope{
		TenantID: "t1", Channel: "room:unserved", MessageID: 1, Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// No instance should have queued anything for it.
	for _, f := range m.fanouts {
		if s := f.Snapshot(); s.MessagesIn != 0 {
			t.Errorf("an instance admitted a message for an unserved channel: %+v", s)
		}
	}
}

// Tenant isolation must hold across instances, not just within one.
func TestMultiInstanceKeepsTenantsIsolated(t *testing.T) {
	m := newMultiInstance(t, 2, 10)
	// Add a second tenant's subscriber on instance zero only.
	conn := gateway.NewConn(999, gateway.ConnConfig{TenantID: "t2", Subject: "u", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: gateway.NewShardSet(8)})
	if _, err := m.stores[0].Subscribe("t2", "room:1", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := m.bus.Publish(context.Background(), bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("for t1"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if conn.Pending() != 0 {
		t.Errorf("a t2 connection received t1's message")
	}
	for _, store := range m.stores {
		if n := store.SubscriberCountFor("t2", "room:1"); n != 0 && store != m.stores[0] {
			t.Errorf("instance outside t2 holds %d t2 subscriptions", n)
		}
	}
}

// Adding an instance must not require any coordination with the existing ones.
// That is the property that makes the fleet scale without a migration step.
func TestInstanceJoinsWithoutCoordination(t *testing.T) {
	m := newMultiInstance(t, 2, 10)
	before := m.totalSubscribers()

	// A third engine joins, sharing the same bus and nothing else.
	store := gateway.NewStore()
	shards := gateway.NewShardSet(128)
	coalesce := gateway.NewCoalescerSet(10, 20, 1<<20)
	now := time.Now()
	fanout := gateway.NewFanout(store, shards, coalesce, func() time.Time { return now })

	for c := 0; c < 10; c++ {
		conn := gateway.NewConn(uint64(90000+c), gateway.ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: shards})
		if _, err := store.Subscribe("t1", "room:1", conn); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = fanout.Run(ctx, m.bus) }()

	// Wait for the new engine's bus subscription to attach. Publishing before it
	// does would make the test pass for the wrong reason.
	deadlineAttach := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineAttach) {
		if m.bus.Subscribers() >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// A message published after the join reaches the new instance's subscribers.
	if err := m.bus.Publish(context.Background(), bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if store.SubscriberCountFor("t1", "room:1") == 10 && fanout.Snapshot().MessagesIn > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if fanout.Snapshot().MessagesIn == 0 {
		t.Error("a newly joined instance did not begin receiving messages")
	}
	// The joining instance's store is not in the fixture's list, so it is
	// counted explicitly.
	if after := m.totalSubscribers() + store.SubscriberCountFor("t1", "room:1"); after != before+10 {
		t.Errorf("total subscribers = %d, want %d", after, before+10)
	}
}

// An instance retiring must release its subscriptions so the remaining
// instances' view of the fleet is not skewed by connections that no longer
// exist.
func TestRetiringInstanceReleasesSubscriptions(t *testing.T) {
	m := newMultiInstance(t, 2, 10)
	before := m.totalSubscribers()

	// Drain and close instance zero.
	m.lifecycles[0].FinishDrain()

	if after := m.totalSubscribers(); after >= before {
		t.Errorf("total subscribers = %d after a drain, want fewer than %d", after, before)
	}
	if n := m.totalSubscribers(); n != 10 {
		t.Errorf("remaining subscribers = %d, want the surviving instance's 10", n)
	}
}
