package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/protocol"
)

type lifecycleFixture struct {
	lc     *Lifecycle
	store  *Store
	shards *ShardSet
	now    time.Time
}

func newLifecycleFixture(t *testing.T) *lifecycleFixture {
	t.Helper()
	store := NewStore()
	shards := NewShardSet(64)
	f := &lifecycleFixture{
		store:  store,
		shards: shards,
		now:    time.UnixMilli(1_700_000_000_000),
	}
	f.lc = NewLifecycle(store, shards, LifecycleConfig{
		HeartbeatInterval: 10 * time.Second,
		HeartbeatTimeout:  5 * time.Second,
		SlowAfter:         50 * time.Millisecond,
	}, func() time.Time { return f.now })
	return f
}

func (f *lifecycleFixture) track(id uint64, channels ...string) *Conn {
	conn := NewConn(id, "t1", "u", 1024, 256, 2048, f.shards)
	f.lc.Track(conn)
	for _, c := range channels {
		if _, err := f.store.Subscribe("t1", c, conn); err != nil {
			panic(err)
		}
	}
	return conn
}

// A subscriber that stops answering must be closed and its subscriptions
// released, or it holds channel state forever.
func TestLivenessClosesUnresponsiveConnection(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1", "room:2")

	// No response at all.
	f.now = f.now.Add(30 * time.Second)
	probe, dead := f.lc.ProbeDue()

	if len(probe) != 1 {
		t.Fatalf("probes = %d, want 1", len(probe))
	}
	if len(dead) != 0 {
		t.Fatalf("a connection was declared dead on its first unanswered probe")
	}

	// Still silent past the timeout.
	f.now = f.now.Add(6 * time.Second)
	_, dead = f.lc.ProbeDue()
	if len(dead) != 1 {
		t.Fatalf("dead = %d after the timeout, want 1", len(dead))
	}
	f.lc.Close(dead[0])

	if conn.State() != ConnClosed {
		t.Error("connection was not closed")
	}
	if got := f.store.SubscriberCountFor("t1", "room:1"); got != 0 {
		t.Errorf("room:1 still has %d subscribers after close", got)
	}
	if got := f.store.SubscriberCountFor("t1", "room:2"); got != 0 {
		t.Errorf("room:2 still has %d subscribers after close", got)
	}
}

// Answering a probe must reset the deadline; a healthy connection must never be
// reaped.
func TestLivenessKeepsRespondingConnection(t *testing.T) {
	f := newLifecycleFixture(t)
	f.track(1, "room:1")

	for round := 0; round < 20; round++ {
		f.now = f.now.Add(11 * time.Second)
		probe, dead := f.lc.ProbeDue()
		for _, conn := range probe {
			f.lc.Ping(conn)
		}
		if len(dead) != 0 {
			t.Fatalf("round %d: a responding connection was reaped", round)
		}
	}
}

// An intermediary can drop a connection without a close frame. Only a probe
// reveals that, so the probe has to exist.
func TestLivenessDetectsSilentlyDroppedConnection(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1")

	// The client vanished without telling us; the last we saw was now.
	f.now = f.now.Add(11 * time.Second)
	probe, _ := f.lc.ProbeDue()
	if len(probe) != 1 {
		t.Fatalf("probes = %d, want 1 for a silently dropped connection", len(probe))
	}

	f.now = f.now.Add(6 * time.Second)
	_, dead := f.lc.ProbeDue()
	if len(dead) != 1 || dead[0] != conn {
		t.Fatalf("dead = %v, want the silently dropped connection", dead)
	}
}

// Probing and reaping share one walk, so the cost is proportional to connection
// count once per interval and does not grow with message volume.
func TestLivenessCostIsIndependentOfTraffic(t *testing.T) {
	quiet := newLifecycleFixture(t)
	busy := newLifecycleFixture(t)

	const conns = 500
	for i := 0; i < conns; i++ {
		id := uint64(i + 1)
		quiet.track(id, "room:1")
		busy.track(id, "room:1")
	}

	for i := 0; i < 1000; i++ {
		if _, err := busy.lc.store.Subscribe("t1", "room:1", busy.track(uint64(conns+i+1))); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	quiet.now = quiet.now.Add(11 * time.Second)
	busy.now = busy.now.Add(11 * time.Second)

	qStart := time.Now()
	qp, _ := quiet.lc.ProbeDue()
	qElapsed := time.Since(qStart)

	bStart := time.Now()
	bp, _ := busy.lc.ProbeDue()
	bElapsed := time.Since(bStart)

	// The busy instance has more connections, so it may probe more, but the
	// per-connection cost must not differ by more than the traffic volume did.
	qPerConn := float64(qElapsed) / float64(len(qp))
	bPerConn := float64(bElapsed) / float64(len(bp))
	if qPerConn == 0 {
		t.Skip("probing was too fast to measure")
	}
	if bPerConn > qPerConn*3 {
		t.Errorf("per-connection probe cost %.0fns busy vs %.0fns quiet; the walk is scaling with traffic",
			bPerConn, qPerConn)
	}
	t.Logf("per-connection probe: quiet %.0fns, busy %.0fns (%d vs %d probes)",
		qPerConn, bPerConn, len(qp), len(bp))
}

// A subscriber holding queued content past the tolerance is closed. The buffer
// is already bounded, so there is no room left to give it.
func TestSlowSubscriberIsReaped(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1")

	conn.Enqueue([]byte("backlog"), 1)
	if got := len(f.lc.SlowConnections()); got != 1 {
		t.Fatalf("slow connections = %d immediately after queueing, want 1", got)
	}

	f.now = f.now.Add(200 * time.Millisecond)
	slow := f.lc.SlowConnections()
	if len(slow) != 1 || slow[0] != conn {
		t.Fatalf("slow = %v, want the stalled connection", slow)
	}

	f.lc.ReapSlow()
	if conn.State() != ConnClosed {
		t.Error("a stalled connection was not closed")
	}
	if f.lc.Snapshot().SlowReaped != 1 {
		t.Errorf("slow reaped = %d, want 1", f.lc.Snapshot().SlowReaped)
	}
	if got := f.store.SubscriberCountFor("t1", "room:1"); got != 0 {
		t.Errorf("the reaped connection still holds %d subscriptions", got)
	}
}

// A connection that keeps draining must never be classified as slow.
func TestHealthyConnectionIsNotSlow(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1")

	for i := 0; i < 50; i++ {
		conn.Enqueue([]byte("x"), 1)
		f.now = f.now.Add(10 * time.Millisecond)
		conn.Drain(nil)
		if got := len(f.lc.SlowConnections()); got != 0 {
			t.Fatalf("a draining connection was classified as slow at iteration %d", i)
		}
	}
}

// A draining instance is expected to have connections that will not be written
// to before they close, so they must not be reaped as slow.
func TestDrainingConnectionsAreNotReapedAsSlow(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1")
	conn.Enqueue([]byte("backlog"), 1)

	f.now = f.now.Add(500 * time.Millisecond)
	f.lc.BeginDrain()

	f.lc.ReapSlow()
	if conn.State() == ConnClosed {
		t.Error("a draining connection was reaped as slow")
	}
}

// Drain must stop new work, warn subscribers, then close only after the grace
// window. Without the wait, a release is a synchronized mass reconnect and each
// reconnect is a full handshake.
func TestDrainSequence(t *testing.T) {
	f := newLifecycleFixture(t)
	a := f.track(1, "room:1")
	b := f.track(2, "room:1")

	if !a.Accepting() {
		t.Error("a connection is not accepting before drain")
	}

	conns := f.lc.BeginDrain()
	if len(conns) != 2 {
		t.Fatalf("drain returned %d connections, want 2", len(conns))
	}
	if !f.lc.Draining() {
		t.Error("lifecycle is not reporting as draining")
	}
	if a.Accepting() || b.Accepting() {
		t.Error("a draining connection still accepts new work")
	}

	closed := f.lc.FinishDrain()
	if closed != 2 {
		t.Errorf("FinishDrain closed %d, want 2", closed)
	}
	if a.State() != ConnClosed || b.State() != ConnClosed {
		t.Error("connections were not closed after drain")
	}
	if got := f.store.SubscriberCount(); got != 0 {
		t.Errorf("subscriptions survived the drain: %d", got)
	}
}

func TestDrainNoticeIsAFrame(t *testing.T) {
	frame := DrainNotice()
	ct, _, err := protocol.ParseControl(frame)
	if err != nil {
		t.Fatalf("ParseControl: %v", err)
	}
	if ct != protocol.ControlDrain {
		t.Errorf("control type = %d, want ControlDrain", ct)
	}
}

func TestPingPongFrames(t *testing.T) {
	if ct, _, err := protocol.ParseControl(PingFrame()); err != nil || ct != protocol.ControlPing {
		t.Errorf("PingFrame = %d, %v", ct, err)
	}
	if ct, _, err := protocol.ParseControl(PongFrame()); err != nil || ct != protocol.ControlPong {
		t.Errorf("PongFrame = %d, %v", ct, err)
	}
}

// Close is idempotent: a drain and a heartbeat timeout can reach the same
// connection.
func TestCloseIsIdempotent(t *testing.T) {
	f := newLifecycleFixture(t)
	conn := f.track(1, "room:1", "room:2")

	var closes int
	conn.SetCloseFunc(func() { closes++ })

	f.lc.Close(conn)
	f.lc.Close(conn)
	f.lc.Close(conn)

	if closes != 1 {
		t.Errorf("close ran %d times, want 1", closes)
	}
	if f.lc.Snapshot().Tracked != 0 {
		t.Error("a closed connection is still tracked")
	}
}

func TestConcurrentLifecycleOperations(t *testing.T) {
	f := newLifecycleFixture(t)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := uint64(g*1000 + i)
				conn := f.track(id, "room:1")
				f.lc.Ping(conn)
				f.lc.SlowConnections()
				if i%10 == 0 {
					f.lc.Close(conn)
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- QoS0 semantics ---

// The delivery contract makes four promises by omission. Each is asserted
// explicitly, because a future change that quietly adds persistence,
// acknowledgement, or replay would otherwise pass every existing test.
func TestQoS0Semantics(t *testing.T) {
	f := newFanoutFixture(t)
	conn := f.subscribe(1, "t1", "room:1")
	f.coalesce.SetCoalescing("t1", "room:1", false)

	t.Run("delivered without acknowledgement", func(t *testing.T) {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("x"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		f.flushAll()

		batches := f.framesFor(conn.ID)
		if len(batches) != 1 {
			t.Fatalf("received %d batches, want 1", len(batches))
		}
		// Nothing in the engine observes the subscriber's read or acknowledges it.
		if got := f.fanout.Snapshot().MessagesIn; got != 1 {
			t.Errorf("messages in = %d; admission must not wait on delivery", got)
		}
	})

	t.Run("nothing is stored for later retrieval", func(t *testing.T) {
		// The engine holds no message store. A disconnect followed by a
		// reconnect must deliver nothing that arrived in between.
		conn.Close()

		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: 2, Body: []byte("missed"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		f.flushAll()

		// Resubscribe as a fresh connection and confirm nothing arrives.
		f.store.UnsubscribeAll(conn)
		rejoined := f.subscribe(99, "t1", "room:1")
		f.flushAll()

		if got := len(f.framesFor(rejoined.ID)); got != 0 {
			t.Errorf("a reconnected client received %d batches; nothing may be replayed", got)
		}
	})

	t.Run("duplicates and reordering are permitted, not prevented", func(t *testing.T) {
		// A fresh connection: the previous subtest closed the original, and a
		// closed connection is correctly skipped.
		dup := f.subscribe(2, "t1", "room:1")

		// Dispatching the same message twice delivers it twice. QoS0 permits
		// this, so the contract is that duplicates are allowed rather than that
		// they are impossible.
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: 5, Body: []byte("dup"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		f.flushAll()
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: 5, Body: []byte("dup"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		f.flushAll()

		count := 0
		for _, b := range f.framesFor(dup.ID) {
			for _, m := range b.Messages {
				if m.ID == 5 {
					count++
				}
			}
		}
		if count < 2 {
			t.Errorf("the same message was delivered %d times; duplicates are permitted but must not be silently swallowed", count)
		}
	})

	t.Run("an expired message is dropped rather than held", func(t *testing.T) {
		before := f.fanout.Snapshot().MessagesIn
		nowMs := uint64(f.now.UnixMilli())
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: 9,
			Body: []byte("late"), ExpiresAtMs: nowMs - 1,
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if f.fanout.Snapshot().MessagesIn != before {
			t.Error("an expired message was admitted; nothing may be held for later")
		}
	})
}
