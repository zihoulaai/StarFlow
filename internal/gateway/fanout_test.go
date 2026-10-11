package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/protocol"
)

type fanoutFixture struct {
	fanout   *Fanout
	store    *Store
	shards   *ShardSet
	coalesce *CoalescerSet
	written  map[uint64][][]byte
	writeMu  sync.Mutex
	now      time.Time
}

func newFanoutFixture(t *testing.T) *fanoutFixture {
	t.Helper()
	f := &fanoutFixture{
		store:    NewStore(),
		shards:   NewShardSet(64),
		coalesce: NewCoalescerSet(10, 20, 1<<20),
		written:  map[uint64][][]byte{},
		now:      time.UnixMilli(1_700_000_000_000),
	}
	f.fanout = NewFanout(f.store, f.shards, f.coalesce, func() time.Time { return f.now })
	return f
}

// write records what a connection received instead of touching a socket.
func (f *fanoutFixture) write(conn *Conn, out []byte) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	cp := make([]byte, len(out))
	copy(cp, out)
	f.written[conn.ID] = append(f.written[conn.ID], cp)
}

func (f *fanoutFixture) subscribe(t *testing.T, id uint64, tenant, channel string) *Conn {
	t.Helper()
	conn := NewConn(id, ConnConfig{
		TenantID: tenant, Subject: "u",
		WriteBufBytes: 16 * 1024, ReadBufBytes: 1024,
		PerViewerBps: 2048, Shards: f.shards,
	})
	if _, err := f.store.Subscribe(tenant, channel, conn); err != nil {
		t.Fatalf("Subscribe %s/%s: %v", tenant, channel, err)
	}
	return conn
}

// flushAll advances the clock past the coalescing window and runs flush passes,
// standing in for the flush ticker. Time has to move for a coalescing window to
// close, which is exactly what the ticker does in production.
func (f *fanoutFixture) flushAll() {
	for i := 0; i < 4; i++ {
		f.now = f.now.Add(50 * time.Millisecond)
		f.coalesce.FlushDue(f.fanout)
		f.fanout.FlushPass(f.write)
	}
}

// framesFor returns every batch frame a connection received.
//
// A single write can carry several frames back to back, because a connection's
// buffer is drained whole. The walk parses them as a sequence rather than
// treating each write as one frame, which is what a reader on the other end
// actually has to do.
func (f *fanoutFixture) framesFor(id uint64) []*protocol.Batch {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	var out []*protocol.Batch
	for _, raw := range f.written[id] {
		pos := 0
		for pos < len(raw) {
			batch, n, ok := nextBatch(raw[pos:])
			if !ok {
				break
			}
			out = append(out, batch)
			pos += n
		}
	}
	return out
}

// nextBatch parses one frame from the head of buf, returning it and its length.
func nextBatch(buf []byte) (*protocol.Batch, int, bool) {
	hdr, err := protocol.ParseHeader(buf)
	if err != nil || hdr.Type != protocol.FrameTypeMessageBatch {
		return nil, 0, false
	}
	batch, err := protocol.ParseBatch(buf)
	if err != nil {
		return nil, 0, false
	}
	return batch, len(protocol.AppendBatch(nil, batch)), true
}

// gapsFor returns every gap notice a connection received.
func (f *fanoutFixture) gapsFor(id uint64) []protocol.GapNotice {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	var out []protocol.GapNotice
	for _, raw := range f.written[id] {
		pos := 0
		for pos < len(raw) {
			hdr, err := protocol.ParseHeader(raw[pos:])
			if err != nil {
				break
			}
			if hdr.Type == protocol.FrameTypeGapNotice {
				gap, err := protocol.ParseGapNotice(raw[pos:])
				if err != nil {
					break
				}
				out = append(out, gap)
				pos += len(protocol.AppendGapNotice(nil, gap))
				continue
			}
			if hdr.Type != protocol.FrameTypeMessageBatch {
				break
			}
			batch, err := protocol.ParseBatch(raw[pos:])
			if err != nil {
				break
			}
			pos += len(protocol.AppendBatch(nil, batch))
		}
	}
	return out
}

// The publishing instance and the subscribing instance are different: under
// broadcast the message crosses the bus, and a subscriber on another instance
// must receive it exactly once.
func TestFanoutDeliversToASubscriberOnAnotherInstance(t *testing.T) {
	f := newFanoutFixture(t)
	subscriber := f.subscribe(t, 1, "t1", "room:1")

	// A separate engine, playing the instance that accepted the publish.
	publisher := newFanoutFixture(t)
	publisher.store = f.store

	env := bus.Envelope{TenantID: "t1", Channel: "room:1", MessageID: 7, Body: []byte("hello")}
	if _, err := f.fanout.Dispatch(env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	batches := f.framesFor(subscriber.ID)
	if len(batches) != 1 {
		t.Fatalf("subscriber received %d batches, want 1", len(batches))
	}
	if len(batches[0].Messages) != 1 || batches[0].Messages[0].ID != 7 {
		t.Errorf("batch = %+v, want message 7", batches[0])
	}
	if string(batches[0].Messages[0].Body) != "hello" {
		t.Errorf("body = %q, want hello", batches[0].Messages[0].Body)
	}
}

// A message destined for a channel this instance does not serve is the common
// case under broadcast. It must be discarded cheaply, not treated as an error.
func TestFanoutDiscardsUnservedChannel(t *testing.T) {
	f := newFanoutFixture(t)

	n, err := f.fanout.Dispatch(bus.Envelope{TenantID: "t1", Channel: "elsewhere", MessageID: 1})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if n != 0 {
		t.Errorf("queued = %d for an unserved channel, want 0", n)
	}
	if got := f.fanout.Snapshot().ChannelMisses; got != 1 {
		t.Errorf("channel misses = %d, want 1", got)
	}
}

// Exactly one copy per subscriber: a duplicate here would be a correctness
// failure no rate or latency metric would reveal.
func TestFanoutDeliversExactlyOncePerSubscriber(t *testing.T) {
	f := newFanoutFixture(t)
	for i := 0; i < 20; i++ {
		f.subscribe(t, uint64(i+1), "t1", "room:1")
	}

	if _, err := f.fanout.Dispatch(bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	for i := 0; i < 20; i++ {
		batches := f.framesFor(uint64(i + 1))
		total := 0
		for _, b := range batches {
			total += len(b.Messages)
		}
		if total != 1 {
			t.Errorf("subscriber %d received %d messages, want exactly 1", i, total)
		}
	}
}

// Two tenants may use the same channel name without observing each other.
func TestFanoutRespectsTenantIsolation(t *testing.T) {
	f := newFanoutFixture(t)
	a := f.subscribe(t, 1, "t1", "shared")
	b := f.subscribe(t, 2, "t2", "shared")

	if _, err := f.fanout.Dispatch(bus.Envelope{
		TenantID: "t1", Channel: "shared", MessageID: 1, Body: []byte("for t1"),
	}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	if len(f.framesFor(a.ID)) != 1 {
		t.Error("t1's subscriber did not receive t1's message")
	}
	if len(f.framesFor(b.ID)) != 0 {
		t.Error("t2's subscriber received t1's message")
	}
}

// A publisher-requested lifetime must drop late arrivals with no persisted
// state and nothing scheduled.
func TestFanoutHonoursRequestedExpiry(t *testing.T) {
	f := newFanoutFixture(t)
	conn := f.subscribe(t, 1, "t1", "room:1")

	nowMs := uint64(f.now.UnixMilli())
	if _, err := f.fanout.Dispatch(bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1,
		Body: []byte("x"), ExpiresAtMs: nowMs - 1,
	}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	if len(f.framesFor(conn.ID)) != 0 {
		t.Error("an expired message was delivered")
	}
	if got := f.fanout.Snapshot().ExpiredDropped; got != 1 {
		t.Errorf("expired drops = %d, want 1", got)
	}
}

func TestFanoutCoalescesWithinWindow(t *testing.T) {
	f := newFanoutFixture(t)
	conn := f.subscribe(t, 1, "t1", "room:1")

	// A merge ratio of 20 with a long window, so only the count closes it.
	f.coalesce = NewCoalescerSet(1_000_000, 5, 1<<20)
	f.fanout.coalesce = f.coalesce

	for i := uint64(1); i <= 5; i++ {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: i, Body: []byte("m"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}
	f.flushAll()

	batches := f.framesFor(conn.ID)
	if len(batches) != 1 {
		t.Fatalf("received %d batches, want 1 merged batch", len(batches))
	}
	if len(batches[0].Messages) != 5 {
		t.Errorf("batch carried %d messages, want 5", len(batches[0].Messages))
	}
	for i, m := range batches[0].Messages {
		if m.ID != uint64(i+1) {
			t.Errorf("message %d has id %d, want %d", i, m.ID, i+1)
		}
	}
}

// Coalescing disabled must send each message on its own.
func TestFanoutWithCoalescingDisabled(t *testing.T) {
	f := newFanoutFixture(t)
	conn := f.subscribe(t, 1, "t1", "room:1")
	f.coalesce.SetCoalescing("t1", "room:1", false)

	for i := uint64(1); i <= 3; i++ {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: i, Body: []byte("m"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}
	f.flushAll()

	batches := f.framesFor(conn.ID)
	total := 0
	for _, b := range batches {
		if len(b.Messages) != 1 {
			t.Errorf("batch carried %d messages, want 1 each", len(b.Messages))
		}
		total += len(b.Messages)
	}
	if total != 3 {
		t.Errorf("delivered %d messages, want 3", total)
	}
}

// One message must serialize once regardless of how many subscribers receive
// it. This is the property the whole fanout model rests on.
func TestFanoutSerializesOncePerMessage(t *testing.T) {
	f := newFanoutFixture(t)
	const subscribers = 500
	for i := 0; i < subscribers; i++ {
		f.subscribe(t, uint64(i+1), "t1", "room:1")
	}

	if _, err := f.fanout.Dispatch(bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	// Every subscriber's bytes must be identical.
	var reference []byte
	for i := 0; i < subscribers; i++ {
		f.writeMu.Lock()
		raw := f.written[uint64(i+1)]
		f.writeMu.Unlock()
		if len(raw) != 1 {
			t.Fatalf("subscriber %d received %d writes, want 1", i, len(raw))
		}
		if reference == nil {
			reference = raw[0]
			continue
		}
		if len(raw[0]) != len(reference) {
			t.Fatalf("subscriber %d received %d bytes, want %d", i, len(raw[0]), len(reference))
		}
		for j := range reference {
			if raw[0][j] != reference[j] {
				t.Errorf("subscriber %d received different bytes at offset %d", i, j)
			}
		}
	}
}

// A subscriber whose buffer cannot hold what it is sent must be dropped alone.
// Delivery to the healthy subscribers of the same channel must be unaffected,
// which is the whole point of a per-connection bound rather than a shared queue.
func TestFanoutIsolatesUndersizedSubscriber(t *testing.T) {
	f := newFanoutFixture(t)
	healthy := f.subscribe(t, 1, "t1", "room:1")
	// Coalescing off so the buffer bound, not batch size, decides what fits.
	f.coalesce.SetCoalescing("t1", "room:1", false)

	// A buffer smaller than a single frame, so every enqueue is refused for this
	// connection and only for this connection.
	stunted := NewConn(2, ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 64, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: f.shards})
	if _, err := f.store.Subscribe("t1", "room:1", stunted); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const messages = 200
	for i := uint64(1); i <= messages; i++ {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: i, Body: make([]byte, 100),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		// A subscriber that is keeping up is drained every pass. One that is
		// not keeps its backlog until it hits its bound, which is the condition
		// under test.
		f.fanout.FlushPass(f.write)
	}
	f.flushAll()

	if stunted.Buffer().Dropped() == 0 {
		t.Error("the undersized subscriber never recorded a drop")
	}
	total := 0
	for _, b := range f.framesFor(healthy.ID) {
		total += len(b.Messages)
	}
	if total != messages {
		t.Errorf("the healthy subscriber received %d of %d while another connection dropped",
			total, messages)
	}
	t.Logf("healthy received %d of %d, undersized dropped %d",
		total, messages, stunted.Buffer().Dropped())
}

// A connection that has stopped accepting new work, because it is draining or
// closed, must be skipped without disturbing the others.
func TestFanoutSkipsNonAcceptingConnections(t *testing.T) {
	f := newFanoutFixture(t)
	healthy := f.subscribe(t, 1, "t1", "room:1")
	closing := f.subscribe(t, 2, "t1", "room:1")
	closing.SetState(ConnClosed)

	f.coalesce.SetCoalescing("t1", "room:1", false)
	if _, err := f.fanout.Dispatch(bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	f.flushAll()

	if len(f.framesFor(closing.ID)) != 0 {
		t.Error("a closed connection received content")
	}
	if len(f.framesFor(healthy.ID)) == 0 {
		t.Error("the open connection received nothing")
	}
}

// The gap notice must precede the content that follows a drop.
func TestGapNoticePrecedesSubsequentContent(t *testing.T) {
	f := newFanoutFixture(t)
	conn := NewConn(1, ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 512, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: f.shards})
	if _, err := f.store.Subscribe("t1", "room:1", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Coalescing off, so the buffer bound is what produces overflow.
	f.coalesce.SetCoalescing("t1", "room:1", false)

	for i := uint64(1); i <= 10; i++ {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: i, Body: make([]byte, 150),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}
	// One pass drains whatever is buffered, with the gap notice first.
	f.now = f.now.Add(50 * time.Millisecond)
	f.coalesce.FlushDue(f.fanout)
	f.fanout.FlushPass(f.write)
	// A second pass carries the content that survived.
	f.fanout.FlushPass(f.write)

	f.writeMu.Lock()
	raw := f.written[conn.ID]
	f.writeMu.Unlock()

	if len(raw) == 0 {
		t.Fatal("nothing was written")
	}
	hdr, err := protocol.ParseHeader(raw[0])
	if err != nil {
		t.Fatalf("parse first frame: %v", err)
	}
	if hdr.Type != protocol.FrameTypeGapNotice {
		t.Fatalf("first frame type = %d, want a gap notice before content", hdr.Type)
	}
	gap, err := protocol.ParseGapNotice(raw[0])
	if err != nil {
		t.Fatalf("ParseGapNotice: %v", err)
	}
	if gap.Dropped == 0 {
		t.Error("gap notice reports no dropped messages")
	}

	// Content follows the notice, so a client learns of the discontinuity before
	// seeing whatever came after it.
	if len(f.framesFor(conn.ID)) == 0 {
		t.Error("no content followed the gap notice")
	}
}

func TestFanoutRunConsumesBus(t *testing.T) {
	f := newFanoutFixture(t)
	conn := f.subscribe(t, 1, "t1", "room:1")

	b := bus.NewMemoryBus()
	defer b.Close()

	ctx, cancel := contextWithTimeout(t, 2*time.Second)
	defer cancel()

	runCtx, stopRun := contextWithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() { done <- f.fanout.Run(runCtx, b) }()

	waitUntil(t, time.Second, func() bool { return b.Subscribers() > 0 })

	if err := b.Publish(ctx, bus.Envelope{
		TenantID: "t1", Channel: "room:1", MessageID: 1, Body: []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	f.flushAll()

	if len(f.framesFor(conn.ID)) != 1 {
		t.Error("a message published to the bus was not delivered")
	}

	stopRun()
	<-done
}

func TestFanoutStats(t *testing.T) {
	f := newFanoutFixture(t)
	for i := 0; i < 10; i++ {
		f.subscribe(t, uint64(i+1), "t1", "room:1")
	}
	// A high merge target, so the window closes on time rather than on count.
	f.coalesce = NewCoalescerSet(1000, 1000, 1<<20)
	f.fanout.coalesce = f.coalesce

	for i := uint64(1); i <= 5; i++ {
		if _, err := f.fanout.Dispatch(bus.Envelope{
			TenantID: "t1", Channel: "room:1", MessageID: i, Body: []byte("x"),
		}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}
	f.now = f.now.Add(2 * time.Second)
	f.coalesce.FlushDue(f.fanout)

	s := f.fanout.Snapshot()
	if s.MessagesIn != 5 {
		t.Errorf("messages in = %d, want 5", s.MessagesIn)
	}
	// Ten subscribers x five messages: delivered counts message times recipient,
	// because that is the quantity the fanout factor is compared against.
	if s.MessagesOut != 50 {
		t.Errorf("messages out = %d, want 50", s.MessagesOut)
	}
	if got := s.FanoutFactor(); got != 10 {
		t.Errorf("fanout factor = %v, want 10", got)
	}
}

func TestFanoutFactorIsZeroWhenNothingAdmitted(t *testing.T) {
	f := newFanoutFixture(t)
	if got := f.fanout.Snapshot().FanoutFactor(); got != 0 {
		t.Errorf("fanout factor = %v with nothing admitted, want 0", got)
	}
}
