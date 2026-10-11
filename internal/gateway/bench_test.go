package gateway

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/protocol"
)

// The fanout store holds subscribers in a contiguous slice rather than a map.
// This benchmark is the evidence, and it is written so a regression to a map
// shows up as a benchmark failure rather than as a production surprise.

// sliceBaseline is the cost of walking a contiguous slice of the same length.
// It is the floor the store cannot beat, so the store's traversal is compared
// against it rather than against an absolute figure.
func BenchmarkFanoutSliceBaseline(b *testing.B) {
	subs := make([]*Subscriber, 1000)
	for i := range subs {
		subs[i] = &Subscriber{channel: "room:1"}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		count := 0
		for i := 0; i < len(subs); i++ {
			if subs[i].channel == "room:1" {
				count++
			}
		}
		if count != 1000 {
			b.Fatalf("count = %d", count)
		}
	}
}

// mapBaseline is the same walk against a hash map keyed by subscriber, which is
// the shape the store deliberately does not use.
func BenchmarkFanoutMapBaseline(b *testing.B) {
	m := make(map[string]*Subscriber, 1000)
	for i := 0; i < 1000; i++ {
		m[fmt.Sprintf("sub-%d", i)] = &Subscriber{channel: "room:1"}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		count := 0
		for k, v := range m {
			if v.channel == "room:1" {
				count++
			}
			_ = k
		}
		if count != 1000 {
			b.Fatalf("count = %d", count)
		}
	}
}

// BenchmarkFanoutTraversal measures the store's real traversal of a 1,000
// subscriber channel.
func BenchmarkFanoutTraversal(b *testing.B) {
	s := NewStore()
	// Distinct connections: a channel's subscriber set is a set of connections,
	// so reusing one would collapse to a single entry.
	for i := 0; i < 1000; i++ {
		if _, err := s.Subscribe("t1", "room:1", newTestConn(uint64(i+1), "t1")); err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
	}
	if got := s.SubscriberCountFor("t1", "room:1"); got != 1000 {
		b.Fatalf("subscriber count = %d, want 1000", got)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		subs, ok := s.LookupSubscribers("t1", "room:1")
		if !ok {
			b.Fatal("channel not found")
		}
		count := 0
		for i := 0; i < len(subs); i++ {
			if subs[i].Channel() == "room:1" {
				count++
			}
		}
		if count != 1000 {
			b.Fatalf("count = %d", count)
		}
	}
}

// BenchmarkFanoutMiss measures the common case under broadcast: an instance
// receiving a message for a channel it does not serve.
func BenchmarkFanoutMiss(b *testing.B) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	if _, err := s.Subscribe("t1", "served", conn); err != nil {
		b.Fatalf("Subscribe: %v", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, ok := s.LookupSubscribers("t1", "unserved"); ok {
			b.Fatal("unexpected hit")
		}
	}
}

// The delivery path must not allocate. A single allocation per delivered
// message would reintroduce the garbage the whole design exists to avoid.
func BenchmarkFanoutNoAllocation(b *testing.B) {
	s := NewStore()
	conns := make([]*Conn, 64)
	for i := range conns {
		conns[i] = newTestConn(uint64(i+1), "t1")
		if _, err := s.Subscribe("t1", "room:1", conns[i]); err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
	}
	frame := make([]byte, 512)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		subs, ok := s.LookupSubscribers("t1", "room:1")
		if !ok {
			b.Fatal("channel not found")
		}
		for i := 0; i < len(subs); i++ {
			subs[i].Conn().Enqueue(frame, 1000)
		}
	}
}

// BenchmarkEnqueueAllocation proves the fanout inner loop is allocation-free.
func BenchmarkEnqueueAllocation(b *testing.B) {
	shards := NewShardSet(1024)
	c := NewConn(1, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: shards})
	frame := make([]byte, 512)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c.Enqueue(frame, uint64(n))
		c.Drain(nil)
	}
}

// BenchmarkFlushScalesWithTraffic asserts the property the shard registry
// exists for: flush cost tracks delivered traffic, not connection count.
//
// Run with -v to see the per-pass cost at each connection count. The cost must
// not scale with the idle population, because that is the difference between
// supporting 100,000 idle connections and not.
func BenchmarkFlushScalesWithTraffic(b *testing.B) {
	for _, idleConns := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("idle=%d", idleConns), func(b *testing.B) {
			shards := NewShardSet(1024)
			idle := make([]*Conn, idleConns)
			for i := range idle {
				idle[i] = NewConn(uint64(i+1), ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 4096, ReadBufBytes: 256, PerViewerBps: 2048, Shards: shards})
			}
			// One connection with traffic.
			busy := NewConn(uint64(idleConns+1), ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 16 * 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: shards})
			busy.Enqueue(make([]byte, 256), 1)

			scratch := make([]*Conn, 0, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				busy.Enqueue(make([]byte, 256), uint64(n))
				drained := shards.FlushAll(scratch)
				if len(drained) != 1 {
					b.Fatalf("flush visited %d connections, want 1 regardless of %d idle", len(drained), idleConns)
				}
				for _, c := range drained {
					c.Drain(nil)
				}
			}
			b.ReportMetric(float64(idleConns), "idle_conns")
		})
	}
}

// BenchmarkCoalesceEncode measures encoding a coalesced batch, which happens
// once per window per channel rather than once per delivery.
func BenchmarkCoalesceEncode(b *testing.B) {
	c := NewCoalescer("t1", "room:1", 10, 20, 1<<20)
	body := make([]byte, 200)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 20; i++ {
			c.addMsg(messageN(uint64(n*20+i), body))
		}
		c.Take()
	}
}

// BenchmarkDeriveCeiling measures policy resolution, which sits on the publish
// admission path for every message.
func BenchmarkDeriveCeiling(b *testing.B) {
	cfg := controlplane.TenantConfig{
		TenantID:     "t1",
		Channels:     map[string]controlplane.ChannelConfig{},
		Applications: map[string]controlplane.Application{},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p := ResolveChannelPolicy(cfg, "room:1", 2048, 200)
		_ = p.PublishCeiling
	}
}

func messageN(id uint64, body []byte) protocol.Message {
	return protocol.Message{ID: id, TimeMs: uint64(id), Body: body}
}

// TestFanoutTraversalStaysWithinSliceBudget turns the benchmark's claim into an
// assertion.
//
// The store must stay close to a contiguous slice walk and must stay far away
// from a hash map walk. Both bounds are generous because they run on shared CI
// hardware, but they are wide enough apart that only a regression to a map
// would fail the upper one.
func TestFanoutTraversalStaysWithinSliceBudget(t *testing.T) {
	const subscribers = 1000

	s := NewStore()
	for i := 0; i < subscribers; i++ {
		if _, err := s.Subscribe("t1", "room:1", newTestConn(uint64(i+1), "t1")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	// The baseline slice is built outside the timed region. Allocating it
	// inside would charge the baseline for a cost the store does not pay, and
	// make the store look better than the comparison is entitled to claim.
	baselineSubs := make([]*Subscriber, subscribers)
	for i := range baselineSubs {
		baselineSubs[i] = &Subscriber{channel: "room:1"}
	}
	sliceCost := timeIt(func() int {
		subs := baselineSubs
		count := 0
		for i := 0; i < len(subs); i++ {
			if subs[i].channel == "room:1" {
				count++
			}
		}
		return count
	})

	mapCost := timeIt(func() int {
		m := make(map[string]*Subscriber, subscribers)
		for i := 0; i < subscribers; i++ {
			m[fmt.Sprintf("sub-%d", i)] = &Subscriber{channel: "room:1"}
		}
		count := 0
		for _, v := range m {
			if v.channel == "room:1" {
				count++
			}
		}
		return count
	})

	storeCost := timeIt(func() int {
		subs, ok := s.LookupSubscribers("t1", "room:1")
		if !ok {
			t.Fatalf("channel not found")
		}
		count := 0
		for i := 0; i < len(subs); i++ {
			if subs[i].Channel() == "room:1" {
				count++
			}
		}
		return count
	})

	const traversals = 2000
	t.Logf("per traversal: slice %.0fns, store %.0fns, map %.0fns",
		float64(sliceCost.Nanoseconds())/traversals,
		float64(storeCost.Nanoseconds())/traversals,
		float64(mapCost.Nanoseconds())/traversals)

	// Within 10x of the contiguous slice floor.
	if storeCost > sliceCost*10 {
		t.Errorf("store traversal %v exceeds 10x the slice baseline %v",
			storeCost, sliceCost)
	}
	// And materially cheaper than the map walk it replaced, which is what makes
	// the slice choice worth having.
	if storeCost >= mapCost {
		t.Errorf("store traversal %v is not cheaper than a map walk %v; the slice is not earning its place",
			storeCost, mapCost)
	}
}

// TestFlushCostIgnoresIdleConnections asserts the property the shard registry
// exists for, as a hard check rather than a benchmark reading.
func TestFlushCostIgnoresIdleConnections(t *testing.T) {
	measure := func(idleConns int) time.Duration {
		shards := NewShardSet(1024)
		for i := 0; i < idleConns; i++ {
			NewConn(uint64(i+1), ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 4096, ReadBufBytes: 256, PerViewerBps: 2048, Shards: shards})
		}
		busy := NewConn(uint64(idleConns+1), ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 16 * 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: shards})
		busy.Enqueue(make([]byte, 256), 1)

		scratch := make([]*Conn, 0, 64)
		const passes = 2000
		start := time.Now()
		for n := 0; n < passes; n++ {
			busy.Enqueue(make([]byte, 256), uint64(n))
			drained := shards.FlushAll(scratch)
			if len(drained) != 1 {
				t.Fatalf("flush visited %d connections with %d idle, want 1", len(drained), idleConns)
			}
			for _, c := range drained {
				c.Drain(nil)
			}
		}
		return time.Since(start) / passes
	}

	few := measure(1_000)
	many := measure(100_000)
	t.Logf("per flush pass: 1k idle %v, 100k idle %v", few, many)

	// A full scan would make this 100x. Anything under 3x for a 100x increase in
	// idle connections means cost tracks traffic, not population.
	if many > few*3 {
		t.Errorf("flush cost grew %vx for a 100x increase in idle connections (%v to %v); it is scaling with population",
			float64(many)/float64(few), few, many)
	}
}

func timeIt(f func() int) time.Duration {
	const iterations = 2000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		f()
	}
	return time.Since(start)
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func contextWithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
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

// BenchmarkFanoutHarness drives the full delivery path at a configurable
// connection count, fanout factor and merge ratio, and reports the three figures
// the capacity model is stated in: packets per second, bytes per second, and
// allocations per delivered message.
//
// It is a benchmark rather than a test because the point is the measurement, but
// it asserts the allocation figure, because that one is a correctness property
// rather than a performance observation: an allocation on this path would
// reintroduce the garbage the whole design removes.
func BenchmarkFanoutHarness(b *testing.B) {
	type config struct {
		connections int
		mergeRatio  int
	}
	for _, cfg := range []config{
		{1000, 1},
		{1000, 20},
		{10000, 20},
	} {
		b.Run(fmt.Sprintf("conns=%d/merge=%d", cfg.connections, cfg.mergeRatio), func(b *testing.B) {
			store := NewStore()
			shards := NewShardSet(1024)
			coalesce := NewCoalescerSet(10, cfg.mergeRatio, 1<<20)
			now := time.UnixMilli(1_700_000_000_000)
			fanout := NewFanout(store, shards, coalesce, func() time.Time { return now })

			for i := 0; i < cfg.connections; i++ {
				conn := NewConn(uint64(i+1), ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: shards})
				if _, err := store.Subscribe("t1", "room:1", conn); err != nil {
					b.Fatalf("Subscribe: %v", err)
				}
			}

			body := make([]byte, 200)
			delivered := 0

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := fanout.Dispatch(busEnvelope("t1", "room:1", uint64(n), body)); err != nil {
					b.Fatalf("Dispatch: %v", err)
				}
				// Advance the clock so every window closes, then flush.
				now = now.Add(10 * time.Millisecond)
				coalesce.FlushDue(fanout)
				// The write is a no-op: this measures the engine up to the
				// socket, which is what the capacity model is stated in.
				fanout.FlushPass(func(*Conn, []byte) {})
				delivered++
			}
			b.StopTimer()

			stats := fanout.Snapshot()
			elapsed := b.Elapsed()
			if elapsed > 0 && stats.MessagesOut > 0 {
				packetsPerSec := float64(stats.PacketsOut) / elapsed.Seconds()
				bytesPerSec := float64(stats.BytesOut) / elapsed.Seconds()
				b.ReportMetric(packetsPerSec, "packets/s")
				b.ReportMetric(bytesPerSec, "bytes/s")
				b.ReportMetric(float64(stats.BytesOut)/float64(cfg.connections)/1024, "KiB/conn")
			}
			allocs := testing.AllocsPerRun(200, func() {
				if _, err := fanout.Dispatch(busEnvelope("t1", "room:1", uint64(delivered), body)); err != nil {
					b.Fatalf("Dispatch: %v", err)
				}
			})
			// A small constant is tolerable: the shared frame is copied into each
			// connection's buffer rather than re-encoded, so the figure must stay
			// flat as the subscriber count grows. Measured at 6 objects per
			// dispatch at a thousand connections, it is not proportional to the
			// ten thousand connection case, which is the property that matters.
			if allocs > 8 {
				b.Errorf("dispatch allocates %.1f objects; the per-subscriber work must not allocate", allocs)
			}
		})
	}
}

func busEnvelope(tenant, channel string, id uint64, body []byte) bus.Envelope {
	return bus.Envelope{
		TenantID: tenant, Channel: channel, MessageID: id,
		TimeMs: uint64(id), Body: body,
	}
}

// BenchmarkLadderSpan measures what the ladder actually controls: the packet
// count a fixed volume of admitted content is turned into.
//
// The span is the number that matters. A stateless topology cannot move a load
// off a saturated instance - there are no connections to migrate - so a burst has
// to be absorbed by widening the ladder rather than by adding capacity, and the
// lever available is packet count. Bytes are fixed by the per-viewer budget and
// merging does not change them, so a wider ladder buys packets, not bandwidth.
//
// It asserts the span, because a narrow ladder would leave adding capacity as the
// only remaining lever, and that lever does not exist for connections that are
// already established.
func BenchmarkLadderSpan(b *testing.B) {
	const conns = 500
	const admitted = 5000

	type row struct {
		level      Level
		packets    uint64
		offered    int
		admitted   int
		multiplier float64
		mergeRatio int
	}
	rows := make([]row, 0, 4)

	for _, l := range []Level{LevelNormal, LevelReduced, LevelSampled, LevelCritical} {
		level := l

		store := NewStore()
		shards := NewShardSet(1024)
		now := time.UnixMilli(1_700_000_000_000)
		fanout := NewFanout(store, shards, nil, func() time.Time { return now })
		for i := 0; i < conns; i++ {
			conn := NewConn(uint64(i+1), ConnConfig{TenantID: "t1", Subject: "u", WriteBufBytes: 16 * 1024, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: shards})
			if _, err := store.Subscribe("t1", "room:1", conn); err != nil {
				b.Fatalf("Subscribe: %v", err)
			}
		}

		degrade := NewDegradation(DegradationConfig{Manual: true, Override: level},
			func() time.Time { return now })
		// One coalescer per rung, with the window that rung implies. The clock
		// does not advance, so every window closes on the merge target alone.
		coalesce := NewCoalescerSet(
			uint64(10*degrade.WindowMultiplier()),
			int(20*degrade.MergeRatioMultiplier()),
			1<<20,
		)
		fanout.coalesce = coalesce

		body := make([]byte, 200)
		seq := uint64(0)
		counted := 0

		b.StartTimer()
		for n := 0; n < admitted; n++ {
			if !degrade.Admit(protocol.PriorityNormal) {
				degrade.RecordShed()
				continue
			}
			seq++
			if !degrade.Sample(seq) {
				degrade.RecordSampled()
				continue
			}
			counted++
			if _, err := fanout.Dispatch(busEnvelope("t1", "room:1", seq, body)); err != nil {
				b.Fatalf("Dispatch: %v", err)
			}
			coalesce.FlushDue(fanout)
			fanout.FlushPass(func(*Conn, []byte) {})
		}
		b.StopTimer()

		rows = append(rows, row{
			level:      level,
			packets:    fanout.Snapshot().PacketsOut,
			offered:    admitted,
			admitted:   counted,
			multiplier: degrade.WindowMultiplier(),
			mergeRatio: int(20 * degrade.MergeRatioMultiplier()),
		})
	}

	if len(rows) == 0 {
		b.Skip("no measurements")
	}
	base := rows[0].packets
	if base == 0 {
		b.Fatal("the normal level produced no packets")
	}
	for _, r := range rows {
		// Packets are reported against offered content, not admitted content,
		// so the levels are directly comparable. A level that drops half the
		// content is buying its reduction partly by throwing content away, and
		// that has to be visible in the figure rather than hidden by normalising
		// on a smaller denominator.
		b.Logf("level %-9s %5d packets for %d offered (%d admitted, %d%%, merge %d, window x%.0f)",
			r.level, r.packets, r.offered, r.admitted,
			100*r.admitted/r.offered, r.mergeRatio, r.multiplier)
	}

	// The span that matters is L0 to L2, the range over which ordinary content is
	// still delivered. L3 sheds it outright, so a ratio taken against L3 would be
	// unbounded and would say nothing about throughput: it measures that the
	// content is gone, not that the instance got faster.
	sampled := rows[2].packets
	if sampled == 0 {
		b.Fatal("the sampled level produced no packets")
	}
	span := float64(base) / float64(sampled)
	b.Logf("packet-count span L0 to L2: %.1fx", span)
	if span < 5 {
		b.Errorf("ladder span is %.1fx, below the 5x the design relies on", span)
	}

	// L3 must still carry the classes that must not be shed, or the ladder would
	// be trading a correctness failure for throughput.
	degrade := NewDegradation(DegradationConfig{Manual: true, Override: LevelCritical}, time.Now)
	if !degrade.Admit(protocol.PrioritySystem) || !degrade.Admit(protocol.PriorityHigh) {
		b.Error("the critical level sheds content that must always be delivered")
	}
}
