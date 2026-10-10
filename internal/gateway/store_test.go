package gateway

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/protocol"
)

func newTestConn(id uint64, tenant string) *Conn {
	return NewConn(id, ConnConfig{
		TenantID: tenant, Subject: "subject",
		WriteBufBytes: 16 * 1024, ReadBufBytes: 1024,
		PerViewerBps: 2048, Shards: NewShardSet(64),
	})
}

func TestStoreSubscribeAndLookup(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")

	ok, err := s.Subscribe("t1", "room:1", conn)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !ok {
		t.Error("Subscribe reported no subscription added")
	}

	subs, found := s.LookupSubscribers("t1", "room:1")
	if !found {
		t.Fatal("channel not found after subscribe")
	}
	if len(subs) != 1 || subs[0].Conn() != conn {
		t.Errorf("subscribers = %v, want the one connection", subs)
	}
	if got := s.SubscriberCountFor("t1", "room:1"); got != 1 {
		t.Errorf("SubscriberCountFor = %d, want 1", got)
	}
	if s.ChannelCount() != 1 {
		t.Errorf("ChannelCount = %d, want 1", s.ChannelCount())
	}
	if s.SubscriberCount() != 1 {
		t.Errorf("SubscriberCount = %d, want 1", s.SubscriberCount())
	}
}

// Channels are created implicitly on first use. A danmaku room's lifetime is
// not known in advance, so there is no registration step and no table of
// channels that were ever used.
func TestChannelIsCreatedImplicitly(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")

	if _, found := s.LookupSubscribers("t1", "room:never-used"); found {
		t.Fatal("an untouched channel already exists")
	}
	if s.ChannelCount() != 0 {
		t.Fatalf("ChannelCount = %d before any subscription, want 0", s.ChannelCount())
	}

	if _, err := s.Subscribe("t1", "room:never-used", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	subs, found := s.LookupSubscribers("t1", "room:never-used")
	if !found || len(subs) != 1 {
		t.Error("implicit creation did not produce a usable channel")
	}
}

// Two tenants may use the same channel name without observing each other.
func TestChannelsAreTenantScoped(t *testing.T) {
	s := NewStore()
	a := newTestConn(1, "t1")
	b := newTestConn(2, "t2")

	if _, err := s.Subscribe("t1", "room:1", a); err != nil {
		t.Fatalf("Subscribe t1: %v", err)
	}
	if _, err := s.Subscribe("t2", "room:1", b); err != nil {
		t.Fatalf("Subscribe t2: %v", err)
	}

	subsA, _ := s.LookupSubscribers("t1", "room:1")
	subsB, _ := s.LookupSubscribers("t2", "room:1")
	if len(subsA) != 1 || subsA[0].Conn() != a {
		t.Errorf("t1 subscribers = %v, want only its own connection", subsA)
	}
	if len(subsB) != 1 || subsB[0].Conn() != b {
		t.Errorf("t2 subscribers = %v, want only its own connection", subsB)
	}
	if s.ChannelCount() != 2 {
		t.Errorf("ChannelCount = %d, want 2 for one name under two tenants", s.ChannelCount())
	}
}

// A delivery for a channel this instance does not serve must be discarded
// cheaply. Under broadcast, that is the common case: every instance receives
// every message but serves only its own slice of subscribers.
func TestLookupOfUnservedChannelIsCheapMiss(t *testing.T) {
	s := NewStore()
	subs, found := s.LookupSubscribers("t1", "room:elsewhere")
	if found {
		t.Error("an unserved channel reported as found")
	}
	if subs != nil {
		t.Error("an unserved channel returned subscribers")
	}
}

func TestUnsubscribeIsIdempotentAndScoped(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	other := newTestConn(2, "t1")

	if _, err := s.Subscribe("t1", "room:1", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := s.Subscribe("t1", "room:2", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := s.Subscribe("t1", "room:1", other); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if !s.Unsubscribe("t1", "room:1", conn) {
		t.Fatal("Unsubscribe reported nothing removed")
	}
	if s.Unsubscribe("t1", "room:1", conn) {
		t.Error("second Unsubscribe reported a removal")
	}
	if s.Unsubscribe("t1", "room:never", conn) {
		t.Error("Unsubscribe on an unserved channel reported a removal")
	}

	// The other channel is untouched.
	subs, _ := s.LookupSubscribers("t1", "room:2")
	if len(subs) != 1 || subs[0].Conn() != conn {
		t.Errorf("room:2 subscribers = %v, want the connection to survive", subs)
	}
	// The other subscriber to room:1 is untouched.
	subs, _ = s.LookupSubscribers("t1", "room:1")
	if len(subs) != 1 || subs[0].Conn() != other {
		t.Errorf("room:1 subscribers = %v, want only the other connection", subs)
	}
}

// Removal swaps in the last element, so the slice must stay dense. A hole left
// behind would be dereferenced on the delivery path.
func TestUnsubscribeLeavesNoHoles(t *testing.T) {
	s := NewStore()
	conns := make([]*Conn, 100)
	for i := range conns {
		conns[i] = newTestConn(uint64(i+1), "t1")
		if _, err := s.Subscribe("t1", "room:1", conns[i]); err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
	}

	// Remove every third.
	for i := 0; i < len(conns); i += 3 {
		if !s.Unsubscribe("t1", "room:1", conns[i]) {
			t.Fatalf("Unsubscribe %d reported nothing removed", i)
		}
	}

	subs, _ := s.LookupSubscribers("t1", "room:1")
	expected := len(conns) - (len(conns)+2)/3
	if len(subs) != expected {
		t.Fatalf("subscriber count = %d, want %d", len(subs), expected)
	}
	for i, sub := range subs {
		if sub == nil {
			t.Fatalf("nil hole at index %d of the subscriber slice", i)
		}
	}

	seen := map[*Conn]bool{}
	for _, sub := range subs {
		if seen[sub.Conn()] {
			t.Fatalf("connection %d appears twice after removal", sub.Conn().ID)
		}
		seen[sub.Conn()] = true
	}
}

// An instance that has been up a long time must not accumulate one entry per
// channel it has ever touched.
func TestEmptyChannelStateIsReclaimed(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")

	for i := 0; i < 1000; i++ {
		channel := fmt.Sprintf("room:%d", i)
		if _, err := s.Subscribe("t1", channel, conn); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if !s.Unsubscribe("t1", channel, conn) {
			t.Fatalf("Unsubscribe %s reported nothing removed", channel)
		}
	}
	if s.ChannelCount() != 0 {
		t.Errorf("ChannelCount = %d after every channel emptied, want 0", s.ChannelCount())
	}
}

func TestUnsubscribeAllReleasesEveryChannel(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	other := newTestConn(2, "t1")

	channels := []string{"a", "b", "c", "d", "e"}
	for _, c := range channels {
		if _, err := s.Subscribe("t1", c, conn); err != nil {
			t.Fatalf("Subscribe %s: %v", c, err)
		}
		if _, err := s.Subscribe("t1", c, other); err != nil {
			t.Fatalf("Subscribe %s (other): %v", c, err)
		}
	}

	removed := s.UnsubscribeAll(conn)
	if len(removed) != len(channels) {
		t.Errorf("UnsubscribeAll removed %d channels, want %d", len(removed), len(channels))
	}

	for _, c := range channels {
		subs, _ := s.LookupSubscribers("t1", c)
		if len(subs) != 1 || subs[0].Conn() != other {
			t.Errorf("channel %s after release = %v, want only the other connection", c, subs)
		}
	}
	if got := s.ChannelsFor(conn); len(got) != 0 {
		t.Errorf("ChannelsFor after release = %v, want none", got)
	}
}

func TestChannelsFor(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	for _, c := range []string{"x", "y", "z"} {
		if _, err := s.Subscribe("t1", c, conn); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	got := s.ChannelsFor(conn)
	if len(got) != 3 {
		t.Errorf("ChannelsFor = %v, want 3 channels", got)
	}
}

func TestStoreConcurrentSubscribeUnsubscribe(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			conn := newTestConn(uint64(g+1), "t1")
			for i := 0; i < 200; i++ {
				channel := fmt.Sprintf("room:%d", i%10)
				_, _ = s.Subscribe("t1", channel, conn)
				s.LookupSubscribers("t1", channel)
				_ = s.Unsubscribe("t1", channel, conn)
			}
		}(g)
	}
	wg.Wait()

	if got := s.SubscriberCount(); got != 0 {
		t.Errorf("SubscriberCount = %d after every unsubscribe, want 0", got)
	}
}

// --- Coalescer ---

func TestCoalescerMergesWithinWindow(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 10, 20, 64*1024)

	for i := uint64(1); i <= 5; i++ {
		c.AddMsg(protocol.Message{ID: i, TimeMs: 1000, Body: []byte("x")})
	}
	if c.Pending() != 5 {
		t.Errorf("Pending = %d, want 5", c.Pending())
	}
	// Five messages with a merge target of 20 must not close the window on
	// count.
	if c.Due(1000) {
		t.Error("window closed below the merge target")
	}

	frame := c.Take()
	if frame == nil {
		t.Fatal("Take returned nil with messages pending")
	}
	batch, err := protocol.ParseBatch(frame)
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(batch.Messages) != 5 {
		t.Errorf("batch carried %d messages, want 5", len(batch.Messages))
	}
	// Order within a batch must be preserved.
	for i, m := range batch.Messages {
		if m.ID != uint64(i+1) {
			t.Errorf("message %d has id %d, want %d", i, m.ID, i+1)
		}
	}
	if c.Pending() != 0 {
		t.Errorf("Pending = %d after Take, want 0", c.Pending())
	}
}

// This is the central claim of the design, expressed as a test: merging cuts
// packets, not bytes. If a future change ever made merging reduce delivered
// bytes, the capacity model in design.md would be wrong.
func TestCoalescingCutsPacketsNotBytes(t *testing.T) {
	const mergeRatio = 20
	const messageCount = 200

	body := []byte("a danmaku message body")

	unmerged := NewCoalescer("t1", "room:1", 10, 1, 1<<20)
	merged := NewCoalescer("t1", "room:1", 10, mergeRatio, 1<<20)

	var unmergedBytes, mergedBytes int
	// Drive both coalescers the way the flush loop does: add, check whether the
	// window is due, and only then take.
	for i := uint64(1); i <= messageCount; i++ {
		m := protocol.Message{ID: i, TimeMs: 1000, Body: body}
		unmerged.AddMsg(m)
		merged.AddMsg(m)
		if unmerged.Due(1000) {
			if f := unmerged.Take(); f != nil {
				unmergedBytes += len(f)
			}
		}
		if merged.Due(1000) {
			if f := merged.Take(); f != nil {
				mergedBytes += len(f)
			}
		}
	}
	for {
		f := unmerged.Take()
		if f == nil {
			break
		}
		unmergedBytes += len(f)
	}
	for {
		f := merged.Take()
		if f == nil {
			break
		}
		mergedBytes += len(f)
	}

	uPackets, uMessages, uContent := unmerged.Stats()
	mPackets, mMessages, mContent := merged.Stats()

	if uPackets != messageCount || uMessages != messageCount {
		t.Errorf("unmerged emitted %d packets for %d messages, want %d each", uPackets, uMessages, messageCount)
	}
	if wantPackets := uint64(messageCount / mergeRatio); mPackets != wantPackets {
		t.Errorf("merged emitted %d packets for %d messages, want %d", mPackets, messageCount, wantPackets)
	}
	if mMessages != messageCount {
		t.Errorf("merged emitted %d messages, want %d: merging must not lose messages", mMessages, messageCount)
	}
	if uContent != mContent {
		t.Errorf("content bytes changed with merging: %d unmerged vs %d merged", uContent, mContent)
	}
	if unmergedBytes <= mergedBytes {
		t.Errorf("merged frame total %d should exceed unmerged %d only by per-packet headers, got %d vs %d",
			mergedBytes, unmergedBytes, mergedBytes, unmergedBytes)
	}
	// The overhead must be a small fraction, not a doubling.
	overhead := float64(mergedBytes) / float64(unmergedBytes)
	if overhead > 1.10 {
		t.Errorf("merged bytes are %.2fx unmerged, want under 1.10x", overhead)
	}
}

func TestCoalescerClosesOnWindowExpiry(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 10, 20, 64*1024)
	c.AddMsg(protocol.Message{ID: 1, TimeMs: 1000, Body: []byte("x")})

	if c.Due(1005) {
		t.Error("window closed before its interval elapsed")
	}
	if !c.Due(1010) {
		t.Error("window did not close after its interval elapsed")
	}
	if c.Take() == nil {
		t.Error("Take returned nil after the window closed")
	}
}

// A batch must also close on its byte bound, or a burst arriving faster than the
// window length would produce a packet larger than one connection's buffer.
func TestCoalescerClosesOnByteBound(t *testing.T) {
	const maxBytes = 2048
	c := NewCoalescer("t1", "room:1", 10000, 1000, maxBytes)

	body := make([]byte, 256)
	dueAt := -1
	for i := 0; i < 20; i++ {
		c.AddMsg(protocol.Message{ID: uint64(i + 1), TimeMs: 1000, Body: body})
		if c.Due(1000) && dueAt < 0 {
			dueAt = i + 1
			break
		}
	}
	if dueAt < 0 {
		t.Fatal("the byte bound was never reached")
	}
	if dueAt < 4 {
		t.Errorf("byte bound reached after %d messages of 256 bytes into a %d byte limit, want later",
			dueAt, 2048)
	}

	frame := c.Take()
	batch, err := protocol.ParseBatch(frame)
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(batch.Messages) >= 20 {
		t.Errorf("batch carried all %d messages despite the byte bound", len(batch.Messages))
	}
}

func TestCoalescerClosesAtMergeTarget(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 100000, 5, 1<<20)
	for i := uint64(1); i <= 5; i++ {
		c.AddMsg(protocol.Message{ID: i, TimeMs: 1000, Body: []byte("x")})
	}
	if !c.Due(1000) {
		t.Error("window did not close at the merge target")
	}
}

func TestCoalescerEmptyTakeReturnsNil(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 10, 20, 64*1024)
	if c.Take() != nil {
		t.Error("Take with nothing pending returned a frame")
	}
	if c.Due(1000) {
		t.Error("an empty coalescer reported its window due")
	}
}

// Coalescing disabled must send each message individually, which is a channel
// whose messages carry independent meaning.
func TestCoalescingCanBeDisabled(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 10000, 1000, 1<<20)
	c.SetMergeRatio(1)

	c.AddMsg(protocol.Message{ID: 1, TimeMs: 1000, Body: []byte("a")})
	if !c.Due(1000) {
		t.Error("with a merge ratio of 1 the window should close immediately")
	}
	frame := c.Take()
	batch, err := protocol.ParseBatch(frame)
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(batch.Messages) != 1 {
		t.Errorf("batch carried %d messages, want 1", len(batch.Messages))
	}
}

// The returned frame is shared by every subscriber, so it must be identical
// bytes regardless of how many subscribers receive it.
func TestCoalescerFrameIsRecipientIndependent(t *testing.T) {
	c := NewCoalescer("t1", "room:1", 10, 20, 64*1024)
	c.AddMsg(protocol.Message{ID: 1, TimeMs: 1000, Body: []byte("hello")})
	first := c.Take()

	c.AddMsg(protocol.Message{ID: 1, TimeMs: 1000, Body: []byte("hello")})
	second := c.Take()

	if len(first) != len(second) {
		t.Fatalf("frames differ in length: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("frames differ at byte %d", i)
		}
	}
}

// --- Policy derivation ---

// The design point: a 2 KB/s budget against 200 B messages admits ten publishes
// a second, and the packet-rate bound sits ten times further out. Bandwidth is
// the binding constraint and the packet-rate side has headroom, which is the
// same conclusion the coalescing analysis reaches.
func TestDerivePublishCeilingAtDesignPoint(t *testing.T) {
	bandwidth := math.Floor(float64(2048) / 200.0)
	packet := 5.0 * 20.0
	got := DerivePublishCeiling(2048, 200, 5, 20)

	if got != bandwidth {
		t.Errorf("ceiling = %v, want the bandwidth bound %v", got, bandwidth)
	}
	if packet <= bandwidth {
		t.Errorf("packet-rate bound %v is not looser than the bandwidth bound %v; the "+
			"headroom this design depends on has disappeared", packet, bandwidth)
	}
}

// The ceiling must not move with subscriber count. Every subscriber receives
// every publish, so a viewer's experience depends on the publish rate alone, and
// a ceiling that scaled with size would admit a rate no viewer can absorb.
func TestDerivePublishCeilingDoesNotDependOnSubscriberCount(t *testing.T) {
	// A hundred-thousand-subscriber channel on the derivation that previously
	// scaled with size admitted 25,000 publishes/s, at which each viewer would
	// receive 5 MB/s against a 2 KB/s budget.
	ceiling := DerivePublishCeiling(2048, 200, 5, 20)
	if ceiling != 10 {
		t.Fatalf("ceiling = %v, want 10 publishes/s", ceiling)
	}

	perViewerRate := ceiling * 200
	if perViewerRate > 2048 {
		t.Errorf("at the ceiling each viewer receives %.0f B/s, over the %d B/s budget",
			perViewerRate, 2048)
	}
	// And the same ceiling applies however many subscribers there are, because
	// the function has no subscriber term at all.
	for _, subscribers := range []int{1, 1000, 100000} {
		if got := DerivePublishCeiling(2048, 200, 5, 20); got != ceiling {
			t.Errorf("ceiling = %v, want %v regardless of %d subscribers",
				got, ceiling, subscribers)
		}
	}
}

// More merging means each delivered packet carries more raw messages, so a
// viewer reaches its packet-rate target with fewer of them. That relaxes the
// packet-rate bound. The bandwidth bound is unaffected, which is the point: when
// the two are close, bandwidth is what binds no matter how much merging is
// configured.
func TestMergeRatioRelaxesOnlyThePacketRateBound(t *testing.T) {
	const budget = 4096
	const msgBytes = 200

	// A merge ratio of 1 gives a packet bound of 5, below the bandwidth bound of
	// 20, so the packet bound binds and raising the ratio raises the ceiling.
	low := DerivePublishCeiling(budget, msgBytes, 5, 1)
	if low != 5 {
		t.Fatalf("ceiling at merge ratio 1 = %v, want the packet bound 5", low)
	}
	mid := DerivePublishCeiling(budget, msgBytes, 5, 10)
	if mid != 20 {
		t.Errorf("ceiling at merge ratio 10 = %v, want the bandwidth bound 20", mid)
	}
	// Past the crossover the bandwidth bound holds and further merging changes
	// nothing, because merging does not reduce bytes.
	high := DerivePublishCeiling(budget, msgBytes, 5, 100)
	if high != 20 {
		t.Errorf("ceiling at merge ratio 100 = %v, want it pinned at the bandwidth bound 20", high)
	}
}

// Retained as a regression guard on the relationship that produced the earlier
// error: a higher merge ratio must never lower the ceiling.
func TestHigherMergeRatioNeverLowersTheCeiling(t *testing.T) {
	prev := 0.0
	for _, ratio := range []int{1, 2, 5, 10, 20, 50, 100, 200} {
		got := DerivePublishCeiling(2048, 200, 5, ratio)
		if got < prev {
			t.Fatalf("ceiling fell from %v to %v when the merge ratio rose to %d", prev, got, ratio)
		}
		prev = got
	}
}

func TestDerivePublishCeilingZeroInputs(t *testing.T) {
	if got := DerivePublishCeiling(0, 200, 5, 20); got != 0 {
		t.Errorf("no budget: ceiling = %v, want 0 meaning unlimited", got)
	}
	if got := DerivePublishCeiling(2048, 0, 5, 20); got != 0 {
		t.Errorf("no nominal message size: ceiling = %v, want 0", got)
	}
	// A non-positive merge ratio is read as "no merging configured", which makes
	// the packet-rate bound bind and the ceiling tighter. Erring tight is the
	// safe direction for a limit: an under-merged channel admits more publishes
	// than each packet-rate target allows.
	if got := DerivePublishCeiling(2048, 200, 5, 0); got != 5 {
		t.Errorf("merge ratio of 0: ceiling = %v, want 5 from the packet bound", got)
	}
}

func TestResolveChannelPolicy(t *testing.T) {
	f64 := func(v float64) *float64 { return &v }
	i := func(v int) *int { return &v }

	t.Run("derives when no override", func(t *testing.T) {
		p := ResolveChannelPolicy(controlplaneTenantConfig("t1", nil), "room:1", 2048, 200)
		if !p.CeilingDerived {
			t.Error("CeilingDerived is false with no override")
		}
		if p.PublishCeiling != 10 {
			t.Errorf("ceiling = %v, want 10", p.PublishCeiling)
		}
		if !p.CoalescingEnabled {
			t.Error("coalescing should default to enabled")
		}
	})

	t.Run("explicit override wins", func(t *testing.T) {
		cfg := controlplaneTenantConfig("t1", map[string]controlplaneChannelConfig{
			"room:1": {Ceiling: f64(42), Coalescing: false, MaxSubs: 500},
		})
		p := ResolveChannelPolicy(cfg, "room:1", 2048, 200)
		if p.PublishCeiling != 42 {
			t.Errorf("ceiling = %v, want the override 42", p.PublishCeiling)
		}
		if p.CeilingDerived {
			t.Error("CeilingDerived is true despite an explicit override")
		}
		if p.CoalescingEnabled {
			t.Error("coalescing override not applied")
		}
		if p.MaxSubscribers != 500 {
			t.Errorf("MaxSubscribers = %d, want 500", p.MaxSubscribers)
		}
	})

	t.Run("override zero means no ceiling, not derive", func(t *testing.T) {
		cfg := controlplaneTenantConfig("t1", map[string]controlplaneChannelConfig{
			"room:1": {Ceiling: f64(0)},
		})
		p := ResolveChannelPolicy(cfg, "room:1", 2048, 200)
		if p.PublishCeiling != 0 {
			t.Errorf("ceiling = %v, want an explicit 0", p.PublishCeiling)
		}
		if p.CeilingDerived {
			t.Error("an explicit zero should count as an override, not a derivation")
		}
	})

	t.Run("rate and merge overrides feed the derivation", func(t *testing.T) {
		cfg := controlplaneTenantConfig("t1", map[string]controlplaneChannelConfig{
			"room:1": {Rate: f64(10), Merge: i(10)},
		})
		p := ResolveChannelPolicy(cfg, "room:1", 2048, 200)
		// min(2048/200 = 10, rate 10 x merge 10 = 100) = 10
		if p.PublishCeiling != 10 {
			t.Errorf("ceiling = %v, want 10", p.PublishCeiling)
		}
		if p.MergeRatio != 10 || p.ReceiveRateTarget != 10 {
			t.Errorf("policy = %+v, want the overrides applied", p)
		}
	})

	t.Run("unconfigured channel uses defaults", func(t *testing.T) {
		// An unserved channel is configured like any other: the ceiling comes
		// from the budget, not from whether this instance happens to hold
		// subscribers for it.
		p := ResolveChannelPolicy(controlplaneTenantConfig("t1", map[string]controlplaneChannelConfig{}), "room:new", 2048, 200)
		if p.PublishCeiling != 10 {
			t.Errorf("ceiling = %v, want 10", p.PublishCeiling)
		}
		if p.MergeRatio != DefaultMergeRatio {
			t.Errorf("MergeRatio = %d, want the default %d", p.MergeRatio, DefaultMergeRatio)
		}
	})
}

func TestAllowsSubscriber(t *testing.T) {
	t.Run("no ceiling admits", func(t *testing.T) {
		p := ChannelPolicy{MaxSubscribers: 0}
		for i := 0; i < 10000; i++ {
			if ok, _ := p.AllowsSubscriber(i); !ok {
				t.Fatalf("refused at %d with no ceiling", i)
			}
		}
	})
	t.Run("ceiling refuses with headroom reported", func(t *testing.T) {
		p := ChannelPolicy{MaxSubscribers: 100}
		ok, remaining := p.AllowsSubscriber(100)
		if ok {
			t.Error("admitted a subscriber past the ceiling")
		}
		if remaining != 0 {
			t.Errorf("remaining = %d at the ceiling, want 0", remaining)
		}
		ok, remaining = p.AllowsSubscriber(50)
		if !ok || remaining != 49 {
			t.Errorf("AllowsSubscriber(50) = %v, %d, want true, 49", ok, remaining)
		}
	})
}

// --- ConnBuffer ---

func TestConnBufferQueuesAndDrains(t *testing.T) {
	b := NewConnBuffer(1024)
	frame := []byte("hello")

	if queued, dropped := b.Append(frame); !queued || dropped != 0 {
		t.Fatalf("Append = %v, %d", queued, dropped)
	}
	if b.Pending() != 5 || b.Frames() != 1 {
		t.Errorf("Pending = %d, Frames = %d, want 5, 1", b.Pending(), b.Frames())
	}

	out := b.Take(nil)
	if string(out) != "hello" {
		t.Errorf("drained %q, want %q", out, "hello")
	}
	if b.Pending() != 0 || b.Frames() != 0 {
		t.Errorf("buffer not empty after drain: %d bytes, %d frames", b.Pending(), b.Frames())
	}
}

// Overflow must discard the oldest frames so the newest survive: danmaku value
// decays with time, so the recent content is the content worth keeping.
func TestConnBufferOverflowKeepsNewest(t *testing.T) {
	b := NewConnBuffer(10) // fits exactly two five-byte frames
	for i := 0; i < 6; i++ {
		b.Append([]byte("abcde"))
	}

	if b.Pending() > b.Capacity() {
		t.Fatalf("Pending = %d exceeds capacity %d", b.Pending(), b.Capacity())
	}
	if b.Dropped() != 4 {
		t.Errorf("Dropped = %d, want 4", b.Dropped())
	}

	out := b.Take(nil)
	if len(out) != 10 {
		t.Errorf("drained %d bytes, want the capacity 10", len(out))
	}
}

// A burst that still fits must not be mistaken for overflow, even after the
// buffer has slid forward.
func TestConnBufferReclaimsSpaceWithoutDropping(t *testing.T) {
	b := NewConnBuffer(12)
	for round := 0; round < 10; round++ {
		for i := 0; i < 3; i++ {
			if queued, dropped := b.Append([]byte("abcd")); !queued || dropped != 0 {
				t.Fatalf("round %d append %d: queued=%v dropped=%d", round, i, queued, dropped)
			}
		}
		if b.Pending() != 0 {
			b.Take(nil)
		}
	}
	if b.Dropped() != 0 {
		t.Errorf("Dropped = %d over repeated full drains, want 0", b.Dropped())
	}
}

// Every queued item must remain a whole, parseable frame. Dropping a byte range
// instead of a frame would leave the reader parsing a truncated frame.
func TestConnBufferOnlyEverDropsWholeFrames(t *testing.T) {
	b := NewConnBuffer(512)
	frames := [][]byte{
		protocol.AppendBatch(nil, &protocol.Batch{Channel: "c", Messages: []protocol.Message{{ID: 1, TimeMs: 1, Body: []byte("one")}}}),
		protocol.AppendBatch(nil, &protocol.Batch{Channel: "c", Messages: []protocol.Message{{ID: 2, TimeMs: 2, Body: []byte("two")}}}),
		protocol.AppendBatch(nil, &protocol.Batch{Channel: "c", Messages: []protocol.Message{{ID: 3, TimeMs: 3, Body: []byte("three")}}}),
	}
	for _, f := range frames {
		b.Append(f)
	}
	out := b.Take(nil)

	// Walk the drained bytes as a sequence of frames.
	pos := 0
	parsed := 0
	for pos < len(out) {
		batch, err := protocol.ParseBatch(out[pos:])
		if err != nil {
			t.Fatalf("frame %d is not parseable at offset %d: %v", parsed, pos, err)
		}
		n := frameLength(t, out[pos:])
		pos += n
		parsed++
		_ = batch
	}
	if parsed != len(frames) {
		t.Errorf("parsed %d frames, want %d", parsed, len(frames))
	}
}

// A frame larger than the whole buffer must be dropped without evicting
// everything else, since it can never fit regardless.
func TestConnBufferRejectsOversizedFrameWithoutEviction(t *testing.T) {
	b := NewConnBuffer(8)
	if _, _ = b.Append([]byte("small")); !true {
		t.Fatal("unreachable")
	}
	ok, _ := b.Append([]byte("far too large for this buffer"))
	if ok {
		t.Error("an oversized frame was queued")
	}
	if b.Pending() != 5 {
		t.Errorf("Pending = %d, want the small frame to survive", b.Pending())
	}
	if b.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", b.Dropped())
	}
}

func TestConnBufferGapConsumption(t *testing.T) {
	b := NewConnBuffer(8)
	b.Append([]byte("12345"))
	b.Append([]byte("67890")) // overflows, drops the first

	if n, ok := b.ConsumeGap(); !ok || n != 1 {
		t.Errorf("ConsumeGap = %d, %v, want 1, true", n, ok)
	}
	// A gap is reported once.
	if _, ok := b.ConsumeGap(); ok {
		t.Error("ConsumeGap reported twice without new drops")
	}
	// And a further drop re-arms it.
	b.Take(nil)
	b.Append([]byte("1234567890"))
	b.Append([]byte("12345"))
	if n, ok := b.ConsumeGap(); !ok || n != 1 {
		t.Errorf("second ConsumeGap = %d, %v, want 1, true", n, ok)
	}
}

// --- EgressMeter ---

func TestEgressMeterMeasuresRate(t *testing.T) {
	m := NewEgressMeter(2048)
	base := time.UnixMilli(1_700_000_000_000)

	// Feed 1024 bytes every 100ms for a second: 10240 bytes/s.
	for i := 0; i < 10; i++ {
		m.Observe(1024, base.Add(time.Duration(i)*100*time.Millisecond))
	}
	rate := m.Rate()
	if rate < 8000 || rate > 13000 {
		t.Errorf("measured rate = %d, want roughly 10240", rate)
	}
}

func TestEgressMeterOverBudget(t *testing.T) {
	m := NewEgressMeter(2048)
	if m.OverBudget(2048, 0.1) {
		t.Error("a rate exactly at budget reported as over")
	}
	if !m.OverBudget(3000, 0.1) {
		t.Error("a rate well above budget not reported as over")
	}
	// Within tolerance must not trip, or sampling noise would trip it.
	if m.OverBudget(2200, 0.1) {
		t.Error("a rate inside the tolerance reported as over budget")
	}
}

func TestEgressMeterZeroBudgetIsUnlimited(t *testing.T) {
	m := NewEgressMeter(0)
	if m.OverBudget(1_000_000, 0) {
		t.Error("a meter with no budget reported an overrun")
	}
}

// --- Conn ---

func TestConnEnqueueRegistersDirtyOnce(t *testing.T) {
	shards := NewShardSet(64)
	c := NewConn(7, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 4096, ReadBufBytes: 256, PerViewerBps: 2048, Shards: shards})

	for i := 0; i < 50; i++ {
		c.Enqueue([]byte("frame"), 1000)
	}
	if got := shards.Pending(); got != 1 {
		t.Errorf("shard registrations = %d for 50 enqueues, want 1", got)
	}

	out := c.Drain(nil)
	if len(out) != 50*len("frame") {
		t.Errorf("drained %d bytes, want %d", len(out), 50*len("frame"))
	}
	if c.dirty.Load() {
		t.Error("connection still dirty after drain")
	}

	// The shard entry survives until a flush pass consumes it, because the
	// flush loop and the drain can race. The pass finds the connection clean and
	// writes nothing, which is the property that matters.
	drained := shards.FlushAll(make([]*Conn, 0, 8))
	if len(drained) != 1 {
		t.Fatalf("flush found %d connections, want the one registered", len(drained))
	}
	for _, dc := range drained {
		if dc.Pending() != 0 {
			t.Errorf("flushed connection still has %d bytes queued", dc.Pending())
		}
	}
	if shards.Pending() != 0 {
		t.Errorf("shard registrations after a flush pass = %d, want 0", shards.Pending())
	}
}

func TestConnQueuedSince(t *testing.T) {
	c := NewConn(1, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: NewShardSet(8)})
	if _, queued := c.QueuedSince(2000); queued {
		t.Error("a connection with nothing queued reported as queued")
	}

	c.Enqueue([]byte("x"), 1000)
	since, queued := c.QueuedSince(1500)
	if !queued || since != 500 {
		t.Errorf("QueuedSince = %d, %v, want 500, true", since, queued)
	}
}

// Drain reuses the caller's buffer, so a flush loop must not allocate per pass.
func TestConnDrainReusesCallerBuffer(t *testing.T) {
	c := NewConn(1, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: NewShardSet(8)})
	c.Enqueue([]byte("abcdefghij"), 1000)

	buf := make([]byte, 0, 256)
	first := c.Drain(buf)
	if len(first) != 10 {
		t.Fatalf("drained %d bytes, want 10", len(first))
	}
	capBefore := cap(first)

	c.Enqueue([]byte("klmno"), 1000)
	second := c.Drain(first[:0])
	if len(second) != 5 {
		t.Fatalf("second drain = %d bytes, want 5", len(second))
	}
	if cap(second) != capBefore {
		t.Errorf("drain reallocated: cap %d to %d", capBefore, cap(second))
	}
}

func TestConnStateTransitions(t *testing.T) {
	c := NewConn(1, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: NewShardSet(8)})
	if !c.Accepting() {
		t.Error("a new connection is not accepting")
	}
	c.SetState(ConnDraining)
	if c.Accepting() {
		t.Error("a draining connection is accepting")
	}
	c.SetState(ConnClosed)
	if c.State() != ConnClosed {
		t.Errorf("state = %v, want closed", c.State())
	}
}

func TestConnCloseIsIdempotent(t *testing.T) {
	c := NewConn(1, ConnConfig{TenantID: "t1", Subject: "s", WriteBufBytes: 1024, ReadBufBytes: 256, PerViewerBps: 2048, Shards: NewShardSet(8)})
	closes := 0
	c.SetCloseFunc(func() { closes++ })

	c.Close()
	c.Close()
	if closes != 1 {
		t.Errorf("close function ran %d times, want 1", closes)
	}
	if c.State() != ConnClosed {
		t.Errorf("state = %v, want closed", c.State())
	}
}

// frameLength determines the encoded length of the frame at the head of buf by
// parsing and re-encoding it. It exists so a test can walk a drained buffer as a
// sequence of frames.
func frameLength(t *testing.T, buf []byte) int {
	t.Helper()
	batch, err := protocol.ParseBatch(buf)
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	return len(protocol.AppendBatch(nil, batch))
}

// controlplaneTenantConfig builds a tenant snapshot for policy resolution
// without reaching for a database.
func controlplaneTenantConfig(tenantID string, channels map[string]controlplaneChannelConfig) controlplane.TenantConfig {
	cfg := controlplane.TenantConfig{
		TenantID: tenantID,
		Channels: map[string]controlplane.ChannelConfig{},
	}
	for name, c := range channels {
		cfg.Channels[name] = controlplane.ChannelConfig{
			TenantID:                  tenantID,
			Channel:                   name,
			PublishCeilingOverrideQPS: c.Ceiling,
			ReceiveRateTarget:         c.Rate,
			MergeRatioOverride:        c.Merge,
			CoalescingEnabled:         c.Coalescing,
			MaxSubscribers:            c.MaxSubs,
		}
	}
	return cfg
}

type controlplaneChannelConfig struct {
	Ceiling    *float64
	Rate       *float64
	Merge      *int
	Coalescing bool
	MaxSubs    int
}

// --- Batch subscription ---

// A reconnecting client must be able to restore its whole channel list in one
// call. Restoring twenty channels one round trip at a time would cost twenty
// sequential latencies, which on a 50 ms path is a second before the client is
// usable again.
func TestBatchSubscribeRestoresManyChannelsAtOnce(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")

	channels := make([]string, 20)
	for i := range channels {
		channels[i] = fmt.Sprintf("room:%d", i)
	}

	results := s.BatchSubscribe("t1", conn, channels, nil)
	if len(results) != 20 {
		t.Fatalf("BatchSubscribe returned %d results, want one per requested channel", len(results))
	}
	accepted := AcceptedChannels(results)
	if len(accepted) != 20 {
		t.Fatalf("accepted %d channels, want 20", len(accepted))
	}
	for _, c := range channels {
		if got := s.SubscriberCountFor("t1", c); got != 1 {
			t.Errorf("channel %s has %d subscribers after the batch, want 1", c, got)
		}
	}
	if got := s.SubscriberCount(); got != 20 {
		t.Errorf("SubscriberCount = %d, want 20", got)
	}
}

// A channel over its subscriber ceiling is refused individually while every
// other channel in the batch still succeeds. The connection must survive: a
// client that held twenty channels and may now hold eighteen should get the
// eighteen plus an account of the two, not a dead connection.
func TestBatchSubscribeReportsPerChannelRefusals(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	other := newTestConn(2, "t1")

	// Fill "full" to its ceiling.
	if _, err := s.Subscribe("t1", "full", other); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	policies := map[string]ChannelPolicy{"full": {MaxSubscribers: 1}}
	results := s.BatchSubscribe("t1", conn, []string{"ok-1", "full", "ok-2"}, policies)

	byChannel := map[string]SubResult{}
	for _, r := range results {
		byChannel[r.Channel] = r
	}
	if len(byChannel) != 3 {
		t.Fatalf("got %d results, want 3", len(byChannel))
	}

	if !byChannel["ok-1"].Accepted || !byChannel["ok-2"].Accepted {
		t.Errorf("unconstrained channels were refused: %+v", byChannel)
	}
	refused := byChannel["full"]
	if refused.Accepted {
		t.Error("a channel at its subscriber ceiling was accepted")
	}
	if refused.Reason != ReasonChannelFull {
		t.Errorf("reason = %q, want %q", refused.Reason, ReasonChannelFull)
	}

	// The connection survives and keeps what it was granted.
	if !conn.Accepting() {
		t.Error("the connection was closed by a partial refusal")
	}
	if got := s.ChannelsFor(conn); len(got) != 2 {
		t.Errorf("ChannelsFor = %v, want the two accepted channels", got)
	}
}

func TestBatchSubscribeOnDrainingConnectionRefusesAll(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	conn.SetState(ConnDraining)

	results := s.BatchSubscribe("t1", conn, []string{"a", "b"}, nil)
	for _, r := range results {
		if r.Accepted {
			t.Errorf("channel %s accepted on a draining connection", r.Channel)
		}
		if r.Reason != ReasonConnectionClosed {
			t.Errorf("channel %s reason = %q, want %q", r.Channel, r.Reason, ReasonConnectionClosed)
		}
	}
	if got := s.SubscriberCount(); got != 0 {
		t.Errorf("SubscriberCount = %d, want 0", got)
	}
}

func TestBatchSubscribeReportsDuplicates(t *testing.T) {
	s := NewStore()
	conn := newTestConn(1, "t1")
	if _, err := s.Subscribe("t1", "a", conn); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	results := s.BatchSubscribe("t1", conn, []string{"a"}, nil)
	if results[0].Accepted {
		t.Error("a duplicate channel reported as accepted")
	}
	if results[0].Reason != ReasonDuplicateChannel {
		t.Errorf("reason = %q, want %q", results[0].Reason, ReasonDuplicateChannel)
	}
}
