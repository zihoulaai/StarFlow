package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/protocol"
)

// Fanout turns admitted messages into delivered packets.
//
// The whole engine is built around one property: a message is serialized once
// and the resulting immutable bytes are copied into every subscriber's queue.
// Everything else here follows from that, and several things that would
// otherwise look reasonable are ruled out by it:
//
//   - No per-recipient rendering. A frame that differed per subscriber would
//     force re-serialization per recipient, which is the cost model this design
//     exists to avoid.
//   - No per-connection write on the delivery path. The delivery path only
//     enqueues; the flush loop owns every socket write.
//   - No subscription map. Fanout iterates a whole set, so a dense slice beats
//     a hash map by roughly two orders of magnitude on cache behaviour.
//
// Under broadcast every instance receives every message and delivers the part
// matching its own local subscriptions. A message for a channel this instance
// does not serve is discarded at the lookup, which is the common case rather
// than an error.
type Fanout struct {
	store    *Store
	shards   *ShardSet
	coalesce *CoalescerSet
	now      func() time.Time

	// stats. All are plain counters incremented in process; nothing here
	// performs a store call, because a synchronous metrics write on the
	// delivery path would reintroduce exactly the cost the design removes.
	messagesIn     atomic.Uint64
	messagesOut    atomic.Uint64
	packetsOut     atomic.Uint64
	bytesOut       atomic.Uint64
	channelMisses  atomic.Uint64
	expiredDropped atomic.Uint64
	enqueueDropped atomic.Uint64

	// Scratch buffers reused across calls so the delivery path allocates
	// nothing on the steady-state path.
	encodeScratch []byte
	dirtyScratch  []*Conn
	writeScratch  []byte
	noticeScratch []byte

	deliverMu sync.Mutex
}

// NewFanout returns an engine over store.
func NewFanout(store *Store, shards *ShardSet, coalesce *CoalescerSet, now func() time.Time) *Fanout {
	if now == nil {
		now = time.Now
	}
	return &Fanout{store: store, shards: shards, coalesce: coalesce, now: now}
}

// Dispatch admits one bus envelope.
//
// It returns the number of connections the message was queued for. A zero with
// no error is the normal case for a message destined for a channel this
// instance does not serve.
func (f *Fanout) Dispatch(env bus.Envelope) (int, error) {
	// The scratch buffers are per engine, so concurrent dispatches would
	// corrupt each other's frames. The lock is taken here rather than per
	// channel because the encode is the only shared mutable state; under the
	// broadcast topology the fast path never reaches the lock at all, since a
	// message for an unserved channel returns before it.
	f.deliverMu.Lock()
	defer f.deliverMu.Unlock()

	nowMs := uint64(f.now().UnixMilli())

	// A publisher-requested lifetime is honoured by dropping late arrivals.
	// Nothing was scheduled or persisted to make this work, which is why a TTL
	// needs no state.
	if env.ExpiresAtMs != 0 && nowMs > env.ExpiresAtMs {
		f.expiredDropped.Add(1)
		return 0, nil
	}

	// The channel is resolved before any per-channel state is allocated. Under
	// broadcast every instance receives every message but serves only its own
	// subscribers, so most messages name a channel this instance does not hold.
	// Checking here discards them without leaving a coalescer behind for every
	// channel on the platform, on every instance.
	if _, served := f.store.LookupSubscribers(env.TenantID, env.Channel); !served {
		f.channelMisses.Add(1)
		return 0, nil
	}

	f.messagesIn.Add(1)

	// The delivery path never writes to a socket. It hands the message to the
	// channel's coalescer, which owns the encode.
	if f.coalesce != nil {
		if !f.coalesce.Enqueue(env.TenantID, env.Channel, protocol.Message{
			ID:     env.MessageID,
			TimeMs: env.TimeMs,
			Class:  protocol.PriorityClass(env.Class),
			Flags:  protocol.MessageFlag(env.Flags),
			Tag:    env.Tag,
			Body:   env.Body,
		}, nowMs) {
			// Coalescing is disabled for this channel, so the message is
			// encoded and delivered on its own rather than merged.
			return f.deliver(env.TenantID, env.Channel, protocol.Batch{
				Channel: env.Channel,
				Messages: []protocol.Message{{
					ID: env.MessageID, TimeMs: env.TimeMs,
					Class: protocol.PriorityClass(env.Class),
					Body:  env.Body,
				}},
			}, nowMs)
		}
		return 0, nil
	}
	return 0, nil
}

// deliver encodes one batch once and copies it into every local subscriber's
// queue.
func (f *Fanout) deliver(tenantID, channel string, batch protocol.Batch, nowMs uint64) (int, error) {
	subs, served := f.store.LookupSubscribers(tenantID, channel)
	if !served {
		// The overwhelmingly common case under broadcast: this instance holds
		// no subscriber for the channel. Discarding here is what makes the
		// broadcast topology viable.
		f.channelMisses.Add(1)
		return 0, nil
	}
	if len(subs) == 0 {
		return 0, nil
	}

	// Serialize once. The returned bytes are immutable and shared by every
	// subscriber below; nothing re-encodes per connection.
	frame := protocol.AppendBatch(f.encodeScratch[:0], &batch)
	f.encodeScratch = frame

	queued := 0
	for i := 0; i < len(subs); i++ {
		conn := subs[i].Conn()
		if !conn.Accepting() {
			continue
		}
		accepted, _ := conn.Enqueue(frame, nowMs)
		if !accepted {
			f.enqueueDropped.Add(1)
			continue
		}
		queued++
		f.bytesOut.Add(uint64(len(frame)))
	}

	f.packetsOut.Add(1)
	// Delivered is message times recipient, not messages. The fanout factor is
	// what an operator compares against the expected channel size, and counting
	// messages here would report 1 for a channel of any size and make the metric
	// useless for detecting a delivery problem.
	f.messagesOut.Add(uint64(len(batch.Messages)) * uint64(queued))
	return queued, nil
}

// FlushPass drains every dirty shard and writes what it finds.
//
// It returns the number of connections that produced bytes. The write itself is
// supplied by the caller because the transport is not the engine's concern; the
// engine decides *which* connections to visit, which is the part that has to
// scale with traffic rather than population.
func (f *Fanout) FlushPass(write func(*Conn, []byte)) int {
	f.deliverMu.Lock()
	defer f.deliverMu.Unlock()

	dirty := f.shards.FlushAll(f.dirtyScratch[:0])
	f.dirtyScratch = dirty

	delivered := 0
	for i := 0; i < len(dirty); i++ {
		conn := dirty[i]

		// A gap notice goes out before the next content, so a client sees the
		// discontinuity before whatever follows it rather than after.
		if dropped, ok := conn.Buffer().ConsumeGap(); ok {
			notice := protocol.AppendGapNotice(f.noticeScratch[:0], protocol.GapNotice{
				Dropped: dropped,
				Reason:  protocol.GapReasonSlowConsumer,
				TimeMs:  uint64(f.now().UnixMilli()),
			})
			f.noticeScratch = notice
			write(conn, notice)
		}

		out := conn.Drain(f.writeScratch[:0])
		f.writeScratch = out
		if len(out) == 0 {
			continue
		}
		conn.Egress().Observe(len(out), f.now())
		write(conn, out)
		delivered++
	}
	return delivered
}

// Run consumes the bus until ctx is cancelled, dispatching every message.
//
// Dispatch runs inline on the receive path rather than through a work queue.
// The work per message is a map lookup and, for served channels, one encode plus
// a memcpy per local subscriber; adding a queue would add a hop and a goroutine
// for no isolation, because the expensive work is already bounded by the number
// of local subscribers.
func (f *Fanout) Run(ctx context.Context, b bus.Bus) error {
	return b.Subscribe(ctx, func(env bus.Envelope) {
		if _, err := f.Dispatch(env); err != nil {
			// A dispatch failure must not kill the receive loop: losing one
			// message is within the delivery contract, losing the subscription
			// is not.
			return
		}
	})
}

// RunFlush drives the flush pass on an interval until ctx is cancelled.
func (f *Fanout) RunFlush(ctx context.Context, interval time.Duration, write func(*Conn, []byte)) {
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.FlushPass(write)
			if f.coalesce != nil {
				f.coalesce.FlushDue(f)
			}
		}
	}
}

// Stats is a snapshot of the delivery counters.
type Stats struct {
	MessagesIn     uint64
	MessagesOut    uint64
	PacketsOut     uint64
	BytesOut       uint64
	ChannelMisses  uint64
	ExpiredDropped uint64
	EnqueueDropped uint64
}

// FanoutFactor returns delivered messages per admitted message.
//
// A factor below 1 means messages are being dropped rather than delivered, which
// is the signal an operator needs: the service is admitting more than it can
// fan out.
func (s Stats) FanoutFactor() float64 {
	if s.MessagesIn == 0 {
		return 0
	}
	return float64(s.MessagesOut) / float64(s.MessagesIn)
}

// Snapshot returns the current counters.
func (f *Fanout) Snapshot() Stats {
	return Stats{
		MessagesIn:     f.messagesIn.Load(),
		MessagesOut:    f.messagesOut.Load(),
		PacketsOut:     f.packetsOut.Load(),
		BytesOut:       f.bytesOut.Load(),
		ChannelMisses:  f.channelMisses.Load(),
		ExpiredDropped: f.expiredDropped.Load(),
		EnqueueDropped: f.enqueueDropped.Load(),
	}
}

// ParseFrames reads a sequence of encoded frames from buf, returning the
// message batches and how many bytes they occupy.
//
// It exists for verification: a test that walks what a connection actually
// received proves the delivery path end to end, which is a different claim from
// counting what the engine queued.
func ParseFrames(buf []byte) ([]protocol.Message, int, bool) {
	hdr, err := protocol.ParseHeader(buf)
	if err != nil || hdr.Type != protocol.FrameTypeMessageBatch {
		return nil, 0, false
	}
	batch, err := protocol.ParseBatch(buf)
	if err != nil {
		return nil, 0, false
	}
	return batch.Messages, len(protocol.AppendBatch(nil, batch)), true
}
