package gateway

import (
	"sync"

	"github.com/starflow/push/internal/protocol"
)

// Coalescer merges messages for a channel that arrive within a window into one
// delivered packet.
//
// It is a latency dial, not a bandwidth dial. Merging twenty messages into one
// packet cuts packet count twentyfold and therefore cuts syscall and CPU cost
// twentyfold, while total delivered bytes stay exactly the same. Anything that
// claims otherwise would mean bandwidth stopped being the binding constraint,
// which is the premise the entire capacity model rests on.
//
// The consequence that matters operationally: past roughly twenty to one, more
// merging stops being free. Each viewer is already receiving about as much
// content as their bandwidth budget allows, so a higher ratio starts forcing
// content loss rather than just adding latency.
type Coalescer struct {
	mu sync.Mutex

	// window is the coalescing interval.
	window uint64 // milliseconds
	// mergeRatio is the target messages per delivered packet.
	mergeRatio int
	// maxBatchBytes closes a batch early so a single packet cannot grow past
	// what one connection's buffer holds.
	maxBatchBytes int

	// pending accumulates messages for the open window.
	pending []protocol.Message
	// pendingBytes tracks the batch size.
	pendingBytes int
	// windowStartMs is when the open window began, or zero when no window is
	// open.
	windowStartMs uint64
	// channel is the channel these messages belong to.
	channel string
	// tenant scopes the channel.
	tenant string

	// scratch is reused for encoding, so the coalescer allocates nothing on the
	// steady-state path.
	scratch []byte

	// stats
	packetsOut  uint64
	messagesOut uint64
	bytesOut    uint64
}

// NewCoalescer returns a coalescer for one channel.
//
// The coalescer is per channel rather than global because a window must not let
// a busy channel delay a quiet one: closing on per-channel timing is what keeps
// latency bounded per channel rather than per instance.
func NewCoalescer(tenant, channel string, windowMs uint64, mergeRatio, maxBatchBytes int) *Coalescer {
	if mergeRatio < 1 {
		mergeRatio = 1
	}
	if maxBatchBytes < 1024 {
		// A batch below one kilobyte would pack almost nothing, making the
		// batching overhead a net loss.
		maxBatchBytes = 1024
	}
	return &Coalescer{
		tenant:        tenant,
		channel:       channel,
		window:        windowMs,
		mergeRatio:    mergeRatio,
		maxBatchBytes: maxBatchBytes,
	}
}

// Add queues a message for the current window, opening the window if none is
// open.
//
// nowMs is the arrival time, deliberately not the message's own timestamp. The
// window bounds delivery latency, so it has to run on when the gateway saw the
// message; keying it on a publisher-supplied or unpopulated timestamp would
// leave a batch that never comes due.
func (c *Coalescer) Add(m protocol.Message, nowMs uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.windowStartMs == 0 || nowMs < c.windowStartMs {
		c.windowStartMs = nowMs
	}
	c.pending = append(c.pending, m)
	c.pendingBytes += len(m.Body)
}

// Due reports whether the open window should close now, either because it has
// run out or because it has reached the merge target.
//
// Count alone is not enough: a batch must also be bounded in bytes so one
// connection's buffer cannot be overrun by a burst that arrives faster than the
// window length.
func (c *Coalescer) Due(nowMs uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dueLocked(nowMs)
}

func (c *Coalescer) dueLocked(nowMs uint64) bool {
	if len(c.pending) == 0 {
		return false
	}
	if len(c.pending) >= c.mergeRatio || c.pendingBytes >= c.maxBatchBytes {
		return true
	}
	if c.windowStartMs != 0 && nowMs >= c.windowStartMs+c.window {
		return true
	}
	return false
}

// Take closes the window and returns the encoded batch, or nil when nothing is
// pending.
//
// The returned bytes are an immutable frame shared by every subscriber of the
// channel. It is valid until the next call to Take, so a caller must enqueue it
// into every subscriber's buffer before calling again.
func (c *Coalescer) Take() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pending) == 0 {
		c.windowStartMs = 0
		c.pendingBytes = 0
		return nil
	}

	batch := protocol.Batch{Channel: c.channel, Messages: c.pending}

	// Encode into the reusable scratch buffer. The frame is handed to every
	// subscriber, so it must be fully written before the next Take overwrites
	// it, which is why callers enqueue before calling Take again.
	c.scratch = protocol.AppendBatch(c.scratch[:0], &batch)
	out := make([]byte, len(c.scratch))
	copy(out, c.scratch)

	c.packetsOut++
	c.messagesOut += uint64(len(c.pending))
	c.bytesOut += uint64(c.pendingBytes)

	c.pending = c.pending[:0]
	c.pendingBytes = 0
	c.windowStartMs = 0
	return out
}

// Pending reports how many messages are waiting.
func (c *Coalescer) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Stats reports packets, messages and content bytes emitted.
//
// Messages and content bytes are the pair that demonstrates the central claim:
// the coalescing ratio is packets-to-messages while content bytes are unchanged
// by merging.
func (c *Coalescer) Stats() (packets, messages, contentBytes uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.packetsOut, c.messagesOut, c.bytesOut
}

// SetMergeRatio changes the merge target, taking effect on the next window.
func (c *Coalescer) SetMergeRatio(ratio int) {
	if ratio < 1 {
		ratio = 1
	}
	c.mu.Lock()
	c.mergeRatio = ratio
	c.mu.Unlock()
}

// SetWindow changes the coalescing interval.
func (c *Coalescer) SetWindow(windowMs uint64) {
	c.mu.Lock()
	c.window = windowMs
	c.mu.Unlock()
}

// TenantID returns the tenant this coalescer belongs to.
func (c *Coalescer) TenantID() string { return c.tenant }

// ChannelName returns the channel this coalescer belongs to.
func (c *Coalescer) ChannelName() string { return c.channel }

// AddMsg queues a message using its own timestamp as the arrival time. It exists
// for tests and for callers that already hold a timestamp they consider
// authoritative; the delivery path uses Add with an explicit arrival time.
func (c *Coalescer) AddMsg(m protocol.Message) {
	c.Add(m, m.TimeMs)
}
