package gateway

import (
	"sync"
	"sync/atomic"
	"time"
)

// ConnBuffer is a fixed-capacity byte queue holding whole encoded frames.
//
// Two properties matter more than throughput here.
//
// The capacity is fixed at allocation and is never grown. Growing on demand
// means one slow subscriber can drive an instance into an allocation storm, which
// is exactly the failure the per-connection bound exists to prevent.
//
// Overflow discards the oldest whole frames, never a byte range. Frame
// boundaries must survive intact or the reader is left parsing a truncated
// frame, so the buffer records each frame's length and only ever drops from a
// boundary.
type ConnBuffer struct {
	mu sync.Mutex
	// data is the fixed backing array.
	data []byte
	// frames records each live frame's length in insertion order.
	frames []int32
	// start is the byte offset of the first live byte in data.
	start int
	// used is the live byte count.
	used int
	// head is the index in frames of the oldest live frame.
	head int
	// count is the number of live frames.
	count int
	// dropped counts frames discarded to make room. It is what a gap notice
	// reports to the subscriber.
	dropped uint64
	// gapPending records that a drop has not yet been reported. The notice is
	// emitted before the next content frame, so a client sees the
	// discontinuity before the content that follows it.
	gapPending bool
}

// NewConnBuffer returns a buffer of exactly capacity bytes.
func NewConnBuffer(capacity int) *ConnBuffer {
	return &ConnBuffer{
		data:   make([]byte, capacity),
		frames: make([]int32, 0, 64),
	}
}

// Append queues a frame, discarding oldest frames when the buffer is full.
//
// It reports whether the frame was queued and how many frames had to be
// discarded. A frame larger than the entire buffer can never be queued; it is
// counted as dropped rather than forcing every other frame out to make room for
// something that will not fit anyway.
func (b *ConnBuffer) Append(frame []byte) (queued bool, discarded int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(frame)
	capacity := len(b.data)
	if n > capacity {
		b.dropped++
		b.gapPending = true
		return false, 0
	}

	// Evict until the live bytes alone leave room, then slide the live bytes to the
	// front so the write position fits.
	//
	// The two steps are separate because they answer different questions.
	// Dropping measures live bytes (used), since discarding the oldest frame
	// frees space that has to be reclaimed from the front. Sliding measures the
	// write position (start+used), since that is where the new frame lands.
	// start and used move together when a frame is dropped, so start+used is
	// invariant under eviction: testing eviction against it would find no room
	// ever and would discard every frame instead of the oldest one.
	for b.used+n > capacity && b.count > 0 {
		b.dropOldestLocked()
		discarded++
	}
	if b.start+b.used+n > capacity {
		b.compactLocked()
	}
	if b.start+b.used+n > capacity {
		// Unreachable: n > capacity was excluded above, and once count reached
		// zero, compaction puts used at zero so n alone fits.
		b.dropped++
		b.gapPending = true
		return false, discarded
	}

	copy(b.data[b.start+b.used:], frame)
	b.used += n
	b.frames = append(b.frames, int32(n))
	b.count++
	return true, discarded
}

func (b *ConnBuffer) dropOldestLocked() {
	if b.count == 0 {
		return
	}
	n := int(b.frames[b.head])
	b.start += n
	b.used -= n
	b.head++
	b.count--
	b.dropped++
	b.gapPending = true
}

// compactLocked slides the live bytes and their frame lengths to the front.
func (b *ConnBuffer) compactLocked() {
	if b.start == 0 {
		return
	}
	copy(b.data, b.data[b.start:b.start+b.used])
	b.start = 0

	// The live frames are the contiguous run frames[head:head+count]; drop the
	// consumed prefix so head can go back to zero.
	n := b.count
	copy(b.frames, b.frames[b.head:b.head+n])
	b.frames = b.frames[:n]
	b.head = 0
}

// Take appends the live bytes to dst and resets the buffer.
//
// dst is supplied by the caller and reused across flushes, so this copies out
// of the ring rather than returning an interior slice that would pin the whole
// buffer for the connection's lifetime.
func (b *ConnBuffer) Take(dst []byte) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.used > 0 {
		dst = append(dst, b.data[b.start:b.start+b.used]...)
	}
	b.start = 0
	b.used = 0
	b.head = 0
	b.count = 0
	b.frames = b.frames[:0]
	return dst
}

// Pending reports how many bytes are queued.
func (b *ConnBuffer) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Frames reports how many whole frames are queued.
func (b *ConnBuffer) Frames() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// Capacity reports the fixed buffer size.
func (b *ConnBuffer) Capacity() int { return len(b.data) }

// Dropped reports how many frames have been discarded for this connection.
func (b *ConnBuffer) Dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// ConsumeGap returns whether drops are unreported, and how many, clearing the
// pending flag. The caller emits a gap notice carrying the count.
func (b *ConnBuffer) ConsumeGap() (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.gapPending || b.dropped == 0 {
		return 0, false
	}
	n := b.dropped
	b.dropped = 0
	b.gapPending = false
	return n, true
}

// ConnState is a connection's lifecycle state.
type ConnState int32

const (
	ConnOpen ConnState = iota
	ConnDraining
	ConnClosed
)

// Conn is one subscriber connection.
//
// Fields touched on the delivery path are either immutable after the handshake
// or guarded by the connection's own lock. No shared lock is taken during a
// fanout, so one slow subscriber's bookkeeping cannot serialise delivery for an
// entire channel.
type Conn struct {
	// ID identifies the connection to the administrative surface.
	ID uint64
	// TenantID and Subject scope this connection's subscriptions.
	TenantID string
	Subject  string

	buf     *ConnBuffer
	readBuf []byte

	state atomic.Int32

	// dirty marks the connection as holding queued bytes, so a flush pass can
	// skip it without touching the buffer at all. This is what makes flush
	// cost proportional to delivered traffic rather than connection count.
	dirty atomic.Bool
	// queuedAtMs is when the connection first queued content, used to identify
	// a subscriber that has not drained within a tolerance.
	queuedAtMs atomic.Uint64

	eg *EgressMeter

	// shards is the dirty-list registry this connection registers with.
	shards *ShardSet

	closeFn   func()
	closeOnce sync.Once
}

// NewConn returns a connection with buffers sized from the delivery config.
func NewConn(id uint64, tenantID, subject string, writeBuf, readBuf, perViewerBps int, shards *ShardSet) *Conn {
	return &Conn{
		ID:       id,
		TenantID: tenantID,
		Subject:  subject,
		buf:      NewConnBuffer(writeBuf),
		readBuf:  make([]byte, readBuf),
		eg:       NewEgressMeter(perViewerBps),
		shards:   shards,
	}
}

// SetCloseFunc installs the transport close. It is separate from construction so
// a connection can be created before its socket exists.
func (c *Conn) SetCloseFunc(fn func()) { c.closeFn = fn }

// Buffer returns the connection's outbound buffer.
func (c *Conn) Buffer() *ConnBuffer { return c.buf }

// Egress returns the connection's egress meter.
func (c *Conn) Egress() *EgressMeter { return c.eg }

// State returns the connection's lifecycle state.
func (c *Conn) State() ConnState { return ConnState(c.state.Load()) }

// SetState moves the connection to a new lifecycle state.
func (c *Conn) SetState(s ConnState) { c.state.Store(int32(s)) }

// Accepting reports whether the connection may take new subscriptions.
func (c *Conn) Accepting() bool { return c.State() == ConnOpen }

// Close performs the transport close exactly once, whichever of drain or
// heartbeat timeout gets there first.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		c.state.Store(int32(ConnClosed))
		if c.closeFn != nil {
			c.closeFn()
		}
	})
}

// Pending reports how many bytes are queued for this connection.
func (c *Conn) Pending() int { return c.buf.Pending() }

// QueuedSince reports how long the connection has held queued content, and
// whether it holds any at all.
func (c *Conn) QueuedSince(nowMs uint64) (uint64, bool) {
	if !c.dirty.Load() {
		return 0, false
	}
	at := c.queuedAtMs.Load()
	if nowMs <= at {
		return 0, true
	}
	return nowMs - at, true
}

// Enqueue queues an already-encoded frame and marks the connection dirty.
//
// The frame is a shared immutable slice produced by the coalescer. Copying it
// into this connection's buffer here is the single memcpy per subscriber that
// the fanout model is built on.
func (c *Conn) Enqueue(frame []byte, nowMs uint64) (queued bool, discarded int) {
	queued, discarded = c.buf.Append(frame)
	if queued {
		c.registerDirty(nowMs)
	}
	return queued, discarded
}

// registerDirty marks the connection for its shard's flush list and records when
// it first queued content.
//
// The transition is guarded by an atomic swap so a connection registering fifty
// times in one window occupies one slot.
func (c *Conn) registerDirty(nowMs uint64) {
	if !c.dirty.Swap(true) {
		c.queuedAtMs.Store(nowMs)
		if c.shards != nil {
			c.shards.Shard(c.ID).add(c)
		}
	}
}

// Drain copies out every queued frame and clears the dirty flag.
func (c *Conn) Drain(dst []byte) []byte {
	out := c.buf.Take(dst)
	c.dirty.Store(false)
	c.queuedAtMs.Store(0)
	return out
}

// EgressMeter accounts a connection's outbound bytes against a per-viewer rate
// budget.
//
// The budget is the number the whole capacity model is derived from: instance
// count follows from connections x budget divided by NIC capacity. This meter
// is what makes that budget enforceable per connection rather than arithmetic
// nobody checks.
//
// It samples rather than measuring every byte. An exact per-connection
// accumulator is written on the delivery path, once per flush; a windowed meter
// reads one mutex at flush time and extrapolates, which is accurate to within a
// sampling period and costs nothing measurable.
type EgressMeter struct {
	budgetPerSec int

	mu          sync.Mutex
	windowStart time.Time
	windowBytes int64
	ratePerSec  int
}

// NewEgressMeter returns a meter enforcing budgetPerSec, where zero means no
// budget is imposed.
func NewEgressMeter(budgetPerSec int) *EgressMeter {
	return &EgressMeter{budgetPerSec: budgetPerSec}
}

// Observe adds n bytes and returns the connection's current measured rate,
// recomputing it if the sampling window has elapsed.
func (e *EgressMeter) Observe(n int, now time.Time) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.windowStart.IsZero() {
		e.windowStart = now
		e.windowBytes = int64(n)
		return e.ratePerSec
	}
	e.windowBytes += int64(n)

	elapsed := now.Sub(e.windowStart)
	if elapsed < minSampleWindow {
		// Too short to extrapolate from; accumulate and wait.
		return e.ratePerSec
	}
	e.ratePerSec = int(float64(e.windowBytes) / elapsed.Seconds())
	e.windowBytes = 0
	e.windowStart = now
	return e.ratePerSec
}

// minSampleWindow is the shortest interval over which a rate is extrapolated.
// Shorter windows amplify scheduling jitter into a noisy reading.
const minSampleWindow = 100 * time.Millisecond

// Rate returns the most recent measured rate.
func (e *EgressMeter) Rate() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ratePerSec
}

// Budget returns the configured per-viewer budget, where zero means unlimited.
func (e *EgressMeter) Budget() int { return e.budgetPerSec }

// OverBudget reports whether rate exceeds the budget by more than tolerance.
//
// The tolerance exists because the reading is extrapolated from a short window.
// Treating the first byte over budget as a breach would trip on sampling noise
// during a burst that the coalescing window is about to absorb anyway.
func (e *EgressMeter) OverBudget(rate int, tolerance float64) bool {
	if e.budgetPerSec <= 0 {
		return false
	}
	return float64(rate) > float64(e.budgetPerSec)*(1+tolerance)
}
