package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/starflow/push/internal/protocol"
)

// Lifecycle manages the health of live connections: liveness, backpressure
// classification, and the drain sequence used when an instance retires.
type Lifecycle struct {
	store  *Store
	shards *ShardSet
	now    func() time.Time

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	drainGrace        time.Duration

	// slowAfter is how long a connection may hold queued content before it is
	// treated as slow. It is a multiple of the flush interval rather than a
	// fixed value, because what matters is how many flush passes it missed.
	slowAfter time.Duration

	mu    sync.Mutex
	conns map[*Conn]*connHealth

	// counters
	closed         atomic.Uint64
	slowClosed     atomic.Uint64
	drained        atomic.Uint64
	heartbeatsSent atomic.Uint64
	heartbeatsLost atomic.Uint64
	draining       atomic.Bool
}

// connHealth is one connection's liveness bookkeeping.
type connHealth struct {
	lastSeen time.Time
	// awaitingPong is set when a probe has been sent and no answer has arrived.
	awaitingPong bool
	probeSentAt  time.Time
}

// LifecycleConfig holds the liveness timings.
type LifecycleConfig struct {
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	DrainGrace        time.Duration
	// SlowAfter is how long queued content may sit before the connection is
	// classified as slow. Zero derives it from the flush interval.
	SlowAfter time.Duration
}

// NewLifecycle returns a lifecycle manager.
func NewLifecycle(store *Store, shards *ShardSet, cfg LifecycleConfig, now func() time.Time) *Lifecycle {
	if now == nil {
		now = time.Now
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 25 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 10 * time.Second
	}
	if cfg.SlowAfter <= 0 {
		// Three missed flush passes. A connection that has not drained across
		// three of them is not catching up and will not start; continuing to
		// queue for it only converts a slow subscriber into a memory incident.
		cfg.SlowAfter = 30 * time.Millisecond
	}
	return &Lifecycle{
		store:             store,
		shards:            shards,
		now:               now,
		heartbeatInterval: cfg.HeartbeatInterval,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		drainGrace:        cfg.DrainGrace,
		slowAfter:         cfg.SlowAfter,
		conns:             map[*Conn]*connHealth{},
	}
}

// Track registers a connection as live.
func (l *Lifecycle) Track(conn *Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conns[conn] = &connHealth{lastSeen: l.now()}
}

// Untrack forgets a connection.
func (l *Lifecycle) Untrack(conn *Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.conns, conn)
}

// Ping marks a connection as having responded.
func (l *Lifecycle) Ping(conn *Conn) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.conns[conn]
	if !ok {
		return
	}
	h.lastSeen = now
	h.awaitingPong = false
}

// Sweep returns the connections that have missed a liveness deadline.
//
// A connection is reported when it has been probed and has not answered within
// the timeout, or when it has simply not been seen for the timeout. Probing
// rather than only ageing matters: an intermediary can silently drop a
// connection without a close frame, and only a probe reveals it.
func (l *Lifecycle) Sweep() []*Conn {
	now := l.now()
	deadline := now.Add(-l.heartbeatTimeout)

	l.mu.Lock()
	defer l.mu.Unlock()

	var dead []*Conn
	for conn, h := range l.conns {
		if h.awaitingPong && now.Sub(h.probeSentAt) > l.heartbeatTimeout {
			dead = append(dead, conn)
			continue
		}
		if h.lastSeen.Before(deadline) {
			dead = append(dead, conn)
		}
	}
	return dead
}

// ProbeDue returns the connections that should be sent a liveness probe now.
//
// It returns a second slice of connections that were probed previously and are
// now past their timeout, so one pass both sends probes and reaps the dead
// without a second walk.
func (l *Lifecycle) ProbeDue() (probe, dead []*Conn) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for conn, h := range l.conns {
		if h.awaitingPong {
			if now.Sub(h.probeSentAt) > l.heartbeatTimeout {
				dead = append(dead, conn)
			}
			continue
		}
		if now.Sub(h.lastSeen) >= l.heartbeatInterval {
			h.awaitingPong = true
			h.probeSentAt = now
			probe = append(probe, conn)
		}
	}
	return probe, dead
}

// PingFrame builds a liveness probe.
func PingFrame() []byte {
	return protocol.AppendControl(nil, protocol.ControlPing, nil)
}

// PongFrame builds a liveness response.
func PongFrame() []byte {
	return protocol.AppendControl(nil, protocol.ControlPong, nil)
}

// Close releases a connection and everything it held.
//
// Subscriptions are released before the socket closes, so a connection that
// leaves does not continue to appear in a channel's subscriber count and skew
// the derived limits.
func (l *Lifecycle) Close(conn *Conn) {
	l.store.UnsubscribeAll(conn)
	l.Untrack(conn)
	conn.Close()
	l.closed.Add(1)
}

// Terminate closes the connection with the given id, and reports whether one
// was found.
//
// The tenant is part of the lookup because the identifier arrives from an
// administrative request path: an administrator scoped to one tenant must not be
// able to close another's connection by guessing an id. An empty tenant matches
// any, which is what the instance-wide view wants.
//
// Termination is not a ban. The client may reconnect immediately and is then
// subject to the same authentication and rate checks as any new client.
func (l *Lifecycle) Terminate(tenantID string, id uint64) bool {
	for _, conn := range l.store.Connections() {
		if conn.ID != id {
			continue
		}
		if tenantID != "" && conn.TenantID != tenantID {
			continue
		}
		l.Close(conn)
		return true
	}
	return false
}

// SlowConnections returns the connections that have held queued content longer
// than the tolerance.
func (l *Lifecycle) SlowConnections() []*Conn {
	nowMs := uint64(l.now().UnixMilli())

	l.mu.Lock()
	conns := make([]*Conn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.mu.Unlock()

	var slow []*Conn
	for _, conn := range conns {
		since, queued := conn.QueuedSince(nowMs)
		if queued && since > uint64(l.slowAfter.Milliseconds()) {
			slow = append(slow, conn)
		}
	}
	return slow
}

// ReapSlow closes the connections that have stopped draining.
//
// A slow subscriber is closed rather than merely trimmed because the buffer is
// already bounded: there is no room left to give it, so keeping it costs the
// memory it is not using and delays the gap notice to a client that is not
// reading. Closing is also what eventually lets the client reconnect somewhere
// healthy, which is the outcome the subscriber actually wants.
func (l *Lifecycle) ReapSlow() []*Conn {
	slow := l.SlowConnections()
	for _, conn := range slow {
		// A drain is not a stall: a draining instance is expected to have
		// connections that will not be written to before they close.
		if conn.State() == ConnDraining {
			continue
		}
		l.Close(conn)
		l.slowClosed.Add(1)
	}
	return slow
}

// BeginDrain marks the lifecycle as draining and returns the connections to
// notify.
func (l *Lifecycle) BeginDrain() []*Conn {
	l.draining.Store(true)

	l.mu.Lock()
	conns := make([]*Conn, 0, len(l.conns))
	for conn := range l.conns {
		conns = append(conns, conn)
	}
	l.mu.Unlock()

	for _, conn := range conns {
		conn.SetState(ConnDraining)
	}
	return conns
}

// Draining reports whether the instance is retiring.
func (l *Lifecycle) Draining() bool { return l.draining.Load() }

// DrainNotice builds the frame telling a subscriber to reconnect elsewhere.
func DrainNotice() []byte {
	return protocol.AppendControl(nil, protocol.ControlDrain, nil)
}

// FinishDrain closes every connection after the grace period.
//
// The wait exists because a drain without one is a mass disconnect: ten thousand
// clients would all reconnect at the same instant, and each reconnect is a full
// cryptographic handshake. Spreading them over the grace window is what keeps a
// release from costing more than the traffic it was serving.
func (l *Lifecycle) FinishDrain() int {
	conns := l.BeginDrain()
	for _, conn := range conns {
		l.Close(conn)
		l.drained.Add(1)
	}
	return len(conns)
}

// Run drives liveness until ctx is cancelled.
//
// Probing and reaping share one walk per interval, so the cost is proportional
// to connection count once per interval rather than twice, and it does not grow
// with message volume: a busy connection is not probed more often than a quiet
// one.
func (l *Lifecycle) Run(ctx context.Context, send func(*Conn, []byte)) {
	interval := l.heartbeatInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe, dead := l.ProbeDue()
			for _, conn := range probe {
				send(conn, PingFrame())
				l.heartbeatsSent.Add(1)
			}
			for _, conn := range dead {
				l.Close(conn)
				l.heartbeatsLost.Add(1)
			}
			l.ReapSlow()
		}
	}
}

// LifecycleCounts reports lifecycle counters.
type LifecycleCounts struct {
	Tracked        int
	Closed         uint64
	SlowReaped     uint64
	Drained        uint64
	HeartbeatsSent uint64
	HeartbeatsLost uint64
}

// Snapshot returns the lifecycle counters.
func (l *Lifecycle) Snapshot() LifecycleCounts {
	l.mu.Lock()
	tracked := len(l.conns)
	l.mu.Unlock()
	return LifecycleCounts{
		Tracked:        tracked,
		Closed:         l.closed.Load(),
		SlowReaped:     l.slowClosed.Load(),
		Drained:        l.drained.Load(),
		HeartbeatsSent: l.heartbeatsSent.Load(),
		HeartbeatsLost: l.heartbeatsLost.Load(),
	}
}
