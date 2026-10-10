package gateway

import "sync"

// ShardSet is the dirty-list registry that decouples flush cost from connection
// count.
//
// The alternative is the obvious one: on every tick, walk every connection and
// check whether it has anything to write. At 100,000 connections and 100 ticks
// per second that is ten million pointless visits per second, almost all of them
// cache misses on a buffer that is empty. The cost tracks connection count, which
// is exactly the quantity the design cannot shrink.
//
// Instead a connection registers itself on the shard that owns it the first time
// it queues something, and deregisters implicitly by being absent from the next
// pass. Flush then touches only connections with pending bytes, so cost tracks
// delivered traffic.
//
// Sharding by connection id keeps two connections on different shards, so the
// flush goroutines do not contend on one lock while writing to unrelated
// sockets.
type ShardSet struct {
	shards []*shard
	mask   uint64
}

// shard is one dirty list.
type shard struct {
	mu sync.Mutex
	// queue holds connections with pending bytes. A connection appears at most
	// once per pass: it registers only on the transition from clean to dirty, so
	// a connection receiving fifty messages in a window still costs one entry.
	queue []*Conn
}

// NewShardSet returns a registry with n shards, rounded up to a power of two so
// the shard index is a mask rather than a modulo.
func NewShardSet(n int) *ShardSet {
	size := 1
	for size < n {
		size <<= 1
	}
	if n < 1 {
		size = 1
	}
	set := &ShardSet{shards: make([]*shard, size), mask: uint64(size - 1)}
	for i := range set.shards {
		set.shards[i] = &shard{}
	}
	return set
}

// Shard returns the shard owning a connection id.
func (s *ShardSet) Shard(connID uint64) *shard {
	return s.shards[connID&s.mask]
}

// ShardCount reports how many shards exist.
func (s *ShardSet) ShardCount() int { return len(s.shards) }

// add registers a connection for the current pass.
//
// Registration is unconditional on the shard side; the caller has already
// performed the clean-to-dirty transition, which is what keeps a connection out
// of the list when it receives fifty messages in one window.
func (sh *shard) add(conn *Conn) {
	sh.mu.Lock()
	sh.queue = append(sh.queue, conn)
	sh.mu.Unlock()
}

// Flush drains the shard's dirty list.
//
// The returned slice is owned by the caller and reused across passes, which is
// what keeps the flush loop allocation-free. An empty shard must return dst
// unchanged rather than truncated: FlushAll walks shards in order, so returning
// a zero-length slice here would discard everything accumulated so far and
// silently skip most connections on every pass.
func (sh *shard) Flush(dst []*Conn) []*Conn {
	sh.mu.Lock()
	if len(sh.queue) == 0 {
		sh.mu.Unlock()
		return dst
	}
	dst = append(dst, sh.queue...)
	sh.queue = sh.queue[:0]
	sh.mu.Unlock()
	return dst
}

// FlushAll drains every shard, returning the connections with pending bytes
// across the whole set.
func (s *ShardSet) FlushAll(dst []*Conn) []*Conn {
	for _, sh := range s.shards {
		dst = sh.Flush(dst)
	}
	return dst
}

// Pending reports how many connections are registered across every shard.
func (s *ShardSet) Pending() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.queue)
		sh.mu.Unlock()
	}
	return n
}
