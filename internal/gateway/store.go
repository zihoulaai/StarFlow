// Package gateway holds the data plane: live subscriber connections, the
// subscription store, and the fanout loop that turns one admitted message into
// many delivered packets.
//
// The performance model the whole design rests on, repeated here because every
// decision below follows from it:
//
//	Serialize a message once into an immutable buffer, then memcpy that buffer
//	into each subscriber's queue.
//
// Two consequences shape this package. Byte-for-byte identical delivery means a
// frame must never vary per recipient, which is why the protocol carries no
// per-subscriber field. And because the work is a memcpy fanout rather than a
// per-connection write, the store holds subscribers as a contiguous slice: a
// hash map costs a cache miss per entry and fanout iterates every entry.
package gateway

import (
	"sync"
	"sync/atomic"
)

// Subscriber is a connection's interest in a channel.
//
// It is deliberately not an interface. The fanout loop touches one of these per
// local subscriber per delivered batch, and an interface value would add a
// pointer chase to the hottest loop in the service for no benefit: there is
// exactly one implementation.
type Subscriber struct {
	conn *Conn
	// channel is retained so a queue drained after its subscription was
	// withdrawn can still be attributed for metrics.
	channel string
}

// Conn returns the connection this subscriber belongs to.
func (s *Subscriber) Conn() *Conn { return s.conn }

// Channel returns the channel name.
func (s *Subscriber) Channel() string { return s.channel }

// channelState is one channel's local subscription set.
//
// The set is a slice rather than a map because fanout is iteration over the
// whole set. A map lookup costs a cache miss per member; a contiguous slice
// walks memory the CPU has already prefetched. Subscription churn is O(n) to
// maintain, which is affordable because danmaku subscriptions are established
// once at connect and rarely change afterwards.
type channelState struct {
	// subs is a dense slice with no nil holes. Removal swaps in the last
	// element and shrinks, which is O(1) and keeps the slice dense; the
	// resulting order change is irrelevant because delivery order within a
	// flush window is established by the batch, not by subscriber order.
	subs []*Subscriber
	// index maps a (connection, channel) pair to its position in subs, so
	// removal does not scan. It is keyed by value rather than by *Subscriber
	// because lookups are by pair: a caller unsubscribing has a Conn and a
	// channel name, not the pointer that Subscribe happened to allocate.
	//
	// This map is touched only on subscribe and unsubscribe, never on the
	// delivery path, so its cost does not affect fanout.
	index map[subKey]int

	// subscriberCount feeds the derived publish ceiling.
	subscriberCount atomic.Int64
}

// Subscribers returns the channel's local subscribers.
//
// The returned slice is the live backing array. Callers on the delivery path
// read it while holding the store read lock and must not retain it past the
// fanout that received it.
func (c *channelState) Subscribers() []*Subscriber { return c.subs }

// Store holds every channel's local subscription set.
//
// It holds no state that must survive a restart and no state shared with another
// instance. Every instance holds whatever subset of the platform's subscribers
// its own load balancer routed to it, and that is the whole contract.
type Store struct {
	mu sync.RWMutex
	// channels is keyed by tenant and channel, because two tenants may use the
	// same channel name without being able to observe each other's messages.
	channels map[channelKey]*channelState
}

// channelKey scopes a channel to its tenant.
type channelKey struct {
	tenant string
	name   string
}

// subKey identifies one connection's subscription to one channel.
type subKey struct {
	conn    *Conn
	channel string
}

// NewStore returns an empty subscription store.
func NewStore() *Store {
	return &Store{channels: map[channelKey]*channelState{}}
}

// ChannelCount reports how many distinct channels this instance is serving.
func (s *Store) ChannelCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels)
}

// SubscriberCount reports the total number of local subscriptions across all
// channels.
func (s *Store) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, c := range s.channels {
		n += len(c.subs)
	}
	return n
}

// Connections returns every distinct connection with at least one local
// subscription.
//
// The admin surface needs this to list live connections. It walks the channels
// and de-duplicates, rather than keeping a separate connection registry, because
// a second registry would be a second thing to keep correct and the connection
// set is already exactly derivable from the subscriptions.
//
// A connection with no subscriptions does not appear. That is not a gap: an
// unauthenticated or empty connection is closed rather than left idle, so there
// is no window in which a live connection is absent from this list.
func (s *Store) Connections() []*Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[*Conn]struct{}, len(s.channels))
	out := make([]*Conn, 0, len(s.channels))
	for _, c := range s.channels {
		for _, sub := range c.subs {
			conn := sub.conn
			if _, ok := seen[conn]; ok {
				continue
			}
			seen[conn] = struct{}{}
			out = append(out, conn)
		}
	}
	return out
}

// lookup resolves a channel without creating it. Used on the delivery path,
// where a message for an unserved channel must be discarded rather than
// allocating state for it.
func (s *Store) lookup(tenant, channel string) (*channelState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[channelKey{tenant, channel}]
	return c, ok
}

// LookupSubscribers returns a channel's local subscribers.
//
// The delivery path calls this once per admitted message. A channel this
// instance does not serve returns ok false, which is the common case: under
// broadcast every instance receives every message but serves only a slice of
// the subscriber population.
func (s *Store) LookupSubscribers(tenant, channel string) ([]*Subscriber, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[channelKey{tenant, channel}]
	if !ok {
		return nil, false
	}
	return c.subs, true
}

// reindex records a moved subscriber's new position.
//
// Swapping the last element into the removed slot is O(1) but invalidates that
// subscriber's index entry. Callers must skip this when the removed
// subscriber was itself the last element: nothing moved, and writing an entry
// for a subscriber that is no longer in the slice would leave an index pointing
// past the end of it.
func (c *channelState) reindex(moved *Subscriber, pos int) {
	if moved == nil {
		return
	}
	c.index[subKey{conn: moved.conn, channel: moved.channel}] = pos
}

// SubscriberCountFor returns the local subscriber count for a channel.
func (s *Store) SubscriberCountFor(tenant, channel string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.channels[channelKey{tenant, channel}]
	if !ok {
		return 0
	}
	return len(c.subs)
}

// Subscribe adds conn to a channel.
//
// It reports whether the subscription was added and, if not, why. The caller
// needs the reason to tell a subscriber which of its channels was refused.
func (s *Store) Subscribe(tenant, channel string, conn *Conn) (bool, error) {
	sub := &Subscriber{conn: conn, channel: channel}
	sk := subKey{conn: conn, channel: channel}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelKey{tenant, channel}
	c, ok := s.channels[key]
	if !ok {
		// A channel is created implicitly on first use. There is no registration
		// step because a danmaku room's lifetime is not known in advance, and a
		// registration table would have to record every channel ever used.
		c = &channelState{index: map[subKey]int{}}
		s.channels[key] = c
	}
	if _, exists := c.index[sk]; exists {
		return false, nil
	}
	c.index[sk] = len(c.subs)
	c.subs = append(c.subs, sub)
	c.subscriberCount.Add(1)
	return true, nil
}

// Unsubscribe removes conn from a channel.
//
// It is idempotent: unsubscribing from a channel that was never subscribed is
// reported as removed rather than as an error, because a client withdrawing a
// subscription it no longer believes it holds is normal during a reconnect
// race.
func (s *Store) Unsubscribe(tenant, channel string, conn *Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelKey{tenant, channel}
	c, ok := s.channels[key]
	if !ok {
		return false
	}

	sk := subKey{conn: conn, channel: channel}
	pos, exists := c.index[sk]
	if !exists {
		return false
	}

	last := len(c.subs) - 1
	moved := c.subs[last]
	c.subs[pos] = moved
	c.subs[last] = nil
	c.subs = c.subs[:last]
	delete(c.index, sk)
	if pos != last {
		c.reindex(moved, pos)
	}
	c.subscriberCount.Add(-1)

	// Reclaim the channel's state once nobody subscribes, so a long-lived
	// instance does not accumulate one entry per channel ever touched.
	if len(c.subs) == 0 {
		delete(s.channels, key)
	}
	return true
}

// UnsubscribeAll removes conn from every channel and reports what it held.
//
// Called when a connection closes. It must find every subscription even though
// the conn is not tracked per-channel, so the channel keys are walked under one
// lock rather than per channel.
func (s *Store) UnsubscribeAll(conn *Conn) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := make([]string, 0, 8)
	for key, c := range s.channels {
		sk := subKey{conn: conn, channel: key.name}
		pos, ok := c.index[sk]
		if !ok {
			continue
		}
		last := len(c.subs) - 1
		moved := c.subs[last]
		c.subs[pos] = moved
		c.subs[last] = nil
		c.subs = c.subs[:last]
		delete(c.index, sk)
		if pos != last {
			c.reindex(moved, pos)
		}
		c.subscriberCount.Add(-1)
		removed = append(removed, key.name)

		if len(c.subs) == 0 {
			delete(s.channels, key)
		}
	}
	return removed
}

// ChannelsFor returns the channel names a connection currently holds.
func (s *Store) ChannelsFor(conn *Conn) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, 8)
	for key, c := range s.channels {
		if _, ok := c.index[subKey{conn: conn, channel: key.name}]; ok {
			out = append(out, key.name)
		}
	}
	return out
}

// SubResult reports what happened to one requested subscription.
type SubResult struct {
	Channel string
	// Accepted is true when the connection now holds the channel.
	Accepted bool
	// Reason explains a refusal. It is empty when Accepted.
	Reason string
	// RemainingSlots is how many more subscribers the channel will accept, used
	// by a client that wants to report "room full" rather than a bare failure.
	RemainingSlots int
}

// Refusal reasons reported per channel.
const (
	ReasonChannelFull       = "channel_subscriber_limit"
	ReasonConnectionClosed  = "connection_not_accepting"
	ReasonChannelNotAllowed = "channel_not_allowed"
	ReasonDuplicateChannel  = "duplicate_channel"
)

// BatchSubscribe applies many subscriptions in one call and reports each.
//
// This exists so a reconnecting client can restore its whole channel list in a
// single handshake. Restoring twenty channels one round trip at a time would
// cost twenty sequential latencies, which on a 50 ms path is a second before the
// client is usable again.
//
// A refusal is per channel and never fails the connection: a client that held
// twenty channels and is now allowed eighteen should get the eighteen plus a
// clear account of the two, not a dead connection.
func (s *Store) BatchSubscribe(tenant string, conn *Conn, channels []string, policies map[string]ChannelPolicy) []SubResult {
	results := make([]SubResult, 0, len(channels))

	for _, channel := range channels {
		res := SubResult{Channel: channel}

		switch {
		case !conn.Accepting():
			res.Reason = ReasonConnectionClosed
		default:
			policy, hasPolicy := policies[channel]
			current := s.SubscriberCountFor(tenant, channel)
			if hasPolicy {
				if ok, remaining := policy.AllowsSubscriber(current); !ok {
					res.Reason = ReasonChannelFull
					res.RemainingSlots = remaining
					results = append(results, res)
					continue
				}
			}

			added, err := s.Subscribe(tenant, channel, conn)
			if err != nil {
				res.Reason = ReasonChannelNotAllowed
			} else if !added {
				res.Reason = ReasonDuplicateChannel
			} else {
				res.Accepted = true
			}
		}
		results = append(results, res)
	}
	return results
}

// AcceptedChannels returns the channels from a batch result that succeeded.
func AcceptedChannels(results []SubResult) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		if r.Accepted {
			out = append(out, r.Channel)
		}
	}
	return out
}
