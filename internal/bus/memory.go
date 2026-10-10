package bus

import (
	"context"
	"sync"
	"sync/atomic"
)

// MemoryBus is an in-process bus.
//
// It serves two purposes. It is the fake the bus contract suite runs against,
// so
// a bus implementation that satisfies the interface is known to preserve the
// properties the fanout path depends on rather than merely compiling. And it is
// the real implementation for single-node deployments and tests, where standing
// up Redis to move bytes between two goroutines in one process would be pure
// overhead.
type MemoryBus struct {
	mu      sync.RWMutex
	msgSubs map[int]func(Envelope)
	cfgSubs map[int]func(ConfigChange)
	nextID  atomic.Int64
	closed  atomic.Bool

	// published counts envelopes handed to Publish, so tests can assert that a
	// message is broadcast rather than routed to one subscriber.
	published atomic.Int64
}

// NewMemoryBus returns an empty in-process bus.
func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		msgSubs: map[int]func(Envelope){},
		cfgSubs: map[int]func(ConfigChange){},
	}
}

// Publish delivers env to every current subscriber.
//
// The envelope body is shared rather than copied, matching the real bus and the
// fanout contract: subscribers must treat an Envelope as read-only.
func (b *MemoryBus) Publish(ctx context.Context, env Envelope) error {
	if b.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.published.Add(1)

	b.mu.RLock()
	handlers := make([]func(Envelope), 0, len(b.msgSubs))
	for _, h := range b.msgSubs {
		handlers = append(handlers, h)
	}
	b.mu.RUnlock()

	for _, h := range handlers {
		h(env)
	}
	return nil
}

// Subscribe registers handler for every envelope until ctx is cancelled or Stop
// is called on the returned subscription.
func (b *MemoryBus) Subscribe(ctx context.Context, handler func(Envelope)) error {
	if b.closed.Load() {
		return ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	id := int(b.nextID.Add(1))

	b.mu.Lock()
	b.msgSubs[id] = handler
	b.mu.Unlock()

	go func() {
		defer cancel()
		<-ctx.Done()
		b.mu.Lock()
		delete(b.msgSubs, id)
		b.mu.Unlock()
	}()
	return nil
}

// PublishChange broadcasts a configuration change.
func (b *MemoryBus) PublishChange(ctx context.Context, c ConfigChange) error {
	if b.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.RLock()
	handlers := make([]func(ConfigChange), 0, len(b.cfgSubs))
	for _, h := range b.cfgSubs {
		handlers = append(handlers, h)
	}
	b.mu.RUnlock()

	for _, h := range handlers {
		h(c)
	}
	return nil
}

// SubscribeChanges registers handler for configuration changes.
func (b *MemoryBus) SubscribeChanges(ctx context.Context, handler func(ConfigChange)) error {
	if b.closed.Load() {
		return ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	id := int(b.nextID.Add(1))

	b.mu.Lock()
	b.cfgSubs[id] = handler
	b.mu.Unlock()

	go func() {
		defer cancel()
		<-ctx.Done()
		b.mu.Lock()
		delete(b.cfgSubs, id)
		b.mu.Unlock()
	}()
	return nil
}

// Close marks the bus closed. Publishing afterwards fails rather than silently
// dropping, so a gateway shutting down does not appear healthy.
func (b *MemoryBus) Close() error {
	b.closed.Store(true)
	b.mu.Lock()
	b.msgSubs = map[int]func(Envelope){}
	b.cfgSubs = map[int]func(ConfigChange){}
	b.mu.Unlock()
	return nil
}

// PublishedCount reports how many envelopes were published.
func (b *MemoryBus) PublishedCount() int64 { return b.published.Load() }

// Subscribers reports how many message handlers are currently registered.
func (b *MemoryBus) Subscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.msgSubs)
}

// ConfigSubscribers reports how many configuration handlers are registered.
// Tests use it to publish only after a subscriber has actually attached, rather
// than sleeping and hoping.
func (b *MemoryBus) ConfigSubscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.cfgSubs)
}
