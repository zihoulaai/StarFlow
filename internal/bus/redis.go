package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBus binds the bus to Redis pub/sub.
//
// Pub/sub rather than a stream or a log, deliberately: design.md requires a
// non-persistent bus because durability is waste under QoS0. A message lost to
// a subscriber-side buffer overflow is a network drop the delivery contract
// already permits, so losing one here changes no guarantee the service makes.
//
// The consequence worth stating: this bus has no redelivery and no ordering
// guarantee across reconnecting subscribers. That is acceptable because every
// consumer treats the stream as a best-effort trigger.
type RedisBus struct {
	client     redis.UniversalClient
	ownsClient bool

	msgChannel string
	cfgChannel string

	mu     sync.Mutex
	msgPS  *redis.PubSub
	cfgPS  *redis.PubSub
	closed bool
}

// RedisConfig selects the Redis deployment.
type RedisConfig struct {
	// Addrs are the cluster or sentinel endpoints. A single entry is a plain
	// connection.
	Addrs []string
	// Username and Password authenticate to Redis.
	Username string
	Password string
	// DB selects the logical database. Ignored by cluster mode.
	DB int
	// ClientTimeout bounds dialling and command latency.
	ClientTimeout time.Duration
}

// NewRedisBus dials Redis and returns a bus bound to the given channels.
func NewRedisBus(cfg RedisConfig, msgChannel, cfgChannel string) (*RedisBus, error) {
	var client redis.UniversalClient
	switch len(cfg.Addrs) {
	case 0:
		return nil, errors.New("bus: at least one redis address is required")
	case 1:
		client = redis.NewClient(&redis.Options{
			Addr:        cfg.Addrs[0],
			Username:    cfg.Username,
			Password:    cfg.Password,
			DB:          cfg.DB,
			DialTimeout: cfg.ClientTimeout,
		})
	default:
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       cfg.Addrs,
			Username:    cfg.Username,
			Password:    cfg.Password,
			DialTimeout: cfg.ClientTimeout,
		})
	}
	return &RedisBus{
		client:     client,
		ownsClient: true,
		msgChannel: msgChannel,
		cfgChannel: cfgChannel,
	}, nil
}

// Publish broadcasts an envelope.
func (b *RedisBus) Publish(ctx context.Context, env Envelope) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return ErrClosed
	}

	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("bus: encode envelope: %w", err)
	}
	if err := b.client.Publish(ctx, b.msgChannel, payload).Err(); err != nil {
		return fmt.Errorf("bus: publish to %s: %w", b.msgChannel, err)
	}
	return nil
}

// Subscribe delivers envelopes until ctx is cancelled.
func (b *RedisBus) Subscribe(ctx context.Context, handler func(Envelope)) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.msgPS == nil {
		b.msgPS = b.client.Subscribe(ctx, b.msgChannel)
	}
	ps := b.msgPS
	b.mu.Unlock()

	ch := ps.Channel(redis.WithChannelSize(1024))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return ErrClosed
			}
			var env Envelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				// A malformed envelope on the wire must not kill the receive
				// loop; dropping it is consistent with the bus being lossy.
				continue
			}
			handler(env)
		}
	}
}

// PublishChange broadcasts a configuration change.
func (b *RedisBus) PublishChange(ctx context.Context, c ConfigChange) error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return ErrClosed
	}

	payload, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("bus: encode config change: %w", err)
	}
	if err := b.client.Publish(ctx, b.cfgChannel, payload).Err(); err != nil {
		return fmt.Errorf("bus: publish change to %s: %w", b.cfgChannel, err)
	}
	return nil
}

// SubscribeChanges delivers configuration changes until ctx is cancelled.
func (b *RedisBus) SubscribeChanges(ctx context.Context, handler func(ConfigChange)) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.cfgPS == nil {
		b.cfgPS = b.client.Subscribe(ctx, b.cfgChannel)
	}
	ps := b.cfgPS
	b.mu.Unlock()

	ch := ps.Channel(redis.WithChannelSize(256))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return ErrClosed
			}
			var c ConfigChange
			if err := json.Unmarshal([]byte(msg.Payload), &c); err != nil {
				continue
			}
			handler(c)
		}
	}
}

// Close releases the subscriptions and, if this bus created the client, the
// client itself.
func (b *RedisBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true

	var firstErr error
	if b.msgPS != nil {
		if err := b.msgPS.Close(); err != nil {
			firstErr = err
		}
	}
	if b.cfgPS != nil {
		if err := b.cfgPS.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.ownsClient {
		if err := b.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
