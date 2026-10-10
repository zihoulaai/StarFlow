// Package bus carries admitted messages and configuration changes between
// gateway instances.
//
// The broadcast topology is what lets a gateway hold no subscription state that
// must be shared or reconciled: every instance receives every admitted message
// and delivers the part matching its own local subscriptions. That choice only
// pays off if the bus is comfortably faster than the socket writes it
// accompanies, which it is by three orders of magnitude at target load.
//
// The interface is deliberately narrow and the concrete choice hidden. design.md
// requires a non-persistent bus: durability is waste under QoS0, and a message
// lost on the bus is indistinguishable from a network drop the protocol already
// permits.
package bus

import (
	"context"
	"errors"
	"time"
)

// ErrClosed is returned when an operation is attempted on a closed bus.
var ErrClosed = errors.New("bus: closed")

// Envelope is an admitted message on the wire between gateways.
//
// It carries no subscriber information at all. That is the property the fanout
// path depends on: one encoded body shared byte-for-byte by every recipient of
// a channel.
type Envelope struct {
	// TenantID scopes the message. Every gateway receiving the envelope drops
	// it immediately if it does not hold a subscriber for this tenant.
	TenantID string
	// Channel is the destination within the tenant.
	Channel string
	// MessageID is the identifier returned to the publisher.
	MessageID uint64
	// TimeMs is the server-assigned publish time.
	TimeMs uint64
	// ExpiresAtMs is the publisher's requested lifetime, or zero when the
	// message does not expire. A gateway that receives the envelope after this
	// time drops it, which is how a publisher-requested TTL is honoured without
	// any persisted state.
	ExpiresAtMs uint64
	// Class drives the degradation ladder.
	Class uint8
	// Flags and Tag carry moderation decoration already applied by the
	// publishing gateway.
	Flags uint8
	Tag   string
	// Body is the opaque publisher payload.
	Body []byte
}

// ConfigChangeKind identifies what a configuration notification changed.
type ConfigChangeKind uint8

const (
	ConfigQuotaChanged        ConfigChangeKind = 1
	ConfigChannelChanged      ConfigChangeKind = 2
	ConfigRulesChanged        ConfigChangeKind = 3
	ConfigRestrictionsChanged ConfigChangeKind = 4
	ConfigSwitchChanged       ConfigChangeKind = 5
	ConfigDegradationChanged  ConfigChangeKind = 6
	ConfigApplicationDisabled ConfigChangeKind = 7
)

// ConfigChange notifies subscribers that control-plane state moved. It carries
// the tenant and the kind, not the new values: a gateway reloads the changed
// subset from the durable store, so a notification can never drift out of sync
// with what was actually persisted.
type ConfigChange struct {
	Kind     ConfigChangeKind
	TenantID string
	// Subject narrows the change, for example the channel name or the rule id.
	// Empty means the whole tenant changed.
	Subject string
}

// Bus is the message path between gateway instances.
type Bus interface {
	// Publish sends an envelope to every subscriber, including the caller's
	// own instance if it subscribes to the same bus.
	Publish(ctx context.Context, env Envelope) error

	// Subscribe delivers envelopes until ctx is cancelled. The handler must not
	// block for long: it runs on the bus client's receive path.
	Subscribe(ctx context.Context, handler func(Envelope)) error

	// Close releases the bus.
	Close() error
}

// ConfigBus is the control-plane change notification path.
type ConfigBus interface {
	// PublishChange notifies other instances that control-plane state moved.
	PublishChange(ctx context.Context, c ConfigChange) error

	// SubscribeChanges delivers change notifications until ctx is cancelled.
	SubscribeChanges(ctx context.Context, handler func(ConfigChange)) error

	Close() error
}

// Subscription is the lifecycle handle returned by a subscribe call, so a
// caller can cancel one handler without tearing down the bus.
type Subscription struct {
	cancel context.CancelFunc
}

// Stop ends the subscription. It is safe to call more than once.
func (s Subscription) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// PropagationInterval is the bound within which a configuration change is
// expected to reach every instance. It exists so tests and the readiness probe
// can reason about "live without restart" as a checkable property.
const PropagationInterval = 2 * time.Second
