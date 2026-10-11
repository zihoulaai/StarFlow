# channel-subscription Specification

## Purpose

Defines the channel addressing model that subscribers receive messages from, how connections declare and change their subscriptions including in bulk at connect time, and how per-channel limits protect the delivery path.

## Requirements

### Requirement: Channel addressing
The system SHALL identify a destination by a channel name scoped to the owning tenant, and SHALL deliver a published message to every subscriber of that channel regardless of which gateway holds the connection.

#### Scenario: Message reaches subscribers across multiple gateways
- **WHEN** a message is published to a channel whose subscribers are connected to several different gateways
- **THEN** every subscriber receives exactly one copy of that message

#### Scenario: Tenant isolation between identically named channels
- **WHEN** two tenants each define a channel with the same name and one publishes to it
- **THEN** only subscribers of the publishing tenant's channel receive the message

### Requirement: Subscribe and unsubscribe
The system SHALL let a connected subscriber declare interest in a channel and later withdraw it, and SHALL apply a withdrawal immediately so no further messages from that channel reach the connection.

#### Scenario: Declaring a subscription
- **WHEN** a subscriber declares interest in a channel it is not yet subscribed to
- **THEN** the system records the subscription and the subscriber receives subsequent messages for that channel

#### Scenario: Withdrawing a subscription
- **WHEN** a subscriber withdraws from a channel
- **THEN** the system stops delivering that channel's messages to the connection without affecting its other subscriptions

#### Scenario: Unsubscribing from a channel never subscribed to
- **WHEN** a subscriber withdraws from a channel it does not hold
- **THEN** the system treats the request as idempotent and reports the channel as unsubscribed

### Requirement: Multiple concurrent channels per connection
The system SHALL let a single connection hold subscriptions to many channels at once, and SHALL deliver messages from each independently.

#### Scenario: Several channels on one connection
- **WHEN** a connection subscribes to three channels and a message arrives on one of them
- **THEN** only the message for that channel is delivered and the other two subscriptions are unaffected

### Requirement: Batch subscription at connect time
The system SHALL accept a subscriber's complete initial subscription list as part of the connect handshake, so that resuming a client does not require one round trip per channel.

#### Scenario: Restoring many subscriptions on reconnect
- **WHEN** a client reconnects and presents twenty previously held channels in its connect request
- **THEN** the system establishes all twenty subscriptions as part of the handshake without additional round trips

#### Scenario: Partial failure during batch subscription
- **WHEN** a batch subscription includes channels that exceed the connection's allowance
- **THEN** the system accepts the channels within the allowance, reports which were rejected and why, and keeps the connection usable

### Requirement: Implicit channel creation
The system SHALL allow messages to be published to a channel that was never explicitly registered, and SHALL treat a channel as existing from first use without a separate creation step.

#### Scenario: Publishing to an unregistered channel
- **WHEN** a publisher sends a message to a channel name that has never been used before
- **THEN** the system delivers the message to any subscribers of that channel and creates no separate registration record

#### Scenario: Channel with no subscribers
- **WHEN** a publisher sends to a channel with no subscribers
- **THEN** the system accepts and discards the message without error and without storing it

### Requirement: Per-channel publish ceiling derived from the per-viewer budget
The system SHALL derive a channel's accepted publish rate from the bandwidth budget each of its subscribers is held to, and from the packet rate those subscribers are expected to receive, rather than from the channel's subscriber count.

#### Scenario: Ceiling follows the configured budget
- **WHEN** a channel's subscribers are each held to a per-viewer bandwidth budget and a per-viewer packet rate
- **THEN** the system derives the channel's publish ceiling from those two figures

#### Scenario: Channels of different sizes share one ceiling
- **WHEN** a small channel and a large channel are configured with the same per-viewer budget and packet rate
- **THEN** both admit publishes at the rate their individual subscribers can absorb, and the larger channel consumes proportionally more aggregate resources without admitting a higher rate

#### Scenario: Budget change moves the ceiling
- **WHEN** the per-viewer budget or the per-viewer packet rate is changed
- **THEN** the derived ceiling changes accordingly for subsequent publishes, without operator intervention and without an instance restart

#### Scenario: Channel with no subscribers imposes no ceiling
- **WHEN** a channel has no subscribers
- **THEN** the system applies no publish ceiling to it, because no subscriber budget can be exceeded

### Requirement: Channel-level configuration
The system SHALL let an administrator configure a channel's publish ceiling, its per-subscriber receive rate target, and whether coalescing applies to it, and SHALL apply those settings to publishes admitted after the change.

#### Scenario: Administrator raises a channel ceiling
- **WHEN** an administrator raises a channel's configured publish ceiling
- **THEN** publishes beyond the previous ceiling are accepted once the new setting takes effect

#### Scenario: Coalescing disabled for a channel
- **WHEN** coalescing is disabled for a channel and messages arrive in quick succession
- **THEN** the system sends each message as its own delivery rather than merging them
