# fanout-delivery Specification

## Purpose

Defines the delivery guarantees subscribers receive at QoS0, how messages are coalesced into fewer packets without changing their content, how slow subscribers are handled without harming healthy ones, and the connection lifecycle of subscribing clients.

## Requirements

### Requirement: At-most-once delivery semantics
The system SHALL deliver messages on a best-effort basis, and SHALL NOT persist, replay, or acknowledge them, so that a message may be lost, duplicated, or delivered out of order.

#### Scenario: Message lost during a delivery failure
- **WHEN** a gateway fails while a message is in flight to subscribers
- **THEN** the system does not recover the message and subscribers may never observe it

#### Scenario: No acknowledgement exchange
- **WHEN** the system delivers a message to a subscriber
- **THEN** it neither waits for nor records an acknowledgement from that subscriber

#### Scenario: No offline retrieval
- **WHEN** a subscriber disconnects and later reconnects
- **THEN** the system does not deliver the messages it missed while disconnected

### Requirement: Coalesced packet delivery
The system SHALL merge messages for a channel that arrive within a configurable window into a single delivered packet that carries their original contents and order, and SHALL leave the total bytes delivered unchanged by that merge.

#### Scenario: Several messages inside one window
- **WHEN** five messages for a channel arrive within one coalescing window
- **THEN** the subscriber receives one packet containing all five in their original order

#### Scenario: Merge does not change delivered volume
- **WHEN** messages are coalesced into a packet
- **THEN** the total content bytes delivered to the subscriber are the same as if each message had been delivered separately

#### Scenario: Window elapses with no further message
- **WHEN** the coalescing window elapses
- **THEN** the system delivers the accumulated messages without waiting for more

#### Scenario: Coalescing bypassed for a channel
- **WHEN** coalescing is disabled for a channel
- **THEN** each message is delivered as its own packet

### Requirement: End-to-end latency budget
The system SHALL deliver a message to subscribers of the same region with a publish-to-delivery latency whose median is below 50 ms, 95th percentile below 100 ms, and 99th percentile below 200 ms, measured under the capacity target load.

#### Scenario: Latency measured under target load
- **WHEN** the service runs at the target platform publish rate with target subscriber counts
- **THEN** the measured publish-to-delivery latency distribution stays within the stated percentiles

### Requirement: Slow subscriber isolation
The system SHALL bound the amount of data queued for any single subscriber, and SHALL drop or degrade for that subscriber alone rather than blocking or degrading delivery to other subscribers of the same channel.

#### Scenario: Subscriber stops reading
- **WHEN** a subscriber stops reading and its queue reaches its bound
- **THEN** the system stops queuing for that subscriber while other subscribers continue receiving normally

#### Scenario: Queue overflow discards according to policy
- **WHEN** a subscriber's queue is full and further messages arrive
- **THEN** the system discards according to the configured policy, keeping the most recent content for a channel whose value decays with time

#### Scenario: Slow subscriber does not consume shared capacity
- **WHEN** a large fraction of a channel's subscribers are slow
- **THEN** the healthy subscribers' latency and delivery rate are unaffected

### Requirement: Delivery gap notification
The system SHALL tell a subscriber when messages were dropped for that connection, so that a client can present a discontinuity rather than silently showing an incomplete stream.

#### Scenario: Drop occurs under slow-consumer handling
- **WHEN** the system drops messages for a subscriber because of overflow or shutdown
- **THEN** the subscriber receives a notification identifying that content was lost before the next delivered content

### Requirement: Connection keepalive
The system SHALL detect an unresponsive subscriber within a configured interval using periodic liveness exchange, and SHALL close connections that stop responding.

#### Scenario: Subscriber stops responding
- **WHEN** a subscriber does not answer a liveness probe within the configured timeout
- **THEN** the system closes the connection and releases its subscriptions

#### Scenario: Heartbeat load is bounded
- **WHEN** many connections are held open
- **THEN** the system keeps liveness cost proportional to connection count rather than growing with message volume

### Requirement: Bulk resubscribe on reconnect
The system SHALL let a reconnecting client restore its prior channel list in the connect handshake, so that a dropped client resumes without one round trip per channel.

#### Scenario: Client resumes after a network drop
- **WHEN** a client reconnects and supplies its previously held channels
- **THEN** its subscriptions are restored as part of the handshake and new messages arrive without further client action

### Requirement: Graceful connection drain
The system SHALL stop accepting new connections on an instance that is being retired, send a drain notice to its existing subscribers, and close those connections only after allowing them time to reconnect elsewhere.

#### Scenario: Instance retires for deployment
- **WHEN** an instance begins draining
- **THEN** it rejects new connection attempts and signals its subscribers to reconnect so they land on other instances

#### Scenario: Subscribers reconnect during drain
- **WHEN** subscribers receive the drain notice and reconnect
- **THEN** they are accepted by another instance and resume their subscriptions there
