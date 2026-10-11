# web-sdk Specification

## Purpose

Defines the behavior of the browser client library that applications embed to subscribe to channels and receive messages, including its connection lifecycle, reconnection behavior, and how it surfaces delivery loss to the calling application.

## Requirements

### Requirement: Connection and authentication
The SDK SHALL establish a WebSocket connection using the application's credentials, and SHALL complete authentication before treating the connection as usable.

#### Scenario: Connect with valid credentials
- **WHEN** an application calls connect with a valid AppKey and Token
- **THEN** the SDK opens the connection, authenticates it, and reports readiness to the caller

#### Scenario: Authentication rejected
- **WHEN** the service rejects the credential
- **THEN** the SDK reports an authentication error to the caller and does not present the connection as ready

#### Scenario: Default to an encrypted transport
- **WHEN** an application connects without overriding the transport
- **THEN** the SDK uses an encrypted WebSocket connection

### Requirement: Subscription management
The SDK SHALL let an application subscribe to and unsubscribe from channels and SHALL track its own subscription set so it can restore that set later.

#### Scenario: Subscribe and receive
- **WHEN** an application subscribes to a channel and a message is published to it
- **THEN** the SDK invokes the application's message callback with that message

#### Scenario: Unsubscribe stops delivery
- **WHEN** an application unsubscribes from a channel
- **THEN** the SDK stops invoking the callback for that channel

#### Scenario: Subscription state is queryable
- **WHEN** an application asks which channels it is subscribed to
- **THEN** the SDK reports its current subscription set

#### Scenario: Bulk subscription in one call
- **WHEN** an application subscribes to many channels in a single call
- **THEN** the SDK sends them as one request rather than issuing a separate request per channel

### Requirement: Automatic reconnection with jitter
The SDK SHALL reconnect automatically after an unexpected disconnection, and SHALL use a backoff whose interval grows across consecutive attempts and includes random jitter, so that many clients recovering from the same outage do not reconnect together.

#### Scenario: Reconnect after an unexpected drop
- **WHEN** a connection drops without the application requesting it
- **THEN** the SDK re-establishes the connection and restores the prior subscriptions without the application acting

#### Scenario: Attempts are spread out
- **WHEN** many clients are disconnected by the same outage and all begin recovering
- **THEN** their reconnect attempts are spread across the backoff interval rather than concentrated at one instant

#### Scenario: Backoff grows across attempts
- **WHEN** repeated connection attempts keep failing
- **THEN** the interval between attempts increases and the SDK reports the connection as degraded to the caller

#### Scenario: Client-initiated disconnect is not retried
- **WHEN** the application closes the connection itself
- **THEN** the SDK does not reconnect

### Requirement: Session resumption to avoid repeated handshakes
The SDK SHALL reuse the transport session it established previously, so that reconnecting does not repeat the full cryptographic handshake cost, and SHALL fall back to a full handshake when resumption is not possible.

#### Scenario: Reconnect after a short outage
- **WHEN** the SDK reconnects within the resumption window
- **THEN** it resumes the prior session instead of performing a full handshake

#### Scenario: Resumption unavailable
- **WHEN** the service does not permit resumption
- **THEN** the SDK performs a full handshake and continues operating normally

### Requirement: Message callbacks
The SDK SHALL deliver each received message to the application through a registered callback and SHALL keep the application callback work off the SDK's connection handling path.

#### Scenario: Callback invocation
- **WHEN** a message arrives for a subscribed channel
- **THEN** the SDK invokes the registered callback with the message payload and its channel

#### Scenario: Slow application callback
- **WHEN** the application's callback takes a long time to return
- **THEN** the SDK continues handling the connection, heartbeats, and reconnect state without waiting for it

#### Scenario: Listener removal
- **WHEN** the application removes its registered callback
- **THEN** the SDK stops invoking it for subsequent messages

### Requirement: Delivery loss surfaced to the application
The SDK SHALL notify the application when the service reports that content was dropped for that connection, so the application can present a discontinuity rather than an apparently complete but truncated stream.

#### Scenario: Gap notification reaches the application
- **WHEN** the service reports that messages were dropped on the SDK's connection
- **THEN** the SDK invokes the application's gap callback so it can indicate the discontinuity

#### Scenario: Application ignores the gap
- **WHEN** an application registers no gap callback
- **THEN** the SDK continues delivering messages normally without surfacing an error for the unused callback

### Requirement: Error and connection state reporting
The SDK SHALL report connection state and errors to the application through callbacks, including authentication failure, server refusal, and terminal connection loss.

#### Scenario: Server refuses the connection
- **WHEN** the service refuses a connection attempt for a stated reason
- **THEN** the SDK reports that reason to the application rather than retrying silently

#### Scenario: Reconnect budget exhausted
- **WHEN** reconnection has not succeeded within the configured maximum attempt window
- **THEN** the SDK reports a terminal failure to the application and stops retrying

### Requirement: Operation under weak or degraded networks
The SDK SHALL continue operating when the network is slow, interrupted, or passes through a proxy, and SHALL prefer resumption and batching over assuming a reliable path.

#### Scenario: Intermittent connectivity
- **WHEN** the network drops and returns repeatedly over a period
- **THEN** the SDK reconnects within its backoff policy and restores the application's subscriptions

#### Scenario: Proxy that delays connection upgrade
- **WHEN** an intermediary delays the connection upgrade
- **THEN** the SDK tolerates the delay within its configured timeouts and reports a timeout if the upgrade never completes

#### Scenario: Connection silently dropped by an intermediary
- **WHEN** the underlying connection disappears without a close frame
- **THEN** the SDK detects it through its liveness exchange and reconnects
