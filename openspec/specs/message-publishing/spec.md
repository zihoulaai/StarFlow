# message-publishing Specification

## Purpose

Defines the publishing interface, the shape and validation of a published message, the admission decisions applied before a message is admitted to the delivery path, and how the system behaves when a publish is refused.

## Requirements

### Requirement: Publish over HTTP
The system SHALL accept message publication over an HTTP endpoint, and SHALL return a per-message outcome to the publisher without waiting for subscriber delivery to complete.

#### Scenario: Accepted publish
- **WHEN** a publisher sends a valid message to an existing channel
- **THEN** the system returns an acceptance carrying the assigned message identifier and does not wait for subscriber delivery

#### Scenario: Publisher observes failure to connect
- **WHEN** the publish endpoint is unreachable from the publisher's network
- **THEN** the publisher can retry without risk of duplicate delivery being treated as an error, since delivery guarantees do not include persistence

### Requirement: Message envelope
The system SHALL assign every accepted message an identifier and a server timestamp, and SHALL preserve publisher-supplied fields including the message body and any expiry the publisher requests.

#### Scenario: System assigns identity
- **WHEN** a message is accepted
- **THEN** the system stamps it with a unique identifier and a server-assigned timestamp that are visible to subscribers

#### Scenario: Publisher supplies an expiry
- **WHEN** a publisher includes a time-to-live value
- **THEN** the system stops delivering that message once the stated lifetime elapses

### Requirement: Size limits
The system SHALL reject a message whose serialized size exceeds the configured maximum, and SHALL default that maximum to 4 KB.

#### Scenario: Oversized message rejected
- **WHEN** a publisher submits a message larger than the configured maximum
- **THEN** the system rejects it with a size error and does not admit it to the delivery path

#### Scenario: Message at the limit accepted
- **WHEN** a publisher submits a message whose serialized size equals the configured maximum
- **THEN** the system accepts it

### Requirement: Publish admission pipeline
The system SHALL apply, in order, the publisher's permission check, the tenant and channel rate limits, duplicate suppression, content moderation, and the size check before admitting a message to the delivery path.

#### Scenario: Message failing a later stage
- **WHEN** a message passes the rate limit but is rejected by content moderation
- **THEN** the system returns a moderation outcome and the message never reaches any subscriber

#### Scenario: Admission outcome is reported
- **WHEN** a publish completes admission
- **THEN** the response tells the publisher whether the message was admitted and, if not, which stage refused it

### Requirement: Duplicate suppression
The system SHALL suppress a repeat of a recently seen publish from the same publisher to the same channel, so that retries and client replays do not fan out more than once.

#### Scenario: Retry of a recent publish
- **WHEN** a publisher retransmits a message it already sent within the suppression window
- **THEN** the system suppresses the repeat and reports the original message identifier

#### Scenario: Legitimate repeat after the window
- **WHEN** the same publisher sends the same content after the suppression window has elapsed
- **THEN** the system treats it as a new message and admits it

### Requirement: Publish failure policy
The system SHALL offer a configurable policy per tenant that selects between discarding an over-limit publish, degrading it to a reduced form, and failing fast with an error, and SHALL apply the configured policy.

#### Scenario: Fail-fast policy under surge
- **WHEN** a tenant configured for fail-fast publishes beyond its limit
- **THEN** the system returns an error to the publisher and drops the message rather than admitting it in reduced form

#### Scenario: Discard policy under surge
- **WHEN** a tenant configured for discard publishes beyond its limit
- **THEN** the system drops the message silently from the publisher's perspective and records the drop

#### Scenario: Degraded policy under surge
- **WHEN** a tenant configured for degrade publishes beyond its limit
- **THEN** the system admits a reduced form of the message rather than discarding or rejecting it

### Requirement: Publisher rate limiting without shared coordination
The system SHALL enforce a tenant's publish ceiling using per-instance counters that sum to the tenant's configured limit, so that admission does not depend on a shared store.

#### Scenario: Instance share of the tenant ceiling
- **WHEN** a tenant is served by several gateway instances and the instance count changes
- **THEN** each instance adjusts its local share of the ceiling and the combined admission rate stays at the configured total, apart from a bounded transient while instances rebalance

#### Scenario: Hot path does not consult a shared store
- **WHEN** a publish is being admitted
- **THEN** the decision is reached from state held on the serving instance without a network call to a shared counter
