# access-control Specification

## Purpose

Defines how tenants and applications onboard, how client connections and publishers prove identity, how publish and subscribe privileges are kept separate, and what transport-level protections the service enforces.

## Requirements

### Requirement: Multi-tenant application onboarding
The system SHALL support multiple isolated tenants, each owning one or more applications identified by an AppKey, and SHALL keep tenant identifiers present on every authenticated connection and publish request.

#### Scenario: Tenant administrator provisions an application
- **WHEN** an administrator creates an application under a tenant
- **THEN** the system issues an AppKey and a Secret, and stores them as the credential pair for that application

#### Scenario: Requests carry tenant identity
- **WHEN** a client connects or a publisher calls the publish API
- **THEN** the system resolves the owning tenant from the presented credential and scopes all subsequent quota and moderation decisions to that tenant

### Requirement: Credential authentication
The system SHALL authenticate client connections and publish requests using an AppKey plus a Token, and SHALL reject any request whose credential is missing, malformed, expired, or signed with an unknown Secret.

#### Scenario: Valid token is accepted
- **WHEN** a client presents a token signed with the application's Secret and within its validity window
- **THEN** the system completes the WebSocket handshake for that application

#### Scenario: Expired token is rejected
- **WHEN** a client presents a token whose expiry timestamp has passed
- **THEN** the system closes the handshake with an authentication failure and records the attempt in the audit log

#### Scenario: Signature does not verify
- **WHEN** a token is signed with a key that does not match the application's Secret
- **THEN** the system rejects the credential without revealing whether the AppKey exists

### Requirement: Publish and subscribe privilege separation
The system SHALL let an application enable publishing, subscribing, or both independently, and SHALL refuse any operation the application is not entitled to.

#### Scenario: Subscribe-only application attempts to publish
- **WHEN** a credential with subscribe-only privilege calls the publish API
- **THEN** the system rejects the request with a permission error and does not admit the message to the fanout path

#### Scenario: Publish-only application connects a subscriber
- **WHEN** a credential with publish-only privilege opens a subscription connection
- **THEN** the system accepts the transport but grants no subscription rights to that connection

### Requirement: Tenant quota enforcement
The system SHALL enforce per-tenant ceilings on simultaneous connections and publish rate, and SHALL enforce a per-tenant ceiling on the number of distinct channels.

#### Scenario: Connection ceiling reached
- **WHEN** a tenant already holds its configured maximum number of simultaneous connections and a further connection authenticates
- **THEN** the system refuses the new connection with a quota error rather than admitting it and degrading existing ones

#### Scenario: Publish rate ceiling reached
- **WHEN** a tenant's publish rate exceeds its configured quota within the enforcement window
- **THEN** the system rejects the excess publishes and increments the tenant's rate-limit rejection metric

### Requirement: Transport security
The system SHALL accept subscriber traffic only over TLS-encrypted WebSocket connections, and system administrators SHALL be able to disable the plaintext endpoint.

#### Scenario: Plaintext WebSocket request
- **WHEN** a client attempts a ws:// connection while the plaintext endpoint is disabled
- **THEN** the system refuses the upgrade before any subscription state is created

#### Scenario: Encrypted connection carries traffic
- **WHEN** a subscriber connects over wss:// and receives messages
- **THEN** all subscription and publish traffic on that connection is encrypted in transit

### Requirement: Cross-origin policy control
The system SHALL apply an administrator-configured origin allowlist to browser-originated requests and SHALL refuse origins that are not allowed.

#### Scenario: Origin absent from allowlist
- **WHEN** a browser handshake presents an Origin header not present in the tenant's allowlist
- **THEN** the system refuses the handshake with an origin policy error

#### Scenario: No Origin header on a non-browser client
- **WHEN** a non-browser client connects without an Origin header
- **THEN** the system applies the configured default for non-browser clients rather than rejecting by default

### Requirement: Replay and tamper rejection
The system SHALL reject publish requests whose token timestamp falls outside the accepted skew window, and SHALL verify message integrity for any request carrying a client-generated signature.

#### Scenario: Stale timestamp
- **WHEN** a publish request presents a token whose timestamp is older than the accepted skew window
- **THEN** the system rejects the request as a replay attempt and records it

#### Scenario: Tampered payload with client signature
- **WHEN** a signed publish request arrives with a payload that does not match the signature
- **THEN** the system rejects the request without forwarding it to the fanout path
