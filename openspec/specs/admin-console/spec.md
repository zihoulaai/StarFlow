# admin-console Specification

## Purpose

Defines the administrative surface for operating the service: managing tenants and applications, observing and acting on live connections, maintaining moderation rules, and configuring quotas, rate limits, and feature switches.

## Requirements

### Requirement: Tenant and application administration
The system SHALL let an authorized administrator create, inspect, update, and disable tenants and their applications, and SHALL stop authenticating new connections for an application as soon as it is disabled.

#### Scenario: Application disabled
- **WHEN** an administrator disables an application
- **THEN** new authentication attempts for that application fail while existing connections are ended by the system

#### Scenario: Credential rotation
- **WHEN** an administrator rotates an application's Secret
- **THEN** tokens signed with the previous Secret stop being accepted

### Requirement: Channel administration
The system SHALL let an administrator inspect a channel's current subscriber count and configured limits, adjust those limits, and apply them to subsequent operations.

#### Scenario: Operator inspects a busy channel
- **WHEN** an administrator requests a channel's state
- **THEN** the system reports its current subscriber count and effective limits

#### Scenario: Adjusted limit applies to subsequent publishes
- **WHEN** an administrator lowers a channel's publish ceiling
- **THEN** publishes above the new ceiling are refused without a restart

### Requirement: Live connection visibility
The system SHALL let an administrator list live connections with their tenant, channel set, and state, and SHALL let an administrator terminate a chosen connection.

#### Scenario: Operator lists connections
- **WHEN** an administrator requests the live connection list
- **THEN** the system returns each connection with its tenant, subscribed channels, and connection state

#### Scenario: Operator terminates a connection
- **WHEN** an administrator terminates a connection
- **THEN** that connection is closed and its subscriptions released, with no effect on other connections

#### Scenario: Terminated client may reconnect
- **WHEN** a client reconnects after its connection was terminated
- **THEN** it is subject to the same authentication and rate checks as any new client

### Requirement: Moderation rule administration
The system SHALL let an administrator maintain sensitive word rules, per-user mutes and bans, and network blocks, and SHALL let those changes take effect without restarting the serving instances.

#### Scenario: Rule added and applied
- **WHEN** an administrator adds a sensitive word rule
- **THEN** matching publishes are handled by the new rule's action without an instance restart

#### Scenario: Ban lifted
- **WHEN** an administrator removes a user ban
- **THEN** that user's publishes are accepted again

### Requirement: Quota and rate limit configuration
The system SHALL let an administrator configure per-tenant connection ceilings, publish rate ceilings, and channel count ceilings, and SHALL apply changes without restarting the serving instances.

#### Scenario: Connection ceiling lowered below current usage
- **WHEN** an administrator lowers a tenant's connection ceiling below its current connection count
- **THEN** existing connections are not forcibly ended and new connections are refused until usage falls below the ceiling

### Requirement: Feature and degradation switches
The system SHALL let an administrator enable or disable individual capabilities and shall let an operator move the service between defined degradation levels that trade delivery volume for stability, and SHALL apply these without a restart.

#### Scenario: Degradation level raised
- **WHEN** an operator raises the degradation level
- **THEN** the system applies that level's fanout and coalescing behavior within the configured propagation interval

#### Scenario: Automatic degradation under load
- **WHEN** load exceeds the automatic threshold for the configured duration
- **THEN** the system raises its degradation level automatically and records that it did so

#### Scenario: Level restored
- **WHEN** load returns below the threshold and the level is restored
- **THEN** the system resumes the normal delivery behavior

### Requirement: Administrative authorization
The system SHALL restrict every administrative operation to authorized principals, and SHALL record each administrative action with its principal and target.

#### Scenario: Unauthorized administration attempt
- **WHEN** a caller without administrative authority invokes an administrative operation
- **THEN** the system refuses it and records the attempt

#### Scenario: Administrative action is attributable
- **WHEN** an administrator changes a quota or a moderation rule
- **THEN** the system records which principal made the change, when, and against what target
