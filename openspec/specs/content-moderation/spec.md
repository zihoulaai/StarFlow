# content-moderation Specification

## Purpose

Defines the content controls applied to every publish before it reaches subscribers, including sensitive word matching, user and network level restrictions, and the audit record the system keeps of those decisions.

## Requirements

### Requirement: Sensitive word matching on the publish path
The system SHALL evaluate each admitted publish against the tenant's active sensitive word list before it reaches any subscriber, and SHALL make that evaluation without a network call on the publish path.

#### Scenario: Word list change takes effect without restart
- **WHEN** an administrator updates the sensitive word list
- **THEN** instances serving that tenant apply the new list without a restart

#### Scenario: List maintained per tenant
- **WHEN** two tenants have different word lists
- **THEN** a publish from one tenant is evaluated only against its own list

### Requirement: Configurable enforcement actions
The system SHALL let an administrator choose, per rule or per tenant, whether a match blocks the publish, replaces the matched content, or allows it while tagging it, and SHALL apply the configured action.

#### Scenario: Blocking action
- **WHEN** a publish matches a rule configured to block
- **THEN** the system refuses it and no subscriber receives it

#### Scenario: Replacement action
- **WHEN** a publish matches a rule configured to replace
- **THEN** subscribers receive the message with the matched content replaced by the configured substitute

#### Scenario: Tagging action
- **WHEN** a publish matches a rule configured to tag
- **THEN** subscribers receive the message with the configured tag attached for their own handling

### Requirement: User mute and ban
The system SHALL let an administrator mute a user for a stated duration and SHALL refuse that user's publishes to the affected channels for the duration, and SHALL support a ban that does not expire.

#### Scenario: Temporary mute
- **WHEN** a user is muted for ten minutes and that user publishes during the mute
- **THEN** the system refuses the publish with a mute outcome and accepts it after the mute expires

#### Scenario: Permanent ban
- **WHEN** a user is permanently banned and that user publishes
- **THEN** the system refuses the publish for as long as the ban remains in force

#### Scenario: Mute applies to the muted identity only
- **WHEN** a muted user's publish is refused
- **THEN** other users' publishes are unaffected

### Requirement: Network level blocking
The system SHALL let an administrator block publishing and subscribing by source IP address, and SHALL enforce that block at connection acceptance and at publish admission.

#### Scenario: Blocked source attempts to subscribe
- **WHEN** a connection arrives from a blocked address
- **THEN** the system refuses the handshake

#### Scenario: Blocked source attempts to publish
- **WHEN** a publish request arrives from a blocked address
- **THEN** the system refuses it

### Requirement: Rate limit on danmaku flooding
The system SHALL apply a per-user publish rate limit on top of the tenant and channel limits, so that a user cannot flood a channel, and SHALL report refusals caused by this limit as a distinct outcome.

#### Scenario: User exceeds the danmaku limit
- **WHEN** a user publishes faster than the per-user limit allows
- **THEN** the system refuses the excess publishes with a per-user rate outcome distinct from tenant or channel limits

#### Scenario: Limit is per user not per connection
- **WHEN** the same user publishes from more than one connection
- **THEN** the per-user limit applies across those connections together

### Requirement: Duplicate and promotional content filtering
The system SHALL let an administrator configure rejection of repeated content from a user within a stated window and of content matching promotional patterns.

#### Scenario: Repeated content within the window
- **WHEN** a user submits content identical to their own recent submission within the configured window
- **THEN** the system rejects it as repeated content

#### Scenario: Promotional pattern matched
- **WHEN** content matches a configured promotional pattern
- **THEN** the system applies the configured action for that rule

### Requirement: Audit record of moderation decisions
The system SHALL record each moderation outcome with its time, the tenant, the channel, the acting identity, the rule applied, and the decision, and SHALL retain it according to the tenant's configured retention.

#### Scenario: Blocked publish is auditable
- **WHEN** a publish is blocked by a rule
- **THEN** the system writes an audit entry identifying the rule and decision, with the message body withheld or redacted according to the tenant's configuration

#### Scenario: Audit retrieval is scoped
- **WHEN** an administrator queries audit entries for a tenant
- **THEN** only entries belonging to that tenant are returned
