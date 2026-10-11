# Spec Delta

## Purpose

Defines what the service exposes about its own behavior: the metrics operators use to judge health and capacity, the alerts raised when those metrics leave acceptable bounds, and the reporting used to validate capacity against target load.

## ADDED Requirements

### Requirement: Connection and message volume metrics
The system SHALL expose the count of live connections, the rate of accepted publishes, the rate of messages delivered to subscribers, and the average fanout factor between the two.

#### Scenario: Fanout factor is observable
- **WHEN** an operator reads delivery metrics
- **THEN** the system reports both the publish rate and the delivery rate so the fanout factor between them is derivable

#### Scenario: Metrics are scoped
- **WHEN** an operator reads metrics for a tenant
- **THEN** the system reports that tenant's connection and message figures separately from platform totals

### Requirement: Latency metrics
The system SHALL expose the distribution of publish-to-delivery latency rather than only its average, so that tail behavior is visible.

#### Scenario: Percentiles are available
- **WHEN** an operator reads latency metrics
- **THEN** median, 95th percentile, and 99th percentile values are available for comparison against the stated budget

### Requirement: Loss and error rate metrics
The system SHALL expose the rate at which messages are dropped, the rate of refused publishes by reason, the number of subscribers currently treated as slow, and outbound bandwidth consumption.

#### Scenario: Drops are attributed
- **WHEN** messages are dropped
- **THEN** the drop metric distinguishes drops caused by slow subscribers from drops caused by rate limiting and from drops caused by degradation

#### Scenario: Slow subscriber count is observable
- **WHEN** subscribers are being treated as slow
- **THEN** the current count is exposed so an operator can correlate it with latency and drop metrics

### Requirement: Metric collection that does not distort the hot path
The system SHALL aggregate metrics on the serving instance and export them periodically, so that recording a metric does not add a synchronous operation to message admission or delivery.

#### Scenario: Per-message events do not each cost a store update
- **WHEN** messages are admitted and delivered at target rate
- **THEN** recording their metrics adds no synchronous call to a shared store on either path

### Requirement: Alerting on unacceptable conditions
The system SHALL raise alerts when delivery latency breaches its budget, when delivery drops exceed a configured rate, when connection attempts fail at an abnormal rate, and when moderation match rates spike unexpectedly.

#### Scenario: Latency breach raises an alert
- **WHEN** publish-to-delivery latency exceeds its configured threshold for the stated interval
- **THEN** the system raises a latency alert naming the affected scope

#### Scenario: Alert identifies its scope
- **WHEN** an alert is raised
- **THEN** it identifies whether it originates from a specific tenant, channel, or instance

#### Scenario: Moderation spike raises an alert
- **WHEN** sensitive word match rate rises sharply against the recent baseline
- **THEN** the system raises an alert, since a sudden rise may indicate a coordinated flood rather than ordinary traffic

### Requirement: Dashboard views
The system SHALL provide dashboards showing live connections, message volumes, fanout factor, latency percentiles, drop rates, and bandwidth, and SHALL allow the scope to be narrowed to a tenant or an instance.

#### Scenario: Operator narrows the scope
- **WHEN** an operator selects a tenant on the dashboard
- **THEN** the displayed figures reflect that tenant rather than the platform total

### Requirement: Capacity reporting under load test
The system SHALL support load testing that drives it to a stated target and reports measured throughput, latency distribution, drop rate, and resource use against the capacity targets.

#### Scenario: Target load is verified
- **WHEN** a load test drives the service to the target connection count and platform publish rate
- **THEN** the report states whether latency and drop rate stayed within budget and what the observed maximum sustainable load was

#### Scenario: Headroom is reported
- **WHEN** a load test finds that targets were met
- **THEN** the report states the headroom observed before a budget was breached

### Requirement: Traceability across the delivery path
The system SHALL be able to follow a publish from admission through to delivery by a correlation identifier, and SHALL retain traces for a sampled subset rather than every message, so that tracing capacity is not a limit on throughput.

#### Scenario: A specific publish can be followed
- **WHEN** an operator searches by a message identifier returned at publish time
- **THEN** the system shows the admission outcome, the channels it entered, and the delivery counts for each

#### Scenario: Tracing is sampled
- **WHEN** the service runs at target rate
- **THEN** only a configured fraction of messages produce a retained trace, with the sampling rate configurable