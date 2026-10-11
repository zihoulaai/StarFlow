# Spec Delta

## Purpose

Defines how the service is delivered and operated as a cloud-hosted workload: the runtime contract that lets instances scale horizontally without coordination, how configuration and secrets reach instances, and how releases and load tests are run.

## ADDED Requirements

### Requirement: Stateless runtime instances
The system SHALL hold no subscription state that must survive or be shared between instances, so that instances can be added or removed without migrating connections or reconciling state.

#### Scenario: Instance added
- **WHEN** a new instance joins the serving set
- **THEN** it begins receiving newly accepted connections and requires no state transfer from existing instances

#### Scenario: Instance removed
- **WHEN** an instance leaves the serving set
- **THEN** no coordination with other instances is required for correctness of delivery to the connections it held

### Requirement: Containerized delivery
The system SHALL ship as a container image that runs without manual preparation beyond declared configuration, and SHALL expose the port its protocols are served on.

#### Scenario: Instance starts from configuration only
- **WHEN** an instance starts with its declared configuration
- **THEN** it is ready to serve without additional manual steps

#### Scenario: Health is reported
- **WHEN** an orchestrator probes the instance
- **THEN** the instance reports whether it is ready to accept connections and work

### Requirement: Configuration distribution
The system SHALL take tenant quotas, limits, moderation rules, feature switches, and degradation levels from a configuration source outside the running instance, and SHALL apply changes to live instances without a restart.

#### Scenario: Configuration change reaches live instances
- **WHEN** an administrator changes a quota or a moderation rule
- **THEN** instances serving that tenant pick up the change within the configured propagation interval without restarting

#### Scenario: Instance holds no durable configuration
- **WHEN** an instance restarts
- **THEN** it reconstructs its configuration from the configuration source rather than from local state

### Requirement: Secret handling
The system SHALL obtain credential signing material from a secret source rather than from image contents or command-line arguments, and SHALL keep administrative access to that source restricted.

#### Scenario: Instance obtains signing material
- **WHEN** an instance starts
- **THEN** it reads its signing material from the configured secret source

#### Scenario: Material is absent from the image
- **WHEN** the built image is inspected
- **THEN** it contains no signing material

### Requirement: Release with graceful handover
The system SHALL be deployable while subscribers are connected, so that a release moves traffic away from retiring instances and back to fresh ones without an outage or a synchronized reconnect.

#### Scenario: Release during an active stream
- **WHEN** a release occurs while subscribers are connected
- **THEN** instances being retired drain first, subscribers reconnect to fresh instances, and no bulk simultaneous disconnection occurs

#### Scenario: Traffic shifts gradually
- **WHEN** a release adds fresh instances and retires old ones
- **THEN** new connections are routed to the fresh instances while existing connections are moved only through the drain path

### Requirement: Coordinated configuration for gradual rollout
The system SHALL let an operator enable a configuration or behavior change for a stated fraction of instances before applying it to all, so that a change can be rolled back by reverting that fraction.

#### Scenario: Partial rollout
- **WHEN** a change is released to a configured fraction of instances
- **THEN** only those instances apply it and the remainder continue on the previous setting

#### Scenario: Rollback
- **WHEN** a change is rolled back
- **THEN** instances return to the previous setting without requiring a rebuild

### Requirement: Region-local operation
The system SHALL be deployable as a single region for the initial release, and SHALL confine subscriber traffic to the serving region rather than requiring cross-region fanout.

#### Scenario: Release targets one region
- **WHEN** the service is released initially
- **THEN** subscribers connect to instances in that region and messages do not require crossing regions to be delivered

### Requirement: Load test tooling and capacity report
The system SHALL provide a load generator able to drive the documented connection count and publish rate, and SHALL produce a report comparing measured throughput, latency, drop rate, and resource use against the stated capacity targets.

#### Scenario: Documented target is reproducible
- **WHEN** the load generator is run at the documented target connection count and platform publish rate
- **THEN** it sustains that load long enough for the service's steady-state behavior to be measured

#### Scenario: Report states conformance
- **WHEN** a load test completes
- **THEN** the report states whether each capacity target was met and gives the observed maximum sustainable load