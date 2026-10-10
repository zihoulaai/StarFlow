# Tasks

## 1. Project foundation

- [x] 1.1 Initialize the Go module with a gateway, control-plane, and shared-protocol layout; verify `go build ./...` succeeds and `go vet ./...` reports nothing
- [x] 1.2 Add runtime dependencies (WebSocket, Aho-Corasick, bus client, database driver, metrics export); verify `go mod tidy` leaves a clean `go.mod` and `go mod verify` passes
- [x] 1.3 Define the versioned wire protocol for the WebSocket frame encoding as a single binary frame shared by all subscribers; verify a round-trip encode/decode test passes and the format admits no per-subscriber variation
- [x] 1.4 Add typed configuration loading with explicit memory limits and GC settings from design.md; verify a test asserts an unset or invalid `GOMEMLIMIT` fails startup rather than falling back to a default

## 2. Control plane persistence and configuration distribution

- [x] 2.1 Create the relational schema for tenants, applications, credentials, channels, moderation rules, bans, network blocks, and audit entries; verify migration applies cleanly to an empty database and re-running is a no-op
- [x] 2.2 Implement repositories for tenants, applications, and credentials with tenant-scoped queries; verify every read path has a test proving a caller cannot retrieve another tenant's row
- [x] 2.3 Implement repositories for moderation rules, bans, and network blocks with effective-window filtering; verify a test proves an expired mute stops blocking and a permanent ban does not
- [x] 2.4 Implement audit entry writes and tenant-scoped retrieval, with message body redaction per tenant setting; verify tests assert an entry is written per moderation decision and that a cross-tenant query returns nothing
- [x] 2.5 Implement configuration publication over pub/sub carrying quota, limit, rule, and switch changes; verify a subscriber test proves a change published to one process reaches another within the configured propagation interval
- [x] 2.6 Implement graceful config reload on each gateway so a change applies without restart; verify a test observes the new value taking effect on a live gateway and asserts the process was not restarted
- [x] 2.7 Document the control-plane data model and the configuration propagation contract in `docs/control-plane.md`; verify every entity and field named in the doc appears in the schema

## 3. Access control

- [x] 3.1 Implement AppKey/Secret issuance and rotation for applications under a tenant; verify tests prove a token signed with a rotated-out Secret is rejected
- [x] 3.2 Implement token issuance and verification with expiry and skew-window enforcement; verify tests cover valid, expired, future-skew, and bad-signature tokens, and that each rejection is audited
- [x] 3.3 Implement publish/subscribe privilege separation on the credential; verify tests prove a subscribe-only credential is refused by the publish endpoint and a publish-only credential is refused subscriptions
- [x] 3.4 Implement per-tenant connection, publish-rate, and channel-count ceilings; verify tests prove each ceiling refuses at its boundary and that a lowered connection ceiling refuses new connections without terminating existing ones
- [x] 3.5 Implement per-instance quota sharding where each instance enforces `configured / instance_count`, with rebalancing on instance-count change; verify a multi-instance test shows the aggregate stays within the configured ceiling apart from the bounded transient
- [x] 3.6 Implement TLS-only enforcement with a configurable plaintext-endpoint kill switch, and origin allowlist enforcement that distinguishes browser clients from non-browser clients; verify tests cover plaintext refusal, disallowed origin, absent Origin, and the disabled-endpoint case

## 4. Channel and subscription model

- [x] 4.1 Implement tenant-scoped channel addressing with implicit creation on first use; verify a test proves publishing to an unknown channel succeeds and creates no registration record
- [x] 4.2 Implement the subscription store using contiguous subscriber slices per channel; verify a benchmark asserts the cost of traversing a 1,000-subscriber channel is within 10x of the equivalent slice traversal baseline and that no hash map appears on the path
- [x] 4.3 Implement subscribe and unsubscribe as idempotent operations affecting only the named channel; verify tests prove withdrawal stops delivery, leaves other subscriptions intact, and is idempotent when repeated
- [x] 4.4 Implement batch subscription at connect time with per-channel acceptance reporting; verify a test proves 20 channels restore in a single handshake with no extra round trips, and that over-allowance channels are rejected individually while the connection survives
- [x] 4.5 Implement subscriber-count tracking per channel with a configurable ceiling; verify a test proves new subscribers are refused past the ceiling with a clear error rather than degrading the instance
- [x] 4.6 Derive per-channel publish ceilings as `subscribers x receive-rate-target / merge-ratio` and recompute on subscriber change; verify tests cover a small channel constrained at its ceiling, a large channel granted proportionate headroom, and recomputation after a subscriber joins and leaves
- [x] 4.7 Implement per-channel configuration for publish ceiling, receive-rate target, and coalescing toggle; verify a test proves a raised ceiling admits previously refused publishes and that disabling coalescing sends messages individually

## 5. Publish path and moderation

- [x] 5.1 Implement the HTTP publish endpoint returning per-message outcome without waiting for delivery; verify a test asserts the response arrives before subscriber delivery completes
- [x] 5.2 Implement message envelope assignment of identifier and server timestamp, honouring publisher-supplied TTL; verify tests prove identifiers are unique and that an expired TTL stops delivery
- [x] 5.3 Implement size validation with a 4 KB default; verify tests cover over-limit rejection, exact-limit acceptance, and a configured non-default limit
- [x] 5.4 Implement the ordered admission pipeline (permission, rate limits, duplicate suppression, moderation, size) reporting which stage refused a message; verify a test covers a message passing rate limits but failing moderation and asserts it never reaches the fanout path
- [x] 5.5 Implement bounded per-publisher duplicate suppression keyed on publisher and channel; verify tests prove a retry within the window is suppressed with the original identifier and that identical content after the window is admitted
- [x] 5.6 Implement the three publish failure policies (discard, degrade, fail-fast) per tenant and apply the configured one; verify a test per policy asserting its observable outcome under over-limit publishing
- [x] 5.7 Implement the Aho-Corasick sensitive word automaton held in process memory with pub/sub-driven invalidation; verify a test updates the word list and proves the new rule applies on a live gateway without restart
- [x] 5.8 Rebuild the automaton from the control plane on startup and fail the readiness probe closed until the rebuild completes; verify a test asserts an instance without a rebuilt automaton never reports ready
- [x] 5.9 Implement block, replace, and tag enforcement actions configurable per rule and per tenant; verify a test per action asserting the exact content subscribers receive
- [x] 5.10 Implement per-user mute, permanent ban, and IP blocking enforced at both handshake and publish admission; verify tests prove the block applies to the named identity only and that lifting a ban restores publishing
- [x] 5.11 Implement the per-user danmaku rate limit as a distinct outcome from tenant and channel limits, aggregated across a user's connections; verify a test proves the same user publishing from two connections shares one limit
- [x] 5.12 Implement duplicate-content and promotional-pattern filtering; verify tests cover both rules and their configured actions
- [x] 5.13 Document the publish API, message envelope, admission ordering, and every refusal outcome code in `docs/publishing.md`; verify each documented outcome code has a matching test and that no code exists without a scenario

## 6. Fanout delivery core

- [x] 6.1 Define a bus interface that hides the concrete implementation and publish admitted messages to it; verify a contract test suite passes against an in-process fake and documents the Redis Pub/Sub binding
- [x] 6.2 Implement gateway-side bus consumption that receives every message and selects only locally subscribed channels; verify a test proves a message reaches a subscriber on a different instance than the publishing one, exactly once
- [x] 6.3 Implement serialize-once delivery where the message is serialized into a shared immutable buffer and copied per connection; verify a benchmark asserts serializations per delivered message is one and that allocations per delivered message is zero on the delivery path
- [x] 6.4 Implement per-connection bounded buffers allocated once at handshake and never grown on demand; verify a test asserts memory per connection stays within the designed budget and that overflow discards rather than allocates
- [x] 6.5 Implement dirty-shard registration on publish and flush over dirty lists only; verify a benchmark asserts flush cost scales with delivered traffic rather than connection count by comparing runs at 10k and 100k idle connections
- [x] 6.6 Implement coalescing that merges messages arriving within a window into one packet preserving content and order, delivers on window expiry without waiting, and leaves total delivered bytes unchanged; verify tests cover each case plus the per-channel bypass
- [x] 6.7 Implement per-connection egress accounting against the 2 KB/s per-viewer budget with an alert threshold; verify a test proves the accounting drives the degradation trigger and reports the configured budget as the reference
- [x] 6.8 Add a fanout benchmark harness that drives a configurable connection count, fanout factor, and merge ratio and reports packets/s, bytes/s, and allocation counts; verify it runs at target load and prints the three figures

## 7. Connection lifecycle

- [x] 7.1 Implement periodic liveness exchange with a configurable timeout that releases subscriptions on failure; verify a test proves an unresponsive connection closes and its subscriptions are released, and that liveness cost scales with connection count not message volume
- [x] 7.2 Implement slow-subscriber detection that bounds per-connection queued data and isolates the slow subscriber without affecting healthy subscribers of the same channel; verify a test proves healthy subscribers' delivery rate and latency are unchanged while a stalled subscriber is dropped
- [x] 7.3 Implement overflow discard keeping the most recent content, since danmaku value decays with time; verify a test proves the newest messages survive a sustained overflow
- [x] 7.4 Implement delivery gap notification sent before the next delivered content after any drop; verify a test proves the notification precedes subsequent content and identifies the drop
- [x] 7.5 Implement graceful drain: refuse new connections, notify subscribers, wait, then close; verify a test proves subscribers reconnect to another instance and resume their subscriptions
- [x] 7.6 Verify QoS0 semantics end to end: no persistence, no acknowledgement exchange, no offline retrieval after reconnect, and duplicates and reordering permitted; verify an integration test asserts each of these four properties

## 8. Degradation ladder

- [x] 8.1 Define degradation levels L0 through L3 spanning coalescing-window increase, sampling, and shedding all but high-priority classes; verify a test enumerates each level's configured behavior
- [x] 8.2 Implement automatic escalation on CPU or egress saturation with a dwell time so momentary spikes do not degrade; verify a test proves a brief spike holds the current level while a sustained one escalates
- [x] 8.3 Implement automatic restoration when load returns below threshold; verify a test proves the level returns to normal and delivery behavior resumes
- [x] 8.4 Expose the ladder through the administrative surface and record automatic transitions in the operational log; verify a test proves an operator-set level takes effect and that automatic transitions are recorded with their cause
- [x] 8.5 Measure the achievable throughput span from L0 to L3 under the benchmark harness; verify the measurement is recorded and that the span is at least 5x, failing the task if it is not

## 9. Administrative surface

- [x] 9.1 Implement tenant and application administration including disable and credential rotation; verify tests prove disabling blocks new authentication while ending existing connections, and rotation invalidates the previous Secret
- [x] 9.2 Implement channel administration exposing subscriber count and effective limits with live adjustment; verify a test proves a lowered limit applies to subsequent publishes without restart
- [x] 9.3 Implement live connection listing with tenant, channel set, and state, plus per-connection termination; verify a test proves termination releases only that connection's subscriptions and that the client may reconnect subject to normal checks
- [x] 9.4 Implement moderation rule, mute, ban, and network block administration with runtime effect; verify tests prove each change applies without restart and that lifting a ban restores publishing
- [x] 9.5 Implement quota and rate limit configuration with runtime effect; verify a test proves lowering a connection ceiling below current usage refuses new connections without terminating existing ones
- [x] 9.6 Implement administrative authorization and attribution, recording principal, time, and target for every administrative action; verify tests prove an unauthorized call is refused and audited, and that a permitted change is attributable
- [x] 9.7 Document every administrative endpoint with its authorization requirement, parameters, and observable effects in `docs/admin-api.md`; verify each documented endpoint has an authorization test

## 10. Observability

- [x] 10.1 Expose live connection count, publish rate, delivery rate, and the derivable fanout factor, scoped per tenant and platform-wide; verify a test asserts both rates are reported so the factor is derivable, and that tenant scope excludes other tenants
- [x] 10.2 Expose publish-to-delivery latency as a distribution with median, 95th, and 99th percentile rather than an average; verify a test asserts all three are queryable
- [x] 10.3 Expose drop rate, refused-publish rate by reason, current slow-subscriber count, and outbound bandwidth; verify a test asserts drops are attributed to slow consumer, rate limit, or degradation distinctly
- [x] 10.4 Aggregate metrics in-process and export periodically so no synchronous store call occurs on admission or delivery; verify a test asserts no shared-store call appears on either path and that counters export correctly after a flush
- [x] 10.5 Implement alerting on latency breach, excess drop rate, abnormal connection failure rate, and moderation match-rate spikes; verify a test per alert condition asserts it fires and names its scope as tenant, channel, or instance
- [x] 10.6 Provide dashboards with scope narrowing to tenant and instance; verify the scope switch displays the selected scope rather than platform totals
- [x] 10.7 Implement sampled cross-path traceability keyed on the publish-time message identifier; verify a test follows one publish from admission through per-channel delivery counts, and that retention is sampled rather than per-message
- [x] 10.8 Document every metric with its name, type, unit, labels, and the decision it supports in `docs/observability.md`; verify each documented metric is emitted by the running service

## 11. Web SDK

- [x] 11.1 Implement connection with credential exchange, encrypted transport by default, and readiness only after authentication succeeds; verify a test asserts authentication failure is reported and the connection is never presented as ready
- [x] 11.2 Implement subscribe and unsubscribe with SDK-tracked subscription state and single-request bulk subscription; verify a test asserts many channels are sent as one request and that queried state matches the SDK's view
- [x] 11.3 Implement message callbacks invoked off the connection-handling path so a slow application callback cannot stall heartbeats or reconnect; verify a test with a deliberately blocking callback asserts the connection stays healthy
- [x] 11.4 Implement the gap callback invoked when the service reports dropped content; verify a test asserts the application is notified and that an unregistered gap callback causes no error
- [x] 11.5 Implement automatic reconnection with growing backoff plus random jitter, skipped on client-initiated close; verify a test asserts attempts spread across the window and that intervals increase across consecutive failures
- [x] 11.6 Implement transport session resumption with fallback to full handshake; verify a test asserts resumption within the window and correct fallback when resumption is refused
- [x] 11.7 Implement connection-state and error callbacks covering authentication failure, server refusal with reason, and terminal loss after the retry budget; verify a test per case asserts the correct reason is surfaced
- [x] 11.8 Implement weak-network tolerance including upgrade timeouts and detection of silently dropped connections via liveness exchange; verify a test simulates each condition and asserts recovery
- [x] 11.9 Add SDK unit tests for all of the above and a browser-based integration test against a running gateway; verify the integration test observes a real message delivered and a real reconnect restoring subscriptions
- [x] 11.10 Document SDK installation, initialization, the full callback surface, and reconnect and backoff tuning in `docs/sdk.md`; verify every documented option appears in the SDK's exported API

## 12. Deployment, load test, and capacity report

- [x] 12.1 Build a container image that starts from configuration alone and exposes readiness and liveness probes; verify the image starts and reports ready with no manual preparation, and contains no signing material
- [x] 12.2 Integrate secret retrieval from a secret source rather than image contents or arguments; verify the built image inspection shows no credential material
- [x] 12.3 Implement coordinated release so retiring instances drain before closing and new connections route to fresh instances; verify a release test under active load shows no bulk simultaneous disconnection
- [x] 12.4 Implement staged configuration rollout to a stated fraction of instances with rollback by reverting that fraction; verify a test proves partial application and that rollback restores the previous setting without a rebuild
- [x] 12.5 Add a multi-instance integration test asserting no duplicate and no missing delivery for a message published to subscribers spread across instances; verify it runs on every instance count used in CI
- [x] 12.6 Build the load generator driving the documented connection count and platform publish rate; verify it sustains the target long enough for steady state to be measured
- [x] 12.7 Produce the capacity report stating conformance for each target (connection count, publish rate, latency percentiles, drop rate, resource use) and the observed maximum sustainable load; verify the report is generated from a real run and states explicitly that MVP-scale evidence does not validate the multi-instance delivery path
- [x] 12.8 Measure `permessage-deflate` effect on a coalesced packet path and record the result in the capacity report, resolving the corresponding open question; verify the measurement is present with before-and-after figures

## Workflow follow-up

- Archive the change after review requirements are satisfied.
- Verify the archived result, and confirm each capability delta landed under `openspec/specs/` with its Purpose intact.