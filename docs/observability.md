# Observability

## The constraint that shapes this

The delivery path admits and delivers thousands of messages per second per
instance. It must not allocate and must not take a lock that could serialise a
fanout.

So **nothing in the metrics path is called synchronously from delivery.** Counters
are plain atomics read at export time. The latency histogram is a fixed-bucket
sketch updated with one atomic add. Everything is scraped on an interval.

Export reads from a snapshot. There is no parallel collector registry to keep in
step with the delivery path, because a second source of truth is a second thing
that can drift.

## Metrics

| Metric | Type | Unit | Labels | Decision it supports |
|---|---|---|---|---|
| `push_connections_live` | gauge | connections | `instance` | Whether this instance is near its connection ceiling. |
| `push_publishes_accepted_total` | counter | messages | `instance` | Publish rate admitted; the input to the fanout factor. |
| `push_publishes_refused_total` | counter | messages | `instance` | Admission rejections; which stage is refusing. |
| `push_deliveries_total` | counter | messages | `instance` | Delivery rate; **counted once per recipient**, not per batch. |
| `push_packets_total` | counter | packets | `instance` | Syscall pressure; the cost the coalescing dial moves. |
| `push_bytes_total` | counter | bytes | `instance` | Egress against the NIC budget. |
| `push_fanout_factor` | gauge | ratio | `instance` | Deliveries per accepted publish. **Below 1 means dropping.** |
| `push_channel_misses_total` | counter | messages | `instance` | Messages for channels this instance does not serve. Common under broadcast; a rising ratio to admits means a subscription problem. |
| `push_expired_total` | counter | messages | `instance` | Messages dropped because their requested lifetime passed. |
| `push_drops_total` | counter | messages | `instance`, `cause` | Content lost, by cause. |
| `push_drop_rate` | gauge | ratio | `instance` | Fraction of offered content lost. |
| `push_slow_subscribers` | gauge | connections | `instance` | Subscribers currently stalled. |
| `push_heartbeats_lost_total` | counter | connections | `instance` | Connections closed for failing liveness. |
| `push_connection_attempts_total` | counter | attempts | `instance` | Subscriber handshakes attempted, successful or not. The denominator for the failure rate. |
| `push_connection_failures_total` | counter | attempts | `instance` | Subscriber handshakes refused. A failure the credential never resolved is counted here but against no tenant. |
| `push_connection_failure_rate` | gauge | ratio | `instance` | Fraction of handshakes refused. Rises on a bad deploy, on scanning, or on a client pointed at the wrong gateway. |
| `push_delivery_latency_ms` | gauge | ms | `instance`, `quantile` | Latency distribution, not an average. |
| `push_tenant_connections_live` | gauge | connections | `instance`, `tenant` | Per-tenant connection use. |
| `push_tenant_publishes_accepted_total` | counter | messages | `instance`, `tenant` | Per-tenant publish rate. |
| `push_tenant_deliveries_total` | counter | messages | `instance`, `tenant` | Per-tenant delivery rate. |
| `push_tenant_bytes_total` | counter | bytes | `instance`, `tenant` | Per-tenant egress, for cost attribution. |

`push_drops_total` carries a `cause` label with three values:

| Cause | Meaning | Response |
|---|---|---|
| `slow_consumer` | A subscriber stopped draining and hit its buffer bound. | A client or network problem. |
| `rate_limit` | A publish was refused by a tenant, channel, or per-user ceiling. | Working as configured, unless the tenant is complaining. |
| `degradation` | The ladder shed content. | Deliberate; check the ladder position. |

**An undifferentiated drop count is useless here.** These three are different
incidents with different responses, and separating them is the reason the label
exists.

## Latency is a distribution, not an average

An average can look healthy while P99 is breaching badly — that is the entire
class of problem this metric exists to find, and an average erases it.

The sketch uses logarithmic fixed buckets with no reallocation, so an observation
costs one atomic add rather than a lock or a growing slice. Percentiles are taken
as the bucket's **upper bound rather than interpolated**, which is conservative —
the right direction for a number whose breach pages someone.

It is deliberately a sketch. Exact per-observation storage would mean a growing
slice on the delivery path, and the precision it buys is not the precision anyone
acts on.

## Alerts

| Alert | Fires when | Severity | Reading it |
|---|---|---|---|
| `delivery_latency_breach` | P99 over 200 ms with traffic flowing | critical | Capacity problem **if drop rates are normal**; a client problem **if slow-subscriber counts are also up**. |
| `drop_rate_high` | Lost content over 5% of offered | warning | Read the `cause` breakdown before acting. |
| `slow_subscribers` | More than 10 stalled connections | warning | Client or network, not capacity. |
| `heartbeat_loss` | More than 100 connections lost to liveness | warning | Network or an intermediary, not individual clients. |
| `connection_failure_rate` | More than 10% of handshakes refused, over at least 20 attempts | warning | Read the scope: a named tenant is a credential or deploy problem on that tenant; instance-wide is scanning, or clients pointed at the wrong gateway. |

The latency alert exists *because* the other signals disambiguate it. A latency
breach with normal drop rates means add capacity; the same breach with a
slow-subscriber spike means a client problem, and paging someone to add capacity
for the second would be the wrong response.

**A held alert does not re-fire.** A sustained breach produces one alert, not one
per scrape. Clearing requires the condition to stay clear for a hysteresis window,
so a metric sitting exactly on its bound does not flap.

### Where alerts go

Alerts are written to **stderr**, one line per alert, prefixed `gateway: alert`.
There is no alerting back end: the line is the whole delivery mechanism, and a
container log collector is the thing that turns it into a page.

```
alert connection_failure_rate severity=warning scope=tenant/t1 19 of 100 attempts failed (19.0%), concentrated on tenant t1
```

**Every alert names its scope**, because a page that cannot be placed is a page
that has to be investigated before it can be acted on. Most conditions are
instance-scoped and carry the instance id. Two are scoped to a tenant when one
holds the bulk of the signal:

- `connection_failure_rate` names a tenant when it accounts for at least two
  thirds of the failures; otherwise it says the failures are not concentrated,
  which points at scanning or a misconfigured client rather than at credentials.
- `moderation_match_rate_spike` is per tenant throughout, since one tenant's
  flood says nothing about another's.

### Moderation match-rate spike

Fires when a tenant's sensitive-word match rate rises to `5x` its baseline.

A steady rate is a policy question. A spike is an incident — usually a
coordinated flood. Two guards keep it honest:

- An **absolute floor** of 1%, so a baseline of zero does not make every
  non-zero rate a spike.
- A **minimum sample** of 100 publishes, so a 90% rate over three messages is not
  an incident.

## Tracing

Traces are keyed on the message identifier returned at publish time and follow
one message from admission through per-channel delivery counts.

**Tracing is sampled, and sampling is what makes tracing possible.** Retaining
every message would make retention itself a limit on throughput, and that cost
would land on the delivery path tracing exists to explain.

- The sampling decision is **derived from the message id**, not drawn randomly.
  The same message is therefore traced on every instance that sees it, and a
  replay cannot manufacture extra trace volume.
- Retention is **bounded**, discarding the oldest under a burst. A trace is a
  diagnostic aid, not a record.

## Dashboard scoping

Every series carries `instance`, and the tenant series carry `tenant` as well, so
a dashboard can be narrowed to either without the scrape having to join against
something else.

Export output is **byte-stable between scrapes**: labels are sorted and samples
within a metric are ordered by their rendered label set. A scrape that reshuffles
series with no counter having moved defeats both diffing and caching.