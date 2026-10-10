# Design

## Context

Greenfield. No existing code, no existing specs. The motivation and scope live in `proposal.md`; behavioral requirements live in the `specs/` deltas. This document covers only how.

The constraints that actually shape the implementation are numeric, and they interlock. Deriving them together is the whole job, because any one of them taken alone gives a different architecture.

## Capacity derivation

The starting assumption was 100,000 connections per instance at 10,000 QPS. That number turned out to contain two orthogonal limits that were being conflated:

```
Connection density limit  -> memory.  10万 x ~50 KB = 5 GB. Needs a 16-32 GB instance.
Throughput limit          -> NIC.3.2 Gbps = ~2M packets/s, reached long before CPU.
```

Once **per-viewer bandwidth (2 KB/s)** is fixed, instance count falls out arithmetically rather than by tuning:

```
instance egress        = connections x 2 KB/s
10万 connections       = 200 MB/s = 1.6 Gbps
                         = 50% of a 3.2 Gbps NIC
CPU at that egress      = 100k x 10 packets/s = 1M packets/s
                         = 50% of a ~2M packets/s ceiling
memory                  = 5 GB of 16 GB
```

All three axes land near 50%. That is deliberate headroom, and it means the instance count is decided by arithmetic, not by benchmark-driven guesswork.

The corollary is important for sizing: **connections are static, bandwidth is not.** The 2 KB/s figure is a per-viewer ceiling, not a mean — a viewer in a quiet channel uses a fraction of it. So the NIC is the axis that saturates first under live traffic, while connection count never moves. Overload protection therefore has to hang off bandwidth and CPU, not off connection count.

### Coalescing does not save bandwidth

This is the single most important correction to the initial model, and it invalidates a whole class of naive sizing:

```
Before merging: 100 messages/s x 200 B  = 20 KB/s per viewer
After 20:1:      5 packets/s x 4 KB     = 20 KB/s per viewer
                 ^^^^^^^^^^^^^^^^^^^^^^    identical
```

Merging reduces **packet count** (so syscall and CPU cost falls by the merge ratio) while leaving **byte count** unchanged. Per instance:

```
CPU wall:    N >= F / (2,000 x merge_ratio)
bandwidth:   N >= F / 2,000          <- unaffected by merge_ratio
```

At a 20:1 merge ratio the CPU wall is 20x further away than the bandwidth wall. Consequences:

- CPU optimization on the delivery path is close to worthless while the NIC is unsaturated. The useful optimizations are the ones that raise *packets per second per core*.
- Tuning "单机性能" means buying a bigger NIC, not writing a tighter loop.
- Overload must be relieved by raising the merge window (a latency cost) or by degrading priority, never by expecting merge alone to bring bytes down.

The corollary for over-subscription: raising merge past roughly 20:1 at 2 KB/s starts forcing content loss, because each viewer is already receiving about all the content the budget allows. Beyond that point the system is trading data, not just latency.

### Per-channel ceilings are derived from the per-viewer budget

The publish ceiling is the same quantity as the per-viewer budget, viewed from
the publish side. It has two independent per-viewer bounds:

```
bandwidth bound:   budget_bytes_per_second / nominal_message_bytes
packet-rate bound: receive_rate_target x merge_ratio

publish_ceiling = floor(min(the two))
```

Neither term involves subscriber count, and that is the point rather than an
oversight. Every subscriber of a channel receives every publish, so a viewer's
experience is determined by the channel's publish rate alone — a 1,000-viewer
channel and a 100,000-viewer channel with the same configuration admit the same
publish rate, and the larger one simply consumes proportionally more aggregate
resources while giving each viewer an identical experience.

At the design point the two bounds sit an order of magnitude apart:

```
budget bound:   2 KB/s / 200 B = 10 publishes/s
packet bound:   5 pkts/s x 20   = 100 publishes/s   <- 10x headroom, CPU is idle
```

which is the same conclusion the coalescing analysis reached: bandwidth binds,
and packet-rate optimization is buying headroom that is not currently scarce.

**A third, genuinely subscriber-dependent bound exists and is deliberately not
in the ceiling.** An instance's egress is bounded by its NIC, so a channel with
many local subscribers has less headroom than the per-viewer bounds suggest:
`nic_headroom / (local_subscribers x nominal_message_bytes)`. Folding it into the
ceiling would make the ceiling move with live load, which is confusing to
operate and awkward to test. Instance saturation is instead handled where it
belongs — by the per-connection egress meter, which enforces the budget
directly, and by the degradation ladder, which sheds load without an operator
acting. The derived ceiling is a guard that fires before those do, not the
primary control.

An earlier draft of this document derived the ceiling as
`subscribers x receive_rate / merge_ratio`. That formula was wrong by roughly
three orders of magnitude: at 100,000 subscribers it admits a rate at which each
viewer would receive 5 MB/s against a 2 KB/s budget. A test asserting that the
ceiling falls as the merge ratio rises is what surfaced the error, and it is
retained so the relationship cannot silently invert again.

## Goals / Non-Goals

**Goals:**

- Make instance count a derivable function of the per-viewer bandwidth budget, so that scaling decisions do not depend on benchmark archaeology.
- Keep the delivery hot path free of network round trips, so that the "no persistence" position is actually achievable rather than aspirational.
- Concentrate all complexity in one place — the fanout loop — and keep everything else conventional.
- Make the broadcast topology correct on day one even though MVP will not exercise it at scale, so that scaling out does not require a redesign.

**Non-Goals:**

- Reaching a large number of instances. The topology is designed so it is *correct* at many instances, not optimized for many instances.
- Proving the distributed path under real load. With 100k platform connections, MVP runs on one or two instances; the broadcast path will not be genuinely exercised.
- Any optimization that raises CPU efficiency while the NIC is the binding constraint.

## Decisions

### Broadcast to all gateways, filter locally

Every admitted message is published to a shared bus; every gateway receives every message and delivers the parts matching its own local subscriptions.

```
Rejected alternative: directed routing (channel -> owning gateway, Redis-backed
  subscription registry, connection migration on failover)

Why rejected:
  - Preserves gateway statelessness (a stated hard requirement). A registry makes
    that claim false.
  - Connection migration on owner failover means a 1,000-viewer room loses all
    of them at once, and the reconnect lands on a different owner, triggering a
    second migration. This is a real oscillation problem, not a theoretical one.
  - The bus is not the bottleneck. Bus load scales with (publish rate x instance
    count); socket writes scale with (publish rate x subscriber count). At 1,000
    QPS platform-wide and 10 instances the bus carries 10k msg/s against 1M
    socket writes/s - three orders of magnitude apart.

Accepted cost: a room cannot be isolated onto dedicated hardware, and every
instance pays filtering cost for channels it does not serve. Both are deferred.
```

### Subscribers held as slices, not maps

```
channel -> subscribers   as []*Conn (contiguous, pointer-chasing minimal)
channel -> subscribers   as map[string]*Conn  (rejected)
```

Fanout is iteration over the whole set, not lookup of one member. A hot channel with 1,000 subscribers costs roughly 2-5 µs traversed as a slice versus 50-100 µs as a map, because the map version takes a cache miss per entry. Any hash map on the delivery path is a defect.

Subscription churn costs O(n) per change. Acceptable: danmaku subscriptions are established at connect and rarely change.

### Serialize once, share the bytes

```
Rejected: per-connection serialization. 1,000 subscribers x 100 QPS = 100k
  serializations/s, each allocating. GC death on the hot path.
```

The message is serialized once into a shared immutable buffer; fanning out is one memcpy per connection. Total copy volume equals egress bytes (200 MB/s at target load), which memcpy handles in roughly 2.5% of one core.

**This is why tag-filtered subscriptions (F4.3) are excluded.** A subscriber with `tags=[vip]` and one with `tags=[]` receive different bytes, which destroys byte sharing and forces re-serialization per distinct filter set. Given the danmaku-first positioning, tags buy nothing and cost the central performance mechanism. If tag filtering is ever required, it belongs on a separate low-volume channel that opts out of byte sharing.

### Dirty-shard flush, never a full scan

```
Rejected: on each tick, walk all 100k connections to find non-empty buffers.
  100k / 1024 shards x 100 ticks/s = 10M pointless visits/s.
```

The publisher appends to a connection's buffer and registers that connection on its shard's dirty list. Flush processes only the dirty list, then truncates it. Flush cost becomes proportional to actual traffic instead of connection count, which is what lets a large idle population cost nothing.

The dirty list is append-deduplicated per flush window, so a connection receiving 50 messages in a window still appears once.

### Sharded local quota instead of a shared counter

A tenant's publish ceiling is divided across instances by count: each instance enforces `configured / instance_count`.

```
Shared counter (rejected): the admission decision becomes a network round trip on
  the hot path, which is precisely what the data-plane split forbids.
```

Correctness is approximate by construction. The error is bounded by one flush interval after a scale event, during which the aggregate could transiently exceed the tenant ceiling by a factor proportional to the instance count change. That is accepted and documented rather than engineered away — the alternative costs more than the failure mode is worth.

### Local in-memory sensitive word automaton

An Aho-Corasick automaton is held per instance and refreshed by pub/sub from the control plane.

```
Shared lookup on each publish (rejected): network round trip per publish.
Periodic file reload (rejected): propagation lag, and needs a reload path.
```

One caveat that must not be missed: an instance holds this state in memory, so losing it means publishing without moderation until it is rebuilt. The rebuild path is therefore treated as a correctness concern, not an operational detail.

### Go, specifically

Rejected: Node.js (fanout throughput roughly an order of magnitude lower, ~50 KB/connection of runtime state, so 100k connections exhausts memory on its own); Java/Netty (viable, but GC pressure on an allocation-sensitive path); Rust (2-3x better on memory density and throughput, which would allow ~3x connections per instance for the same hardware).

Go's headroom is sufficient and its cost is the strongest argument: the NIC binds long before the CPU does, so Rust's advantage buys nothing at this capacity point, and the extra instances Rust would save are the same instances the bandwidth budget already dictates. Go also keeps the team able to maintain a hot path optimized around cache locality and zero-allocation appends.

Operational settings that follow from the memory model: `GOMEMLIMIT` set explicitly with an elevated `GOGC` so GC runs at a fraction of the default frequency against a ~5 GB live heap, and a hard rule of zero allocation on the delivery path (buffers allocated once at handshake, then appended to without reallocating).

### One bus, chosen for being non-persistent

Redis Pub/Sub, behind an interface. NATS Core is an equally good fit and the interface must not leak the choice.

Kafka and Pulsar are excluded for this role: their durability is pure waste when QoS0 permits loss, and their delivery latency is worse. Notably, a message lost on the bus is equivalent to a network drop that the protocol already tolerates, so bus loss fits the existing semantics rather than violating them.

### Degradation ladder rather than more capacity

Levels L0 through L3 raise the coalescing window, raise the merge target, apply
sampling, and finally shed all but high-priority classes. Each step multiplies
achievable per-instance throughput because it cuts packet count on the dominant
cost.

The design intent is that the *span* of this ladder is wide, not that L0 is as
fast as possible. A wide span absorbs a burst that would otherwise require
capacity that cannot be added, because in a stateless topology there are no
connections to migrate: an instance that is already saturated cannot be helped by
adding an instance that receives no traffic.

**Two corrections came out of measuring it.**

First, widening the coalescing window on its own does nothing. With a fixed merge
target the window closes on the target before it reaches its deadline, so packet
count does not fall. The merge target is the lever; the window only bounds how
stale a packet may be. The first version of the ladder scaled the window and
measured no change in throughput at all.

Second, the measured span is wider than originally estimated. Against a fixed
volume of *offered* content, at 500 connections:

```
level normal     250 packets for 5000 offered (100% admitted, merge 20,  window x1)
level reduced     47 packets                      ( 76% admitted, merge 80,  window x4)
level sampled     12 packets                      ( 50% admitted, merge 200, window x10)
level critical     0 packets                      (  0% admitted, merge 400, window x20)

packet-count span L0 to L2: 20.8x
```

The figure is stated against offered content rather than admitted content on
purpose. A level that drops half the content is buying part of its reduction by
throwing content away, and normalising on a smaller denominator would hide that.

Two caveats on the number:

- The span that matters for throughput is **L0 to L2**. L3 sheds ordinary
  content outright, so a ratio taken against L3 is unbounded and measures that
  the content is gone, not that the instance got faster.
- **L3 makes a busy room go silent rather than degrade gracefully.** Ordinary
  danmaku is dropped entirely. For a live stream that may be worse than the
  outage the ladder was meant to prevent, and a rung that keeps a trickle of
  normal content between Sampled and Critical is worth considering.

Automatic escalation is triggered by CPU or egress saturation with a dwell time,
so a momentary spike does not cause a visible quality drop. Restoration is held
for a longer window than escalation, because a load oscillating around the
threshold would otherwise drive the ladder up and down at the dwell rate, which
a viewer sees as repeated stuttering.

### Drain before retire

A retiring instance rejects new connections, sends a drain notice, waits, then closes. The SDK treats the notice as an instruction to reconnect elsewhere.

Without this, any deploy is a synchronized mass disconnect: 100k reconnects at once, each requiring a full TLS handshake, which is more expensive than the steady-state traffic it was serving. Session resumption in the SDK is what keeps this affordable.

## Risks / Trade-offs

**[An instance loses its in-memory moderation automaton and accepts unmoderated publishes]** → Persist the automaton source in the control plane, rebuild on start, and have the readiness probe fail closed until the rebuild completes, so an instance without moderation never joins the serving set.

**[GPU-free CPU over-optimization wastes effort while the NIC binds]** → Treat CPU-headroom-on-paper as a non-goal; instrument egress against the per-viewer budget and optimize CPU only after egress is saturated.

**[Quota sharding overshoots the tenant ceiling during scale events]** → Accept the bounded transient, size the local share with a safety factor, and alert on observed publish rate exceeding the configured ceiling so the overshoot is visible.

**[Fanout topology is built but never validated at scale]** → Ship it correct rather than simplified, add an integration test that runs many instances and asserts no duplicate or missing delivery, and state plainly in the capacity report that MVP-scale evidence does not validate the multi-instance path.

**[Message buffers for a slow subscriber grow and exhaust memory]** → Hard per-connection buffer bound, allocated at handshake rather than grown on demand, with the oldest content discarded on overflow because danmaku value decays with time.

**[Messages have no identity-based dedup guarantee, so a client retry looks like a new message]** → Bounded per-publisher suppression window keyed on publisher and channel; document that suppression is best-effort within that window, since no persistence exists to build exact dedup on.

**[Goroutine-per-connection model degrades under 100k connections if write paths block]** → Never write from the delivery path directly; enqueue only, and let the flush path own all socket writes.

**[A room far larger than the target 1,000-viewer profile exceeds the single-instance budget]** → Detect subscription count against the configured ceiling and refuse new subscribers to that channel with a clear error rather than letting a single room degrade the instance.

**[Byte-level fanout sharing assumes a single message encoding for all subscribers]** → Encode the contract in the protocol: one binary frame format, no per-subscriber variation. The tag-filtering exclusion exists to protect this invariant.

## Migration Plan

Greenfield, so no migration. Delivery order:

1. Bring up control plane (schema, config store, moderation rules, secrets) with a single-tenant bootstrap.
2. Bring up a single gateway with subscribe and publish paths wired end to end, ignoring scale.
3. Add the bus and verify multi-instance delivery correctness before anything else scales.
4. Run the load test at documented target, then at 3x target, and record both in the capacity report.

Rollback is a deployment revert plus drain. Because instances are stateless and hold no durable delivery state, there is nothing to migrate back and no data to reconcile.

## Open Questions

- Whether per-viewer bandwidth should be a single global budget or a per-tenant configurable value. Per-tenant is likely correct (a premium customer may warrant a larger budget) but it adds a per-viewer lookup on the delivery path. Answerable without changing the specs; defaults to global with a per-tenant override if one is ever needed.
- Whether `permessage-deflate` should be on by default. Compression ratio improves sharply once coalesced packets reach ~4 KB, but it costs CPU on the exact path where CPU is otherwise idle — which suggests it is nearly free here. Needs a measurement, not a decision.
- The concrete bus. Redis Pub/Sub versus NATS Core is an operational preference (existing team expertise, already-operated infrastructure) and the interface deliberately hides it.