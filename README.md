# StarFlow

[English](README.md) | [简体中文](README.zh-CN.md)

A multi-tenant realtime push service for the web, built for the danmaku (弹幕)
workload: very high fanout per channel, very short message lifetime, one-way
broadcast.

Go service, JavaScript/TypeScript Web SDK.

**One line:** a stateless WebSocket gateway that fans a published message to
every subscriber of its channel, with quotas, moderation and a degradation
ladder, and a control plane that owns the configuration behind it.

## Contents

- [What it is not](#what-it-is-not)
- [The binaries](#the-binaries)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Testing](#testing)
- [Performance](#performance)
- [Capacity status](#capacity-status)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Documentation](#documentation)

## What it is not

The design deliberately excludes message persistence, history, offline
delivery, replay, per-message ACK, global ordering, and channel sharding. It is
**QoS0**: nothing is stored, nothing is retransmitted, loss and reordering are
permitted. That choice is what makes 100,000 connections per instance a memory
question rather than a durability question.

## The binaries

| Binary | What it does |
|---|---|
| `cmd/gateway` | Data plane and control plane in one process, which is the right default for this service's first deployments: the target instance count is small, and the control plane is already reached only through its store interface. |
| `cmd/loadgen` | Load generator. Holds a subscriber population, publishes into it, and writes a capacity report. Exits non-zero when a target is missed. |

## Quick start

Build from the vendored tree; no network access needed:

```bash
go build -mod=vendor -o bin/gateway ./cmd/gateway
go build -mod=vendor -o bin/loadgen ./cmd/loadgen
```

Run it. `GOMEMLIMIT_BYTES` is the one setting with no default — the process
refuses to start without it, because a limit that silently defaults to unlimited
fails as an out-of-memory kill with nothing in the logs to explain it:

```bash
export GOMEMLIMIT_BYTES=$((4*1024*1024*1024))
export CONTROL_DSN="starflow.db?_pragma=foreign_keys(1)"
export SECRET_SOURCE=env SECRET_REF=PUSH_TOKEN_SECRET
export PUSH_TOKEN_SECRET="$(openssl rand -hex 32)"

# Seed the first administrator. An instance with no grant has no reachable
# administrator, so this is how it becomes administrable.
export BOOTSTRAP_TENANT=t1 BOOTSTRAP_SUBJECT=ops

go run ./cmd/gateway
```

Three listeners, deliberately separate:

| Port | Surface |
|---|---|
| `:8080` | `/publish` (HTTP), `/sub` (WebSocket) |
| `:8081` | `/admin/*` |
| `:8082` | `/readyz`, `/livez`, `/metrics` |

Create a tenant and an application, then publish. The secret is returned once
and never again:

```bash
curl -s -X POST localhost:8081/admin/tenants \
  -H 'X-Tenant-Id: t1' -H 'X-Admin-Subject: ops' \
  -H 'Content-Type: application/json' -d '{"name":"demo"}'

curl -s -X POST localhost:8081/admin/applications \
  -H 'X-Tenant-Id: t1' -H 'X-Admin-Subject: ops' \
  -H 'Content-Type: application/json' \
  -d '{"name":"web","can_publish":true,"can_subscribe":true}'
# -> {"application":{"app_key":"..."},"secret":"..."}
```

```bash
curl -s -X POST localhost:8080/publish \
  -H 'X-App-Key: <app_key>' -H 'X-App-Secret: <secret>' \
  -H 'Content-Type: application/json' -d '{"channel":"room:1","body":"hello"}'
# -> {"accepted":true,"outcome":"accepted","message_id":...}
```

The response returns the admission outcome without waiting for delivery, so a
publisher's latency never depends on any subscriber's socket. `outcome` is
present on every response, including refusals, so a rate limit is
distinguishable from a moderation block without parsing prose:

| Outcome | HTTP | Meaning |
|---|---:|---|
| `accepted` | 202 | Admitted for delivery. |
| `degraded` | 202 | Admitted in reduced form under the degrade policy. |
| `dropped` | 202 | Refused under the discard policy. **Deliberately success-shaped** — an error would invite the retries the policy exists to suppress. |
| `user_rate_limited` / `tenant_rate_limited` / `channel_rate_limited` | 429 | A ceiling was exceeded. |
| `blocked` | 422 | A moderation rule matched with a blocking action. |
| `too_large` | 413 | Admitted content exceeds the limit. |
| `duplicate` | 200 | A retry of a recent publish, or the publisher's own repeated content. |
| `unauthenticated` | 401 | The credential was rejected. |

The admission order is a contract, because the reported `stage` depends on it:
permission, rate limit, mute/ban, duplicate, moderation, size. Size is checked
last against the *admitted* content, so a replacement rule that shrinks a
message can bring it inside the limit.

Then subscribe. From `sdk/`:

```ts
import { PushClient } from '@starflow/push';

const client = new PushClient({
  url: 'ws://127.0.0.1:8080/sub',
  appKey: '<app_key>',
  token: '<secret>',     // or a signed token
  channels: ['room:1'],
  callbacks: {
    onMessage: (channel, message) => console.log(channel, message.body),
    onGap: (channel, gap) => console.warn(`dropped ${gap.dropped} messages`),
  },
});

await client.connect();
```

`connect()` resolves when the client is authenticated **and** its initial
subscriptions are in place, not at socket open.

### SDK behaviours worth knowing

- **Reconnection is jittered.** A gateway restart drops every connection at the
  same instant; fixed retries would produce a thundering herd on the fresh
  instance. The delay doubles on consecutive failure and is randomised around
  the base. Lowering `jitterRatio` towards 0 trades recovery fairness for a
  predictable first retry — usually the wrong trade under a mass reconnect.
- **A refused channel is reported, not silently dropped.** `onError` fires with
  `server-refused` and the channel leaves the SDK's set, so the application can
  show "room full" instead of waiting for a stream that never arrives.
- **Authentication failure is terminal.** A bad credential is not retried. It
  cannot become valid by retrying, and hammering the gateway with it is how one
  misconfigured client becomes a load event.
- **Silent drops are detected by probing.** An intermediary can drop a
  connection with no close frame; the SDK probes at `heartbeatIntervalMs` and
  reconnects when nothing answers within twice that.

Build and test it separately — the SDK is its own package, not part of the Go
module:

```bash
cd sdk
npm install
npm test      # builds, then runs the TypeScript test suite
```

## Configuration

Every setting is an environment variable, read by `internal/config`. Validation
rejects a configuration that would run the service in a state it cannot be
diagnosed from later, and names the offending variable.

### Required

| Variable | Meaning |
|---|---|
| `GOMEMLIMIT_BYTES` | Go soft memory limit. No default. |

### Listeners

| Variable | Default |
|---|---|
| `LISTEN` | `:8080` |
| `ADMIN_LISTEN` | `:8081` |
| `READY_LISTEN` | `:8082` |
| `INSTANCE_ID` | `0` — must be unique among concurrently running gateways; it is part of every message id |

### Message bus

| Variable | Default |
|---|---|
| `BUS_KIND` | `memory` — or `redis` |
| `BUS_ADDR` | `127.0.0.1:6379` |
| `BUS_PASSWORD` | empty |
| `BUS_CHANNEL` | `push:admitted` |
| `BUS_CONFIG_CHANNEL` | `push:config` |

### Control plane and secrets

| Variable | Default |
|---|---|
| `CONTROL_DSN` | empty — a SQLite DSN |
| `CONTROL_MAX_OPEN_CONNS` | `16` |
| `CONTROL_POLL_INTERVAL` | `15s` — reconcile backstop to pub/sub propagation |
| `SECRET_SOURCE` | `env` — or `file` |
| `SECRET_REF` | `PUSH_TOKEN_SECRET` |
| `BOOTSTRAP_TENANT` / `BOOTSTRAP_SUBJECT` | empty — **required together** |
| `BOOTSTRAP_ROLE` | `admin` |

Signing material comes from a secret source, never from an argument: an argument
is visible in the process table to every other process on the host.

### Delivery

| Variable | Default | Meaning |
|---|---|---|
| `DELIVERY_FLUSH_INTERVAL` | `10ms` | The coalescing window. The latency dial. |
| `DELIVERY_MERGE_RATIO` | `20` | Coalescing target, not a guarantee. |
| `DELIVERY_WRITE_BUFFER_BYTES` | `16KB` | Per-connection egress buffer, allocated once at handshake. |
| `DELIVERY_READ_BUFFER_BYTES` | `4KB` | Per-connection control-frame read buffer. |
| `DELIVERY_COMPRESS` | `true` | permessage-deflate on delivered batches. |
| `DELIVERY_COMPRESS_MIN_BATCH` | `5` | Refused at startup below 2. |
| `DELIVERY_PER_VIEWER_BPS` | `2048` | The number the whole capacity model is derived from. |
| `DELIVERY_HEARTBEAT_INTERVAL` | `25s` | |
| `DELIVERY_HEARTBEAT_TIMEOUT` | `10s` | |
| `DELIVERY_DRAIN_GRACE` | `30s` | |
| `DELIVERY_SHARDS` | `1024` | Dirty-list shards the flush loop works over. |
| `AUTH_TOKEN_TTL` | `5m` | |
| `AUTH_TOKEN_SKEW` | `2m` | |

### Runtime

| Variable | Default |
|---|---|
| `GOGC` | `400` |
| `GOMAXPROCS` | `0` — runtime default |

## Testing

```bash
go test ./...
go vet ./...
gofmt -l ./cmd ./internal

go test -bench=. ./internal/gateway   # fanout, flush and store benchmarks
```

`cmd/loadgen` generates the capacity report. It exits non-zero when a target is
missed, so a capacity claim fails a pipeline rather than producing a document
nobody reads:

```bash
go run ./cmd/loadgen \
  --publish-url http://127.0.0.1:8080/publish \
  --tenant t1 --app-key <key> --secret <secret> \
  --connections 1000 --publish-qps 10 \
  --duration 30s --ramp 5s --report c1000.json
```

The publish rate here is 10 msg/s, not 1,000. At 200 B that is 2 KB/s per viewer,
which is exactly the per-viewer budget — so the run spends the whole bandwidth
allowance on every subscriber. 1,000 msg/s is the *platform* target and is only
reachable spread over channels; sending it to one channel asks every viewer to
take 200 KB/s.

### Container

```bash
docker build -t starflow .
```

The module tree is vendored, so the build needs no network access. The image has
no shell and no package manager, and `USER` is numeric — `nonroot` is a
distroless-specific name that fails on any base without that passwd entry, while
`65532` resolves without a lookup. The signing material is never baked in: it is
read from a secret source at start-up, so the image carries no credential.

## Performance

Measured on `internal/gateway` benchmarks, single developer machine:

| Shape | Result |
|---|---|
| Fanout, 1,000 connections, merge 1 | 881 MB/s, 6 allocs/op |
| Fanout, 1,000 connections, merge 20 | 909 MB/s, 6 allocs/op |
| Fanout, 10,000 connections, merge 20 | 569 MB/s, 14 allocs/op |
| Subscription store traversal | 1,000 ns — 1.02× a contiguous slice, 188× faster than a hash map |
| Flush pass, 1,000 idle connections | 16.2 µs |
| Flush pass, 100,000 idle connections | 16.5 µs |

Two things those numbers are evidence for. Allocations do not scale with
subscriber count — the work per message is one encode plus a memcpy per
subscriber, none of it re-encoded. And a 100× increase in idle connections costs
1.02× more per flush pass, which is what makes a large idle population free.
Both are asserted as tests, so a regression to a map, or a flush cost that grows
with connection count, fails the build.

### Compression is decided per batch, not per setting

| Batch size | Deflated / raw |
|---|---|
| 1 | **1.123** — it expands |
| 5 | 0.308 |
| 20 | **0.12** — 88% saved |

A single unmerged 200-byte message compresses to 112% of its original size,
because deflate framing costs more than the payload saves. So `DELIVERY_COMPRESS`
cannot mean "compress everything": the decision is made per batch on its real
size in `protocol.ShouldCompress`. A config-level rule keyed on `MergeRatio`
would look right and still compress exactly the batches that expand, because
`MergeRatio` is a target — a lightly loaded channel emits one-message batches
even at 20.

### Coalescing is a latency dial, not a bandwidth dial

Merging 20:1 cuts packet count by 20× and leaves byte count unchanged:

```
before merging:  100 messages/s x 200 B = 20 KB/s per viewer
after  20:1:       5 packets/s   x 4 KB = 20 KB/s per viewer
                                                    identical
```

It buys CPU and syscall headroom, never egress. On a bandwidth-bound instance,
CPU work on the delivery path is close to worthless while the NIC is
unsaturated; tuning throughput means a bigger NIC, not a tighter loop. Merely
raising the merge ratio past roughly 20:1 at 2 KB/s starts forcing content loss,
because each viewer is already receiving about all the content the budget allows.

## Capacity status

Documented targets and what has actually been measured:

| Target | Status |
|---|---|
| Connections per instance — 100,000 | **Not verified.** Measured to 10,000; the 16k ephemeral-client-port ceiling on the measuring host bounds the test, not the service. |
| Platform publish rate — 1,000 msg/s | **Not verified.** Measured at 10 msg/s, the rate the per-viewer budget allows on one channel. |
| Per-viewer bandwidth — 2 KB/s | **Verified at 1,000 connections**: 2,004 B/s. |
| Delivery latency p50/p95/p99 — 50/100/200 ms | **Partially verified.** In budget at 1,000 connections; over at 5,000 and 10,000. |
| Multi-instance delivery, exactly once | **Verified** across 1, 2 and 5 instances. |

Across every end-to-end run so far: zero dial failures, zero gap notices, every
admitted message delivered. The failures above are all latency, which is the
opposite of what a fanout path usually degrades into.

Acceptance below 100% is expected and is not a failure by itself. The discard
policy deliberately refuses publishes without telling the publisher, which
lowers the acceptance rate by design; what matters is that the refusal is
attributed, so the report separates `accepted` and `degraded` from each refusal
code.

## Architecture

```
        publishers                    subscribers
            |                              |
      POST /publish                 WebSocket /sub
            |                              |
     publish admission               WebSocket transport
     permission → rate limit           hello handshake, auth
     → mute/ban → duplicate            subscribe / unsubscribe
     → moderation → size               read loop, close handling
            |                              |
            └──────────► bus ◄─────────────┘
                    memory | redis
                         |
        every gateway receives, filters against its own
        connection store, delivers on its flush interval
                         |
              coalesce → size-check → compress? → one frame
                         |
              fanout: one encoded batch, one memcpy per socket
```

Two properties carry most of the load.

**The data plane shares no subscription state.** Gateways never shard channels
and never migrate connections. A published message is broadcast to every
gateway and filtered locally, which costs one redundant delivery per instance
that does not serve the channel and buys horizontal scaling without a discovery
mechanism.

**The hot path never touches the control plane.** Every admission check reads
from an in-memory snapshot or a process-local counter: moderation rules are
compiled into an Aho-Corasick automaton held in process memory, tenant ceilings
are enforced with per-instance counters summing to the tenant limit, and ban
checks resolve against a snapshot. The consequence worth being explicit about:
an instance that has lost its in-memory state would be publishing without
moderation until it is rebuilt, which is why an instance whose automaton is not
compiled never reports ready and never joins the serving set. Readiness fails
closed.

**Delivered frames are newline-delimited JSON, and identifiers in them are
decimal strings.** A message id holds 63 bits, which a JavaScript number rounds
silently above 2^53; emitting it as a number would let two distinct messages
reach a client looking identical. Changing it later would be a breaking wire
change, so it is asserted by a test rather than left implicit.

### Configuration propagation

An administrative write persists, then publishes a `ConfigChange` carrying the
tenant, the kind, and an optional subject — never the values. Each gateway
reloads the changed subset from the store, so a notification cannot leave a
gateway holding values that were never persisted. A periodic poll is the
backstop that guarantees convergence when the bus is lossy.

### The degradation ladder

| Level | Meaning | Admitted | Merge target |
|---|---|---:|---:|
| L0 | normal | 100% | 20 |
| L1 | reduced | 76% | 80 |
| L2 | sampled | 50% | 200 |
| L3 | critical | 0% of ordinary content | 400 |

Spanning L0 to L2 is a 20.8× reduction in packets per 5,000 offered. It is
stated against *offered* content so the levels are directly comparable. The
ladder can be pinned or released through the admin API, and no cluster
coordination is involved — each instance holds its own rung.

## Repository layout

| Path | Contents |
|---|---|
| `cmd/gateway` | The one binary that runs the service. |
| `cmd/loadgen` | Load generator and capacity report. |
| `internal/publish` | Admission pipeline and publish handler. |
| `internal/gateway` | Connection store, shard set, coalescing, fanout, flush, transport. |
| `internal/auth` | Credentials, privileges, quotas, tenant rate limiting. |
| `internal/moderation` | Aho-Corasick sensitive-word matcher and promotional rules. |
| `internal/controlplane` | Store, migrations, tenant snapshots, config reload. |
| `internal/bus` | In-memory and Redis pub/sub. |
| `internal/admin`, `internal/adminadapters` | Admin API and the adapters that bind it to the stores. |
| `internal/readiness`, `internal/observability` | Probes, metrics, alerts, tracing. |
| `internal/protocol` | Frames, ids, batch compression. |
| `sdk/` | The `@starflow/push` TypeScript SDK. |

## Documentation

| File | Contents |
|---|---|
| `docs/publishing.md` | Publish API, admission order, outcome codes, duplicate handling. |
| `docs/control-plane.md` | Data model, configuration propagation, migrations. |
| `docs/admin-api.md` | Admin surface, roles, effects worth knowing before calling. |
| `docs/observability.md` | Metrics, alerts, tracing. |
| `docs/capacity-report.md` | Derivation, measurements, conformance table. |
| `docs/sdk.md` | Web SDK options, callbacks, behaviours. |

