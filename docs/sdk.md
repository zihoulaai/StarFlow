# Web SDK

```bash
npm install @starflow/push
```

## Quick start

```ts
import { PushClient } from '@starflow/push';

const client = new PushClient({
  url: 'wss://gateway.example.com/ws',
  appKey: 'ak_...',
  token: '...',
  channels: ['room:1234'],
  callbacks: {
    onMessage: (channel, message) => {
      console.log(channel, message.body);
    },
    onGap: (channel, gap) => {
      // The service dropped content for this connection. Render a discontinuity
      // rather than silently showing a truncated stream.
      console.warn(`dropped ${gap.dropped} messages`);
    },
    onError: (err) => console.warn(err.reason, err.message),
  },
});

await client.connect();
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `url` | required | WebSocket endpoint. |
| `appKey` | required | Application key. |
| `token` | required | Token authenticating this client. |
| `channels` | — | Channels to restore on connect. |
| `resume` | `true` | Offer a prior transport session on reconnect. |
| `backoff.initialMs` | 500 | First retry delay. |
| `backoff.maxMs` | 30000 | Ceiling for the retry delay. |
| `backoff.jitterRatio` | 0.5 | Fraction of the interval randomised, 0–1. |
| `backoff.maxAttempts` | 10 | Attempts before giving up. |
| `upgradeTimeoutMs` | 10000 | How long to wait for the upgrade. |
| `heartbeatIntervalMs` | 25000 | Liveness probe interval. |
| `callbacks` | — | See below. |

The URL is used exactly as supplied. **The SDK never rewrites a secure scheme to
plaintext and never falls back to one** when a secure connection fails, because a
silent downgrade would encrypt nothing while appearing to work.

## Callbacks

| Callback | Signature | When |
|---|---|---|
| `onMessage` | `(channel, message) => void` | A message arrives on a subscribed channel. |
| `onGap` | `(channel, gap) => void` | The service reports dropped content for this connection. |
| `onStateChange` | `(state) => void` | The connection state changes. |
| `onError` | `(error) => void` | Authentication failure, server refusal, or transport failure. |

**All are optional.** Registering no `onGap` is a supported choice: some
applications prefer a silently shorter stream to a visible gap.

## Connection states

```
idle → connecting → authenticating → ready
                                ↓
                            reconnecting ↔ ready
                                ↓
                         failed | closing → closed
```

`ready` means authenticated **and** the initial subscriptions are in place. The
`connect()` promise resolves at that point, not at socket open.

## Behaviours worth knowing

### Reconnection is jittered, and that is the point

When a gateway restarts, every connection it held drops at the same instant. With
fixed retry intervals the whole population reconnects together, and the fresh
instance receives a thundering herd on top of the load it was just started to
serve.

The delay doubles on each consecutive failure and is randomised by
`jitterRatio` around the base. **Lowering the jitter ratio towards 0 trades
recovery fairness for a more predictable first retry**, which is usually the
wrong trade under a mass reconnect.

`backoff.maxAttempts` bounds the retry budget. On exhaustion the client enters
`failed` and reports a `retry-exhausted` error; it does not keep trying.

### Subscriptions are restored automatically

The SDK tracks its own channel set and replays it **in the handshake** on every
reconnect. Restoring twenty channels one round trip at a time would cost twenty
sequential latencies — on a 50 ms path, a second before the client is usable
again.

Subscribing before `connect()` also works: the channels are recorded and sent
with the first handshake rather than being lost.

### A refused channel is reported, not silently dropped

```ts
client.subscribe(['room:ok', 'room:full']);
// -> onError with reason 'server-refused' for room:full
```

The refused channel is removed from the SDK's set so the application can show
"room full" instead of waiting for a stream that never arrives.

### Authentication failures are terminal

A rejected credential is **not retried**. It cannot become valid by retrying, and
hammering the gateway with a bad token is how one misconfigured client becomes a
load event. The client enters `failed` with reason `authentication`.

### Client-initiated close is not retried

`close()` stops reconnecting. An application that closed the connection has said
what it wanted, and reconnecting anyway would be surprising and would leak a
connection per close.

### Drain notices reconnect immediately

When the service reports that an instance is retiring, the client reconnects at
once rather than waiting for the next backoff tick. Otherwise a release strands
every subscriber for up to the first retry interval.

### Session resumption

With `resume: true` (the default) the client offers the session handle it
received, so a reconnect avoids a full cryptographic handshake. The field is
omitted on a first connection, so the service can distinguish "no session to
resume" from "resumption declined" and fall back to a full handshake.

This matters most in mobile WebViews and corporate proxies, where the handshake
may be the dominant cost of a reconnect.

### Silent drops are detected by probing

An intermediary can drop a connection without a close frame. The SDK probes at
`heartbeatIntervalMs` and reconnects when no answer arrives within twice that.
Without the probe the client would sit on a dead socket reporting `ready`, and
the application would see a stream that had quietly stopped.

### An upgrade timeout covers intermediaries that accept and stall

Some proxies complete the TCP connection and then never finish the WebSocket
upgrade. `upgradeTimeoutMs` bounds that wait, so the client reports a timeout
rather than waiting indefinitely with no signal to the application.

### Callbacks cannot break the client

A callback that throws, blocks, or is absent cannot take the connection down.
Errors from a message callback are reported through `onError` and the connection
continues — a slow synchronous callback must not be able to deadlock the client
out of noticing it has been dropped.

## Backoff tuning

| Symptom | Setting |
|---|---|
| Reconnects feel synchronised, instances fall over on restart | Raise `jitterRatio` |
| Clients give up too early during a long outage | Raise `maxAttempts` |
| First retry feels too slow | Lower `initialMs` |
| Too much load on a struggling instance | Raise `initialMs` |

`jitterRatio: 0` gives fixed delays and loses the spread entirely. `1` gives full
jitter, which can retry almost immediately. The default of `0.5` spreads
reconnects across the interval while keeping the exponential growth on average.