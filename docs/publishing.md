# Publish API

One endpoint. It admits a message onto the delivery path and returns the
admission outcome **without waiting for delivery**.

That last part is the design, not an omission. Waiting for delivery would make a
publisher's latency depend on every subscriber's socket, coupling the publish side
to the whole fanout path. A message that is admitted has been handed to the bus;
what happens to it afterwards is visible through traceability, not through this
response.

## Endpoint

```
POST /publish
Content-Type: application/json
```

### Request

| Field | Type | Required | Meaning |
|---|---|---|---|
| `channel` | string | yes | Destination within the caller's tenant. |
| `body` | string | yes | Opaque publisher content. |
| `ttl_ms` | integer | no | Requested lifetime. Zero uses the service default. |
| `priority` | string | no | `normal`, `high`, or `system`. Drives the degradation ladder. |
| `signature` | string | no | Client integrity value, verified against the token key. |

The tenant is **not** a request field. It is resolved from the credential, so a
publisher cannot address another tenant's channels by naming them.

### Response

| Field | Type | Meaning |
|---|---|---|
| `accepted` | boolean | Whether the message reached the delivery path. |
| `outcome` | string | The admission outcome code. Present on every response. |
| `message_id` | integer | Traceable identifier. Set on acceptance. |
| `message_time_ms` | integer | Server-assigned timestamp. |
| `stage` | string | Which pipeline stage produced a refusal. |
| `rule_id` | string | The moderation rule behind a block. |
| `detail` | string | Operator-facing context. Never content from the message. |

`outcome` is present on every response so a publisher can distinguish a rate limit
from a moderation block without parsing prose or inferring it from a status code.

## Admission order

The order is a contract, because the reported refusal depends on it.

```
1. `permission`   can this credential publish at all?
2. `rate_limit`   per-user, then tenant, then channel
3. `permission`   is this publisher muted or banned?
4. `duplicate`    is this a retry, or a user's own repetition?
5. `moderation`   does the content match a rule?
6. `size`         is the admitted content within the limit?
```

The reported `stage` is one of `permission`, `rate_limit`, `duplicate`,
`moderation`, or `size`.

Two consequences worth stating:

- The **per-user** limit runs before the tenant limit because it is the more
  specific of the two. A single flooding user should be stopped before it
  consumes the tenant's shared budget.
- **Size is checked after moderation**, against the *admitted* content. A
  replacement rule that shrinks content can therefore bring an otherwise
  oversized message inside the limit.

## Outcome codes

| Outcome | HTTP | Stage | Meaning |
|---|---:|---|---|
| `accepted` | 202 | — | Admitted for delivery. |
| `degraded` | 202 | rate_limit | Admitted in reduced form under the degrade policy. |
| `dropped` | 202 | rate_limit | Refused under the discard policy. **Deliberately success-shaped.** |
| `forbidden` | 403 | permission | The credential lacks the publish privilege. |
| `muted` | 403 | permission | The publisher is muted for the affected channels. |
| `banned` | 403 | permission | The publisher is banned. |
| `user_rate_limited` | 429 | rate_limit | The per-user publish rate was exceeded. |
| `tenant_rate_limited` | 429 | rate_limit | The tenant publish ceiling was exceeded. |
| `channel_rate_limited` | 429 | rate_limit | The channel's derived ceiling was exceeded. |
| `duplicate` | 200 | duplicate | A retry of a recent publish, or the publisher's own repeated content. |
| `blocked` | 422 | moderation | A moderation rule matched with a blocking action. |
| `too_large` | 413 | size | The admitted content exceeds the message size limit. |
| `unauthenticated` | 401 | — | The credential was rejected. |
| `bad_request` | 400 | — | Malformed request, missing channel, or oversized upload. |
| `method_not_allowed` | 405 | — | The endpoint requires POST. |
| `bus_unavailable` | 503 | — | The message was not accepted for delivery. |

### Why `dropped` returns 202

The discard policy exists to stop a flood from amplifying into retries. Returning
an error would invite exactly the retry traffic the policy suppresses. The
response still carries `outcome: "dropped"` so an operator reading the log can
see it, but the status tells a client there is nothing to retry.

## Duplicate handling

Two separate rules, with different scope:

- **Retransmission suppression** — keyed on tenant, publisher and channel, and
  the content is compared. It catches a client retrying a request whose response
  it never saw. Reporting the original `message_id` tells the client what it
  duplicated.
- **Repeated content filtering** — keyed on tenant and publisher only, ignoring
  the channel. It catches copy-paste flooding, which is a property of a user
  rather than of a request.

Both are bounded windows, not permanent: content outside the window is a new
publish. A permanent suppression would make a legitimate repeat unpublishable.

## Message size

The default ceiling is 4 KB, applied to the **admitted** content. A separate
transport-level cap rejects a body that is too large to read at all, before it is
buffered.

## Message lifetime

`ttl_ms` travels on the envelope as an absolute expiry. A gateway that receives
the message after that time drops it. This is how a publisher-requested TTL is
honoured with no persisted state — nothing is scheduled, and nothing is stored
for the gateway to look up later.

## Readiness

An instance reports ready only once every configured tenant's moderation matcher
is compiled and the bus is connected.

The matcher is held in process memory, so an instance that started without it, or
lost it, would be publishing with **no moderation at all**. The gate exists to
keep such an instance out of the serving set rather than letting it serve
under-moderated. See `internal/publish/readiness.go`.

## Traceability

`message_id` is the correlation identifier for the whole path: admission, the
envelope, per-channel delivery counts, and the audit entry. Retention is sampled
rather than per-message, because tracing every message would itself be a limit on
throughput.