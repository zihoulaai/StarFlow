# Control plane

The control plane holds everything the service must not lose. None of it is read
on the message delivery path — see [Why the hot path never touches this](#why-the-hot-path-never-touches-this).

## Data model

Nine entities. Every one of them is tenant-scoped except `tenants` itself, and
every tenant-scoped table carries a `tenant_id` that is part of its primary key
or an indexed column so that a query cannot omit the filter by accident.

### `tenants`

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. Generated. |
| `name` | TEXT | Display name. |
| `audit_retention_days` | INTEGER | Default 30. Drives `RetentionCutoff`. |
| `redact_audit_bodies` | INTEGER | Default 1 (redact). Applied when an audit entry is written. |
| `created_at_ms` | INTEGER | |

### `applications`

One credential set and one privilege grant.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. |
| `tenant_id` | TEXT | References `tenants`, cascades on delete. |
| `name` | TEXT | |
| `app_key` | TEXT | Unique. The only globally-scoped identifier in the schema. |
| `secret_hash` | TEXT | SHA-256 digest. The plaintext is returned once at creation and never stored. |
| `secret_prev_hash` | TEXT | Previous digest, retained only during a rotation grace. |
| `secret_prev_valid_until_ms` | INTEGER | 0 means no grace: the rotated-out secret stops working immediately. |
| `can_publish` | INTEGER | Privilege separation lives here. |
| `can_subscribe` | INTEGER | |
| `enabled` | INTEGER | 0 ends existing connections and refuses new authentication. |
| `created_at_ms` | INTEGER | |

`app_key` is deliberately not tenant-scoped: a client presents the key and the
tenant is discovered from it. This is the only read path in the schema that is
not filtered by tenant.

### `channel_config`

Optional per-channel overrides. A channel exists implicitly from first use; this
table holds configuration *about* a channel, not the channel itself.

| Column | Type | Notes |
|---|---|---|
| `tenant_id`, `channel` | TEXT | Composite primary key. |
| `publish_ceiling_override_qps` | REAL | **NULL means derive it.** The derived value depends on subscriber count, which is gateway runtime state. |
| `receive_rate_target` | REAL | NULL means use the instance default. |
| `merge_ratio_override` | INTEGER | NULL means use the instance default. |
| `coalescing_enabled` | INTEGER | Default 1. |
| `max_subscribers` | INTEGER | 0 means no ceiling. |
| `updated_at_ms` | INTEGER | |

NULL and zero are different things here. `max_subscribers = 0` is "no ceiling";
`publish_ceiling_override_qps IS NULL` is "derive from subscriber count".

### `tenant_quotas`

| Column | Type | Notes |
|---|---|---|
| `tenant_id` | TEXT | Primary key. |
| `max_connections` | INTEGER | Refused at the boundary; lowering below current usage refuses new connections without ending existing ones. |
| `max_publish_qps` | INTEGER | The platform-wide figure the instance count is derived from. |
| `max_channels` | INTEGER | |
| `publish_failure_policy` | TEXT | `discard`, `degrade`, or `fail_fast`. |
| `per_user_publish_qps` | INTEGER | Aggregated across a user's connections. |
| `duplicate_window_ms` | INTEGER | Suppression window for retried publishes. |
| `duplicate_content_enabled` | INTEGER | |
| `promo_pattern_enabled` | INTEGER | |
| `updated_at_ms` | INTEGER | |

### `moderation_rules`

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. |
| `tenant_id` | TEXT | Cascades. |
| `kind` | TEXT | `sensitive_word` or `promotional`. |
| `pattern` | TEXT | |
| `action` | TEXT | `replace`, `tag`, or `block`. |
| `replacement` | TEXT | Used by `replace`. |
| `tag` | TEXT | Used by `tag`. |
| `enabled` | INTEGER | |
| `updated_at_ms` | INTEGER | |

### `subject_restrictions`

Mutes and bans. **The only difference between the two is the expiry window.**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. |
| `tenant_id` | TEXT | Cascades. |
| `subject_kind` | TEXT | `user` or `ip`. |
| `subject_id` | TEXT | |
| `action` | TEXT | `mute` or `ban`. |
| `reason` | TEXT | |
| `expires_at_ms` | INTEGER | **0 means no expiry — a permanent ban.** |
| `created_at_ms` | INTEGER | |

Lapsed restrictions are retained so an operator can see that a mute was applied;
`ActiveRestrictionsFor` filters on the window in the query rather than in Go.

### `network_blocks`

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. |
| `tenant_id` | TEXT | Cascades. |
| `cidr` | TEXT | |
| `reason` | TEXT | |
| `enabled` | INTEGER | |
| `created_at_ms` | INTEGER | |

### `admin_principals`

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Primary key. |
| `tenant_id`, `subject` | TEXT | Unique together. |
| `role` | TEXT | `admin` or `viewer`. Only `admin` may mutate. |
| `enabled` | INTEGER | |
| `created_at_ms` | INTEGER | |

### `audit_entries`

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER | Autoincrement. |
| `tenant_id` | TEXT | Indexed with `at_ms`. |
| `at_ms` | INTEGER | |
| `kind` | TEXT | `moderation`, `auth`, or `admin`. |
| `subject`, `channel`, `message_id`, `rule_id` | TEXT | |
| `decision` | TEXT | |
| `body` | TEXT | **Already redacted at write time.** |

Redaction happens on write rather than on read so that a mis-scoped query cannot
surface content that should not have been retained. A redacted entry stores
`[redacted]` rather than an empty string, so an auditor can tell redaction apart
from an empty body.

## Zero values vs column defaults

Every `Enabled` / `RedactAuditBodies` column defaults to 1 in the schema, and the
repository layer defaults the corresponding Go field to `true` on create. This is
deliberate and load-bearing: Go's zero value for a `bool` is `false`, so without
the repository default a caller building a struct literal would store a
disabled rule, a non-redacting tenant, or a non-functioning network block. Those
failures look like broken configuration rather than unset flags.

The same reasoning applies to `UpsertAdminPrincipal`, where the default keeps a
grant from being stored with no authority.

## Configuration propagation

An administrative write does two things, in order:

1. Persists to the table above.
2. Publishes a `ConfigChange` on the configuration bus channel.

The notification carries **the tenant, the kind, and an optional subject — never
the values.** Every receiving gateway reloads the changed subset from the store.
A notification therefore cannot leave a gateway holding values that were never
persisted, which is the failure mode of the alternative (ship the payload in the
notification) where a dropped message leaves instances permanently divergent.

```
  admin write            bus                 gateway
      |                    |                    |
      v                    |                    |
  persist  --------------> |                    |
      |                    |                    |
      v                    |                    |
  publish change --------->|------ notify ----->|
      |                    |                    v
      |                    |              reload from store
      |                    |                    |
      |                    |                    v
      |                    |              swap snapshot
```

A gateway subscribes at start-up and holds each tenant's configuration as an
immutable `TenantConfig` snapshot. Reload swaps the whole snapshot atomically, so
a request in flight keeps serving the configuration it started with rather than
observing a half-applied change.

`ControlPlanePollInterval` runs a periodic full reconcile as a backstop. Pub/sub
is the fast path; the poll is what guarantees convergence if a notification is
lost, which a lossy bus permits.

### Failure handling

A reload that fails increments a failure counter and records the error; it does
**not** stop the reloader. One transient store outage must not freeze this
instance's view of every tenant indefinitely. The readiness and alerting paths
consult the failure count.

### Propagation bound

`bus.PropagationInterval` is 2 seconds. It exists so "applies without restart" is
a checkable property: tests assert a change reaches a second subscriber inside
that bound rather than sleeping and hoping.

## Why the hot path never touches this

The publish admission pipeline consults, in order: permission, rate limits,
duplicate suppression, moderation, size. Every one of those reads from an
in-memory snapshot or a process-local counter.

- Sensitive words are compiled into an Aho-Corasick automaton held in process
  memory, rebuilt from `moderation_rules` at start-up and invalidated by the bus.
- Tenant publish ceilings are enforced with per-instance counters summing to the
  tenant's configured limit, not a shared counter.
- Ban checks resolve against a snapshot, not a query per publish.

The consequence worth being explicit about: **an instance that has lost its
in-memory state is publishing without moderation until it is rebuilt.** The
readiness probe therefore fails closed — an instance that has not rebuilt its
automaton never reports ready and never joins the serving set.

## Migrations

Forward-only. `schema_migrations` records the applied version, so calling
`Migrate` on every process start is a no-op. Each migration runs in its own
transaction, so a failing statement rolls back that migration entirely rather
than leaving a half-built schema behind.

Applied statements are never edited. A schema change is a new entry with a higher
version.