# Admin API

Every operation here changes what the data plane does, so two properties are
load-bearing throughout:

- **Every call is authorized** against a named principal *within one tenant*.
- **Every change is attributable** — who, when, against what.

An administrative change that cannot be traced is indistinguishable from a bug.

The surface binds **separately from the publish and subscriber ports**, so it can
be kept off the public network without affecting data-plane routing.

## Identifying the caller

Two headers, both required:

| Header | Meaning |
|---|---|
| `X-Tenant-Id` | The tenant being acted on. |
| `X-Admin-Subject` | The principal making the call. |

The tenant comes from the request, never from the caller. A principal granted in
tenant A cannot act on tenant B by naming it: authorization is resolved as
`(tenant, subject)` and a principal with no grant in that tenant is refused.

## Roles

| Role | May read | May change |
|---|---|---|
| `viewer` | yes | no |
| `admin` | yes | yes |

Splitting read from write is what makes it safe to give an operator visibility
into a busy instance without giving them the ability to disable a tenant's
authentication.

An unknown principal returns **401**. A known principal without the required
role returns **403**. The two are distinct: one means "who are you", the other
means "not you".

## Endpoints

| Method | Path | Role | Purpose |
|---|---|---|---|
| `GET` | `/admin/tenants` | viewer | List tenants. |
| `POST` | `/admin/applications` | admin | Create an application; returns its secret **once**. |
| `GET` | `/admin/applications` | viewer | List the tenant's applications. |
| `GET` | `/admin/applications/{id}` | viewer | Read one application. |
| `PATCH` | `/admin/applications/{id}` | admin | Enable or disable an application. |
| `POST` | `/admin/applications/{id}/rotate` | admin | Rotate the secret; returns the new one **once**. |
| `GET` | `/admin/quotas/{tenant}` | viewer | Read the tenant's ceilings. |
| `PUT` | `/admin/quotas/{tenant}` | admin | Replace the tenant's ceilings. |
| `GET` | `/admin/channels/{channel}` | viewer | Read a channel's effective limits. |
| `PATCH` | `/admin/channels/{channel}` | admin | Set subscriber ceiling and publish ceiling. |
| `GET` | `/admin/connections` | viewer | List live connections. |
| `GET` | `/admin/connections/{id}` | viewer | Read one connection. |
| `DELETE` | `/admin/connections/{id}` | admin | Terminate one connection. |
| `GET` | `/admin/degradation` | viewer | Read the ladder position. |
| `PUT` | `/admin/degradation` | admin | Pin or release the ladder. |

## Effects worth knowing before calling

**Disabling an application** ends its live connections and refuses new
authentication. The audit entry records `disabled; live connections ended`, so
an operator investigating a dropped client later can see why.

**Rotating a secret** returns the new secret once and never again. The previous
digest is retained with **no grace window**, so the rotated-out secret stops
verifying immediately. Rotation is recorded as such.

**Lowering a connection ceiling** below current usage refuses new connections and
**does not end existing ones**. An operator reducing a limit should be able to do
so without inadvertently disconnecting everyone already connected. The change is
recorded, because "I lowered it and nothing happened" and "the change never
applied" are otherwise indistinguishable.

**Terminating a connection** is not a ban. The client may reconnect and is
subject to the normal authentication and rate checks. The audit entry says so,
because the two are routinely confused.

**A channel's publish ceiling** is reported with a `ceiling_override` flag
distinguishing an explicit override from the derived value. An operator who
cannot tell those apart cannot explain a channel's behaviour.

**Setting a degradation level out of range** is refused with 400 rather than
clamped, so a typo does not silently select the wrong rung. Levels are 0
(normal) through 3 (critical).

## Audit trail

Each mutating call writes one entry:

| Field | Meaning |
|---|---|
| `tenant_id` | The tenant acted on. |
| `principal` | Who made the change. |
| `operation` | A stable name, **not** the URL, so a route rename does not fragment the trail. |
| `target` | What was changed. |
| `detail` | Operator-facing context. **Never a secret.** |
| `at` | When. |

**Reads are not audited as changes.** Otherwise the trail cannot answer what
changed, which is the only question it exists to answer.

## Audit operations

| Operation | Recorded when |
|---|---|
| `tenant.create` | A tenant is created. |
| `tenant.update` | A tenant is renamed. |
| `application.create` | An application is created. |
| `application.set_enabled` | An application is enabled or disabled. |
| `application.rotate_secret` | A secret is rotated. |
| `quota.set` | Ceilings are replaced. |
| `channel.set_limits` | A channel's limits change. |
| `connection.terminate` | A connection is closed. |
| `degradation.set` | The ladder is pinned or released. |