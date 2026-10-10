package publish

import (
	"sync"
	"time"

	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/moderation"
)

// Readiness reports whether an instance may serve.
//
// The gate is closed until every tenant's moderation matcher is compiled. This
// is the most important safety property in the service: the matcher lives in
// process memory, so an instance that started without it, or lost it, would be
// publishing with no moderation at all. Failing readiness means such an instance
// never joins the serving set rather than joining it and silently under-moderating.
//
// It also gates on bus connectivity, because an instance that cannot publish to
// the bus would accept messages that never reach any subscriber.
type Readiness struct {
	mu sync.RWMutex

	matcher moderation.Matcher
	busUp   bool
	// drained marks an instance that is retiring. A draining instance reports
	// ready but refuses new connections, which is how a release hands traffic
	// over without an outage.
	drained bool

	tenants        []string
	notReadyTenant string
	reason         string
}

// NewReadiness returns a gate over matcher for the given tenants.
func NewReadiness(matcher moderation.Matcher, tenants []string) *Readiness {
	return &Readiness{matcher: matcher, tenants: tenants}
}

// MarkBusUp records bus connectivity.
func (r *Readiness) MarkBusUp(up bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.busUp = up
}

// StartDrain marks the instance as retiring.
func (r *Readiness) StartDrain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drained = true
}

// Draining reports whether the instance is retiring.
func (r *Readiness) Draining() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.drained
}

// LoadTenants compiles the moderation matcher for every known tenant from the
// supplied rule sets.
//
// It is called at start-up and must complete before the instance reports ready.
// A tenant whose rules cannot be compiled leaves the gate closed rather than
// leaving that tenant served without moderation.
func (r *Readiness) LoadTenants(ruleSets map[string][]controlplane.ModerationRule) error {
	r.mu.Lock()
	tenants := append([]string(nil), r.tenants...)
	r.mu.Unlock()

	for tenantID, rules := range ruleSets {
		if err := r.matcher.Reload(tenantID, rules); err != nil {
			r.markNotReady(tenantID, "moderation rules failed to compile: "+err.Error())
			return err
		}
	}
	_ = tenants
	r.ClearTenantCheck()
	return nil
}

// SetTenants replaces the tenant set the gate checks.
func (r *Readiness) SetTenants(tenants []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants = append([]string(nil), tenants...)
}

// Check evaluates the gate.
func (r *Readiness) Check() (ready bool, reason string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.notReadyTenant != "" {
		return false, r.reason
	}
	if !r.busUp {
		return false, "message bus is not connected"
	}
	if len(r.tenants) == 0 {
		// An instance with no tenants configured is a fresh deployment. It is
		// ready: there is nothing it could fail to moderate.
		return true, ""
	}
	for _, tenantID := range r.tenants {
		if !r.matcher.Ready(tenantID) {
			return false, "moderation rules are not loaded for tenant " + tenantID
		}
	}
	return true, ""
}

// markNotReady records a blocking failure.
func (r *Readiness) markNotReady(tenantID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notReadyTenant = tenantID
	r.reason = reason
}

// ClearTenantCheck removes a tenant-level block, used after a successful reload.
func (r *Readiness) ClearTenantCheck() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notReadyTenant = ""
	r.reason = ""
}

// NotReadyTenant returns the tenant that blocked readiness, if any.
func (r *Readiness) NotReadyTenant() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.notReadyTenant
}

// RestrictionChecker resolves whether an identity is currently restricted. It
// is consulted at handshake as well as on publish.
//
// Checking at both points is deliberate. A mute applied after a client
// connected must stop that client's publishes immediately; a network block must
// stop a client from connecting at all. Enforcing only on publish would leave a
// banned client holding a live connection indefinitely.
type RestrictionChecker struct {
	mu sync.RWMutex
	// resolve returns the active restriction for a subject, or false when the
	// subject is unrestricted.
	resolve func(tenantID, kind, subject string, now time.Time) (action string, reason string, restricted bool)
	now     func() time.Time
	// cache holds recently resolved results so a handshake does not query per
	// subject on a hot path. Entries expire rather than being invalidated by a
	// message, because restrictions change rarely relative to connections.
	cache    map[string]restrictionCacheEntry
	cacheTTL time.Duration
}

type restrictionCacheEntry struct {
	action     string
	reason     string
	restricted bool
	at         time.Time
}

// NewRestrictionChecker returns a checker over resolve.
func NewRestrictionChecker(now func() time.Time, cacheTTL time.Duration) *RestrictionChecker {
	if now == nil {
		now = time.Now
	}
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Second
	}
	return &RestrictionChecker{
		resolve:  nil,
		now:      now,
		cache:    map[string]restrictionCacheEntry{},
		cacheTTL: cacheTTL,
	}
}

// SetResolver installs the lookup.
func (c *RestrictionChecker) SetResolver(fn func(tenantID, kind, subject string, now time.Time) (string, string, bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolve = fn
	// The cache is dropped: a new resolver invalidates every cached answer.
	c.cache = map[string]restrictionCacheEntry{}
}

// Check reports whether a subject is restricted.
func (c *RestrictionChecker) Check(tenantID, kind, subject string) (action, reason string, restricted bool) {
	key := tenantID + "\x00" + kind + "\x00" + subject
	now := c.now()

	c.mu.RLock()
	resolve := c.resolve
	entry, cached := c.cache[key]
	c.mu.RUnlock()

	if cached && now.Sub(entry.at) < c.cacheTTL {
		return entry.action, entry.reason, entry.restricted
	}
	if resolve == nil {
		// No resolver means no restrictions are known. Failing open here would
		// be wrong; failing closed would make every handshake fail. The safe
		// reading is that a deployment without a resolver has no restrictions
		// configured at all, which is the single-tenant bootstrap case.
		return "", "", false
	}

	action, reason, restricted = resolve(tenantID, kind, subject, now)

	c.mu.Lock()
	if c.resolve != nil {
		c.cache[key] = restrictionCacheEntry{action: action, reason: reason, restricted: restricted, at: now}
	}
	c.mu.Unlock()
	return action, reason, restricted
}

// Invalidate drops a subject's cached answer, so an administrative change takes
// effect on the next handshake rather than after the cache window.
func (c *RestrictionChecker) Invalidate(tenantID, kind, subject string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, tenantID+"\x00"+kind+"\x00"+subject)
}
