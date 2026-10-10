package auth

import (
	"sync"
	"time"
)

// ShardedLimiter enforces a platform-wide ceiling from N instances without any
// shared counter.
//
// Each instance admits up to configured/instances. The aggregate therefore sits
// at the configured total, apart from a bounded transient after an instance
// joins or leaves, during which every instance is still using its previous
// share. That transient is accepted rather than engineered away: making it exact
// would mean a shared counter, which puts a network round trip on the publish
// admission path, and that is the one thing the data-plane split forbids.
//
// The error is bounded and one-directional. After a scale-out from N to N+k the
// aggregate can briefly reach configured x (N+k)/N, so the safety factor
// configured here is what keeps an instance count change from overshooting into
// a tenant's real limit.
type ShardedLimiter struct {
	mu            sync.RWMutex
	configured    float64
	instanceCount int
	safetyFactor  float64

	windowStart time.Time
	windowCount int64
}

// NewShardedLimiter returns a limiter for one instance of a fleet of
// instanceCount. A configured rate of zero means unlimited.
func NewShardedLimiter(configured float64, instanceCount int, safetyFactor float64, now time.Time) *ShardedLimiter {
	if instanceCount < 1 {
		instanceCount = 1
	}
	if safetyFactor <= 0 {
		safetyFactor = 1
	}
	return &ShardedLimiter{
		configured:    configured,
		instanceCount: instanceCount,
		safetyFactor:  safetyFactor,
		windowStart:   now,
	}
}

// LocalShare returns the rate this instance admits, after the safety factor.
//
// The share is recomputed on every instance-count change rather than
// distributed, because there is no other place to compute it: agreeing on a
// split would require the coordination this design exists to avoid.
func (l *ShardedLimiter) LocalShare() float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.configured <= 0 {
		return 0
	}
	return l.configured / float64(l.instanceCount) / l.safetyFactor
}

// SetInstanceCount updates the fleet size and resets the current window.
//
// The window is reset rather than scaled in place: the previous window was
// accumulated against the old share, so carrying it over would let one
// instance keep the old, larger allowance for a whole window after the fleet
// grew around it.
func (l *ShardedLimiter) SetInstanceCount(n int, now time.Time) {
	if n < 1 {
		n = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.instanceCount = n
	l.windowStart = now
	l.windowCount = 0
}

// SetConfigured replaces the tenant's configured ceiling.
func (l *ShardedLimiter) SetConfigured(rate float64, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.configured = rate
	l.windowStart = now
	l.windowCount = 0
}

// InstanceCount reports the fleet size this limiter is dividing against.
func (l *ShardedLimiter) InstanceCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.instanceCount
}

// Allow reports whether one more unit may be admitted, and consumes it if so.
//
// The window is a fixed one-second bucket rather than a sliding log: a sliding
// window would need per-request history, and at publish rates in the thousands
// per second that bookkeeping costs more than the accuracy is worth.
func (l *ShardedLimiter) Allow(n int, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.configured <= 0 {
		return true
	}
	if now.Sub(l.windowStart) >= time.Second {
		l.windowStart = now
		l.windowCount = 0
	}

	share := l.configured / float64(l.instanceCount) / l.safetyFactor
	limit := int64(share)
	if l.windowCount >= limit {
		return false
	}
	l.windowCount += int64(n)
	return true
}

// Used reports how much of the current window has been consumed.
func (l *ShardedLimiter) Used() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.windowCount
}

// TenantLimiter holds one ShardedLimiter per tenant plus its connection ceiling.
type TenantLimiter struct {
	mu       sync.RWMutex
	byTenant map[string]*tenantLimiters
	fleet    int
	safety   float64
	now      func() time.Time

	// instances is this instance's own connection count, per tenant. The
	// ceiling is compared against a locally observed count because connection
	// state is not shared between instances.
	instances map[string]int
}

type tenantLimiters struct {
	connections *ShardedLimiter
	publish     *ShardedLimiter
}

func newTenantLimiters(publishRate float64, connCeiling float64, n int, safety float64, now time.Time) *tenantLimiters {
	return &tenantLimiters{
		connections: NewShardedLimiter(connCeiling, 1, 1, now),
		publish:     NewShardedLimiter(publishRate, n, safety, now),
	}
}

// NewTenantLimiter returns a limiter for a fleet of instanceCount instances.
func NewTenantLimiter(instanceCount int, safetyFactor float64, now func() time.Time) *TenantLimiter {
	if instanceCount < 1 {
		instanceCount = 1
	}
	if now == nil {
		now = time.Now
	}
	return &TenantLimiter{
		byTenant:  map[string]*tenantLimiters{},
		instances: map[string]int{},
		fleet:     instanceCount,
		safety:    safetyFactor,
		now:       now,
	}
}

// Configure installs or replaces a tenant's ceilings.
func (l *TenantLimiter) Configure(tenantID string, connCeiling, publishRate int, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.byTenant[tenantID] = newTenantLimiters(float64(publishRate), float64(connCeiling), l.fleet, l.safety, now)
}

// AllowPublish reports whether the tenant may publish one more message.
func (l *TenantLimiter) AllowPublish(tenantID string, now time.Time) bool {
	l.mu.RLock()
	t, ok := l.byTenant[tenantID]
	l.mu.RUnlock()
	if !ok {
		// An unconfigured tenant is unlimited rather than refused: configuration
		// arrival is not the same event as the first publish.
		return true
	}
	return t.publish.Allow(1, now)
}

// AllowConnection reports whether one more connection may be admitted.
//
// The check is against this instance's observed count, not a global one. Each
// instance admits its own share of the tenant's ceiling, so lowering the
// ceiling below current usage refuses new connections while leaving existing
// ones untouched, which is what an operator reducing a ceiling expects.
func (l *TenantLimiter) AllowConnection(tenantID string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	current := l.instances[tenantID]
	t, ok := l.byTenant[tenantID]
	if !ok {
		// An unconfigured tenant is admitted rather than refused: the arrival of
		// configuration is not the same event as the first connection.
		l.instances[tenantID] = current + 1
		return true
	}

	ceiling := t.connections.LocalShare()
	if ceiling > 0 && float64(current)+1 > ceiling {
		return false
	}
	l.instances[tenantID] = current + 1
	return true
}

// ReleaseConnection returns a slot after a connection closes.
func (l *TenantLimiter) ReleaseConnection(tenantID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := l.instances[tenantID]; n > 0 {
		l.instances[tenantID] = n - 1
	}
}

// LocalConnections reports this instance's observed connection count.
func (l *TenantLimiter) LocalConnections(tenantID string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.instances[tenantID]
}

// SetInstanceCount rebalances every tenant's publish share after a fleet change.
func (l *TenantLimiter) SetInstanceCount(n int, now time.Time) {
	if n < 1 {
		n = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fleet = n
	for _, t := range l.byTenant {
		t.publish.SetInstanceCount(n, now)
	}
}
