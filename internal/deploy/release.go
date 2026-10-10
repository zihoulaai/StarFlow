package deploy

import (
	"sort"
	"sync"
	"time"
)

// Rollout applies a configuration change to a stated fraction of instances.
//
// Staged rollout exists so a change that turns out to be wrong is caught on a
// fraction of the fleet rather than on all of it. Rollback is therefore not a
// separate mechanism: it is the same operation with the fraction reduced, which
// is what makes reverting cheap enough to actually do.
type Rollout struct {
	mu      sync.Mutex
	applied map[string]string
	// instances is the fleet, ordered deterministically so a rollout of 25%
	// always selects the same instances. Random selection would make a rollback
	// apply to a different set than the rollout did.
	instances []string
}

// NewRollout returns a rollout over a fleet.
func NewRollout(instances []string) *Rollout {
	sorted := append([]string(nil), instances...)
	sort.Strings(sorted)
	return &Rollout{applied: map[string]string{}, instances: sorted}
}

// Apply sets key to value on the first fraction of the fleet.
//
// A fraction of zero is refused: it would report success while changing nothing,
// which reads as "the change is live and working" when it is not.
func (r *Rollout) Apply(key, value string, fraction float64) ([]string, error) {
	if fraction <= 0 || fraction > 1 {
		return nil, errInvalidFraction
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	count := int(float64(len(r.instances)) * fraction)
	if fraction > 0 && count == 0 {
		// A small fraction of a small fleet still has to reach one instance, or
		// a staged rollout silently does nothing on a two-instance deployment.
		count = 1
	}
	selected := r.instances[:count]

	for _, id := range selected {
		r.applied[id] = key + "=" + value
	}
	return append([]string(nil), selected...), nil
}

// Revert returns the instances matching a rollout back to the previous value.
//
// It is the same selection the rollout used, which is the point: rolling back to
// a different set of instances than were rolled forward would leave part of the
// fleet on the value being withdrawn.
func (r *Rollout) Revert(key string, fraction float64, previous string) ([]string, error) {
	if fraction <= 0 || fraction > 1 {
		return nil, errInvalidFraction
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	count := int(float64(len(r.instances)) * fraction)
	if fraction > 0 && count == 0 {
		count = 1
	}
	selected := r.instances[:count]
	for _, id := range selected {
		r.applied[id] = key + "=" + previous
	}
	return append([]string(nil), selected...), nil
}

// State returns an instance's current value for a key.
func (r *Rollout) State(instance, key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.applied[instance]
	if !ok {
		return "", false
	}
	want := key + "="
	if len(v) < len(want) || v[:len(want)] != want {
		return "", false
	}
	return v[len(want):], true
}

// InstancesWithValue returns the instances currently holding a value, which is
// what an operator needs to confirm a rollback actually landed everywhere.
func (r *Rollout) InstancesWithValue(key, value string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	want := key + "=" + value
	var out []string
	for id, v := range r.applied {
		if v == want {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Fleet returns the fleet in selection order.
func (r *Rollout) Fleet() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.instances...)
}

var errInvalidFraction = errInvalid{}

type errInvalid struct{}

func (errInvalid) Error() string {
	return "deploy: rollout fraction must be greater than 0 and at most 1"
}

// Release coordinates an instance retiring from the serving set.
type Release struct {
	mu sync.Mutex
	// accepting reports whether the instance is still taking new connections.
	accepting bool
	draining  bool
	drainedAt time.Time
	// grace is how long subscribers are given to reconnect elsewhere.
	grace time.Duration
	// onDrain notifies subscribers to reconnect.
	onDrain func()
	// onClose closes the remaining connections.
	onClose func() int
	now     func() time.Time
}

// NewRelease returns a release coordinator.
func NewRelease(grace time.Duration, onDrain func(), onClose func() int, now func() time.Time) *Release {
	if now == nil {
		now = time.Now
	}
	return &Release{
		accepting: true,
		grace:     grace,
		onDrain:   onDrain,
		onClose:   onClose,
		now:       now,
	}
}

// Accepting reports whether new connections are taken.
func (r *Release) Accepting() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accepting
}

// Draining reports whether the release has begun.
func (r *Release) Draining() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.draining
}

// BeginDrain stops new connections and warns subscribers to reconnect.
//
// The order matters. New connections stop first so the instance does not acquire
// work it is about to abandon, then subscribers are told to move. Both happen
// before anything is closed.
func (r *Release) BeginDrain() int {
	r.mu.Lock()
	if r.draining {
		n := 0
		r.mu.Unlock()
		return n
	}
	r.accepting = false
	r.draining = true
	r.drainedAt = r.now()
	onDrain := r.onDrain
	r.mu.Unlock()

	if onDrain != nil {
		onDrain()
	}
	return 0
}

// Finish closes the remaining connections once the grace period has elapsed.
//
// The wait is the whole point of a graceful release. Without it, ten thousand
// subscribers disconnect at the same instant and each reconnect is a full
// cryptographic handshake, so the cost of the release exceeds the cost of the
// traffic the instance was serving.
func (r *Release) Finish() (closed int, early bool) {
	r.mu.Lock()
	if !r.draining {
		r.mu.Unlock()
		return 0, false
	}
	due := r.now().Sub(r.drainedAt) >= r.grace
	onClose := r.onClose
	r.mu.Unlock()

	if !due {
		return 0, false
	}
	if onClose == nil {
		return 0, true
	}
	return onClose(), true
}

// Remaining reports how much grace time is left.
func (r *Release) Remaining() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.draining {
		return 0
	}
	left := r.grace - r.now().Sub(r.drainedAt)
	if left < 0 {
		return 0
	}
	return left
}
