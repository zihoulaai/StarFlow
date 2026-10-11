package observability

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// AlertCondition is a rule that fires on a metric breaching a bound.
type AlertCondition struct {
	// Name identifies the alert in the emitted record.
	Name string
	// Scope is what the alert is about: a tenant, a channel, or the instance.
	Scope string
	// Breached reports whether the alert should fire for this snapshot.
	Breached func(Snapshot) bool
	// Severity is informational, warning, or critical.
	Severity string
}

// Alert is a fired condition.
type Alert struct {
	Condition AlertCondition
	// Value is the figure that breached, for the alert text.
	Value string
	At    time.Time
}

// alertRule is the set of conditions the service evaluates.
//
// Each one exists because a specific incident looks different from the others in
// the numbers. A latency breach with normal drop rates is a capacity problem; the
// same latency with a slow-subscriber spike is a client problem, and paging
// someone to add capacity for the second would be the wrong response.
//
// It is unexported because each condition carries a Breached closure: an
// exported slice would let any importer invert or disable an alert for every
// evaluator in the process.
var alertRule = []AlertCondition{
	{
		Name:     "delivery_latency_breach",
		Scope:    "instance",
		Severity: "critical",
		Breached: func(s Snapshot) bool {
			return s.PublishesAccepted > 0 && s.LatencyP99Ms > LatencyBudgetP99Ms
		},
	},
	{
		Name:     "drop_rate_high",
		Scope:    "instance",
		Severity: "warning",
		Breached: func(s Snapshot) bool {
			return s.DropRate > DropRateBudget
		},
	},
	{
		Name:     "slow_subscribers",
		Scope:    "instance",
		Severity: "warning",
		Breached: func(s Snapshot) bool {
			return s.SlowSubscribers > SlowSubscriberThreshold
		},
	},
	{
		Name:     "heartbeat_loss",
		Scope:    "instance",
		Severity: "warning",
		Breached: func(s Snapshot) bool {
			return s.HeartbeatsLost > HeartbeatLossThreshold
		},
	},
	{
		Name:     "connection_failure_rate",
		Scope:    "instance",
		Severity: "warning",
		Breached: func(s Snapshot) bool {
			// The sample floor matters more here than for any other condition:
			// one failed handshake out of one attempt is a 100% rate, and
			// paging on that would train an operator to ignore the alert.
			return s.ConnAttempts >= ConnFailureMinAttempts &&
				s.ConnFailureRate > ConnFailureRateBudget
		},
	},
}

// Budgets, stated so the alert bounds and the documented targets cannot drift
// apart silently.
const (
	// LatencyBudgetP99Ms is the P99 publish-to-delivery budget.
	LatencyBudgetP99Ms = 200.0
	// DropRateBudget is the tolerated fraction of lost content.
	DropRateBudget = 0.05
	// SlowSubscriberThreshold is how many stalled subscribers are tolerated
	// before it is worth an operator's attention.
	SlowSubscriberThreshold = 10
	// HeartbeatLossThreshold is how many connections lost to liveness before it
	// suggests a network problem rather than individual clients.
	HeartbeatLossThreshold = 100
	// ConnFailureRateBudget is the fraction of connection attempts that may
	// fail before it looks abnormal rather than like the background rate of
	// stale SDKs and misconfigured clients.
	ConnFailureRateBudget = 0.10
	// ConnFailureMinAttempts is the sample size below which the failure rate is
	// not trusted. Without it a single failed attempt is a 100% rate.
	ConnFailureMinAttempts = 20
)

// MatchRateAlert fires on a sensitive word match rate that spikes.
//
// A sudden rise is the signature of a coordinated flood rather than ordinary
// traffic, and it is the one moderation signal worth alerting on: a steady rate
// is a policy question, a spike is an incident.
type MatchRateAlert struct {
	// Baseline is the recent match rate.
	Baseline float64
	// Current is the rate now.
	Current float64
	// TenantID is the scope.
	TenantID string
	// MinimumPublishes guards against a spike computed from a handful of
	// messages, where any rate is meaningless.
	MinimumPublishes uint64
}

// Fired reports whether the match rate spiked.
func (a MatchRateAlert) Fired() bool {
	if a.Current < MatchRateSpikeFloor {
		return false
	}
	if a.MinimumPublishes < MatchRateMinimumPublishes {
		return false
	}
	return a.Current >= a.Baseline*MatchRateSpikeFactor
}

// Match-rate alert thresholds.
const (
	// MatchRateSpikeFactor is how far above baseline the rate must be.
	MatchRateSpikeFactor = 5.0
	// MatchRateSpikeFloor is an absolute floor, so a baseline of zero does not
	// make any non-zero rate a spike.
	MatchRateSpikeFloor = 0.01
	// MatchRateMinimumPublishes is the sample size below which the rate is not
	// trusted.
	MatchRateMinimumPublishes = 100
)

// Evaluator fires alerts from snapshots.
type Evaluator struct {
	mu sync.Mutex

	fired map[string]time.Time
	now   func() time.Time
	// hysteresis is how long a condition must stay clear before its alert is
	// considered resolved. Without it an alert flaps on a metric sitting exactly
	// on its bound.
	hysteresis time.Duration
}

// NewEvaluator returns an evaluator.
func NewEvaluator(now func() time.Time, hysteresis time.Duration) *Evaluator {
	if now == nil {
		now = time.Now
	}
	if hysteresis <= 0 {
		hysteresis = 2 * time.Minute
	}
	return &Evaluator{fired: map[string]time.Time{}, now: now, hysteresis: hysteresis}
}

// Evaluate returns the alerts firing for this snapshot.
//
// A condition that has been firing is not returned again until it clears, so a
// sustained breach produces one alert rather than one per scrape.
func (e *Evaluator) Evaluate(s Snapshot) []Alert {
	now := e.now()
	var out []Alert

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, c := range alertRule {
		key := c.Name + "|" + c.Scope
		if c.Breached(s) {
			if _, already := e.fired[key]; already {
				continue
			}
			e.fired[key] = now
			out = append(out, Alert{Condition: c, Value: describe(c, s), At: now})
			continue
		}
		// Resolved, after the hysteresis window.
		if at, ok := e.fired[key]; ok && now.Sub(at) >= e.hysteresis {
			delete(e.fired, key)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Condition.Name < out[j].Condition.Name })
	return out
}

// Active returns the names of conditions currently held.
func (e *Evaluator) Active() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.fired))
	for k := range e.fired {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func describe(c AlertCondition, s Snapshot) string {
	switch c.Name {
	case "delivery_latency_breach":
		return fmt.Sprintf("p99 %.0fms over the %.0fms budget", s.LatencyP99Ms, LatencyBudgetP99Ms)
	case "drop_rate_high":
		return fmt.Sprintf("drop rate %.3f over the %.3f budget", s.DropRate, DropRateBudget)
	case "slow_subscribers":
		return fmt.Sprintf("%d subscribers stalled, threshold %d", s.SlowSubscribers, SlowSubscriberThreshold)
	case "heartbeat_loss":
		return fmt.Sprintf("%d connections lost to liveness, threshold %d", s.HeartbeatsLost, HeartbeatLossThreshold)
	case "connection_failure_rate":
		// The scope line names a tenant only when one holds the bulk of the
		// failures. An operator reading "instance" knows to look at scanning
		// or at clients pointed at the wrong gateway; reading a tenant name
		// tells them whose credentials to rotate.
		if s.FailingTenant != "" {
			return fmt.Sprintf("%d of %d attempts failed (%.1f%%), concentrated on tenant %s",
				s.ConnFailures, s.ConnAttempts, s.ConnFailureRate*100, s.FailingTenant)
		}
		return fmt.Sprintf("%d of %d attempts failed (%.1f%%), not concentrated on one tenant",
			s.ConnFailures, s.ConnAttempts, s.ConnFailureRate*100)
	default:
		return c.Name
	}
}

// Trace is one retained sample of a message's path through the service.
type Trace struct {
	MessageID uint64
	TenantID  string
	Channel   string
	// Delivered counts subscribers reached.
	Delivered int
	// Dropped counts subscribers that lost it.
	Dropped int
	// Class is the priority the message was admitted at.
	Class uint8
	At    time.Time
}

// TraceStore retains a sampled subset of traces.
//
// Sampling is not an optimisation here, it is the reason tracing is possible at
// all: retaining every message would make the retention itself a limit on
// throughput, and the cost would land on the delivery path that tracing is meant
// to explain.
type TraceStore struct {
	mu sync.Mutex
	// rate is the fraction of messages traced, from 1 meaning all.
	rate float64
	// max bounds retention so a burst cannot exhaust memory.
	max    int
	traces []Trace
	seq    uint64
	now    func() time.Time
	seen   map[uint64]int
}

// NewTraceStore returns a store retaining max traces at the given rate.
func NewTraceStore(rate float64, max int, now func() time.Time) *TraceStore {
	if now == nil {
		now = time.Now
	}
	if rate <= 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &TraceStore{rate: rate, max: max, now: now, seen: map[uint64]int{}}
}

// ShouldTrace reports whether this message should be traced.
func (s *TraceStore) ShouldTrace(id uint64) bool {
	if s.rate <= 0 {
		return false
	}
	if s.rate >= 1 {
		return true
	}
	// The decision is derived from the message id rather than drawn randomly,
	// so the same message is traced on every instance that sees it and a
	// replay cannot manufacture extra trace volume.
	threshold := uint64(s.rate * 65536)
	h := id * 2654435761
	h ^= h >> 16
	return uint32(h)&0xFFFF < uint32(threshold)
}

// Record stores a trace, replacing any earlier partial record for the same
// message. A message is recorded once and updated as delivery proceeds, so the
// trace is a path rather than a series of disconnected events.
func (s *TraceStore) Record(t Trace) {
	if s.rate <= 0 {
		return
	}
	if !s.ShouldTrace(t.MessageID) {
		return
	}
	t.At = s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.seen[t.MessageID]; ok {
		s.traces[idx] = t
		return
	}
	if len(s.traces) >= s.max {
		// Bounded retention: the oldest is discarded. A trace is a diagnostic
		// aid, not a record, so losing the oldest under a burst is acceptable
		// where growing without bound is not.
		delete(s.seen, s.traces[0].MessageID)
		s.traces = s.traces[1:]
	}
	s.traces = append(s.traces, t)
	s.seen[t.MessageID] = len(s.traces) - 1
}

// Trace returns a trace by message id.
func (s *TraceStore) Trace(id uint64) (Trace, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.seen[id]
	if !ok {
		return Trace{}, false
	}
	return s.traces[idx], true
}

// Len returns the number of retained traces.
func (s *TraceStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.traces)
}

// Rate returns the configured sampling rate.
func (s *TraceStore) Rate() float64 { return s.rate }
