package observability

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// MatchRates is the per-tenant sensitive-word counting the spike alert reads.
//
// It is declared here rather than imported from the moderation package so the
// consumer owns the interface it needs and the two packages stay independent:
// nothing in observability has to know how the counts are produced.
type MatchRates interface {
	// MatchRate is the tenant's rate in the current window.
	MatchRate(tenantID string) float64
	// Baseline is the tenant's rate over the preceding windows.
	Baseline(tenantID string) float64
	// Publishes is the current window's sample size.
	Publishes(tenantID string) uint64
	// Tenants enumerates every tenant being counted.
	Tenants() []string
}

// AlertSink receives an alert as it fires.
//
// The service has no alerting back end to send to, so this is where a page
// would be raised from. Keeping it a function rather than a concrete writer
// means the loop can be tested by capturing what it emits.
type AlertSink func(alert string)

// StderrSink returns a sink that writes one line per alert to w.
//
// Alerts go to stderr rather than the structured log because this is the
// service's only output channel that an operator or a container log collector
// is guaranteed to see, and the line is already shaped for a human to read.
func StderrSink(w io.Writer) AlertSink {
	return func(alert string) {
		fmt.Fprintf(w, "gateway: %s\n", alert)
	}
}

// Monitor evaluates conditions on an interval and reports what fires.
//
// It owns two evaluation paths because they judge different things. The
// instance conditions read a Snapshot and are held per condition. The
// match-rate spike is per tenant, reads a counter rather than a snapshot, and
// is held per tenant, because one tenant's flood is not evidence about
// another's.
type Monitor struct {
	metrics   *Metrics
	evaluator *Evaluator
	rates     MatchRates
	sink      AlertSink
	interval  time.Duration
	// hysteresis is how long a match-rate alert must stay clear before it is
	// considered resolved. It defaults to the same value the Evaluator uses so
	// the two paths behave alike.
	hysteresis time.Duration
	now        func() time.Time
	instanceID string

	mu sync.Mutex
	// rateFired holds the last time each tenant's spike alert fired, keyed by
	// tenant. Without it a sustained flood pages once per interval.
	rateFired map[string]time.Time
}

// MonitorConfig holds the monitor's collaborators.
type MonitorConfig struct {
	Metrics   *Metrics
	Evaluator *Evaluator
	// Rates may be nil, in which case the match-rate spike is not evaluated.
	Rates MatchRates
	Sink  AlertSink
	// Interval is how often conditions are evaluated. Zero uses a default.
	Interval time.Duration
	// Hysteresis is how long a match-rate alert must stay clear before it
	// resolves. Zero uses the Evaluator's default.
	Hysteresis time.Duration
	// InstanceID names this instance in alert text, so an operator reading a
	// line knows which instance raised it.
	InstanceID string
	Now        func() time.Time
}

// NewMonitor returns a monitor ready to Run.
func NewMonitor(cfg MonitorConfig) *Monitor {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	hyst := cfg.Hysteresis
	if hyst <= 0 {
		hyst = 2 * time.Minute
	}
	sink := cfg.Sink
	if sink == nil {
		sink = func(string) {}
	}
	return &Monitor{
		metrics:    cfg.Metrics,
		evaluator:  cfg.Evaluator,
		rates:      cfg.Rates,
		sink:       sink,
		interval:   interval,
		hysteresis: hyst,
		now:        now,
		instanceID: cfg.InstanceID,
		rateFired:  map[string]time.Time{},
	}
}

// Run evaluates on the interval until ctx is cancelled.
//
// It evaluates immediately before waiting, so a breach present at startup is
// reported at startup rather than one interval later.
func (m *Monitor) Run(ctx context.Context) {
	m.Evaluate()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Evaluate()
		}
	}
}

// Evaluate runs every condition once and reports what fires.
func (m *Monitor) Evaluate() {
	if m.evaluator != nil && m.metrics != nil {
		snapshot := m.metrics.Snapshot()
		for _, a := range m.evaluator.Evaluate(snapshot) {
			m.sink(m.formatAlert(a, snapshot))
		}
	}
	m.evaluateMatchRates()
}

// formatAlert renders one instance-scoped alert as a single line.
//
// The line names the alert, its severity and its scope, because a page an
// operator cannot place is a page they have to investigate before acting on.
//
// A condition whose scope depends on the snapshot gets that dependency
// substituted here rather than in the alert text: connection failures are
// instance-scoped until one tenant accounts for the bulk of them, and the scope
// field is what an operator filters on, so the tenant has to be in it and not
// only in the prose.
func (m *Monitor) formatAlert(a Alert, snapshot Snapshot) string {
	scope := a.Condition.Scope
	if a.Condition.Name == "connection_failure_rate" && snapshot.FailingTenant != "" {
		scope = "tenant/" + snapshot.FailingTenant
	}
	if m.instanceID != "" && !strings.HasPrefix(scope, "tenant/") {
		scope = fmt.Sprintf("%s/%s", scope, m.instanceID)
	}
	return fmt.Sprintf("alert %s severity=%s scope=%s %s",
		a.Condition.Name, a.Condition.Severity, scope, a.Value)
}

// evaluateMatchRates checks the moderation spike for every counted tenant.
func (m *Monitor) evaluateMatchRates() {
	if m.rates == nil {
		return
	}
	now := m.now()

	for _, tenantID := range m.rates.Tenants() {
		alert := MatchRateAlert{
			TenantID:         tenantID,
			Current:          m.rates.MatchRate(tenantID),
			Baseline:         m.rates.Baseline(tenantID),
			MinimumPublishes: m.rates.Publishes(tenantID),
		}

		m.mu.Lock()
		last, held := m.rateFired[tenantID]
		switch {
		case alert.Fired():
			if held {
				// A sustained spike is one event, not one per interval.
				m.mu.Unlock()
				continue
			}
			m.rateFired[tenantID] = now
			m.mu.Unlock()
			m.sink(m.formatMatchRateAlert(alert))

		case held && now.Sub(last) >= m.hysteresis:
			// Clear requires the same dwell as the instance conditions, so a
			// rate sitting on the factor does not flap.
			delete(m.rateFired, tenantID)
			m.mu.Unlock()

		default:
			m.mu.Unlock()
		}
	}
}

// formatMatchRateAlert renders a spike as a single line naming its tenant.
func (m *Monitor) formatMatchRateAlert(a MatchRateAlert) string {
	return fmt.Sprintf("alert moderation_match_rate_spike severity=critical scope=tenant/%s "+
		"rate %.4f against baseline %.4f over %d publishes",
		a.TenantID, a.Current, a.Baseline, a.MinimumPublishes)
}
