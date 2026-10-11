package observability

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// stubRates is a MatchRates whose figures the test sets directly, so the
// monitor's behaviour is exercised without waiting out a real counting window.
type stubRates struct {
	mu        sync.Mutex
	current   map[string]float64
	baseline  map[string]float64
	publishes map[string]uint64
}

func newStubRates() *stubRates {
	return &stubRates{
		current:   map[string]float64{},
		baseline:  map[string]float64{},
		publishes: map[string]uint64{},
	}
}

func (s *stubRates) set(tenantID string, current, baseline float64, publishes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current[tenantID] = current
	s.baseline[tenantID] = baseline
	s.publishes[tenantID] = publishes
}

func (s *stubRates) MatchRate(tenantID string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current[tenantID]
}

func (s *stubRates) Baseline(tenantID string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.baseline[tenantID]
}

func (s *stubRates) Publishes(tenantID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishes[tenantID]
}

func (s *stubRates) Tenants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.current))
	for id := range s.current {
		out = append(out, id)
	}
	return out
}

// breachingMetrics returns metrics whose P99 latency is over budget.
//
// The latency sketch is a log-bucketed histogram, so one slow sample reports the
// upper bound of the bucket it fell into rather than its own duration. Enough
// samples have to land in a bucket above the budget before the reported P99
// crosses it, which is the sketch behaving conservatively rather than a test
// having to work around it.
func breachingMetrics() *Metrics {
	m := NewMetrics(time.Now)
	for i := 0; i < 200; i++ {
		m.ObservePublish("t1")
		m.ObserveLatency(400 * time.Millisecond)
	}
	return m
}

// recorder collects what the monitor emitted.
type recorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *recorder) sink() AlertSink {
	return func(line string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.lines = append(r.lines, line)
	}
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lines...)
}

// The wiring has to produce a line an operator can act on. A condition that
// fires but names neither its severity nor its scope is a log entry, not an
// alert.
func TestMonitorEmitsAnAlertLineForABreach(t *testing.T) {
	rec := &recorder{}
	metrics := breachingMetrics()

	mon := NewMonitor(MonitorConfig{
		Metrics:    metrics,
		Evaluator:  NewEvaluator(time.Now, time.Minute),
		Sink:       rec.sink(),
		InstanceID: "i7",
	})
	mon.Evaluate()

	lines := rec.all()
	if len(lines) != 1 {
		t.Fatalf("emitted %d lines, want 1: %v", len(lines), lines)
	}
	line := lines[0]
	for _, want := range []string{"delivery_latency_breach", "severity=critical", "scope=instance/i7"} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q does not name %q", line, want)
		}
	}
}

// A breach that persists is one incident. Re-evaluating must not turn it into a
// line per interval, which is the difference between a page and a flood of log
// entries nobody reads.
func TestMonitorDoesNotRefireAHeldAlert(t *testing.T) {
	rec := &recorder{}
	metrics := breachingMetrics()

	mon := NewMonitor(MonitorConfig{
		Metrics:   metrics,
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Sink:      rec.sink(),
	})
	for i := 0; i < 5; i++ {
		mon.Evaluate()
	}
	if got := len(rec.all()); got != 1 {
		t.Errorf("emitted %d lines for one sustained breach, want 1", got)
	}
}

// The match-rate spike is judged per tenant, so one tenant's flood must not
// mark another's as already reported.
func TestMonitorScopesMatchRateAlertsToTheirTenant(t *testing.T) {
	rec := &recorder{}
	rates := newStubRates()
	// t1 spikes against a non-zero baseline; t2 sits at its own steady rate.
	rates.set("t1", 0.50, 0.02, 5000)
	rates.set("t2", 0.02, 0.02, 5000)

	mon := NewMonitor(MonitorConfig{
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Rates:     rates,
		Sink:      rec.sink(),
	})
	mon.Evaluate()

	lines := rec.all()
	if len(lines) != 1 {
		t.Fatalf("emitted %d lines, want exactly the one tenant that spiked: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "scope=tenant/t1") {
		t.Errorf("line %q does not name the tenant that spiked", lines[0])
	}
}

// A tenant at a steady rate is a policy question, not an incident. Firing here
// would train an operator to ignore the alert.
func TestMonitorIgnoresAStableMatchRate(t *testing.T) {
	rec := &recorder{}
	rates := newStubRates()
	rates.set("t1", 0.10, 0.09, 5000)

	mon := NewMonitor(MonitorConfig{
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Rates:     rates,
		Sink:      rec.sink(),
	})
	mon.Evaluate()

	if lines := rec.all(); len(lines) != 0 {
		t.Errorf("emitted %v for a steady rate, want nothing", lines)
	}
}

// The sample guard exists so a 90% rate over three messages is not an incident.
func TestMonitorIgnoresAMatchRateSpikeFromTooFewPublishes(t *testing.T) {
	rec := &recorder{}
	rates := newStubRates()
	rates.set("t1", 0.90, 0.01, 3)

	mon := NewMonitor(MonitorConfig{
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Rates:     rates,
		Sink:      rec.sink(),
	})
	mon.Evaluate()

	if lines := rec.all(); len(lines) != 0 {
		t.Errorf("emitted %v from a sample of 3 publishes, want nothing", lines)
	}
}

// A sustained spike is one event, and clearing requires the same dwell the
// instance conditions use, so a rate sitting on the factor does not flap.
func TestMonitorHoldsAndThenClearsAMatchRateAlert(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	rec := &recorder{}
	rates := newStubRates()
	rates.set("t1", 0.50, 0.02, 5000)

	mon := NewMonitor(MonitorConfig{
		Evaluator:  NewEvaluator(func() time.Time { return now }, time.Minute),
		Rates:      rates,
		Sink:       rec.sink(),
		Hysteresis: 2 * time.Minute,
		Now:        func() time.Time { return now },
	})

	mon.Evaluate()
	mon.Evaluate()
	if got := len(rec.all()); got != 1 {
		t.Fatalf("emitted %d lines for a spike evaluated twice, want 1", got)
	}

	// The rate recovers but has not been clear long enough.
	now = now.Add(time.Minute)
	rates.set("t1", 0.02, 0.02, 5000)
	mon.Evaluate()
	mon.Evaluate()
	if got := len(rec.all()); got != 1 {
		t.Fatalf("emitted %d lines after a brief recovery, want the held alert to stay at 1", got)
	}

	// Clear for longer than the hysteresis window: it may fire again if the rate
	// spikes a second time, which is a real second incident.
	now = now.Add(3 * time.Minute)
	mon.Evaluate()
	rates.set("t1", 0.50, 0.02, 5000)
	mon.Evaluate()
	if got := len(rec.all()); got != 2 {
		t.Errorf("emitted %d lines, want a second alert for a spike after the alert cleared", got)
	}
}

// Run must return when its context is cancelled, or shutdown waits on a loop
// that will not stop.
func TestMonitorRunStopsOnContextCancel(t *testing.T) {
	mon := NewMonitor(MonitorConfig{
		Metrics:   NewMetrics(time.Now),
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Sink:      func(string) {},
		Interval:  time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		mon.Run(ctx)
		close(stopped)
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of its context being cancelled")
	}
}

// Run evaluates before waiting, so a breach present at startup is reported at
// startup rather than one interval later.
func TestMonitorRunEvaluatesImmediately(t *testing.T) {
	rec := &recorder{}
	metrics := breachingMetrics()

	mon := NewMonitor(MonitorConfig{
		Metrics:   metrics,
		Evaluator: NewEvaluator(time.Now, time.Minute),
		Sink:      rec.sink(),
		Interval:  time.Hour, // long enough that only the immediate pass can fire
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mon.Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for len(rec.all()) == 0 {
		select {
		case <-deadline:
			t.Fatal("Run did not evaluate before its first interval elapsed")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done
}

// The service has no alerting back end, so the sink is the whole delivery
// mechanism. It has to actually write, and to prefix lines so they are
// attributable among everything else on stderr.
func TestStderrSinkWritesOneLinePerAlert(t *testing.T) {
	var buf strings.Builder
	StderrSink(&buf)("alert drop_rate_high severity=warning scope=instance drop rate 0.100")
	StderrSink(&buf)("alert heartbeat_loss severity=warning scope=instance")

	got := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(got) != 2 {
		t.Fatalf("wrote %d lines, want 2: %q", len(got), buf.String())
	}
	for i, line := range got {
		if !strings.HasPrefix(line, "gateway: alert ") {
			t.Errorf("line %d = %q, want a gateway-prefixed alert", i, line)
		}
	}
}
