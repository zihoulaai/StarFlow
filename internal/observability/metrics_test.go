package observability

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPublishAndDeliveryCountersAreSeparate(t *testing.T) {
	m := NewMetrics(time.Now)

	m.ObservePublish("t1")
	m.ObservePublish("t1")
	m.ObservePublishRefused("t1")

	// One message to ten subscribers.
	m.ObserveDelivery("t1", 10, 2000, 1)

	s := m.Snapshot()
	if s.PublishesAccepted != 2 || s.PublishesRefused != 1 {
		t.Errorf("publishes = %d accepted, %d refused, want 2 and 1",
			s.PublishesAccepted, s.PublishesRefused)
	}
	if s.DeliveriesOut != 10 {
		t.Errorf("deliveries = %d, want 10", s.DeliveriesOut)
	}
	// The fanout factor must be derivable from the two rates, which is the only
	// reason an operator compares them.
	if got := s.FanoutFactor; got != 5 {
		t.Errorf("fanout factor = %v, want 5", got)
	}
}

func TestTenantScopeExcludesOtherTenants(t *testing.T) {
	m := NewMetrics(time.Now)

	for i := 0; i < 5; i++ {
		m.ObservePublish("t1")
		m.ObserveDelivery("t1", 3, 600, 1)
	}
	m.ObservePublish("t2")
	m.ObserveDelivery("t2", 1, 200, 1)

	a, ok := m.TenantSnapshot("t1")
	if !ok {
		t.Fatal("t1 has no snapshot")
	}
	if a.PublishesAccepted != 5 || a.DeliveriesOut != 15 {
		t.Errorf("t1 = %+v, want 5 publishes and 15 deliveries", a)
	}

	b, _ := m.TenantSnapshot("t2")
	if b.PublishesAccepted != 1 || b.DeliveriesOut != 1 {
		t.Errorf("t2 = %+v, want 1 and 1; t1's traffic leaked in", b)
	}

	// The platform total is the sum of both, and the tenant view must not
	// double-count into it.
	s := m.Snapshot()
	if s.PublishesAccepted != 6 {
		t.Errorf("platform publishes = %d, want 6", s.PublishesAccepted)
	}
	if ids := m.TenantIDs(); len(ids) != 2 {
		t.Errorf("tenant ids = %v, want 2", ids)
	}
}

// A percentile is the whole point: an average that looks healthy can coexist
// with a P99 that is breaching badly.
func TestLatencyPercentilesNotJustAverage(t *testing.T) {
	m := NewMetrics(time.Now)

	// 98 observations fast, 2 very slow. With 100 samples the 99th value is
	// still fast, so the outlier needs to be more than one to move P99; that is
	// exactly why an average alone would have hidden this.
	for i := 0; i < 98; i++ {
		m.ObserveLatency(5 * time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		m.ObserveLatency(2 * time.Second)
	}

	s := m.Snapshot()
	if s.LatencyP50Ms > 10 {
		t.Errorf("p50 = %.1fms, want near the fast observations", s.LatencyP50Ms)
	}
	if s.LatencyP99Ms <= LatencyBudgetP99Ms {
		t.Errorf("p99 = %.1fms, want it to reflect the outlier and breach the budget", s.LatencyP99Ms)
	}
	if got := m.latency.Count(); got != 100 {
		t.Errorf("observations = %d, want 100", got)
	}
}

func TestLatencySketchIsConservative(t *testing.T) {
	s := NewLatencySketch()
	// 150 ms exactly: the P99 budget is 200, so this must not breach.
	for i := 0; i < 100; i++ {
		s.Observe(150 * time.Millisecond)
	}
	_, _, p99 := s.Percentiles()
	if p99 > LatencyBudgetP99Ms {
		t.Errorf("p99 = %.0fms for a uniform 150ms load, want at or below the budget", p99)
	}
}

func TestDropsAreAttributedByCause(t *testing.T) {
	m := NewMetrics(time.Now)

	m.ObserveDrop(DropSlowConsumer, 30)
	m.ObserveDrop(DropRateLimit, 10)
	m.ObserveDrop(DropDegradation, 60)

	s := m.Snapshot()
	if s.DropsSlowConsumer != 30 || s.DropsRateLimit != 10 || s.DropsDegradation != 60 {
		t.Errorf("drops = %+v, want them attributed separately", s)
	}

	// The reasons are the point: an undifferentiated drop count cannot tell a
	// client problem from a rate limit from a deliberate degradation.
	for cause, want := range map[DropCause]uint64{
		DropSlowConsumer: 30, DropRateLimit: 10, DropDegradation: 60,
	} {
		if got := s.dropFor(cause); got != want {
			t.Errorf("%s drops = %d, want %d", cause, got, want)
		}
	}
}

func (s Snapshot) dropFor(c DropCause) uint64 {
	switch c {
	case DropSlowConsumer:
		return s.DropsSlowConsumer
	case DropRateLimit:
		return s.DropsRateLimit
	default:
		return s.DropsDegradation
	}
}

func TestSlowSubscribersAndHeartbeatLossAreVisible(t *testing.T) {
	m := NewMetrics(time.Now)
	m.SetSlowSubscribers(25)
	m.ObserveHeartbeatLost()
	m.ObserveHeartbeatLost()

	s := m.Snapshot()
	if s.SlowSubscribers != 25 {
		t.Errorf("slow subscribers = %d, want 25", s.SlowSubscribers)
	}
	if s.HeartbeatsLost != 2 {
		t.Errorf("heartbeats lost = %d, want 2", s.HeartbeatsLost)
	}
}

func TestConnectionGauge(t *testing.T) {
	m := NewMetrics(time.Now)
	for i := 0; i < 10; i++ {
		m.ConnectionOpened("t1")
	}
	for i := 0; i < 4; i++ {
		m.ConnectionClosed("t1")
	}
	if got := m.Snapshot().ConnsLive; got != 6 {
		t.Errorf("live connections = %d, want 6", got)
	}
	a, _ := m.TenantSnapshot("t1")
	if a.ConnsLive != 6 {
		t.Errorf("tenant live connections = %d, want 6", a.ConnsLive)
	}
}

// --- Alerts ---

func TestLatencyBreachFiresAndNamesItsScope(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	eval := NewEvaluator(func() time.Time { return now }, time.Minute)

	s := Snapshot{PublishesAccepted: 1000, LatencyP99Ms: 400}
	alerts := eval.Evaluate(s)
	if len(alerts) != 1 {
		t.Fatalf("alerts = %d, want 1: %+v", len(alerts), alerts)
	}
	a := alerts[0]
	if a.Condition.Name != "delivery_latency_breach" {
		t.Errorf("name = %q", a.Condition.Name)
	}
	if a.Condition.Scope == "" {
		t.Error("alert has no scope; a page must say what it is about")
	}
	if a.Condition.Severity != "critical" {
		t.Errorf("severity = %q, want critical", a.Condition.Severity)
	}
	if a.Value == "" {
		t.Error("alert carries no figure")
	}
}

// A sustained breach produces one alert, not one per scrape.
func TestAlertDoesNotRepeatWhileHeld(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	eval := NewEvaluator(func() time.Time { return now }, time.Minute)
	s := Snapshot{PublishesAccepted: 1000, LatencyP99Ms: 400}

	if len(eval.Evaluate(s)) != 1 {
		t.Fatal("first evaluation did not fire")
	}
	for i := 0; i < 10; i++ {
		if got := eval.Evaluate(s); len(got) != 0 {
			t.Fatalf("evaluation %d re-fired a held alert", i)
		}
	}
	if len(eval.Active()) != 1 {
		t.Errorf("active = %v, want one", eval.Active())
	}
}

// A metric sitting exactly on its bound would flap without hysteresis.
func TestAlertClearsOnlyAfterHysteresis(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	eval := NewEvaluator(func() time.Time { return now }, time.Minute)
	breach := Snapshot{PublishesAccepted: 1000, LatencyP99Ms: 400}
	clear := Snapshot{PublishesAccepted: 1000, LatencyP99Ms: 10}

	eval.Evaluate(breach)

	now = now.Add(30 * time.Second)
	eval.Evaluate(clear)
	if len(eval.Active()) != 1 {
		t.Error("alert cleared before the hysteresis window elapsed")
	}

	now = now.Add(2 * time.Minute)
	eval.Evaluate(clear)
	if len(eval.Active()) != 0 {
		t.Error("alert did not clear after the hysteresis window")
	}

	// And it must be able to fire again.
	if got := eval.Evaluate(breach); len(got) != 1 {
		t.Error("alert did not re-fire after clearing")
	}
}

func TestDropRateAlert(t *testing.T) {
	now := time.Now
	eval := NewEvaluator(now, time.Minute)

	// A high drop rate with drops present. DropRate is a derived field, so it is
	// populated the way a snapshot would populate it.
	s := Snapshot{
		PublishesAccepted: 100,
		DropsSlowConsumer: 50, DropsRateLimit: 50,
		DropRate: 100.0 / 200.0,
	}
	if got := eval.Evaluate(s); len(got) != 1 || got[0].Condition.Name != "drop_rate_high" {
		t.Errorf("alerts = %+v, want a drop-rate alert", got)
	}
}

func TestSlowSubscriberAndHeartbeatAlerts(t *testing.T) {
	eval := NewEvaluator(time.Now, time.Minute)

	s := Snapshot{PublishesAccepted: 100, SlowSubscribers: 50, HeartbeatsLost: 500}
	names := map[string]bool{}
	for _, a := range eval.Evaluate(s) {
		names[a.Condition.Name] = true
	}
	if !names["slow_subscribers"] {
		t.Error("slow subscriber alert did not fire")
	}
	if !names["heartbeat_loss"] {
		t.Error("heartbeat loss alert did not fire")
	}
}

// A spike computed from three messages is noise, not an incident.
func TestMatchRateAlertIgnoresSmallSamples(t *testing.T) {
	alert := MatchRateAlert{
		Baseline: 0.01, Current: 0.9, TenantID: "t1",
		MinimumPublishes: 3,
	}
	if alert.Fired() {
		t.Error("a 90% match rate over three messages fired an alert")
	}

	alert.MinimumPublishes = 100
	if !alert.Fired() {
		t.Error("a 90% match rate over a real sample did not fire")
	}
}

// A baseline of zero must not make every non-zero rate a spike.
func TestMatchRateAlertRespectsAbsoluteFloor(t *testing.T) {
	alert := MatchRateAlert{Baseline: 0, Current: 0.001, MinimumPublishes: 1000}
	if alert.Fired() {
		t.Error("a 0.1% match rate against a zero baseline fired")
	}
}

func TestMatchRateAlertNeedsProportionalRise(t *testing.T) {
	// Twice the baseline is not a spike.
	if (MatchRateAlert{Baseline: 0.10, Current: 0.20, MinimumPublishes: 1000}).Fired() {
		t.Error("a doubling of the match rate fired")
	}
	// Five times is.
	if !(MatchRateAlert{Baseline: 0.02, Current: 0.15, MinimumPublishes: 1000}).Fired() {
		t.Error("a fivefold rise did not fire")
	}
}

// --- Tracing ---

func TestTraceFollowsAMessageAcrossItsPath(t *testing.T) {
	s := NewTraceStore(1, 100, time.Now)

	s.Record(Trace{MessageID: 42, TenantID: "t1", Channel: "room:1", Delivered: 0})
	s.Record(Trace{MessageID: 42, TenantID: "t1", Channel: "room:1", Delivered: 10, Dropped: 2})

	got, ok := s.Trace(42)
	if !ok {
		t.Fatal("trace not found")
	}
	if got.Delivered != 10 || got.Dropped != 2 {
		t.Errorf("trace = %+v, want the final counts", got)
	}
	if s.Len() != 1 {
		t.Errorf("retained %d traces for one message, want 1", s.Len())
	}
}

// Tracing every message would make retention itself a throughput limit, so the
// store samples.
func TestTraceSamplingIsFractional(t *testing.T) {
	s := NewTraceStore(0.1, 10000, time.Now)

	traced := 0
	for i := uint64(0); i < 10000; i++ {
		if s.ShouldTrace(i) {
			traced++
		}
	}
	if traced < 800 || traced > 1200 {
		t.Errorf("traced %d of 10000 at rate 0.1, want about 1000", traced)
	}
}

func TestTraceSamplingIsDeterministic(t *testing.T) {
	// The same message must be traced on every instance that sees it, or a
	// partial path is all anyone ever gets.
	s := NewTraceStore(0.1, 10000, time.Now)
	for _, id := range []uint64{1, 2, 3, 4, 5, 100, 1000, 99999} {
		first := s.ShouldTrace(id)
		for i := 0; i < 10; i++ {
			if s.ShouldTrace(id) != first {
				t.Fatalf("sampling decision for %d is not deterministic", id)
			}
		}
	}
}

func TestTraceSamplingDisabled(t *testing.T) {
	s := NewTraceStore(0, 100, time.Now)
	if s.ShouldTrace(1) {
		t.Error("tracing sampled at rate zero")
	}
	s.Record(Trace{MessageID: 1})
	if s.Len() != 0 {
		t.Error("a trace was retained with sampling disabled")
	}
}

func TestTraceRetentionIsBounded(t *testing.T) {
	s := NewTraceStore(1, 10, time.Now)
	for i := uint64(0); i < 100; i++ {
		s.Record(Trace{MessageID: i, Delivered: 1})
	}
	if s.Len() != 10 {
		t.Errorf("retained %d traces, want the configured bound of 10", s.Len())
	}
	if _, ok := s.Trace(99); !ok {
		t.Error("the most recent trace was discarded")
	}
	if _, ok := s.Trace(0); ok {
		t.Error("the oldest trace survived; the bound is not being enforced")
	}
}

func TestConcurrentObservation(t *testing.T) {
	m := NewMetrics(time.Now)
	traces := NewTraceStore(0.5, 1000, time.Now)
	eval := NewEvaluator(time.Now, time.Second)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.ObservePublish("t1")
				m.ObserveDelivery("t1", 3, 600, 1)
				m.ObserveLatency(10 * time.Millisecond)
				m.ObserveDrop(DropSlowConsumer, 1)
				traces.Record(Trace{MessageID: uint64(i), Delivered: 3})
				eval.Evaluate(m.Snapshot())
				m.TenantSnapshot("t1")
				m.Snapshot()
			}
		}(g)
	}
	wg.Wait()

	if m.Snapshot().PublishesAccepted != 4000 {
		t.Errorf("publishes = %d, want 4000", m.Snapshot().PublishesAccepted)
	}
}

// --- Export ---

// The two headline rates must both be exported, because the fanout factor is
// only derivable if a reader has both.
func TestExporterEmitsBothRatesForTheFanoutFactor(t *testing.T) {
	m := NewMetrics(time.Now)
	for i := 0; i < 10; i++ {
		m.ObservePublish("t1")
		m.ObserveDelivery("t1", 5, 1000, 1)
	}
	out := NewExporter(m, "gw-1", nil).Render()

	for _, want := range []string{
		"push_publishes_accepted_total",
		"push_deliveries_total",
		"push_fanout_factor",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("export is missing %s", want)
		}
	}
	if !strings.Contains(out, `push_fanout_factor{instance="gw-1"} 5`) {
		t.Errorf("fanout factor not exported with its value:\n%s", out)
	}
}

func TestExporterIncludesTenantSeries(t *testing.T) {
	m := NewMetrics(time.Now)
	m.ObservePublish("t1")
	m.ObservePublish("t2")
	m.ObserveDelivery("t1", 3, 600, 1)

	out := NewExporter(m, "gw-1", nil).Render()
	if !strings.Contains(out, `tenant="t1"`) {
		t.Error("t1 series missing; a dashboard cannot be scoped without it")
	}
	if !strings.Contains(out, `tenant="t2"`) {
		t.Error("t2 series missing")
	}
}

func TestExporterOutputIsStable(t *testing.T) {
	m := NewMetrics(time.Now)
	m.ObserveDrop(DropDegradation, 1)
	m.ObserveDrop(DropSlowConsumer, 2)
	m.ObserveDrop(DropRateLimit, 3)

	e := NewExporter(m, "gw-1", nil)
	first := e.Render()
	for i := 0; i < 5; i++ {
		if e.Render() != first {
			t.Fatal("export output changed between scrapes with no counter movement")
		}
	}
	// Labels are sorted, so the cause series appear in a fixed order.
	dep := strings.Index(first, `cause="degradation"`)
	slow := strings.Index(first, `cause="slow_consumer"`)
	if dep < 0 || slow < 0 || dep > slow {
		t.Error("cause series are not in a stable sorted order")
	}
}

func TestExporterEmitsExtraSamples(t *testing.T) {
	m := NewMetrics(time.Now)
	e := NewExporter(m, "gw-1", func() []Sample {
		return []Sample{
			{Name: "push_degradation_level", Help: "Current ladder position.", Value: 2},
		}
	})
	if !strings.Contains(e.Render(), "push_degradation_level") {
		t.Error("extra sample not exported")
	}
}

// The exporter must read only from the snapshot: nothing here may reach back
// into the delivery path.
func TestExporterTouchesNoDeliveryPath(t *testing.T) {
	m := NewMetrics(time.Now)
	for i := 0; i < 100; i++ {
		m.ObservePublish("t1")
		m.ObserveDelivery("t1", 2, 400, 1)
		m.ObserveLatency(10 * time.Millisecond)
	}
	e := NewExporter(m, "gw-1", nil)
	// Exporting repeatedly must not disturb the counters it reads.
	before := m.Snapshot()
	for i := 0; i < 10; i++ {
		e.Render()
	}
	after := m.Snapshot()

	if before.PublishesAccepted != after.PublishesAccepted ||
		before.DeliveriesOut != after.DeliveriesOut ||
		before.BytesOut != after.BytesOut ||
		before.LatencyP99Ms != after.LatencyP99Ms {
		t.Errorf("exporting changed the counters:\nbefore %+v\nafter %+v", before, after)
	}
}

// TestDocumentedMetricsAreEmitted keeps docs/observability.md honest.
//
// A documented metric that is never exported is a dashboard panel that stays
// empty, which reads as "no problems" rather than as "no data".
func TestDocumentedMetricsAreEmitted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "observability.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	m := NewMetrics(time.Now)
	m.ObservePublish("t1")
	m.ObservePublishRefused("t1")
	m.ObserveDelivery("t1", 5, 1000, 1)
	m.ObserveLatency(10 * time.Millisecond)
	m.ObserveDrop(DropSlowConsumer, 1)
	m.ObserveDrop(DropRateLimit, 1)
	m.ObserveDrop(DropDegradation, 1)
	m.ConnectionOpened("t1")
	m.ObserveChannelMiss()
	m.ObserveExpired(1)
	m.SetSlowSubscribers(1)
	m.ObserveHeartbeatLost()
	out := NewExporter(m, "gw-1", nil).Render()

	// The first column of every metric table row is the metric name.
	row := regexp.MustCompile("(?m)^\\| `(push_[a-z_]+)` \\|")
	names := map[string]bool{}
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		names[m[1]] = true
	}
	if len(names) == 0 {
		t.Fatal("no metrics parsed from the document")
	}
	for name := range names {
		if !strings.Contains(out, name) {
			t.Errorf("metric %s is documented but never exported", name)
		}
	}
	t.Logf("%d documented metrics, all exported", len(names))
}

// Every drop cause named in the document must be a value the exporter emits.
func TestDocumentedDropCausesAreEmitted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "observability.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	m := NewMetrics(time.Now)
	for _, c := range []DropCause{DropSlowConsumer, DropRateLimit, DropDegradation} {
		m.ObserveDrop(c, 1)
	}
	out := NewExporter(m, "gw-1", nil).Render()

	// Only the drop-cause table is scanned. Every other table in the document
	// also begins with a backticked identifier, so an unscoped match would treat
	// metric names as drop causes.
	section := between(doc, "`cause` label with three values", "## Latency is a distribution")
	if section == "" {
		t.Fatal("the drop-cause table was not found in the document")
	}
	row := regexp.MustCompile("(?m)^\\| `([a-z_]+)` \\|")
	found := 0
	for _, m := range row.FindAllStringSubmatch(section, -1) {
		cause := m[1]
		found++
		if !strings.Contains(out, `cause="`+cause+`"`) {
			t.Errorf("drop cause %q is documented but never emitted", cause)
		}
	}
	if found != 3 {
		t.Errorf("found %d drop causes in the document, want 3", found)
	}
}

func between(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}
	rest := s[i:]
	j := strings.Index(rest, end)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// Every alert the document names must be a condition the evaluator can fire.
func TestDocumentedAlertsExist(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "observability.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	emitted := map[string]bool{}
	for _, c := range alertRule {
		emitted[c.Name] = true
	}
	// The trace-level alerts are documented separately from the snapshot rules.
	row := regexp.MustCompile("(?m)^\\| `([a-z_]+)` \\| .* \\| (critical|warning) \\|")
	documented := 0
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		name := m[1]
		documented++
		if !emitted[name] {
			t.Errorf("alert %q is documented but not in the rule set", name)
		}
	}
	if documented == 0 {
		t.Fatal("no alerts parsed from the document")
	}
	t.Logf("%d alerts documented, all present in the rule set", documented)
}

// The latency budget in the document and the budget the alert uses must be one
// number, or the page and the target drift apart.
func TestDocumentedLatencyBudgetMatchesTheAlert(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "observability.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	if !strings.Contains(doc, "200 ms") {
		t.Error("the documented P99 budget does not name 200 ms")
	}
	// And the alert must breach at exactly that budget, not at some other
	// figure nobody wrote down.
	cond := alertRule[0]
	if cond.Breached(Snapshot{PublishesAccepted: 1, LatencyP99Ms: LatencyBudgetP99Ms}) {
		t.Errorf("the latency alert fires exactly at the %.0fms budget", LatencyBudgetP99Ms)
	}
	if !cond.Breached(Snapshot{PublishesAccepted: 1, LatencyP99Ms: LatencyBudgetP99Ms + 1}) {
		t.Errorf("the latency alert does not fire just over the %.0fms budget", LatencyBudgetP99Ms)
	}
	// And it must not fire with no traffic, or a quiet instance pages all night.
	if cond.Breached(Snapshot{LatencyP99Ms: 10000}) {
		t.Error("the latency alert fired with no publishes admitted")
	}
}
