// Command loadgen drives the service at a documented target and reports what it
// observed.
//
// It exists to make the capacity claims in the design falsifiable. A target
// written down and never measured is a wish; this turns each one into a number
// that can fail.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	publishURL  string
	tenant      string
	appKey      string
	secret      string
	channel     string
	connections int
	publishQPS  int
	bodyBytes   int
	duration    time.Duration
	concurrency int
	reportPath  string
	rampWindow  time.Duration
	channelSpan int
}

// channels returns the channels subscribers spread across.
//
// The per-channel publish ceiling is derived from the per-viewer bandwidth
// budget, so publishing a platform rate to one channel would exceed it by two
// orders of magnitude. Spreading the load keeps the measurement inside the
// condition the capacity model describes.
func (c config) channels() []string {
	if c.channelSpan <= 1 {
		return []string{c.channel}
	}
	out := make([]string, 0, c.channelSpan)
	for i := 0; i < c.channelSpan; i++ {
		out = append(out, fmt.Sprintf("%s:%d", c.channel, i))
	}
	return out
}

func main() {
	var c config
	flag.StringVar(&c.publishURL, "publish-url", "http://127.0.0.1:8080/publish", "publish endpoint")
	flag.StringVar(&c.tenant, "tenant", "", "tenant id")
	flag.StringVar(&c.appKey, "app-key", "", "application key")
	flag.StringVar(&c.secret, "secret", "", "application secret")
	flag.StringVar(&c.channel, "channel", "room:1", "channel to publish to")
	flag.IntVar(&c.connections, "connections", 100_000, "subscriber connections to open")
	flag.IntVar(&c.publishQPS, "publish-qps", 1000, "documented platform publish rate")
	flag.IntVar(&c.bodyBytes, "body-bytes", 200, "message size")
	flag.DurationVar(&c.duration, "duration", 60*time.Second, "steady-state measurement window")
	flag.IntVar(&c.concurrency, "concurrency", 64, "concurrent publish workers")
	flag.StringVar(&c.reportPath, "report", "", "write the report here as JSON")
	flag.DurationVar(&c.rampWindow, "ramp", 30*time.Second, "how long to spread the connection ramp over")
	flag.IntVar(&c.channelSpan, "channel-span", 1, "spread subscribers and publishes over this many channels")
	flag.Parse()

	if c.tenant == "" || c.appKey == "" || c.secret == "" {
		fatal("--tenant, --app-key and --secret are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	subs := newSubscribers(c)
	gen := &generator{cfg: c, client: &http.Client{Timeout: 10 * time.Second}, subs: subs}
	gen.runPrefix = uint64(time.Now().UnixNano())

	// The subscribers go up first. A publish-only measurement says nothing about
	// how many connections the instance holds or what it pushes to them, which
	// are the numbers the capacity model is derived from.
	fmt.Printf("opening %d subscriber connections over %s...\n", c.connections, c.rampWindow)
	rampErr := make(chan error, 1)
	go func() { rampErr <- subs.ramp(ctx, c.rampWindow) }()
	select {
	case err := <-rampErr:
		if err != nil {
			fatal("ramp: %v", err)
		}
	case <-time.After(c.rampWindow + 60*time.Second):
		fatal("ramp did not finish: %d of %d connections ready", subs.ready.Load(), c.connections)
	}
	fmt.Printf("%d subscribers ready\n", subs.ready.Load())

	fmt.Printf("warming up for 5s...\n")
	if err := gen.warmup(ctx, 5*time.Second); err != nil {
		fatal("warmup: %v", err)
	}
	fmt.Printf("steady state for %s at %d publishes/s across %d workers\n",
		c.duration, c.publishQPS, c.concurrency)

	report := gen.run(ctx)
	report.print(os.Stdout)

	delivery := subs.snapshot()
	delivery.print(os.Stdout)
	report.Delivery = &delivery
	report.FanoutFactor = delivery.fanoutFactor(report.Accepted)
	report.BytesDelivered = delivery.BytesIn
	report.checkDelivery()

	if c.reportPath != "" {
		if err := report.writeJSON(c.reportPath); err != nil {
			fatal("write report: %v", err)
		}
		fmt.Printf("report written to %s\n", c.reportPath)
	}

	// The exit code carries the conformance verdict, so a capacity claim fails
	// a pipeline rather than producing a document nobody reads.
	if !report.Conforms {
		os.Exit(1)
	}
}

type generator struct {
	cfg    config
	client *http.Client
	subs   *subscribers
	// nextChannel spreads publishes across the configured span so a platform
	// rate does not land on one channel and exceed its derived ceiling.
	nextChannel atomic.Uint64
	// runPrefix makes this invocation's bodies distinct from every other run's
	// against the same instance.
	runPrefix uint64
	// seq numbers message bodies across the whole run.
	//
	// It is shared between warmup and the measurement window rather than
	// restarted for each: two phases issuing the same sequence numbers produce
	// byte-identical bodies, and the admission path's dedup window then
	// suppresses the measurement window as a repeat of the warmup.
	seq atomic.Uint64

	sent      atomic.Uint64
	accepted  atomic.Uint64
	refused   atomic.Uint64
	failed    atomic.Uint64
	bytesSent atomic.Uint64
	refusedBy sync.Map // string -> *atomic.Uint64
	latencies []time.Duration
	latencyMu sync.Mutex
	outcomes  sync.Map
}

// Targets the report is judged against. These are the documented numbers; they
// live next to the measurement so the two cannot drift apart silently.
// Targets the report is judged against. These are the documented numbers; they
// live next to the measurement so the two cannot drift apart silently.
//
// The latency budgets are publish-to-delivery: the interval from a message being
// admitted to it reaching a subscriber. Admission is asynchronous by design, so
// that interval is the one a viewer experiences and the one the targets name.
// The publish endpoint's own round trip is a different quantity with no
// documented budget, and is held to a ceiling instead so a stalled admission
// path cannot pass unnoticed.
const (
	targetDeliveryP50Ms = 50.0
	targetDeliveryP95Ms = 100.0
	targetDeliveryP99Ms = 200.0
	// targetAcceptanceMs is a sanity ceiling on the admission round trip, not a
	// capacity claim. Admission returns as soon as the message is queued.
	targetAcceptanceMs = 50.0
	targetAccept       = 0.99
)

// messageBody returns a body carrying a unique counter.
//
// The admission path suppresses a repeat of the same content from the same
// publisher within a window, which is the right behaviour and wrong for a load
// generator: a constant body is suppressed after the first message and the run
// measures the dedup window rather than the delivery path.
//
// The counter is encoded as hexadecimal rather than written as raw bytes. A body
// travels as a JSON string, and an arbitrary 8-byte sequence is not valid UTF-8:
// the JSON encoder replaces invalid bytes with U+FFFD, so counters landing in the
// same invalid range collapsed onto one string and were then suppressed as
// duplicates. The symptom was a run reporting ~40% duplicates with no two bodies
// equal beforehand, which pointed at the encoder rather than at dedup.
//
// The run prefix separates invocations: numbering restarts at one every time, so
// a second run against the same instance would otherwise replay the first run's
// bodies into the dedup window it left behind.
func (g *generator) messageBody(i uint64, size int) []byte {
	body := make([]byte, size)
	for j := range body {
		body[j] = 'a'
	}
	if size == 0 {
		return body
	}
	// Sixteen hexadecimal characters per value, written as text so the body stays
	// valid UTF-8 end to end.
	tail := fmt.Sprintf("%016x%016x", g.runPrefix, i)
	if size < len(tail) {
		// Too small for both values: the counter alone still has to vary, or
		// every message would carry identical content.
		tail = fmt.Sprintf("%x", i)
	}
	if size < len(tail) {
		if size > 0 {
			body[size-1] = byte('0' + i%10)
		}
		return body
	}
	copy(body[size-len(tail):], tail)
	return body
}

// warmup lets the process settle before the measurement window.
//
// It publishes gently and treats a failure as information rather than as an
// abort: the purpose is to warm caches and let the connection ramp finish
// landing, not to test admission, so one refused or slow publish during warmup
// says nothing about the run that follows.
func (g *generator) warmup(ctx context.Context, d time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond) // 50/s, well under any ceiling
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			g.publish(ctx, g.messageBody(g.seq.Add(1), g.cfg.bodyBytes))
		}
	}
}

func (g *generator) run(ctx context.Context) report {
	ctx, cancel := context.WithTimeout(ctx, g.cfg.duration)
	defer cancel()

	// The counters are reset here so the report describes the measurement window
	// alone. Warmup shares them, and a report that folded its traffic in would
	// state an offered rate the run never produced — which at a warmup rate above
	// the target reads as a faster instance than was measured.
	g.resetCounters()
	if g.subs != nil {
		g.subs.reset()
	}

	// One shared ticker paces the target rate; workers pull from it. That keeps
	// the offered rate exact rather than approximately what N workers can push.
	interval := time.Second / time.Duration(max(1, g.cfg.publishQPS))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	for w := 0; w < g.cfg.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// A fresh body per message: a shared one is suppressed by
					// the dedup window after the first publish, and the run
					// would measure suppression rather than delivery.
					g.publish(ctx, g.messageBody(g.seq.Add(1), g.cfg.bodyBytes))
				}
			}
		}()
	}
	wg.Wait()

	return g.snapshot(g.cfg.duration)
}

func (g *generator) publish(ctx context.Context, body []byte) (string, error) {
	g.sent.Add(1)
	g.bytesSent.Add(uint64(len(body)))

	start := time.Now()
	outcome, messageID, err := g.post(ctx, body)
	g.latencyMu.Lock()
	g.latencies = append(g.latencies, time.Since(start))
	g.latencyMu.Unlock()

	g.countOutcome(outcome)

	if err != nil {
		g.failed.Add(1)
		return outcome, err
	}
	switch outcome {
	case "accepted", "degraded", "dropped":
		g.accepted.Add(1)
		// The send time is recorded only for an admitted message: a refused one
		// has no delivery to measure, and storing it would grow the map with
		// entries no subscriber will ever match.
		if g.subs != nil && messageID != 0 {
			g.subs.noteSent(messageID, start)
		}
	default:
		g.refused.Add(1)
	}
	return outcome, nil
}

func (g *generator) countOutcome(outcome string) {
	v, ok := g.outcomes.Load(outcome)
	if !ok {
		v = new(atomic.Uint64)
		g.outcomes.Store(outcome, v)
	}
	v.(*atomic.Uint64).Add(1)
}

func (g *generator) post(ctx context.Context, body []byte) (string, uint64, error) {
	channels := g.cfg.channels()
	channel := channels[int(g.nextChannel.Add(1)-1)%len(channels)]

	payload, err := json.Marshal(map[string]any{
		"channel": channel,
		"body":    string(body),
	})
	if err != nil {
		return "", 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.publishURL, bytes.NewReader(payload))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Key", g.cfg.appKey)
	req.Header.Set("X-App-Secret", g.cfg.secret)
	req.Header.Set("X-Tenant-Id", g.cfg.tenant)

	resp, err := g.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	var out struct {
		Outcome   string `json:"outcome"`
		MessageID uint64 `json:"message_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Outcome == "" {
		return "unknown", 0, nil
	}
	return out.Outcome, out.MessageID, nil
}

// resetCounters zeroes the publish-side measurements so they describe only the
// measurement window.
//
// The per-run body prefix deliberately does not reset: it exists to keep this
// run's messages distinct from every other run's, and the counter continuing
// across the boundary only makes them more distinct.
func (g *generator) resetCounters() {
	g.sent.Store(0)
	g.accepted.Store(0)
	g.refused.Store(0)
	g.failed.Store(0)
	g.bytesSent.Store(0)
	g.outcomes.Range(func(k, _ any) bool {
		g.outcomes.Delete(k)
		return true
	})
	g.latencyMu.Lock()
	g.latencies = nil
	g.latencyMu.Unlock()
}

func (g *generator) snapshot(elapsed time.Duration) report {
	g.latencyMu.Lock()
	lat := append([]time.Duration(nil), g.latencies...)
	g.latencyMu.Unlock()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	sent := g.sent.Load()
	accepted := g.accepted.Load()
	refused := g.refused.Load()
	failed := g.failed.Load()

	r := report{
		DurationSec:  elapsed.Seconds(),
		TargetConns:  g.cfg.connections,
		TargetQPS:    g.cfg.publishQPS,
		Sent:         sent,
		Accepted:     accepted,
		Refused:      refused,
		Failed:       failed,
		OfferedQPS:   float64(sent) / elapsed.Seconds(),
		BytesSent:    g.bytesSent.Load(),
		Outcomes:     map[string]uint64{},
		LatencyP50Ms: percentileMs(lat, 0.50),
		LatencyP95Ms: percentileMs(lat, 0.95),
		LatencyP99Ms: percentileMs(lat, 0.99),
	}
	if sent > 0 {
		r.AcceptRate = float64(accepted) / float64(sent)
	}
	g.outcomes.Range(func(k, v any) bool {
		r.Outcomes[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})

	// Conformance is stated against the documented numbers, and each failing
	// item is named so a report is actionable rather than merely red.
	//
	// The documented latency budgets belong to delivery and are judged in
	// checkDelivery. What is judged here is the admission path, against a
	// ceiling rather than a target.
	if r.LatencyP99Ms > targetAcceptanceMs {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"publish p99 %.1fms over the %.0fms admission ceiling", r.LatencyP99Ms, targetAcceptanceMs))
	}
	if r.AcceptRate < targetAccept {
		r.Failures = append(r.Failures, fmt.Sprintf("accept rate %.3f below the %.2f floor", r.AcceptRate, targetAccept))
	}
	if r.OfferedQPS < float64(g.cfg.publishQPS)*0.95 {
		r.Failures = append(r.Failures, fmt.Sprintf("offered %.0f/s below the target %d/s", r.OfferedQPS, g.cfg.publishQPS))
	}
	r.Conforms = len(r.Failures) == 0
	return r
}

// checkDelivery judges the subscriber half.
//
// It runs after the publish side has been judged rather than inside it, because
// a report that says nothing about connections, egress or delivery is not a
// capacity result even when every publish latency passed.
func (r *report) checkDelivery() {
	if r.Delivery == nil {
		return
	}
	d := r.Delivery

	if int64(r.TargetConns) > 0 && d.Ready < int64(r.TargetConns) {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"%d of %d connections ready", d.Ready, r.TargetConns))
	}
	if d.Delivered == 0 && r.Accepted > 0 {
		// The dangerous case: every publish was admitted and nothing reached a
		// viewer. Passing the publish budgets says nothing about delivery, so
		// this has to be a failure rather than an absent check.
		r.Failures = append(r.Failures,
			fmt.Sprintf("%d messages admitted but none delivered to any subscriber", r.Accepted))
		return
	}
	if d.GapNotices > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"%d gap notices: subscribers could not keep up and lost content", d.GapNotices))
	}
	if d.DeliveryP99Ms > targetDeliveryP99Ms {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"delivery p99 %.1fms over the %.0fms budget", d.DeliveryP99Ms, targetDeliveryP99Ms))
	}
	if d.DeliveryP95Ms > targetDeliveryP95Ms {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"delivery p95 %.1fms over the %.0fms budget", d.DeliveryP95Ms, targetDeliveryP95Ms))
	}
	if d.DeliveryP50Ms > targetDeliveryP50Ms {
		r.Failures = append(r.Failures, fmt.Sprintf(
			"delivery p50 %.1fms over the %.0fms budget", d.DeliveryP50Ms, targetDeliveryP50Ms))
	}
	r.Conforms = len(r.Failures) == 0
}

type report struct {
	DurationSec float64 `json:"duration_seconds"`
	TargetConns int     `json:"target_connections"`
	TargetQPS   int     `json:"target_publish_qps"`

	Sent       uint64  `json:"sent"`
	Accepted   uint64  `json:"accepted"`
	Refused    uint64  `json:"refused"`
	Failed     uint64  `json:"failed"`
	OfferedQPS float64 `json:"offered_qps"`
	BytesSent  uint64  `json:"bytes_sent"`
	AcceptRate float64 `json:"accept_rate"`

	Outcomes map[string]uint64 `json:"outcomes"`

	LatencyP50Ms float64 `json:"latency_p50_ms"`
	LatencyP95Ms float64 `json:"latency_p95_ms"`
	LatencyP99Ms float64 `json:"latency_p99_ms"`

	// Delivery is the subscriber half of the measurement, nil when no
	// subscribers were opened. It is the only place the figures the capacity
	// model is derived from appear: connections held, bytes pushed to them, and
	// how long a message took to reach a viewer rather than to leave the
	// admission endpoint.
	Delivery *subscriberReport `json:"delivery,omitempty"`
	// FanoutFactor is deliveries per admitted message, zero when no subscriber
	// was connected.
	FanoutFactor float64 `json:"fanout_factor"`
	// BytesDelivered counts message bodies that reached a subscriber.
	BytesDelivered uint64 `json:"bytes_delivered"`

	Conforms bool     `json:"conforms"`
	Failures []string `json:"failures,omitempty"`

	// MultiInstanceValidated records whether any run exercised the multi-instance
	// delivery path. It is carried in the report rather than assumed, because a
	// single-instance result says nothing about the broadcast topology.
	MultiInstanceValidated bool `json:"multi_instance_validated"`
}

func (r report) print(w *os.File) {
	fmt.Fprintf(w, "\n=== capacity report ===\n")
	fmt.Fprintf(w, "duration            %.1fs\n", r.DurationSec)
	fmt.Fprintf(w, "target connections   %d\n", r.TargetConns)
	fmt.Fprintf(w, "target publish rate  %d/s\n", r.TargetQPS)
	fmt.Fprintf(w, "offered rate         %.0f/s\n", r.OfferedQPS)
	fmt.Fprintf(w, "sent / accepted     %d / %d (%.3f)\n", r.Sent, r.Accepted, r.AcceptRate)
	fmt.Fprintf(w, "refused / failed     %d / %d\n", r.Refused, r.Failed)
	fmt.Fprintf(w, "latency p50/p95/p99  %.1f / %.1f / %.1f ms\n",
		r.LatencyP50Ms, r.LatencyP95Ms, r.LatencyP99Ms)
	fmt.Fprintf(w, "multi-instance path  validated=%v\n", r.MultiInstanceValidated)

	if len(r.Outcomes) > 0 {
		keys := make([]string, 0, len(r.Outcomes))
		for k := range r.Outcomes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(w, "outcomes             ")
		for _, k := range keys {
			fmt.Fprintf(w, "%s=%d ", k, r.Outcomes[k])
		}
		fmt.Fprintln(w)
	}

	if r.Conforms {
		fmt.Fprintln(w, "RESULT: conforms to every documented target")
		return
	}
	fmt.Fprintln(w, "RESULT: does NOT conform")
	for _, f := range r.Failures {
		fmt.Fprintf(w, "  - %s\n", f)
	}
}

func (r report) writeJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func percentileMs(sorted []time.Duration, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * q)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Nanoseconds()) / 1e6
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadgen: "+format+"\n", args...)
	os.Exit(2)
}
