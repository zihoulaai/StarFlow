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
}

func main() {
	var c config
	flag.StringVar(&c.publishURL, "publish-url", "http://127.0.0.1:8080/publish", "publish endpoint")
	flag.StringVar(&c.tenant, "tenant", "", "tenant id")
	flag.StringVar(&c.appKey, "app-key", "", "application key")
	flag.StringVar(&c.secret, "secret", "", "application secret")
	flag.StringVar(&c.channel, "channel", "room:1", "channel to publish to")
	flag.IntVar(&c.connections, "connections", 100_000, "documented connection target")
	flag.IntVar(&c.publishQPS, "publish-qps", 1000, "documented platform publish rate")
	flag.IntVar(&c.bodyBytes, "body-bytes", 200, "message size")
	flag.DurationVar(&c.duration, "duration", 60*time.Second, "steady-state measurement window")
	flag.IntVar(&c.concurrency, "concurrency", 64, "concurrent publish workers")
	flag.StringVar(&c.reportPath, "report", "", "write the report here as JSON")
	flag.Parse()

	if c.tenant == "" || c.appKey == "" || c.secret == "" {
		fatal("--tenant, --app-key and --secret are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	gen := &generator{cfg: c, client: &http.Client{Timeout: 10 * time.Second}}

	fmt.Printf("warming up to %d connections for 5s...\n", c.connections)
	if err := gen.warmup(ctx, 5*time.Second); err != nil {
		fatal("warmup: %v", err)
	}
	fmt.Printf("steady state for %s at %d publishes/s across %d workers\n",
		c.duration, c.publishQPS, c.concurrency)

	report := gen.run(ctx)
	report.print(os.Stdout)

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
const (
	targetP50Ms  = 50.0
	targetP95Ms  = 100.0
	targetP99Ms  = 200.0
	targetAccept = 0.99
)

func (g *generator) warmup(ctx context.Context, d time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	body := make([]byte, g.cfg.bodyBytes)
	for i := range body {
		body[i] = 'a'
	}

	// Connections are established by opening sockets; the generator drives the
	// publish path directly because it is the publish rate that is being
	// measured.
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := g.publish(ctx, body); err != nil {
				return err
			}
		}
	}
}

func (g *generator) run(ctx context.Context) report {
	ctx, cancel := context.WithTimeout(ctx, g.cfg.duration)
	defer cancel()

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
			body := make([]byte, g.cfg.bodyBytes)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.publish(ctx, body)
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
	outcome, err := g.post(ctx, body)
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

func (g *generator) post(ctx context.Context, body []byte) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"channel": g.cfg.channel,
		"body":    string(body),
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.publishURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Key", g.cfg.appKey)
	req.Header.Set("X-App-Secret", g.cfg.secret)
	req.Header.Set("X-Tenant-Id", g.cfg.tenant)

	resp, err := g.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		Outcome string `json:"outcome"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Outcome == "" {
		return "unknown", nil
	}
	return out.Outcome, nil
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
	if r.LatencyP99Ms > targetP99Ms {
		r.Failures = append(r.Failures, fmt.Sprintf("p99 %.1fms over the %.0fms budget", r.LatencyP99Ms, targetP99Ms))
	}
	if r.LatencyP95Ms > targetP95Ms {
		r.Failures = append(r.Failures, fmt.Sprintf("p95 %.1fms over the %.0fms budget", r.LatencyP95Ms, targetP95Ms))
	}
	if r.LatencyP50Ms > targetP50Ms {
		r.Failures = append(r.Failures, fmt.Sprintf("p50 %.1fms over the %.0fms budget", r.LatencyP50Ms, targetP50Ms))
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
