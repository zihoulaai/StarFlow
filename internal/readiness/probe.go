// Package readiness answers whether an instance may serve.
//
// The gate is closed by default and opens only once every check passes. That
// direction matters: an instance that fails a check for any reason is not
// serving, rather than serving under a degraded view of its own configuration.
//
// The most important check is moderation readiness. The sensitive word automaton
// is held in process memory, so an instance that started without it, or lost it,
// would be publishing with no moderation at all. Failing readiness keeps such an
// instance out of the serving set rather than letting it under-moderate.
package readiness

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check is one gate.
type Check struct {
	// Name identifies the gate in the probe output.
	Name string
	// Probe returns nil when the check passes.
	Probe func(context.Context) error
}

// Probe renders the readiness and liveness responses.
type Probe struct {
	mu sync.RWMutex

	checks []Check
	// drainFunc reports whether the instance is retiring.
	drainFunc func() bool
	// readyOverride pins readiness, for a deployment that manages it elsewhere.
	readyOverride *bool

	// metricsWriter renders the exposition text on the probe port, so metrics
	// are reachable without exposing a second listener on the data plane.
	metricsWriter func(http.ResponseWriter)

	lastCheck time.Time
	// failures records consecutive failures per check, which is what
	// distinguishes a starting instance from a broken one.
	failures map[string]int
}

// New returns a probe over checks.
func New(checks ...Check) *Probe {
	return &Probe{checks: checks, failures: map[string]int{}}
}

// SetMetricsWriter installs the metrics renderer.
func (p *Probe) SetMetricsWriter(fn func(http.ResponseWriter)) { p.metricsWriter = fn }

// SetDrain installs the drain reporter.
func (p *Probe) SetDrain(fn func() bool) { p.drainFunc = fn }

// SetReadyOverride pins readiness, bypassing the checks. It exists for a
// deployment that runs its own gating and would otherwise be second-guessed.
func (p *Probe) SetReadyOverride(v bool) { p.readyOverride = &v }

// Result is the outcome of a probe.
type Result struct {
	Ready bool
	// Draining is reported separately from not-ready: a draining instance is
	// still serving its existing connections and must not be pulled out of
	// rotation.
	Draining bool
	// Checks maps check name to its error, empty when all pass.
	Checks map[string]string
}

// Ready runs every check and returns the result.
func (p *Probe) Ready(ctx context.Context) Result {
	res := Result{Checks: map[string]string{}}

	if p.drainFunc != nil {
		res.Draining = p.drainFunc()
	}

	p.mu.RLock()
	override := p.readyOverride
	p.mu.RUnlock()
	if override != nil {
		res.Ready = *override
		p.record(true)
		return res
	}

	p.mu.RLock()
	checks := append([]Check(nil), p.checks...)
	p.mu.RUnlock()

	for _, c := range checks {
		if err := c.Probe(ctx); err != nil {
			res.Checks[c.Name] = err.Error()
			p.recordFailure(c.Name)
		} else {
			p.recordSuccess(c.Name)
		}
	}
	res.Ready = len(res.Checks) == 0

	p.mu.Lock()
	p.lastCheck = time.Now()
	p.mu.Unlock()
	return res
}

// Alive reports process liveness.
//
// It is deliberately weaker than readiness: an instance whose store is
// unreachable is not ready, but it is still alive, and restarting it would not
// fix the store. Conflating the two would turn a control-plane outage into a
// restart loop.
func (p *Probe) Alive() bool {
	return true
}

func (p *Probe) record(ready bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !ready {
		p.failures = map[string]int{}
	}
}

func (p *Probe) recordFailure(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[name]++
}

func (p *Probe) recordSuccess(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.failures, name)
}

// ConsecutiveFailures returns how many times a check has failed in a row.
func (p *Probe) ConsecutiveFailures(name string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.failures[name]
}

// Handler serves the probe endpoints on a port separate from data-plane traffic.
func (p *Probe) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		res := p.Ready(r.Context())
		// A draining instance stays ready. Pulling it out of rotation during a
		// release is exactly the opposite of what a release wants.
		status := http.StatusOK
		if !res.Ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, res)
	})

	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		status := http.StatusOK
		if !p.Alive() {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]bool{"alive": p.Alive()})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if p.metricsWriter != nil {
			p.metricsWriter(w)
		}
	})

	return mux
}
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
