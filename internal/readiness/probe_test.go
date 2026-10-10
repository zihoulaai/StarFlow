package readiness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadyOnlyWhenEveryCheckPasses(t *testing.T) {
	var storeUp, busUp, moderationUp atomic.Bool
	storeUp.Store(true)
	busUp.Store(true)
	moderationUp.Store(true)

	p := New(
		Check{Name: "control_plane", Probe: func(context.Context) error {
			if !storeUp.Load() {
				return errors.New("store unreachable")
			}
			return nil
		}},
		Check{Name: "bus", Probe: func(context.Context) error {
			if !busUp.Load() {
				return errors.New("bus disconnected")
			}
			return nil
		}},
		// This is the one that matters: an instance without its word list is
		// publishing with no moderation at all.
		Check{Name: "moderation_loaded", Probe: func(context.Context) error {
			if !moderationUp.Load() {
				return errors.New("moderation rules are not loaded")
			}
			return nil
		}},
	)

	if res := p.Ready(context.Background()); !res.Ready {
		t.Fatalf("not ready with every check passing: %v", res.Checks)
	}

	moderationUp.Store(false)
	res := p.Ready(context.Background())
	if res.Ready {
		t.Fatal("an instance without its moderation rules reported ready")
	}
	if _, reported := res.Checks["moderation_loaded"]; !reported {
		t.Errorf("the failing check is not named: %v", res.Checks)
	}

	moderationUp.Store(true)
	if res := p.Ready(context.Background()); !res.Ready {
		t.Errorf("did not become ready again: %v", res.Checks)
	}
}

func TestEveryFailingCheckIsNamed(t *testing.T) {
	p := New(
		Check{Name: "a", Probe: func(context.Context) error { return errors.New("boom") }},
		Check{Name: "b", Probe: func(context.Context) error { return errors.New("bang") }},
	)
	res := p.Ready(context.Background())
	if len(res.Checks) != 2 {
		t.Errorf("reported %d failures, want both: %v", len(res.Checks), res.Checks)
	}
}

func TestConsecutiveFailuresDistinguishStartingFromBroken(t *testing.T) {
	var up atomic.Bool
	up.Store(false)
	p := New(Check{Name: "bus", Probe: func(context.Context) error {
		if up.Load() {
			return nil
		}
		return errors.New("down")
	}})

	for i := 0; i < 5; i++ {
		p.Ready(context.Background())
	}
	if got := p.ConsecutiveFailures("bus"); got != 5 {
		t.Errorf("consecutive failures = %d, want 5", got)
	}

	up.Store(true)
	p.Ready(context.Background())
	if got := p.ConsecutiveFailures("bus"); got != 0 {
		t.Errorf("consecutive failures = %d after recovery, want 0", got)
	}
}

// A draining instance must stay ready: pulling it out of rotation during a
// release is the opposite of what a release wants.
func TestDrainingInstanceStaysReady(t *testing.T) {
	p := New(Check{Name: "bus", Probe: func(context.Context) error { return nil }})
	p.SetDrain(func() bool { return true })

	res := p.Ready(context.Background())
	if !res.Ready {
		t.Error("a draining instance reported not ready; it would be pulled from rotation")
	}
	if !res.Draining {
		t.Error("draining is not reported")
	}
}

func TestReadyOverride(t *testing.T) {
	p := New(Check{Name: "bus", Probe: func(context.Context) error { return errors.New("down") }})
	if res := p.Ready(context.Background()); res.Ready {
		t.Fatal("an instance with a failing check reported ready before override")
	}

	p.SetReadyOverride(true)
	if res := p.Ready(context.Background()); !res.Ready {
		t.Error("an explicit override was second-guessed")
	}
}

// Liveness is deliberately weaker than readiness: an instance whose store is
// unreachable is not ready, but it is alive, and restarting it would not fix the
// store. Conflating the two turns a control-plane outage into a restart loop.
func TestAliveIsWeakerThanReady(t *testing.T) {
	p := New(Check{Name: "store", Probe: func(context.Context) error { return errors.New("unreachable") }})

	if res := p.Ready(context.Background()); res.Ready {
		t.Error("ready with an unreachable store")
	}
	if !p.Alive() {
		t.Error("an instance with an unreachable store reported dead; this would restart-loop")
	}
}

func TestProbeEndpoints(t *testing.T) {
	var up atomic.Bool
	p := New(Check{Name: "bus", Probe: func(context.Context) error {
		if up.Load() {
			return nil
		}
		return errors.New("down")
	}})
	p.SetMetricsWriter(func(w http.ResponseWriter) {
		w.Write([]byte("push_connections_live 0\n"))
	})
	h := p.Handler()

	// Not ready.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503", rec.Code)
	}

	// Alive regardless: the check is failing but the process is fine.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("livez status = %d, want 200 even with a failing check", rec.Code)
	}

	up.Store(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("readyz status = %d once passing, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := rec.Body.String(); body == "" {
		t.Error("metrics endpoint rendered nothing")
	}
}

func TestConcurrentProbes(t *testing.T) {
	p := New(
		Check{Name: "a", Probe: func(context.Context) error { return nil }},
		Check{Name: "b", Probe: func(context.Context) error { return errors.New("down") }},
	)
	h := p.Handler()

	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 200; i++ {
				p.Ready(context.Background())
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/readyz", nil))
			}
		}()
	}
	for g := 0; g < 8; g++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("probes did not complete")
		}
	}
}
