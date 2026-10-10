package deploy

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A staged rollout must be reversible on the same instances it moved forward,
// or part of the fleet keeps the value being withdrawn.
func TestRolloutIsStagedAndReversible(t *testing.T) {
	r := NewRollout([]string{"c", "a", "b", "d"})

	selected, err := r.Apply("degradation", "manual", 0.5)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("selected %v, want half the fleet", selected)
	}
	// Selection is deterministic, so a rollback lands on the same instances.
	if selected[0] != "a" || selected[1] != "b" {
		t.Errorf("selected %v, want a stable prefix of the sorted fleet", selected)
	}

	reverted, err := r.Revert("degradation", 0.5, "auto")
	if err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if len(reverted) != 2 {
		t.Fatalf("reverted %v, want the same count", reverted)
	}

	for _, id := range reverted {
		v, ok := r.State(id, "degradation")
		if !ok || v != "auto" {
			t.Errorf("instance %s holds %q after rollback, want auto", id, v)
		}
	}
	// And the instances that never had the change are untouched.
	for _, id := range []string{"c", "d"} {
		if _, ok := r.State(id, "degradation"); ok {
			t.Errorf("instance %s was touched but never selected", id)
		}
	}
}

// A zero fraction must be refused rather than reporting success while changing
// nothing, which would read as "the change is live and working".
func TestZeroFractionIsRefused(t *testing.T) {
	r := NewRollout([]string{"a", "b"})
	if _, err := r.Apply("k", "v", 0); err == nil {
		t.Error("a zero fraction was accepted")
	}
	if _, err := r.Revert("k", 0, "prev"); err == nil {
		t.Error("reverting a zero fraction was accepted")
	}
}

func TestFractionAboveOneIsRefused(t *testing.T) {
	r := NewRollout([]string{"a"})
	if _, err := r.Apply("k", "v", 1.5); err == nil {
		t.Error("a fraction above 1 was accepted")
	}
}

// A small fraction of a small fleet must still reach one instance, or a staged
// rollout silently does nothing on a two-instance deployment.
func TestSmallFractionOfSmallFleetReachesOneInstance(t *testing.T) {
	r := NewRollout([]string{"a", "b"})
	selected, err := r.Apply("k", "v", 0.01)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(selected) != 1 {
		t.Errorf("selected %v, want at least one instance", selected)
	}
}

func TestFullRolloutReachesWholeFleet(t *testing.T) {
	fleet := []string{"a", "b", "c"}
	r := NewRollout(fleet)
	selected, err := r.Apply("k", "v", 1)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(selected) != len(fleet) {
		t.Errorf("selected %v, want the whole fleet", selected)
	}
	if got := r.InstancesWithValue("k", "v"); len(got) != len(fleet) {
		t.Errorf("instances holding the value = %v, want all", got)
	}
}

func TestStateIsScopedToItsKey(t *testing.T) {
	r := NewRollout([]string{"a"})
	if _, err := r.Apply("one", "1", 1); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := r.State("a", "two"); ok {
		t.Error("a value for one key was reported for another")
	}
	if v, ok := r.State("a", "one"); !ok || v != "1" {
		t.Errorf("State = %q, %v", v, ok)
	}
}

// Fleet order is deterministic, or a rollback would apply to a different set of
// instances than the rollout did.
func TestFleetOrderIsStable(t *testing.T) {
	a := NewRollout([]string{"z", "m", "a"})
	b := NewRollout([]string{"a", "m", "z"})
	if strings.Join(a.Fleet(), ",") != strings.Join(b.Fleet(), ",") {
		t.Errorf("fleet order differs: %v vs %v", a.Fleet(), b.Fleet())
	}
}

// --- Release ---

func TestDrainStopsNewWorkBeforeClosingAnything(t *testing.T) {
	var drained, closed atomic.Int32
	r := NewRelease(time.Minute,
		func() { drained.Add(1) },
		func() int { closed.Add(1); return 10 },
		func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	if !r.Accepting() {
		t.Fatal("a fresh release is not accepting")
	}

	r.BeginDrain()

	if r.Accepting() {
		t.Error("a draining release still accepts new connections; it would take work it is about to abandon")
	}
	if !r.Draining() {
		t.Error("draining is not reported")
	}
	if drained.Load() != 1 {
		t.Error("subscribers were not told to reconnect")
	}
	if closed.Load() != 0 {
		t.Error("connections were closed during drain; the grace period was skipped")
	}
}

func TestFinishWaitsForTheGracePeriod(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	var closed atomic.Int32
	r := NewRelease(30*time.Second,
		func() {},
		func() int { closed.Add(1); return 5 },
		func() time.Time { return now })

	r.BeginDrain()

	// Before the grace period, nothing is closed.
	if _, early := r.Finish(); early {
		t.Error("connections were closed before the grace period elapsed")
	}
	if r.Remaining() <= 0 {
		t.Error("no grace time reported remaining")
	}

	now = now.Add(31 * time.Second)
	n, early := r.Finish()
	if !early {
		t.Fatal("connections were not closed after the grace period")
	}
	if n != 5 {
		t.Errorf("closed %d, want 5", n)
	}
	if r.Remaining() != 0 {
		t.Errorf("remaining = %v after finishing, want 0", r.Remaining())
	}
}

// A release that finishes before its grace period ends would disconnect every
// subscriber at once, which is the outcome the grace exists to prevent.
func TestGraceOutlivesAMassReconnect(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	r := NewRelease(30*time.Second, func() {}, func() int { return 1000 },
		func() time.Time { return now })
	r.BeginDrain()

	// Subscribers reconnect within a few seconds, which is the case the grace
	// is sized for.
	now = now.Add(5 * time.Second)
	if _, early := r.Finish(); early {
		t.Error("the release closed connections while subscribers were still reconnecting")
	}
}

func TestBeginDrainIsIdempotent(t *testing.T) {
	var drained atomic.Int32
	r := NewRelease(time.Second, func() { drained.Add(1) }, nil,
		func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	r.BeginDrain()
	r.BeginDrain()
	r.BeginDrain()

	if drained.Load() != 1 {
		t.Errorf("subscribers warned %d times, want 1", drained.Load())
	}
}

func TestFinishWithoutDrainDoesNothing(t *testing.T) {
	r := NewRelease(time.Second, nil, func() int { return 100 }, time.Now)
	if _, early := r.Finish(); early {
		t.Error("Finish acted on a release that never drained")
	}
}

func TestConcurrentRolloutAndRelease(t *testing.T) {
	fleet := []string{"a", "b", "c", "d", "e", "f"}
	r := NewRollout(fleet)

	now := time.Now()
	rel := NewRelease(time.Second, func() {}, func() int { return 0 }, func() time.Time { return now })

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, _ = r.Apply("k", "v", 0.5)
				_, _ = r.Revert("k", 0.5, "auto")
				r.Fleet()
				r.InstancesWithValue("k", "v")
				rel.Accepting()
				rel.BeginDrain()
				_, _ = rel.Finish()
			}
		}(g)
	}
	wg.Wait()
}
