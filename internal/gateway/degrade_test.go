package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/starflow/push/internal/protocol"
)

// fakeClock is an advanceable clock safe for concurrent use.
//
// The ladder tests advance the clock from a second goroutine while the manager
// reads it, so a plain time.Time would be a data race rather than merely a
// sloppy test.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(time.UnixMilli(1_700_000_000_000).UnixMilli())
	return c
}

func (c *fakeClock) Now() time.Time { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) Add(d time.Duration) time.Time {
	return time.UnixMilli(c.ms.Add(int64(d.Milliseconds())))
}

func newDegradeFixture(t *testing.T) (*Degradation, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	d := NewDegradation(DegradationConfig{Dwell: 100 * time.Millisecond, RestoreAfter: time.Second},
		clock.Now)
	return d, clock
}

func TestEveryLevelHasDefinedPolicy(t *testing.T) {
	for l := LevelNormal; l <= MaxLevel; l++ {
		p := Policy(l)
		if p.WindowMultiplier < 1 {
			t.Errorf("level %s window multiplier = %v, want at least 1", l, p.WindowMultiplier)
		}
		if p.SampleRate <= 0 || p.SampleRate > 1 {
			t.Errorf("level %s sample rate = %v, want (0, 1]", l, p.SampleRate)
		}
		if len(p.AdmitClasses) == 0 {
			t.Errorf("level %s admits nothing at all", l)
		}
		// System content must survive every rung: a moderation notice or a ban
		// that is shed under load is a correctness failure, not a quality one.
		if !p.AdmitClasses[protocol.PrioritySystem] {
			t.Errorf("level %s sheds system priority", l)
		}
	}
}

// The ladder's value is its span: a wide span absorbs a burst that would
// otherwise need capacity that cannot be added for existing connections.
func TestLadderSpanIsMonotonic(t *testing.T) {
	prevWindow := 0.0
	prevShed := false
	for l := LevelNormal; l <= MaxLevel; l++ {
		p := Policy(l)
		if p.WindowMultiplier < prevWindow {
			t.Errorf("level %s window multiplier %v is below the level below it",
				l, p.WindowMultiplier)
		}
		prevWindow = p.WindowMultiplier

		normalAdmitted := p.AdmitClasses[protocol.PriorityNormal]
		if prevShed && normalAdmitted {
			t.Errorf("level %s re-admits normal content after shedding it", l)
		}
		prevShed = !normalAdmitted
	}
}

// A momentary spike must not degrade visible quality. That is the entire reason
// the ladder waits rather than reacting to each pass.
func TestBriefSpikeDoesNotEscalate(t *testing.T) {
	d, clock := newDegradeFixture(t)

	d.Observe(true)
	if l := d.Level(); l != LevelNormal {
		t.Errorf("level = %s after a single saturated pass, want normal", l)
	}

	// Still saturated, but under the dwell.
	clock.Add(50 * time.Millisecond)
	d.Observe(true)
	if l := d.Level(); l != LevelNormal {
		t.Errorf("level = %s before the dwell elapsed, want normal", l)
	}
}

func TestSustainedLoadEscalates(t *testing.T) {
	d, clock := newDegradeFixture(t)

	d.Observe(true)
	for i := 0; i < 10 && d.Level() < MaxLevel; i++ {
		clock.Add(150 * time.Millisecond)
		d.Observe(true)
	}

	if d.Level() != MaxLevel {
		t.Fatalf("level = %s after sustained saturation, want %s", d.Level(), MaxLevel)
	}
	if d.Snapshot().Escalations == 0 {
		t.Error("escalations were not counted")
	}
}

func TestRecoveryRestoresGradually(t *testing.T) {
	d, clock := newDegradeFixture(t)

	// Climb to the top.
	d.Observe(true)
	for i := 0; i < 20 && d.Level() < MaxLevel; i++ {
		clock.Add(150 * time.Millisecond)
		d.Observe(true)
	}
	if d.Level() != MaxLevel {
		t.Fatalf("did not reach the top level")
	}

	// Load recovers; restoration is gradual and held for a window.
	d.Observe(false)
	if d.Level() != MaxLevel {
		t.Errorf("level dropped to %s immediately on recovery", d.Level())
	}
	for i := 0; i < 10 && d.Level() > LevelNormal; i++ {
		clock.Add(1500 * time.Millisecond)
		d.Observe(false)
	}
	if d.Level() != LevelNormal {
		t.Errorf("level = %s after sustained recovery, want normal", d.Level())
	}
	if d.Snapshot().Restorations == 0 {
		t.Error("restorations were not counted")
	}
}

// A load oscillating around the threshold must not drive the ladder up and down,
// which a user would see as repeated stuttering.
func TestOscillatingLoadDoesNotThrash(t *testing.T) {
	d, clock := newDegradeFixture(t)

	d.Observe(true)
	clock.Add(150 * time.Millisecond)
	d.Observe(true) // escalates
	start := d.Level()

	// Alternate faster than the dwell.
	for i := 0; i < 50; i++ {
		clock.Add(20 * time.Millisecond)
		d.Observe(i%2 == 0)
	}
	if got := d.Level(); got < start {
		t.Errorf("level fell from %s to %s while load oscillated", start, got)
	}
}

// An operator override wins over the metrics, until it is explicitly released.
func TestManualOverride(t *testing.T) {
	d, _ := newDegradeFixture(t)

	d.SetManual(true, LevelCritical)
	if d.Level() != LevelCritical {
		t.Fatalf("level = %s, want the manual setting", d.Level())
	}
	if !d.Snapshot().Manual {
		t.Error("manual flag not reported")
	}

	// Saturation cannot move a manually pinned ladder.
	d.Observe(true)
	if d.Level() != LevelCritical {
		t.Errorf("level = %s, want the manual setting to survive saturation", d.Level())
	}

	d.SetManual(false, LevelNormal)
	if d.Level() != LevelNormal {
		t.Errorf("level = %s after releasing the override, want normal", d.Level())
	}
}

// System content must always be deliverable: a moderation notice shed under load
// is a correctness failure.
func TestSystemPrioritySurvivesEveryLevel(t *testing.T) {
	d, _ := newDegradeFixture(t)
	for l := LevelNormal; l <= MaxLevel; l++ {
		d.SetManual(true, l)
		if !d.Admit(protocol.PrioritySystem) {
			t.Errorf("level %s sheds system content", l)
		}
	}
}

func TestSheddingAtCriticalLevel(t *testing.T) {
	d, _ := newDegradeFixture(t)
	d.SetManual(true, LevelCritical)

	if d.Admit(protocol.PriorityNormal) {
		t.Error("normal content admitted at the critical level")
	}
	if !d.Admit(protocol.PriorityHigh) {
		t.Error("high priority shed at the critical level")
	}
	if !d.Admit(protocol.PrioritySystem) {
		t.Error("system content shed at the critical level")
	}
}

// Sampling must reduce volume without ever admitting everything once the rate is
// below one, and must not be pathologically lumpy over a window.
func TestSamplingRate(t *testing.T) {
	d, _ := newDegradeFixture(t)
	d.SetManual(true, LevelSampled)

	rate := Policy(LevelSampled).SampleRate
	admitted := 0
	const total = 100_000
	for i := uint64(0); i < total; i++ {
		if d.Sample(i) {
			admitted++
		}
	}
	got := float64(admitted) / total
	if got < rate-0.01 || got > rate+0.01 {
		t.Errorf("sample rate = %.3f, want about %.2f", got, rate)
	}
}

func TestSamplingAtNormalAdmitsEverything(t *testing.T) {
	d, _ := newDegradeFixture(t)
	d.SetManual(true, LevelNormal)
	for i := uint64(0); i < 1000; i++ {
		if !d.Sample(i) {
			t.Fatalf("sequence %d was sampled out at the normal level", i)
		}
	}
}

func TestWindowMultiplierGrowsWithLevel(t *testing.T) {
	d, _ := newDegradeFixture(t)
	prev := 0.0
	for l := LevelNormal; l <= MaxLevel; l++ {
		d.SetManual(true, l)
		got := d.WindowMultiplier()
		if got < prev {
			t.Errorf("level %s window multiplier %v is below the level below it", l, got)
		}
		prev = got
	}
	if d.WindowMultiplier() <= 1 {
		t.Error("the top level does not widen the coalescing window")
	}
}

func TestConcurrentLadderMovement(t *testing.T) {
	d, clock := newDegradeFixture(t)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				d.Observe(i%3 == 0)
				d.Level()
				d.Policy()
				d.Sample(uint64(i))
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			clock.Add(50 * time.Millisecond)
			d.Observe(true)
		}
	}()
	wg.Wait()
}
