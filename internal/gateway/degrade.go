package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/starflow/push/internal/protocol"
)

// Level is a position on the degradation ladder.
//
// The ladder exists because a stateless topology has no way to move a load off a
// saturated instance: there are no connections to migrate, and adding an
// instance does nothing for connections already established. The ladder is the
// only lever available, which is why its *span* matters more than the speed of
// any single level: each level trades delivery quality for throughput, and a
// wide span absorbs a burst that would otherwise require capacity that does not
// exist yet.
type Level int

const (
	// LevelNormal delivers everything, coalescing only.
	LevelNormal Level = 0
	// LevelReduced widens the coalescing window and begins sampling normal
	// content.
	LevelReduced Level = 1
	// LevelSampled widens the window further and samples hard.
	LevelSampled Level = 2
	// LevelCritical delivers only system and high priority content. Ordinary
	// danmaku is shed.
	LevelCritical Level = 3
)

// MaxLevel is the highest rung.
const MaxLevel = LevelCritical

func (l Level) String() string {
	switch l {
	case LevelNormal:
		return "normal"
	case LevelReduced:
		return "reduced"
	case LevelSampled:
		return "sampled"
	case LevelCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// LevelPolicy is what one rung of the ladder does.
type LevelPolicy struct {
	// WindowMultiplier scales the coalescing window. It bounds how stale a
	// delivered packet may be.
	WindowMultiplier float64
	// MergeRatioMultiplier scales how many raw messages are merged into one
	// packet, and this is the lever that actually moves packet count.
	//
	// Widening the window alone is not enough: with a fixed merge target the
	// window closes on the target before it reaches its deadline, so the packet
	// count does not fall. A measurement of the ladder found exactly that - the
	// window multiplier changed and throughput did not move.
	MergeRatioMultiplier float64
	// SampleRate is the fraction of normal-priority content admitted, from 1
	// meaning all of it.
	SampleRate float64
	// AdmitClasses is the set of priority classes delivered at this level.
	// Content outside it is shed.
	AdmitClasses map[protocol.PriorityClass]bool
}

// levelPolicies is the ladder.
//
// The sampling rates are deliberately coarse. A fine-grained sampling decision
// per message would need a per-channel counter on the hot path; deciding once
// per window instead costs nothing and is what makes the ladder cheap enough to
// be used.
var levelPolicies = [...]LevelPolicy{
	LevelNormal: {
		WindowMultiplier:     1,
		MergeRatioMultiplier: 1,
		SampleRate:           1,
		AdmitClasses: map[protocol.PriorityClass]bool{
			protocol.PriorityNormal: true,
			protocol.PriorityHigh:   true,
			protocol.PrioritySystem: true,
		},
	},
	LevelReduced: {
		WindowMultiplier:     4,
		MergeRatioMultiplier: 4,
		SampleRate:           0.75,
		AdmitClasses: map[protocol.PriorityClass]bool{
			protocol.PriorityNormal: true,
			protocol.PriorityHigh:   true,
			protocol.PrioritySystem: true,
		},
	},
	LevelSampled: {
		WindowMultiplier:     10,
		MergeRatioMultiplier: 10,
		SampleRate:           0.5,
		AdmitClasses: map[protocol.PriorityClass]bool{
			protocol.PriorityNormal: true,
			protocol.PriorityHigh:   true,
			protocol.PrioritySystem: true,
		},
	},
	LevelCritical: {
		WindowMultiplier:     20,
		MergeRatioMultiplier: 20,
		SampleRate:           1,
		// Ordinary content is shed outright. Sampling at 100% here would waste
		// the packet budget on content that is about to be thrown away.
		AdmitClasses: map[protocol.PriorityClass]bool{
			protocol.PriorityHigh:   true,
			protocol.PrioritySystem: true,
		},
	},
}

// Policy returns the settings for a level.
func Policy(l Level) LevelPolicy {
	if l < LevelNormal {
		l = LevelNormal
	}
	if l > MaxLevel {
		l = MaxLevel
	}
	return levelPolicies[l]
}

// Degradation manages the ladder position.
type Degradation struct {
	mu sync.RWMutex

	level        Level
	manual       bool
	overridden   Level
	dwell        time.Duration
	restoreAfter time.Duration

	now func() time.Time

	// saturatingSince is when load first looked saturated. Escalation waits for
	// it to persist, so a momentary spike does not degrade visible quality.
	saturatingSince time.Time
	clearSince      time.Time

	escalations  atomic.Uint64
	restorations atomic.Uint64
	sheddings    atomic.Uint64
	samplings    atomic.Uint64
}

// DegradationConfig holds the ladder's timing.
type DegradationConfig struct {
	// Manual pins the ladder regardless of load.
	Manual bool
	// Override is the level to pin when Manual is set.
	Override Level
	// Dwell is how long saturation must persist before escalating.
	Dwell time.Duration
	// RestoreAfter is how long load must be below threshold before restoring.
	RestoreAfter time.Duration
}

// NewDegradation returns a ladder manager.
func NewDegradation(cfg DegradationConfig, now func() time.Time) *Degradation {
	if now == nil {
		now = time.Now
	}
	if cfg.Dwell <= 0 {
		// Three flush intervals at the default rate. Shorter would escalate on a
		// single slow pass; longer would leave the instance saturated for a
		// visible period before reacting.
		cfg.Dwell = 30 * time.Millisecond
	}
	if cfg.RestoreAfter <= 0 {
		cfg.RestoreAfter = 2 * time.Second
	}
	initial := LevelNormal
	if cfg.Manual {
		// The override must be applied here. Starting at normal regardless would
		// leave the ladder a rung below where the caller asked until the next
		// SetManual call, and every read before that would be wrong.
		initial = cfg.Override
		if initial < LevelNormal {
			initial = LevelNormal
		}
		if initial > MaxLevel {
			initial = MaxLevel
		}
	}
	return &Degradation{
		level:        initial,
		manual:       cfg.Manual,
		overridden:   initial,
		dwell:        cfg.Dwell,
		restoreAfter: cfg.RestoreAfter,
		now:          now,
	}
}

// Level returns the current rung.
func (d *Degradation) Level() Level {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.level
}

// Policy returns the current rung's settings.
func (d *Degradation) Policy() LevelPolicy { return Policy(d.Level()) }

// SetManual pins or releases the ladder.
//
// A manual setting is an operator saying they know something the metrics do not,
// so it wins until it is explicitly released.
func (d *Degradation) SetManual(on bool, level Level) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.manual = on
	d.overridden = level
	d.saturatingSince = time.Time{}
	d.clearSince = time.Time{}

	if on {
		d.setLevelLocked(level)
		return
	}
	// Releasing an override returns the ladder to normal and resumes automatic
	// movement. Holding the pinned level after the operator stops pinning it
	// would leave the service degraded for a reason nobody can see.
	d.setLevelLocked(LevelNormal)
}

func (d *Degradation) setLevelLocked(l Level) {
	if l < LevelNormal {
		l = LevelNormal
	}
	if l > MaxLevel {
		l = MaxLevel
	}
	d.level = l
}

// Observe reports whether the instance is saturated, and moves the ladder.
//
// The dwell requirement is the reason this is not a straight comparison: a
// single saturated flush pass during a traffic spike would otherwise drop
// delivery quality for the whole spike, which is the opposite of what the ladder
// is for.
func (d *Degradation) Observe(saturated bool) {
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.manual {
		d.setLevelLocked(d.overridden)
		return
	}

	switch {
	case saturated:
		d.clearSince = time.Time{}
		if d.saturatingSince.IsZero() {
			d.saturatingSince = now
			return
		}
		if now.Sub(d.saturatingSince) < d.dwell {
			return
		}
		if d.level < MaxLevel {
			d.setLevelLocked(d.level + 1)
			d.escalations.Add(1)
		}
		// Restart the dwell so each further step is spaced by the same interval
		// rather than escalating on every pass once the dwell has elapsed.
		d.saturatingSince = now

	case !d.saturatingSince.IsZero():
		// Load recovered, but restoration is held until it has stayed recovered.
		// Without that, a load oscillating around the threshold would drive the
		// ladder up and down at the dwell rate, which a user sees as the stream
		// repeatedly stuttering.
		d.saturatingSince = time.Time{}
		d.clearSince = now
		return

	default:
		if d.level == LevelNormal {
			return
		}
		if d.clearSince.IsZero() {
			d.clearSince = now
			return
		}
		if now.Sub(d.clearSince) < d.restoreAfter {
			return
		}
		d.setLevelLocked(d.level - 1)
		d.restorations.Add(1)
		d.clearSince = now
	}
}

// Admit reports whether a message class survives the current level.
func (d *Degradation) Admit(class protocol.PriorityClass) bool {
	return d.Policy().AdmitClasses[class]
}

// Sample reports whether a normal-priority message survives sampling at the
// current level.
//
// The decision is deterministic so a message is not sampled out on one instance
// and admitted on another, and stateless so it costs nothing on the hot path.
// The sequence is run through a multiplicative hash before the comparison,
// because comparing the raw counter modulo the period makes the decision
// periodic: over any window shorter than the period the rate is whatever the
// window happens to align with, which shows up as the ladder visibly
// stuttering at a fixed rhythm rather than thinning out smoothly.
func (d *Degradation) Sample(sequence uint64) bool {
	rate := d.Policy().SampleRate
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}
	// Knuth's multiplicative hash spreads a sequential counter over the space;
	// the fold mixes the high bits down so the low 16 bits carry the entropy.
	h := sequence * 2654435761
	h ^= h >> 16
	h *= 0x2545F491
	h ^= h >> 16

	threshold := uint32(rate * 65536)
	return uint32(h)&0xFFFF < threshold
}

// WindowMultiplier returns the coalescing window scale for the current level.
func (d *Degradation) WindowMultiplier() float64 { return d.Policy().WindowMultiplier }

// MergeRatioMultiplier returns the merge-target scale for the current level.
func (d *Degradation) MergeRatioMultiplier() float64 {
	return d.Policy().MergeRatioMultiplier
}

// Counts reports ladder activity.
type Counts struct {
	Level        Level
	Manual       bool
	Escalations  uint64
	Restorations uint64
	Shedded      uint64
	Sampled      uint64
}

// Snapshot returns the ladder counters.
func (d *Degradation) Snapshot() Counts {
	d.mu.RLock()
	level, manual := d.level, d.manual
	d.mu.RUnlock()
	return Counts{
		Level:        level,
		Manual:       manual,
		Escalations:  d.escalations.Load(),
		Restorations: d.restorations.Load(),
		Shedded:      d.sheddings.Load(),
		Sampled:      d.samplings.Load(),
	}
}

// RecordShed counts a message dropped by the ladder.
func (d *Degradation) RecordShed() { d.sheddings.Add(1) }

// RecordSampled counts a message dropped by sampling.
func (d *Degradation) RecordSampled() { d.samplings.Add(1) }
