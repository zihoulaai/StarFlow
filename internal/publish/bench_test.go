package publish

import (
	"strings"
	"testing"
	"time"

	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/moderation"
	"github.com/starflow/push/internal/protocol"
)

func newBenchHarness(b *testing.B) *Pipeline {
	b.Helper()
	ids, err := protocol.NewIDGen(1)
	if err != nil {
		b.Fatalf("NewIDGen: %v", err)
	}
	matcher := moderation.NewAutomaton()
	if err := matcher.Reload("t1", nil); err != nil {
		b.Fatalf("Reload: %v", err)
	}
	return New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
}

// BenchmarkAdmitClean measures the hot path: a clean message that passes all
// stages and is admitted. This is the common case under normal operation.
func BenchmarkAdmitClean(b *testing.B) {
	p := newBenchHarness(b)
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte("a perfectly ordinary piece of danmaku"),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitWithModeration measures admission when moderation evaluates
// but finds no matches. The matcher still runs its automaton over the body.
func BenchmarkAdmitWithModeration(b *testing.B) {
	p := newBenchHarness(b)
	// Add a few rules so the automaton has state, but none match.
	matcher := moderation.NewAutomaton()
	if err := matcher.Reload("t1", []controlplane.ModerationRule{
		{Pattern: "badword", Action: controlplane.ActionBlock, Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r1"},
		{Pattern: "forbidden", Action: controlplane.ActionBlock, Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r2"},
	}); err != nil {
		b.Fatalf("Reload: %v", err)
	}
	// Replace the pipeline's matcher
	p = New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                p.deps.IDs, // reuse IDGen
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte("a perfectly ordinary piece of danmaku"),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitWithDuplication measures admission with dedup enabled but no
// actual duplicates. The dedup window is checked but no entry matches.
// Each message has a unique body so the dedup check passes.
func BenchmarkAdmitWithDuplication(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return true },
		DuplicateWindow:    func(string) time.Duration { return 5 * time.Second },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		body := []byte("message " + string(rune('a'+n%26)) + string(rune('0'+n/26%10)))
		req := Request{
			TenantID: "t1", Channel: "room:1", Subject: "u1",
			Body: body,
			Now:  now,
		}
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitDuplicateHit measures the duplicate suppression path: the
// exact same body from the same publisher within the window is suppressed.
func BenchmarkAdmitDuplicateHit(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return true },
		DuplicateWindow:    func(string) time.Duration { return 5 * time.Second },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	// Prime the window
	body := []byte("identical content")
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: body, Now: now}
	_ = p.Admit(req)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); res.Outcome != OutcomeDuplicate {
			b.Fatalf("expected duplicate, got %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitContentDuplicateHit measures the content-duplicate filter:
// same user, same content, different channel.
func BenchmarkAdmitContentDuplicateHit(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return true },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	body := []byte("spam spam spam")
	// Prime
	_ = p.Admit(Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: body, Now: now})
	req := Request{TenantID: "t1", Channel: "room:2", Subject: "u1", Body: body, Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); res.Outcome != OutcomeDuplicate {
			b.Fatalf("expected duplicate, got %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitWithChannelCeiling measures the channel rate limit check.
func BenchmarkAdmitWithChannelCeiling(b *testing.B) {
	p := newBenchHarness(b)
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte("hello"),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitRateLimited measures the tenant rate limit refusal path
// (fail-fast policy).
func BenchmarkAdmitRateLimited(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return false }, // always rate limited
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: []byte("x"), Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = p.Admit(req)
	}
}

// BenchmarkAdmitModerationBlock measures the moderation block path.
func BenchmarkAdmitModerationBlock(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	if err := matcher.Reload("t1", []controlplane.ModerationRule{
		{Pattern: "forbidden", Action: controlplane.ActionBlock, Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r1"},
	}); err != nil {
		b.Fatalf("Reload: %v", err)
	}
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: []byte("say forbidden things"), Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = p.Admit(req)
	}
}

// BenchmarkAdmitModerationReplace measures the moderation replacement path.
func BenchmarkAdmitModerationReplace(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	if err := matcher.Reload("t1", []controlplane.ModerationRule{
		{Pattern: "badword", Action: controlplane.ActionReplace, Replacement: "***", Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r1"},
	}); err != nil {
		b.Fatalf("Reload: %v", err)
	}
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return true },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyFailFast },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	body := []byte("say badword now")
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: body, Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitDegraded measures the degrade policy path.
func BenchmarkAdmitDegraded(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return false },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyDegrade },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte("degraded"),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: []byte("hello"), Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() || res.Outcome != OutcomeDegraded {
			b.Fatalf("unexpected: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitDiscarded measures the discard policy path.
func BenchmarkAdmitDiscarded(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	matcher := moderation.NewAutomaton()
	_ = matcher.Reload("t1", nil)
	now := time.UnixMilli(1_700_000_000_000)
	p := New(Deps{
		Privileges:         func(string) bool { return true },
		ChannelCeiling:     func(string, string) float64 { return 0 },
		AllowTenantPublish: func(string, time.Time) bool { return false },
		AllowUserPublish:   func(string, string, time.Time) bool { return true },
		Restriction:        func(string, string, string, time.Time) (controlplane.Restriction, bool) { return controlplane.Restriction{}, false },
		Policy:             func(string) string { return controlplane.PolicyDiscard },
		Matcher:            matcher,
		DuplicateEnabled:   func(string) bool { return false },
		DuplicateContentEnabled: func(string) bool { return false },
		PromoEnabled:       func(string) bool { return true },
		IDs:                ids,
		MaxMessageBytes:    4096,
		DegradeBody:        []byte(""),
		Audit:              func(string, string, string, string, string, string) {},
		Observe:            func(string, bool, time.Time) {},
	})
	req := Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: []byte("hello"), Now: now}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = p.Admit(req)
	}
}

// BenchmarkIDGenNext measures the ID generator in isolation.
func BenchmarkIDGenNext(b *testing.B) {
	ids, _ := protocol.NewIDGen(1)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = ids.Next()
	}
}

// BenchmarkAdmitConcurrent measures concurrent admission throughput.
func BenchmarkAdmitConcurrent(b *testing.B) {
	p := newBenchHarness(b)
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte("concurrent admission"),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if res := p.Admit(req); !res.Accepted() {
				b.Fatalf("unexpected refusal: %s", res.Outcome)
			}
		}
	})
}

// BenchmarkAdmitLargeBody measures admission with a larger body (near limit).
func BenchmarkAdmitLargeBody(b *testing.B) {
	p := newBenchHarness(b)
	body := strings.Repeat("x", 4000)
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte(body),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if res := p.Admit(req); !res.Accepted() {
			b.Fatalf("unexpected refusal: %s", res.Outcome)
		}
	}
}

// BenchmarkAdmitSizeCheckRefusal measures the size check refusal path.
func BenchmarkAdmitSizeCheckRefusal(b *testing.B) {
	p := newBenchHarness(b)
	body := strings.Repeat("x", 5000)
	req := Request{
		TenantID: "t1", Channel: "room:1", Subject: "u1",
		Body: []byte(body),
		Now:  time.UnixMilli(1_700_000_000_000),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = p.Admit(req)
	}
}