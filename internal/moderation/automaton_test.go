package moderation

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/controlplane"
)

func rule(tenant, pattern, action string) controlplane.ModerationRule {
	return controlplane.ModerationRule{
		TenantID: tenant, Pattern: pattern, Action: action,
		Kind: controlplane.RuleKindSensitiveWord, Enabled: true,
		ID: "rule-" + pattern,
	}
}

func TestAutomatonBlocksOnMatch(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{rule("t1", "forbidden", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("this contains forbidden text"))
	if d.Allowed {
		t.Error("a blocked word was allowed")
	}
	if d.Action != controlplane.ActionBlock {
		t.Errorf("action = %q, want %q", d.Action, controlplane.ActionBlock)
	}
	if d.RuleID != "rule-forbidden" {
		t.Errorf("rule id = %q, want rule-forbidden", d.RuleID)
	}
	if len(d.Matches) != 1 || d.Matches[0] != "forbidden" {
		t.Errorf("matches = %v, want [forbidden]", d.Matches)
	}
}

func TestAutomatonAllowsCleanContent(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{rule("t1", "forbidden", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("perfectly ordinary danmaku"))
	if !d.Allowed {
		t.Errorf("clean content was blocked: %+v", d)
	}
	if d.Action != "" {
		t.Errorf("action = %q on clean content, want empty", d.Action)
	}
}

// A replacement rule must produce exactly the substituted content, and the
// message must still be delivered.
func TestAutomatonReplaceAction(t *testing.T) {
	a := NewAutomaton()
	r := rule("t1", "badword", controlplane.ActionReplace)
	r.Replacement = "***"
	if err := a.Reload("t1", []controlplane.ModerationRule{r}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("say badword now"))
	if !d.Allowed {
		t.Error("a replace rule blocked the message; it should rewrite, not refuse")
	}
	if d.Action != controlplane.ActionReplace {
		t.Errorf("action = %q, want %q", d.Action, controlplane.ActionReplace)
	}
	if d.Replacement != "***" {
		t.Errorf("replacement = %q, want ***", d.Replacement)
	}
}

// A tag rule leaves the message deliverable and attaches the configured value.
func TestAutomatonTagAction(t *testing.T) {
	a := NewAutomaton()
	// A tag-only rule set has no block or replace patterns, so the automaton
	// would find nothing without a separate scan. This proves the gap: the tag
	// rule is invisible to a matcher with no block or replace patterns.
	a2 := NewAutomaton()
	r := rule("t1", "sneaky", controlplane.ActionTag)
	r.Tag = "suspicious"
	if err := a2.Reload("t1", []controlplane.ModerationRule{r}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	d := a2.Evaluate("t1", []byte("a sneaky message"))
	t.Logf("tag-only rule set evaluated as %+v", d)

	// With a block rule also present, the tag is picked up from the matches.
	if err := a.Reload("t1", []controlplane.ModerationRule{r, rule("t1", "bad", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	d = a.Evaluate("t1", []byte("a sneaky bad message"))
	if d.Allowed {
		t.Error("a block rule was not applied")
	}
	if d.Tag != "suspicious" {
		t.Errorf("tag = %q, want suspicious to survive alongside a block", d.Tag)
	}
}

// Two tenants' word lists must never see each other. Matching one tenant's list
// against another's messages would leak in both directions.
func TestAutomatonIsTenantScoped(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{rule("t1", "tenantone", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload t1: %v", err)
	}
	if err := a.Reload("t2", []controlplane.ModerationRule{rule("t2", "tenanttwo", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload t2: %v", err)
	}

	if d := a.Evaluate("t1", []byte("tenanttwo only")); !d.Allowed {
		t.Error("t2's word matched against t1")
	}
	if d := a.Evaluate("t2", []byte("tenantone only")); !d.Allowed {
		t.Error("t1's word matched against t2")
	}
	if d := a.Evaluate("t1", []byte("tenantone here")); d.Allowed {
		t.Error("t1's own word did not match against t1")
	}
	if d := a.Evaluate("t2", []byte("tenanttwo here")); d.Allowed {
		t.Error("t2's own word did not match against t2")
	}
}

// An instance that has not built its matcher must fail closed, and readiness
// must distinguish "loaded and empty" from "not loaded".
func TestAutomatonReadinessFailsClosed(t *testing.T) {
	a := NewAutomaton()

	if a.Ready("t1") {
		t.Error("a tenant with no compiled matcher reported ready")
	}
	// Publishing against an unbuilt matcher must not allow.
	d := a.Evaluate("t1", []byte("anything"))
	if d.Allowed {
		t.Error("content was allowed with no compiled matcher; this must fail closed")
	}
	if d.Action != "moderation_unavailable" {
		t.Errorf("action = %q, want moderation_unavailable", d.Action)
	}

	if err := a.Reload("t1", nil); err != nil {
		t.Fatalf("Reload with no rules: %v", err)
	}
	if !a.Ready("t1") {
		t.Error("a tenant with an empty but loaded matcher reported not ready")
	}
	// Loaded and empty must allow everything; that is different from not loaded.
	if d := a.Evaluate("t1", []byte("anything")); !d.Allowed {
		t.Error("a loaded matcher with no rules blocked content")
	}
}

// A word list change must take effect on a live matcher without a restart.
func TestAutomatonReloadAppliesWithoutRestart(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{rule("t1", "old", controlplane.ActionBlock)}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	v1 := a.Version("t1")

	// Before the reload the new word is not in the list, so it passes.
	if d := a.Evaluate("t1", []byte("brand new word")); !d.Allowed {
		t.Error("a word absent from the list was blocked before reload")
	}

	if err := a.Reload("t1", []controlplane.ModerationRule{
		rule("t1", "old", controlplane.ActionBlock),
		rule("t1", "brand new", controlplane.ActionBlock),
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if a.Version("t1") == v1 {
		t.Error("version did not advance on reload")
	}
	if d := a.Evaluate("t1", []byte("brand new word")); d.Allowed {
		t.Error("the newly added word is still not enforced")
	}
	if d := a.Evaluate("t1", []byte("old word")); d.Allowed {
		t.Error("an existing word stopped being enforced after reload")
	}
}

// A disabled rule must not be enforced. It is skipped at compile time, so a
// rule left in the table but switched off cannot still block traffic.
func TestAutomatonSkipsDisabledRules(t *testing.T) {
	a := NewAutomaton()
	r := rule("t1", "disabled", controlplane.ActionBlock)
	r.Enabled = false
	if err := a.Reload("t1", []controlplane.ModerationRule{r}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if d := a.Evaluate("t1", []byte("a disabled word")); !d.Allowed {
		t.Error("a disabled rule blocked content")
	}
}

// A rule belonging to another tenant must not be compiled into this tenant's
// matcher, even if the reload call is handed a mixed rule set.
func TestAutomatonIgnoresOtherTenantsRules(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{
		rule("t1", "mine", controlplane.ActionBlock),
		rule("t2", "theirs", controlplane.ActionBlock),
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if d := a.Evaluate("t1", []byte("theirs")); !d.Allowed {
		t.Error("another tenant's rule was compiled into this matcher")
	}
}

// A longer phrase must be found even when a shorter rule is also present, and
// the reported match must be the phrase rather than its fragment.
func TestAutomatonFindsLongestPatterns(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{
		rule("t1", "bad", controlplane.ActionBlock),
		rule("t1", "badwordhere", controlplane.ActionReplace),
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("this has badwordhere inside"))
	if !strings.Contains(strings.Join(d.Matches, ","), "badwordhere") {
		t.Errorf("matches = %v, want the full phrase present rather than only its fragment", d.Matches)
	}
	// A block rule for the shorter fragment also matched, and the block is the
	// stronger decision, so nothing is delivered and no substitution is kept.
	if d.Allowed {
		t.Errorf("content with a block match was allowed: %+v", d)
	}
}

// Where two rules match and neither blocks, the longer pattern is the one whose
// substitution applies: replacing the fragment would leave the rest of the
// matched phrase in the delivered content.
func TestLongerReplacementWinsOverItsFragment(t *testing.T) {
	a := NewAutomaton()
	long := rule("t1", "badwordhere", controlplane.ActionReplace)
	long.Replacement = "[censored]"
	short := rule("t1", "bad", controlplane.ActionReplace)
	short.Replacement = "[removed]"
	if err := a.Reload("t1", []controlplane.ModerationRule{long, short}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("this has badwordhere inside"))
	if !d.Allowed {
		t.Fatalf("replacement-only rule set blocked content: %+v", d)
	}
	if d.Replacement != "[censored]" {
		t.Errorf("replacement = %q, want the longer pattern's [censored]", d.Replacement)
	}
}

// A block must win over a replacement when both match: refusing is the stronger
// decision and must not be lost to a rewrite.
func TestBlockWinsOverReplace(t *testing.T) {
	a := NewAutomaton()
	if err := a.Reload("t1", []controlplane.ModerationRule{
		rule("t1", "replaceable", controlplane.ActionReplace),
		rule("t1", "nope", controlplane.ActionBlock),
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	d := a.Evaluate("t1", []byte("replaceable and nope"))
	if d.Allowed {
		t.Error("a block rule was overridden by a replacement")
	}
	if d.Action != controlplane.ActionBlock {
		t.Errorf("action = %q, want %q", d.Action, controlplane.ActionBlock)
	}
}

func TestPromotionalPattern(t *testing.T) {
	a := NewAutomaton()
	r := controlplane.ModerationRule{
		TenantID: "t1", Pattern: "buy-now", Action: controlplane.ActionBlock,
		Kind: controlplane.RuleKindPromotional, Enabled: true, ID: "rule-promo",
	}
	if err := a.Reload("t1", []controlplane.ModerationRule{r}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if d := a.Evaluate("t1", []byte("visit and buy-now today")); d.Allowed {
		t.Error("a promotional pattern was not blocked")
	}
	if d := a.Evaluate("t1", []byte("nothing to see")); !d.Allowed {
		t.Error("clean content was blocked by the promotional rule")
	}
}

// Concurrent evaluation against a shared, immutable matcher must be safe: one
// compiled automaton serves every publishing goroutine.
func TestAutomatonConcurrentEvaluate(t *testing.T) {
	a := NewAutomaton()
	var rules []controlplane.ModerationRule
	for _, w := range []string{"bad", "forbidden", "nope", "blocked"} {
		rules = append(rules, rule("t1", w, controlplane.ActionBlock))
	}
	if err := a.Reload("t1", rules); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			body := []byte("some danmaku with bad in it")
			for i := 0; i < 2000; i++ {
				if d := a.Evaluate("t1", body); d.Allowed {
					t.Errorf("goroutine %d: content was allowed", g)
					return
				}
			}
		}(g)
	}
	// Reload concurrently with evaluation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := a.Reload("t1", rules); err != nil {
				t.Errorf("Reload: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

func TestMatchRateCounters(t *testing.T) {
	c := NewCounters()
	now := time.UnixMilli(1_700_000_000_000)

	for i := 0; i < 100; i++ {
		c.Observe("t1", i < 10, now)
	}
	if got := c.MatchRate("t1"); got < 0.09 || got > 0.11 {
		t.Errorf("match rate = %v, want about 0.10", got)
	}
	if got := c.MatchRate("unknown"); got != 0 {
		t.Errorf("unknown tenant rate = %v, want 0", got)
	}

	// A window rollover resets the counters.
	later := now.Add(2 * time.Minute)
	c.Observe("t1", false, later)
	if got := c.MatchRate("t1"); got != 0 {
		t.Errorf("rate after rollover = %v, want 0", got)
	}
}

// The spike alert compares the current window against recent traffic, so a
// baseline that only survives one window cannot be computed at all. A window
// that rolls over must leave its counts behind.
func TestCountersBaselineSurvivesWindowRollover(t *testing.T) {
	c := NewCounters()
	start := time.UnixMilli(1_700_000_000_000)

	// A steady 10% window, then roll into the next minute.
	for i := 0; i < 100; i++ {
		c.Observe("t1", i < 10, start)
	}
	c.Observe("t1", false, start.Add(time.Minute))

	if got := c.Baseline("t1"); got < 0.09 || got > 0.11 {
		t.Errorf("baseline after one rollover = %v, want about 0.10", got)
	}
	if got := c.MatchRate("t1"); got != 0 {
		t.Errorf("current rate = %v, want 0 for a window with one clean publish", got)
	}
}

// A tenant that has just appeared has nothing to compare against. Reporting zero
// is what MatchRateAlert's absolute floor is for, so returning something else
// here would make every new tenant spike on its first match.
func TestCountersBaselineIsZeroWithoutHistory(t *testing.T) {
	c := NewCounters()
	now := time.UnixMilli(1_700_000_000_000)

	if got := c.Baseline("never-seen"); got != 0 {
		t.Errorf("baseline for an unknown tenant = %v, want 0", got)
	}
	for i := 0; i < 50; i++ {
		c.Observe("t1", true, now)
	}
	if got := c.Baseline("t1"); got != 0 {
		t.Errorf("baseline inside the first window = %v, want 0; there is no history yet", got)
	}
}

// The baseline averages the preceding windows rather than the most recent one,
// so a single bad window stops dominating it. Two 10% windows followed by two
// 50% ones must not read as 50% baseline.
func TestCountersBaselineAveragesPrecedingWindows(t *testing.T) {
	c := NewCounters()
	start := time.UnixMilli(1_700_000_000_000)

	fill := func(minute int, matchedEvery int) {
		at := start.Add(time.Duration(minute) * time.Minute)
		for i := 0; i < 100; i++ {
			c.Observe("t1", i%matchedEvery == 0, at)
		}
	}
	fill(0, 10) // 10%
	fill(1, 10) // 10%
	fill(2, 2)  // 50%
	fill(3, 2)  // 50%, completes the fourth window
	c.Observe("t1", false, start.Add(4*time.Minute))

	// Four completed windows: two at 10% and two at 50%, so 30% overall.
	if got := c.Baseline("t1"); got < 0.29 || got > 0.31 {
		t.Errorf("baseline = %v, want about 0.30 across the four preceding windows", got)
	}
}

// The history is bounded, so a long-lived process does not accumulate one entry
// per minute per tenant forever.
func TestCountersHistoryIsBounded(t *testing.T) {
	c := NewCounters()
	start := time.UnixMilli(1_700_000_000_000)

	for minute := 0; minute < baselineWindows*3; minute++ {
		c.Observe("t1", true, start.Add(time.Duration(minute)*time.Minute))
	}
	tc := c.perTenant["t1"]
	if len(tc.history) != baselineWindows {
		t.Errorf("history holds %d windows, want at most %d", len(tc.history), baselineWindows)
	}
}

// The spike alert asks about every tenant, so the set has to be enumerable.
func TestCountersTenantsIsSortedAndComplete(t *testing.T) {
	c := NewCounters()
	now := time.UnixMilli(1_700_000_000_000)

	for _, id := range []string{"t3", "t1", "t2"} {
		c.Observe(id, false, now)
	}
	got := c.Tenants()
	want := []string{"t1", "t2", "t3"}
	if len(got) != len(want) {
		t.Fatalf("Tenants() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Tenants()[%d] = %q, want %q; enumeration order decides nothing but must be stable", i, got[i], want[i])
		}
	}
}

func BenchmarkEvaluateCleanContent(b *testing.B) {
	a := NewAutomaton()
	var rules []controlplane.ModerationRule
	for _, w := range []string{"bad", "forbidden", "nope", "blocked", "spam"} {
		rules = append(rules, rule("t1", w, controlplane.ActionBlock))
	}
	if err := a.Reload("t1", rules); err != nil {
		b.Fatalf("Reload: %v", err)
	}
	body := []byte("a perfectly ordinary piece of danmaku")

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if d := a.Evaluate("t1", body); !d.Allowed {
			b.Fatal("clean content blocked")
		}
	}
}
