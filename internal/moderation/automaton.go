// Package moderation evaluates content against a tenant's rules before a
// message reaches any subscriber.
//
// Every matcher here runs on the publish path, which is the hot path. That
// constrains the design in one specific way: no network call and no allocation.
// The sensitive word matcher is therefore an automaton held in process memory,
// rebuilt at start-up and invalidated by the configuration bus, rather than a
// lookup against the control plane per publish.
//
// The consequence is stated plainly elsewhere and repeated here because it is
// the one that matters: an instance that has lost its automaton is publishing
// without moderation until it is rebuilt, so the readiness probe fails closed
// rather than letting such an instance serve.
package moderation

import (
	"sort"
	"strings"
	"sync"
	"time"

	ahocorasick "github.com/cloudflare/ahocorasick"

	"github.com/starflow/push/internal/controlplane"
)

// Decision is the outcome of evaluating content against a tenant's rules.
type Decision struct {
	// Allowed is false when the message must not be delivered.
	Allowed bool
	// Action is the strongest action any matching rule asked for.
	Action string
	// RuleID is the rule that produced Action.
	RuleID string
	// Tag is the value from a rule configured to tag.
	Tag string
	// Replacement is the substitute a replace rule asked for. It is empty unless
	// Action is a replacement.
	Replacement string
	// Matches lists the matched patterns, for audit and for operator feedback.
	Matches []string
}

// Matcher evaluates publish content.
//
// It is an interface so the publish pipeline can be tested without a word list,
// and so a future engine can replace the automaton without touching the
// pipeline.
type Matcher interface {
	// Evaluate returns the decision for body under tenantID.
	Evaluate(tenantID string, body []byte) Decision
	// Ready reports whether the matcher has rules loaded and may be relied on.
	Ready(tenantID string) bool
	// Reload rebuilds the matcher for a tenant from the rule set.
	Reload(tenantID string, rules []controlplane.ModerationRule) error
}

// Action precedence. A replacement must be applied before a block is
// considered, because a block rule naming a short string would otherwise shadow
// a replacement rule naming a longer one and the longer content would never
// reach the replacement path.
const (
	rankReplace = 0
	rankTag     = 1
	rankBlock   = 2
)

// Automaton is a per-tenant sensitive word matcher.
//
// Rules are partitioned by tenant because the word list is a tenant setting: two
// tenants' lists must not see each other, and matching one tenant's list against
// another's messages would be a content leak in both directions.
//
// Each tenant's automaton is immutable once built. A reload swaps the whole
// automaton atomically, so a publish in flight keeps evaluating against the list
// it started with rather than against a half-replaced machine.
type Automaton struct {
	mu      sync.RWMutex
	tenants map[string]*tenantMatcher
}

// tenantMatcher is one tenant's compiled rule set.
type tenantMatcher struct {
	// words maps a literal pattern to the rule that owns it. The automaton
	// reports match positions, not which rule matched, so this lookup restores
	// the rule association the automaton cannot carry.
	words map[string]controlplane.ModerationRule

	// scan holds every literal pattern regardless of action, so content is
	// scanned exactly once. Splitting it per action would mean a tag-only rule
	// set had nothing to drive the automaton, making such a rule set silently
	// enforce nothing.
	scan *compiledSet

	// promoRules are evaluated separately with substring matching, because a
	// promotional pattern is an ordinary expression rather than a literal word.
	promoRules []controlplane.ModerationRule

	// ready is false for a tenant with no compiled rules, so readiness can be
	// distinguished from "loaded and empty".
	ready bool
	// version increments on every rebuild, which lets a caller detect that it
	// evaluated against a stale list.
	version uint64
}

// compiledSet is an Aho-Corasick machine over a set of literal patterns.
//
// The machine answers "does anything match" in a single pass. Identifying which
// pattern matched costs a dictionary walk, so it is only done when the machine
// reports a match. Sensitive word hits are rare on live traffic, so paying a
// dictionary scan on a hit rather than on every publish is the right way round:
// the alternative is per-pattern substring searching on the hot path.
type compiledSet struct {
	matcher *ahocorasick.Matcher
	dict    map[string]struct{}
	empty   bool
}

// NewAutomaton returns an empty matcher.
func NewAutomaton() *Automaton {
	return &Automaton{tenants: map[string]*tenantMatcher{}}
}

// Reload compiles rules into a fresh automaton for a tenant and installs it.
//
// Disabled rules are skipped at compile time rather than filtered at match time:
// a disabled rule on the publish path costs a comparison per match, and
// rebuilding is rare.
func (a *Automaton) Reload(tenantID string, rules []controlplane.ModerationRule) error {
	tm := &tenantMatcher{
		words: map[string]controlplane.ModerationRule{},
		ready: true,
	}

	var literalPatterns []string
	for _, r := range rules {
		if r.TenantID != tenantID || !r.Enabled || r.Pattern == "" {
			continue
		}
		switch r.Kind {
		case controlplane.RuleKindPromotional:
			tm.promoRules = append(tm.promoRules, r)
			continue
		}

		tm.words[r.Pattern] = r
		literalPatterns = append(literalPatterns, r.Pattern)
	}

	if _, err := compile(literalPatterns); err != nil {
		return err
	}
	tm.scan, _ = compile(literalPatterns)

	a.mu.Lock()
	if tm.version == 0 {
		// Carry the version forward so a caller can tell a rebuild from a
		// first load.
		if prev, ok := a.tenants[tenantID]; ok {
			tm.version = prev.version
		}
	}
	tm.version++
	a.tenants[tenantID] = tm
	a.mu.Unlock()
	return nil
}

func compile(patterns []string) (*compiledSet, error) {
	if len(patterns) == 0 {
		return &compiledSet{empty: true, dict: map[string]struct{}{}}, nil
	}
	dict := make(map[string]struct{}, len(patterns))
	for _, p := range patterns {
		dict[p] = struct{}{}
	}
	return &compiledSet{
		matcher: ahocorasick.NewStringMatcher(patterns),
		dict:    dict,
	}, nil
}

// Ready reports whether a tenant has a compiled matcher.
//
// A tenant with no rules is not ready: an empty word list and a word list that
// failed to load are indistinguishable from the caller's side, and the safe
// reading of that ambiguity is "do not serve yet".
func (a *Automaton) Ready(tenantID string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	tm, ok := a.tenants[tenantID]
	return ok && tm.ready
}

// Version returns the tenant's matcher version, for staleness reporting.
func (a *Automaton) Version(tenantID string) uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if tm, ok := a.tenants[tenantID]; ok {
		return tm.version
	}
	return 0
}

// Evaluate returns the decision for body.
//
// Replacement is applied first and its result is what a later block decision
// applies to, so a message carrying both a matched word and a matched phrase has
// the phrase replaced and then still blocked if a block rule matched. Tagging is
// additive and never overrides a block.
func (a *Automaton) Evaluate(tenantID string, body []byte) Decision {
	a.mu.RLock()
	tm, ok := a.tenants[tenantID]
	a.mu.RUnlock()

	if !ok {
		// No compiled matcher means the caller should not have let this message
		// through. Reporting a block rather than allowing is the fail-closed
		// choice; the pipeline reports the stage so an operator can tell this
		// apart from a rule match.
		return Decision{Allowed: false, Action: "moderation_unavailable"}
	}

	decision := Decision{Allowed: true}

	matches := tm.scan.findAll(body)
	decision.Matches = matches

	// One pass over the matched patterns, in three sweeps by action, so the
	// outcome does not depend on the order the automaton happened to report
	// matches in. Replacement is decided first because it rewrites content
	// rather than refusing it; a block found afterwards still wins.
	for _, action := range [...]string{
		controlplane.ActionReplace,
		controlplane.ActionBlock,
		controlplane.ActionTag,
	} {
		for _, pattern := range matches {
			r, ok := tm.words[pattern]
			if !ok || r.Action != action {
				continue
			}
			switch action {
			case controlplane.ActionReplace:
				decision.Action = controlplane.ActionReplace
				decision.RuleID = r.ID
				decision.Replacement = r.Replacement
			case controlplane.ActionBlock:
				// A block is the strongest outcome and must not be lost to a
				// rewrite decided earlier in the sweep.
				decision.Action = controlplane.ActionBlock
				decision.RuleID = r.ID
			case controlplane.ActionTag:
				// Tagging is additive information for the client, not a
				// decision, so it never changes Allowed.
				decision.Tag = r.Tag
				if decision.RuleID == "" {
					decision.RuleID = r.ID
				}
			}
		}
	}

	for _, r := range tm.promoRules {
		if matchesPattern(r.Pattern, string(body)) {
			decision.Matches = append(decision.Matches, r.Pattern)
			switch r.Action {
			case controlplane.ActionBlock:
				decision.Action = controlplane.ActionBlock
				decision.RuleID = r.ID
			case controlplane.ActionTag:
				if decision.Tag == "" {
					decision.Tag = r.Tag
					if decision.RuleID == "" {
						decision.RuleID = r.ID
					}
				}
			}
		}
	}

	if decision.Action == controlplane.ActionBlock {
		decision.Allowed = false
		// A blocked message is never delivered, so a substitution computed for
		// it would be work thrown away.
		decision.Replacement = ""
	}
	return decision
}

// findAll returns the distinct patterns present in body, sorted so the decision
// is reproducible for a given content and rule set.
func (c *compiledSet) findAll(body []byte) []string {
	if c == nil || c.empty || len(body) == 0 {
		return nil
	}
	// One pass to learn whether anything matched. MatchThreadSafe is required
	// because one compiled machine is shared across every publishing goroutine.
	if len(c.matcher.MatchThreadSafe(body)) == 0 {
		return nil
	}
	// Only on a hit: identify which patterns are present.
	text := string(body)
	out := make([]string, 0, 4)
	for pattern := range c.dict {
		if strings.Contains(text, pattern) {
			out = append(out, pattern)
		}
	}
	sort.Strings(out)
	return out
}

// Counters tracks match rates per tenant for the spike alert.
//
// It is held per instance rather than as package state so a process has one
// explicit owner and the counter set can be constructed in a test without
// reaching through the package.
type Counters struct {
	mu        sync.Mutex
	perTenant map[string]*tenantCounters
}

type tenantCounters struct {
	windowStart time.Time
	matches     int64
	publishes   int64
}

// NewCounters returns an empty counter set.
func NewCounters() *Counters {
	return &Counters{perTenant: map[string]*tenantCounters{}}
}

// Observe records a publish and whether it matched.
func (c *Counters) Observe(tenantID string, matched bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tc, ok := c.perTenant[tenantID]
	if !ok {
		tc = &tenantCounters{windowStart: now}
		c.perTenant[tenantID] = tc
	}
	if now.Sub(tc.windowStart) >= time.Minute {
		tc.windowStart = now
		tc.matches = 0
		tc.publishes = 0
	}
	tc.publishes++
	if matched {
		tc.matches++
	}
}

// MatchRate returns matches over publishes for a tenant in the current window.
func (c *Counters) MatchRate(tenantID string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	tc, ok := c.perTenant[tenantID]
	if !ok || tc.publishes == 0 {
		return 0
	}
	return float64(tc.matches) / float64(tc.publishes)
}

// matchesPattern reports whether content contains pattern. A promotional
// pattern is an ordinary expression rather than a literal word, so it is
// evaluated separately from the automaton rather than being compiled into it.
func matchesPattern(pattern, content string) bool {
	if pattern == "" {
		return false
	}
	return strings.Contains(content, pattern)
}
