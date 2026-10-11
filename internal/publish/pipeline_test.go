package publish

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/moderation"
	"github.com/starflow/push/internal/protocol"
)

type harness struct {
	pipeline *Pipeline
	// matcher is the automaton the pipeline was built with. The harness holds
	// it because it also owns rule loading; reaching back through the pipeline
	// for it would mean adding an accessor to the production type for tests.
	matcher moderation.Matcher
	now     time.Time

	canPublish  bool
	tenantAllow bool
	userAllow   bool
	ceiling     float64
	policy      string
	restriction *controlplane.Restriction
	dedupOn     bool
	dedupWindow time.Duration
	dupContent  bool
	blocked     bool

	// auditMu guards audit, which the pipeline appends to from whichever
	// goroutine is admitting.
	auditMu sync.Mutex
	audit   []string

	// observeMu guards observed, which the pipeline appends to from whichever
	// goroutine is admitting.
	observeMu sync.Mutex
	observed  []string
}

// observedTenants returns the scopes Observe was called with.
func (h *harness) observedTenants() []string {
	h.observeMu.Lock()
	defer h.observeMu.Unlock()
	return append([]string(nil), h.observed...)
}

// auditOutcomes returns a copy of the recorded outcomes.
func (h *harness) auditOutcomes() []string {
	h.auditMu.Lock()
	defer h.auditMu.Unlock()
	return append([]string(nil), h.audit...)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ids, err := protocol.NewIDGen(1)
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	h := &harness{
		matcher:     moderation.NewAutomaton(),
		now:         time.UnixMilli(1_700_000_000_000),
		canPublish:  true,
		tenantAllow: true,
		userAllow:   true,
		ceiling:     0,
		policy:      controlplane.PolicyFailFast,
		dedupOn:     true,
		dedupWindow: 3 * time.Second,
		dupContent:  true,
	}
	matcher := h.matcher
	// A tenant with no configured words still has a loaded, empty matcher. The
	// distinction matters: an unloaded matcher fails closed and would block every
	// publish, which is the correct behaviour for a serving instance that has not
	// rebuilt its rules, but is not the state a configured tenant is in.
	if err := matcher.Reload("t1", nil); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	h.pipeline = New(Deps{
		Privileges:         func(string) bool { return h.canPublish },
		ChannelCeiling:     func(string, string) float64 { return h.ceiling },
		AllowTenantPublish: func(string, time.Time) bool { return h.tenantAllow },
		AllowUserPublish:   func(string, string, time.Time) bool { return h.userAllow },
		Restriction: func(string, string, string, time.Time) (controlplane.Restriction, bool) {
			if h.restriction != nil {
				return *h.restriction, true
			}
			return controlplane.Restriction{}, false
		},
		Policy:                  func(string) string { return h.policy },
		Matcher:                 matcher,
		DuplicateEnabled:        func(string) bool { return h.dedupOn },
		DuplicateWindow:         func(string) time.Duration { return h.dedupWindow },
		DuplicateContentEnabled: func(string) bool { return h.dupContent },
		PromoEnabled:            func(string) bool { return true },
		IDs:                     ids,
		MaxMessageBytes:         4096,
		DegradeBody:             []byte(""),
		Audit: func(subject, channel, messageID, ruleID, outcome, detail string) {
			h.auditMu.Lock()
			h.audit = append(h.audit, outcome)
			h.auditMu.Unlock()
		},
		Observe: func(tenantID string, matched bool, now time.Time) {
			h.observeMu.Lock()
			h.observed = append(h.observed, tenantID)
			h.observeMu.Unlock()
		},
	})
	return h
}

// reloadWords installs a moderation rule for the harness's tenant.
func (h *harness) reloadWords(t *testing.T, r controlplane.ModerationRule) {
	t.Helper()
	r.TenantID = "t1"
	if r.Kind == "" {
		r.Kind = controlplane.RuleKindSensitiveWord
	}
	r.Enabled = true
	if err := h.matcher.Reload("t1", []controlplane.ModerationRule{r}); err != nil {
		t.Fatalf("Reload rule %q: %v", r.Pattern, err)
	}
}

func (h *harness) req(body string) Request {
	return Request{TenantID: "t1", Channel: "room:1", Subject: "u1", Body: []byte(body), Now: h.now}
}

func TestAcceptedMessageGetsIdentityAndBody(t *testing.T) {
	h := newHarness(t)
	res := h.pipeline.Admit(h.req("hello"))

	if !res.Accepted() {
		t.Fatalf("outcome = %q, want accepted", res.Outcome)
	}
	if res.MessageID == 0 {
		t.Error("accepted message has no identifier")
	}
	if string(res.Body) != "hello" {
		t.Errorf("body = %q, want the submitted content", res.Body)
	}
	outcomes := h.auditOutcomes()
	if len(outcomes) != 1 || outcomes[0] != string(OutcomeAccepted) {
		t.Errorf("audit = %v, want one accepted entry", outcomes)
	}
}

// The match-rate counters are keyed by tenant, so the scope Observe is handed
// has to be the tenant. Passing the publishing subject instead buckets every
// publisher together: one tenant's traffic lands in another's window, and a
// tenant's rate becomes a function of how many distinct subjects it has rather
// than of how much of its content matched.
func TestObserveReportsTheTenantNotThePublishingSubject(t *testing.T) {
	h := newHarness(t)
	h.pipeline.Admit(h.req("hello"))

	got := h.observedTenants()
	if len(got) != 1 {
		t.Fatalf("Observe called %d times, want 1", len(got))
	}
	if got[0] == h.req("hello").Subject {
		t.Errorf("Observe received the publishing subject %q; the counters are keyed by tenant", got[0])
	}
	if got[0] != "t1" {
		t.Errorf("Observe received scope %q, want the tenant %q", got[0], "t1")
	}
}

// Identifiers must be unique across a burst, because the coalescer delta-encodes
// timestamps against the first message in a batch and clients trace by id.
func TestMessageIdentifiersAreUnique(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false
	h.dupContent = false

	seen := map[uint64]bool{}
	for i := 0; i < 5000; i++ {
		res := h.pipeline.Admit(h.req("same body every time"))
		if !res.Accepted() {
			t.Fatalf("iteration %d refused: %s", i, res.Outcome)
		}
		if seen[res.MessageID] {
			t.Fatalf("identifier %d was issued twice", res.MessageID)
		}
		seen[res.MessageID] = true
	}
}

func TestPermissionRefusal(t *testing.T) {
	h := newHarness(t)
	h.canPublish = false

	res := h.pipeline.Admit(h.req("hello"))
	if res.Accepted() {
		t.Fatal("a credential without the publish privilege was admitted")
	}
	if res.Outcome != OutcomeForbidden || res.Stage != StagePermission {
		t.Errorf("outcome = %q at stage %q, want forbidden at permission", res.Outcome, res.Stage)
	}
}

// The stage order is a contract: a message refused for size must not be reported
// as a moderation block, and vice versa.
func TestStageOrderIsObservable(t *testing.T) {
	t.Run("moderation before size", func(t *testing.T) {
		h := newHarness(t)
		if err := h.matcher.Reload("t1", []controlplane.ModerationRule{{
			TenantID: "t1", Pattern: "forbidden", Action: controlplane.ActionBlock,
			Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r1",
		}}); err != nil {
			t.Fatalf("Reload: %v", err)
		}
		// Content that is both oversized and blocked must report moderation,
		// because moderation runs first.
		res := h.pipeline.Admit(h.req(strings.Repeat("forbidden ", 1000)))
		if res.Stage != StageModeration {
			t.Errorf("stage = %q, want moderation to run before size", res.Stage)
		}
	})

	t.Run("rate limit before moderation", func(t *testing.T) {
		h := newHarness(t)
		h.tenantAllow = false
		res := h.pipeline.Admit(h.req("hello"))
		if res.Stage != StageRateLimit {
			t.Errorf("stage = %q, want rate limit to run before moderation", res.Stage)
		}
	})

	t.Run("duplicate before moderation", func(t *testing.T) {
		h := newHarness(t)
		first := h.pipeline.Admit(h.req("repeat me"))
		if !first.Accepted() {
			t.Fatalf("first publish refused: %s", first.Outcome)
		}
		res := h.pipeline.Admit(h.req("repeat me"))
		if res.Outcome != OutcomeDuplicate || res.Stage != StageDuplicate {
			t.Errorf("outcome = %q at stage %q, want duplicate at the duplicate stage",
				res.Outcome, res.Stage)
		}
	})
}

func TestPerUserLimitIsDistinctFromTenantLimit(t *testing.T) {
	h := newHarness(t)
	h.userAllow = false

	res := h.pipeline.Admit(h.req("hello"))
	if res.Outcome != OutcomeUserLimited {
		t.Errorf("outcome = %q, want %q so a flooding user is distinguishable", res.Outcome, OutcomeUserLimited)
	}
	if res.Stage != StageRateLimit {
		t.Errorf("stage = %q, want %q", res.Stage, StageRateLimit)
	}
}

func TestChannelCeilingRefuses(t *testing.T) {
	h := newHarness(t)
	h.ceiling = 2

	for i := 0; i < 2; i++ {
		if res := h.pipeline.Admit(h.req("message " + string(rune('a'+i)))); !res.Accepted() {
			t.Fatalf("publish %d refused under the ceiling: %s", i, res.Outcome)
		}
	}
	res := h.pipeline.Admit(h.req("message c"))
	if res.Outcome != OutcomeChannelLimited {
		t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeChannelLimited)
	}
}

func TestMuteAndBan(t *testing.T) {
	t.Run("mute", func(t *testing.T) {
		h := newHarness(t)
		h.restriction = &controlplane.Restriction{Action: controlplane.RestrictionMute, Reason: "flooding"}
		res := h.pipeline.Admit(h.req("hello"))
		if res.Outcome != OutcomeMuted {
			t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeMuted)
		}
		if res.Detail != "flooding" {
			t.Errorf("detail = %q, want the restriction reason", res.Detail)
		}
	})

	t.Run("ban", func(t *testing.T) {
		h := newHarness(t)
		h.restriction = &controlplane.Restriction{Action: controlplane.RestrictionBan, Reason: "abuse"}
		if res := h.pipeline.Admit(h.req("hello")); res.Outcome != OutcomeBanned {
			t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeBanned)
		}
	})
}

func TestSizeLimit(t *testing.T) {
	h := newHarness(t)

	// Exactly at the limit is accepted.
	atLimit := strings.Repeat("a", 4096)
	if res := h.pipeline.Admit(h.req(atLimit)); !res.Accepted() {
		t.Errorf("a message exactly at the limit was refused: %s", res.Outcome)
	}
	over := strings.Repeat("a", 4097)
	res := h.pipeline.Admit(h.req(over))
	if res.Outcome != OutcomeTooLarge || res.Stage != StageSize {
		t.Errorf("outcome = %q at stage %q, want too_large at size", res.Outcome, res.Stage)
	}
	if !strings.Contains(res.Detail, "4097") {
		t.Errorf("detail = %q, want the observed size", res.Detail)
	}
}

func TestDuplicateSuppression(t *testing.T) {
	h := newHarness(t)
	h.dupContent = false

	first := h.pipeline.Admit(h.req("identical"))
	if !first.Accepted() {
		t.Fatalf("first refused: %s", first.Outcome)
	}
	second := h.pipeline.Admit(h.req("identical"))
	if second.Outcome != OutcomeDuplicate {
		t.Fatalf("second refused as %q, want duplicate", second.Outcome)
	}
	if second.MessageID != first.MessageID {
		t.Errorf("suppressed retry reported id %d, want the original %d",
			second.MessageID, first.MessageID)
	}

	// Past the window it is a new message.
	h.now = h.now.Add(10 * time.Second)
	if res := h.pipeline.Admit(h.req("identical")); !res.Accepted() {
		t.Errorf("identical content after the window was refused: %s", res.Outcome)
	}
}

// The suppression window must stay bounded, or the map grows with every message
// ever published.
func TestSuppressionWindowStaysBounded(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false
	h.dupContent = false

	for i := 0; i < 1000; i++ {
		h.now = h.now.Add(time.Second)
		h.pipeline.Admit(Request{TenantID: "t1", Channel: "room:1", Subject: "u1",
			Body: []byte("x"), Now: h.now})
	}
	if got := h.pipeline.DedupSize(); got > 1000 {
		t.Errorf("suppression entries = %d after 1000 publishes spread over time", got)
	}
}

// The same bytes sent to two different channels is not a retry. The content
// filter is a separate, channel-independent rule about a user's own repetition,
// so it is disabled here to isolate the retransmission check.
func TestSameContentDifferentChannelsIsNotDuplicate(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false
	h.dupContent = false

	req := h.req("shared content")
	req.Channel = "room:1"
	if res := h.pipeline.Admit(req); !res.Accepted() {
		t.Fatalf("first refused: %s", res.Outcome)
	}
	req.Channel = "room:2"
	if res := h.pipeline.Admit(req); !res.Accepted() {
		t.Errorf("identical content to another channel was refused as %s", res.Outcome)
	}
}

func TestPublishFailurePolicies(t *testing.T) {
	t.Run("fail fast", func(t *testing.T) {
		h := newHarness(t)
		h.tenantAllow = false
		h.policy = controlplane.PolicyFailFast
		res := h.pipeline.Admit(h.req("hello"))
		if res.Outcome != OutcomeTenantLimited {
			t.Errorf("outcome = %q, want the refusal surfaced to the publisher", res.Outcome)
		}
		if res.Accepted() {
			t.Error("fail-fast reported acceptance")
		}
	})

	t.Run("discard", func(t *testing.T) {
		h := newHarness(t)
		h.tenantAllow = false
		h.policy = controlplane.PolicyDiscard
		res := h.pipeline.Admit(h.req("hello"))
		if res.Outcome != OutcomeDropped {
			t.Errorf("outcome = %q, want dropped", res.Outcome)
		}
		// A discard must not look like an acceptance, or the publisher would
		// believe the message is queued.
		if res.Accepted() {
			t.Error("discard reported acceptance; a client would never retry")
		}
		if res.MessageID != 0 {
			t.Errorf("discard issued identifier %d", res.MessageID)
		}
	})

	t.Run("degrade", func(t *testing.T) {
		h := newHarness(t)
		h.tenantAllow = false
		h.policy = controlplane.PolicyDegrade
		res := h.pipeline.Admit(h.req("hello"))
		if !res.Accepted() {
			t.Errorf("outcome = %q, want a degraded acceptance", res.Outcome)
		}
		if res.Outcome != OutcomeDegraded {
			t.Errorf("outcome = %q, want degraded", res.Outcome)
		}
		if res.MessageID == 0 {
			t.Error("a degraded message must still be traceable")
		}
		if len(res.Body) != 0 {
			t.Errorf("degraded body = %q, want the reduced form", res.Body)
		}
	})
}

func TestModerationBlockReachesNoSubscriber(t *testing.T) {
	h := newHarness(t)
	if err := h.matcher.Reload("t1", []controlplane.ModerationRule{{
		TenantID: "t1", Pattern: "forbidden", Action: controlplane.ActionBlock,
		Kind: controlplane.RuleKindSensitiveWord, Enabled: true, ID: "r1",
	}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	res := h.pipeline.Admit(h.req("say forbidden things"))
	if res.Outcome != OutcomeBlocked {
		t.Fatalf("outcome = %q, want blocked", res.Outcome)
	}
	if res.RuleID != "r1" {
		t.Errorf("rule id = %q, want r1 for the audit trail", res.RuleID)
	}
	if res.MessageID != 0 {
		t.Error("a blocked message was issued an identifier; it never reached delivery")
	}
	if len(res.Body) != 0 {
		t.Error("a blocked message carries a body")
	}
}

func TestModerationReplacementIsApplied(t *testing.T) {
	h := newHarness(t)
	if err := h.matcher.Reload("t1", []controlplane.ModerationRule{{
		TenantID: "t1", Pattern: "badword", Action: controlplane.ActionReplace,
		Replacement: "***", Kind: controlplane.RuleKindSensitiveWord,
		Enabled: true, ID: "r1",
	}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	res := h.pipeline.Admit(h.req("say badword now"))
	if !res.Accepted() {
		t.Fatalf("outcome = %q, want accepted", res.Outcome)
	}
	if strings.Contains(string(res.Body), "badword") {
		t.Errorf("body = %q, want the matched content replaced", res.Body)
	}
	if !strings.Contains(string(res.Body), "***") {
		t.Errorf("body = %q, want the replacement present", res.Body)
	}
}

// The replaced body is what the size limit applies to, so a replacement that
// shrinks content can bring an otherwise oversized message inside the limit.
func TestSizeAppliesAfterReplacement(t *testing.T) {
	h := newHarness(t)
	if err := h.matcher.Reload("t1", []controlplane.ModerationRule{{
		TenantID: "t1", Pattern: "badword", Action: controlplane.ActionReplace,
		Replacement: "*", Kind: controlplane.RuleKindSensitiveWord,
		Enabled: true, ID: "r1",
	}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// 600 x 7 bytes = 4200 raw, which shrinks to 600 after replacement.
	body := strings.Repeat("badword", 600)
	res := h.pipeline.Admit(h.req(body))
	if !res.Accepted() {
		t.Errorf("outcome = %q, want accepted after replacement shrank it below the limit", res.Outcome)
	}
}

// A publisher sending two different messages within the suppression window is
// not retrying, and neither message may be silently swallowed.
func TestDistinctMessagesFromOnePublisherAreNotSuppressed(t *testing.T) {
	h := newHarness(t)
	h.dupContent = false

	for i := 0; i < 50; i++ {
		req := h.req("message " + string(rune('a'+i%26)) + string(rune('0'+i/26)))
		if res := h.pipeline.Admit(req); !res.Accepted() {
			t.Fatalf("distinct message %d refused as %s", i, res.Outcome)
		}
	}
}

func TestPublishAuditorSeesEveryDecision(t *testing.T) {
	h := newHarness(t)
	ids := make([]string, 0, 4)
	h.pipeline.SetAuditor(func(res Result, req Request) {
		ids = append(ids, string(res.Outcome))
	})

	h.pipeline.Admit(h.req("one"))
	h.canPublish = false
	h.pipeline.Admit(h.req("two"))

	if len(ids) != 2 || ids[0] != string(OutcomeAccepted) || ids[1] != string(OutcomeForbidden) {
		t.Errorf("auditor saw %v, want [accepted forbidden]", ids)
	}
}

func TestPipelineConcurrentAdmission(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false
	h.dupContent = false

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				h.pipeline.Admit(Request{
					TenantID: "t1", Channel: "room:1",
					Subject: string(rune('a' + g)),
					Body:    []byte("concurrent"), Now: h.now,
				})
			}
		}(g)
	}
	wg.Wait()
}

// A user's own repeated content is a separate rule from retransmission
// suppression: it is about copy-paste flooding rather than a client retry, and
// it applies regardless of which channel the content went to.
func TestRepeatedContentFromOneUserIsFiltered(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false

	if res := h.pipeline.Admit(h.req("spam spam spam")); !res.Accepted() {
		t.Fatalf("first occurrence refused: %s", res.Outcome)
	}

	h.now = h.now.Add(2 * time.Second)
	res := h.pipeline.Admit(h.req("spam spam spam"))
	if res.Outcome != OutcomeDuplicate {
		t.Fatalf("outcome = %q, want duplicate for a user's own repeated content", res.Outcome)
	}
	if res.Stage != StageDuplicate {
		t.Errorf("stage = %q, want %q", res.Stage, StageDuplicate)
	}

	// Past the content window it is allowed again.
	h.now = h.now.Add(10 * time.Second)
	if res := h.pipeline.Admit(h.req("spam spam spam")); !res.Accepted() {
		t.Errorf("repeated content past the window was refused as %s", res.Outcome)
	}
}

// The content filter must not fire for different users saying the same thing.
func TestRepeatedContentIsPerUser(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false

	if res := h.pipeline.Admit(h.req("hello there")); !res.Accepted() {
		t.Fatalf("first refused: %s", res.Outcome)
	}
	other := h.req("hello there")
	other.Subject = "u2"
	if res := h.pipeline.Admit(other); !res.Accepted() {
		t.Errorf("a different user repeating the same content was refused as %s", res.Outcome)
	}
}

// The promotional filter is a tenant rule applied on the publish path.
func TestPromotionalPatternIsFiltered(t *testing.T) {
	h := newHarness(t)
	h.dedupOn = false
	h.dupContent = false
	h.reloadWords(t, controlplane.ModerationRule{
		Pattern: "buy-now", Action: controlplane.ActionBlock,
		Kind: controlplane.RuleKindPromotional, ID: "promo",
	})

	if res := h.pipeline.Admit(h.req("visit and buy-now today")); res.Outcome != OutcomeBlocked {
		t.Errorf("outcome = %q, want blocked for a promotional pattern", res.Outcome)
	}
	if res := h.pipeline.Admit(h.req("ordinary danmaku")); !res.Accepted() {
		t.Errorf("ordinary content refused as %s", res.Outcome)
	}
}
