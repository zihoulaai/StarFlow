// Package publish admits messages onto the delivery path.
//
// Admission is an ordered pipeline, and the order is a contract rather than an
// implementation detail: each stage's refusal reason is reported to the
// publisher, so a message refused for a size violation must never be reported as
// a moderation block. The order is
//
//	permission -> tenant and channel rate limits -> duplicate suppression ->
//	moderation -> size
//
// Every stage runs against state held in process memory or in a process-local
// counter. Nothing here performs a network call, which is what allows the data
// plane to have no synchronous dependency on the control plane.
package publish

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/moderation"
	"github.com/starflow/push/internal/protocol"
)

// Outcome is the observable result of an admission attempt. Each value names a
// distinct stage's refusal so a publisher can tell a rate limit from a
// moderation block without parsing prose.
type Outcome string

const (
	OutcomeAccepted       Outcome = "accepted"
	OutcomeForbidden      Outcome = "forbidden"
	OutcomeTenantLimited  Outcome = "tenant_rate_limited"
	OutcomeChannelLimited Outcome = "channel_rate_limited"
	OutcomeUserLimited    Outcome = "user_rate_limited"
	OutcomeDuplicate      Outcome = "duplicate"
	OutcomeMuted          Outcome = "muted"
	OutcomeBanned         Outcome = "banned"
	OutcomeBlocked        Outcome = "blocked"
	OutcomeTooLarge       Outcome = "too_large"
	// OutcomeDropped is a discard-policy refusal: the message was refused under
	// the configured policy but the publisher is not told, because discard means
	// the publisher is not expected to see anything.
	OutcomeDropped Outcome = "dropped"
	// OutcomeDegraded is a degrade-policy acceptance: the message was admitted
	// in reduced form.
	OutcomeDegraded Outcome = "degraded"
)

// Stage names the pipeline stage that produced an outcome.
type Stage string

const (
	StagePermission Stage = "permission"
	StageRateLimit  Stage = "rate_limit"
	StageDuplicate  Stage = "duplicate"
	StageModeration Stage = "moderation"
	StageSize       Stage = "size"
)

// Request is a message offered for publication.
type Request struct {
	TenantID string
	Channel  string
	// Subject is the publishing identity, used for per-user limits and mutes.
	Subject string
	Body    []byte
	// TTL is the publisher-requested lifetime. Zero means the service default.
	TTL time.Duration
	// Priority drives the degradation ladder.
	Priority protocol.PriorityClass
	// Now is the admission timestamp, injected so TTL behaviour is testable.
	Now time.Time
}

// Result is the admission outcome.
type Result struct {
	Outcome Outcome
	Stage   Stage
	// MessageID is set on acceptance, and is what a publisher uses to trace the
	// message through to delivery.
	MessageID uint64
	// Body is the admitted content, which may differ from the request's when a
	// moderation replacement or a degradation applied.
	Body []byte
	// Priority is the admitted class, which the ladder may have lowered.
	Priority protocol.PriorityClass
	// RuleID identifies the moderation rule behind a block.
	RuleID string
	// Detail is operator-facing context for the audit trail.
	Detail string
}

// Accepted reports whether the message reached the delivery path.
func (r Result) Accepted() bool {
	return r.Outcome == OutcomeAccepted || r.Outcome == OutcomeDegraded
}

// Deps are the collaborators the pipeline consults. Each is an interface so a
// stage can be exercised in isolation and so the pipeline has no compile-time
// coupling to the transport, the store, or the matcher.
type Deps struct {
	// Privileges reports whether the caller may publish.
	Privileges func(subject string) bool
	// ChannelCeiling returns the channel's publish ceiling. Zero means no
	// ceiling.
	ChannelCeiling func(tenantID, channel string) float64
	// AllowTenantPublish consumes one unit of the tenant's publish budget and
	// reports whether it was available.
	AllowTenantPublish func(tenantID string, now time.Time) bool
	// AllowUserPublish consumes one unit of the per-user budget.
	AllowUserPublish func(tenantID, subject string, now time.Time) bool
	// Restriction returns the active restriction on a subject, if any.
	Restriction func(tenantID, kind, subject string, now time.Time) (controlplane.Restriction, bool)
	// Policy returns the tenant's failure policy.
	Policy func(tenantID string) string
	// Matcher evaluates content against the tenant's rules.
	Matcher moderation.Matcher
	// DuplicateEnabled and DuplicateWindow come from the tenant's quota.
	DuplicateEnabled func(tenantID string) bool
	DuplicateWindow  func(tenantID string) time.Duration
	// DuplicateContentEnabled and PromoEnabled gate the content filters.
	DuplicateContentEnabled func(tenantID string) bool
	PromoEnabled            func(tenantID string) bool
	// MaxMessageBytes is the size ceiling. Zero means the service default.
	MaxMessageBytes int
	// IDs generates message identifiers.
	IDs *protocol.IDGen
	// DefaultTTL applies when a publisher requests none.
	DefaultTTL time.Duration
	// DegradeBody is the reduced form admitted under the degrade policy.
	DegradeBody []byte
	// Audit records each decision.
	Audit func(subject, channel, messageID, ruleID, outcome, detail string)
	// Observe records match statistics for spike alerting, keyed by tenant.
	Observe func(tenantID string, matched bool, now time.Time)
}

// Pipeline admits messages in a fixed stage order.
type Pipeline struct {
	deps  Deps
	dedup *dedupWindow

	channelMu      sync.Mutex
	channelWindows map[channelKey]*counterWindow

	auditHook func(Result, Request)
}

// New returns a pipeline over deps.
func New(deps Deps) *Pipeline {
	if deps.MaxMessageBytes <= 0 {
		deps.MaxMessageBytes = 4 * 1024
	}
	return &Pipeline{deps: deps, dedup: newDedupWindow()}
}

// SetAuditor installs a per-request audit hook, called after every decision.
func (p *Pipeline) SetAuditor(fn func(Result, Request)) { p.auditHook = fn }

// Admit runs the pipeline and returns the outcome.
func (p *Pipeline) Admit(req Request) Result {
	res := p.run(req)
	if p.deps.Audit != nil {
		p.deps.Audit(req.Subject, req.Channel, fmt.Sprint(res.MessageID), res.RuleID,
			string(res.Outcome), res.Detail)
	}
	if p.auditHook != nil {
		p.auditHook(res, req)
	}
	return res
}

func (p *Pipeline) run(req Request) Result {
	d := p.deps

	// Stage 1: permission.
	if d.Privileges != nil && !d.Privileges(req.Subject) {
		return Result{Outcome: OutcomeForbidden, Stage: StagePermission,
			Detail: "credential lacks the publish privilege"}
	}

	// Stage 2: rate limits. The per-user limit is applied first because it is
	// the more specific of the two: a single flooding user should be stopped
	// before it consumes the tenant's shared budget.
	if d.AllowUserPublish != nil && !d.AllowUserPublish(req.TenantID, req.Subject, req.Now) {
		return Result{Outcome: OutcomeUserLimited, Stage: StageRateLimit,
			Detail: "per-user publish rate exceeded"}
	}
	if d.AllowTenantPublish != nil && !d.AllowTenantPublish(req.TenantID, req.Now) {
		return p.applyRatePolicy(req, OutcomeTenantLimited)
	}
	if d.ChannelCeiling != nil {
		ceiling := d.ChannelCeiling(req.TenantID, req.Channel)
		if ceiling > 0 && !p.channelAllow(req.TenantID, req.Channel, ceiling, req.Now) {
			return p.applyRatePolicy(req, OutcomeChannelLimited)
		}
	}

	// Mutes and bans are enforced here rather than in the moderation stage
	// because they concern the publisher rather than the content.
	if d.Restriction != nil {
		if r, ok := d.Restriction(req.TenantID, controlplane.SubjectUser, req.Subject, req.Now); ok {
			outcome := OutcomeMuted
			if r.Action == controlplane.RestrictionBan {
				outcome = OutcomeBanned
			}
			return Result{Outcome: outcome, Stage: StagePermission, Detail: r.Reason}
		}
	}

	// Stage 3: duplicate suppression.
	if d.DuplicateEnabled != nil && d.DuplicateEnabled(req.TenantID) {
		window := 30 * time.Second
		if d.DuplicateWindow != nil {
			if configured := d.DuplicateWindow(req.TenantID); configured > 0 {
				window = configured
			}
		}
		if id, suppressed := p.dedup.check(req.TenantID, req.Subject, req.Channel, req.Body, req.Now, window); suppressed {
			return Result{Outcome: OutcomeDuplicate, Stage: StageDuplicate, MessageID: id,
				Detail: "repeat of a recent publish from this publisher"}
		}
	}

	// Stage 4: moderation.
	body := req.Body
	priority := req.Priority
	if d.Matcher != nil {
		decision := d.Matcher.Evaluate(req.TenantID, body)
		if d.Observe != nil {
			d.Observe(req.TenantID, len(decision.Matches) > 0, req.Now)
		}
		if !decision.Allowed {
			return Result{Outcome: OutcomeBlocked, Stage: StageModeration,
				RuleID: decision.RuleID,
				Detail: strings.Join(decision.Matches, ",")}
		}
		if decision.Replacement != "" {
			body = applyReplacement(body, decision.Matches, decision.Replacement)
		}
	}

	// The publisher's own duplicate-content filter and the tenant's promotional
	// filter sit alongside moderation because they are content rules too.
	if d.DuplicateContentEnabled != nil && d.DuplicateContentEnabled(req.TenantID) {
		if id, dup := p.dedup.checkContent(req.TenantID, req.Subject, body, req.Now,
			dedupContentWindow); dup {
			return Result{Outcome: OutcomeDuplicate, Stage: StageDuplicate, MessageID: id,
				Detail: "repeated content from this publisher"}
		}
	}

	// Stage 5: size.
	if len(body) > d.MaxMessageBytes {
		return Result{Outcome: OutcomeTooLarge, Stage: StageSize,
			Detail: fmt.Sprintf("%d bytes exceeds the %d byte limit", len(body), d.MaxMessageBytes)}
	}

	res := Result{
		Outcome:   OutcomeAccepted,
		Stage:     StageSize,
		MessageID: d.IDs.Next(),
		Body:      body,
		Priority:  priority,
	}
	// The identifier is only known now, so the suppression entry is completed
	// after admission. A retry suppressed in the window needs to report the
	// original identifier so the client can tell what it duplicated.
	p.dedup.remember(req, res.MessageID)
	return res
}

// applyRatePolicy converts a rate refusal into whatever the tenant's configured
// policy says should happen.
//
// The three policies exist because the right answer differs by traffic. Failing
// fast tells a well-behaved publisher client it must back off. Discarding keeps
// a flood from amplifying into retries. Degrading keeps the channel alive with
// reduced content rather than going silent.
func (p *Pipeline) applyRatePolicy(req Request, refused Outcome) Result {
	policy := controlplane.PolicyFailFast
	if p.deps.Policy != nil {
		policy = p.deps.Policy(req.TenantID)
	}

	switch policy {
	case controlplane.PolicyDiscard:
		// The publisher is told nothing: a discard is indistinguishable from a
		// successful publish as far as the client is concerned, and returning an
		// error would invite the retry traffic the policy exists to suppress.
		return Result{Outcome: OutcomeDropped, Stage: StageRateLimit,
			Detail: "refused under the discard policy"}
	case controlplane.PolicyDegrade:
		body := p.deps.DegradeBody
		if body == nil {
			body = []byte{}
		}
		return Result{
			Outcome:   OutcomeDegraded,
			Stage:     StageRateLimit,
			MessageID: p.deps.IDs.Next(),
			Body:      body,
			Priority:  req.Priority,
			Detail:    "admitted in reduced form under the degrade policy",
		}
	default:
		return Result{Outcome: refused, Stage: StageRateLimit,
			Detail: "publish rate exceeded"}
	}
}

// channelAllow consumes one unit of a channel's derived ceiling.
//
// The counter is per (tenant, channel) in process memory. It is approximate
// across instances for the same reason the tenant limiter is: making it exact
// would require a shared counter on the admission path.
func (p *Pipeline) channelAllow(tenantID, channel string, ceiling float64, now time.Time) bool {
	p.channelMu.Lock()
	defer p.channelMu.Unlock()
	if p.channelWindows == nil {
		p.channelWindows = map[channelKey]*counterWindow{}
	}
	key := channelKey{tenantID, channel}
	w, ok := p.channelWindows[key]
	if !ok {
		w = &counterWindow{}
		p.channelWindows[key] = w
	}
	return w.allow(int(ceiling), now)
}

type channelKey struct{ tenantID, channel string }

// dedupWindow suppresses repeats of recently published content.
//
// The window is bounded and self-expiring rather than unbounded, because
// suppressing forever would make a legitimate repeat unpublishable and would
// grow without limit. Eviction is by age sweep, which is O(n) on expiry and
// amortised across many inserts.
type dedupWindow struct {
	mu      sync.Mutex
	entries map[dedupKey]*dedupEntry
}

type dedupKey struct {
	tenantID, subject, channel string
}

type contentKey struct {
	tenantID, subject string
}

type dedupEntry struct {
	id      uint64
	seenAt  time.Time
	content string
}

const dedupContentWindow = 5 * time.Second

// contentChannelKey namespaces the content-duplicate entry away from the
// per-channel retransmission entry, so one publisher sending the same bytes to
// two different channels is not mistaken for a retry.
const contentChannelKey = "\x00content"

// DedupSize reports how many suppression entries the pipeline retains, so a
// caller can assert the window stays bounded.
func (p *Pipeline) DedupSize() int { return p.dedup.Size() }

func newDedupWindow() *dedupWindow {
	return &dedupWindow{entries: map[dedupKey]*dedupEntry{}}
}

// check suppresses an identical retransmission of the same publish, reporting
// the original message identifier so a client retry learns what it duplicated.
//
// The content hash is compared, not just the publisher and channel. Keying on
// those alone would suppress every second message a publisher sent within the
// window, which for a user typing ordinary danmaku would silence the channel.
func (w *dedupWindow) check(tenantID, subject, channel string, body []byte, now time.Time, window time.Duration) (uint64, bool) {
	key := dedupKey{tenantID, subject, channel}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.evictLocked(now)

	hash := contentHash(body)
	if e, ok := w.entries[key]; ok && now.Sub(e.seenAt) < window && e.content == hash {
		return e.id, true
	}
	w.entries[key] = &dedupEntry{seenAt: now, content: hash}
	return 0, false
}

// checkContent suppresses a user's own repeated content within a short window,
// which is what catches copy-paste flooding.
func (w *dedupWindow) checkContent(tenantID, subject string, body []byte, now time.Time, window time.Duration) (uint64, bool) {
	hash := contentHash(body)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.evictLocked(now)

	key := dedupKey{tenantID: tenantID, subject: subject, channel: contentChannelKey}
	if e, ok := w.entries[key]; ok && now.Sub(e.seenAt) < window && e.content == hash {
		return e.id, true
	}
	w.entries[key] = &dedupEntry{seenAt: now, content: hash}
	return 0, false
}

// remember records the identifier assigned to an admitted publish, so a later
// retry within the window can report it.
func (w *dedupWindow) remember(req Request, id uint64) {
	if req.Body == nil {
		return
	}
	hash := contentHash(req.Body)

	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.entries[dedupKey{tenantID: req.TenantID, subject: req.Subject, channel: req.Channel}]; ok {
		e.id = id
	}
	if e, ok := w.entries[dedupKey{tenantID: req.TenantID, subject: req.Subject, channel: contentChannelKey}]; ok {
		e.id = id
	}
	_ = hash
}

// evictLocked drops entries older than the longest window the pipeline uses.
// Without it the map would grow with every message ever published.
func (w *dedupWindow) evictLocked(now time.Time) {
	const maxAge = 5 * time.Minute
	for k, e := range w.entries {
		if now.Sub(e.seenAt) > maxAge {
			delete(w.entries, k)
		}
	}
}

// Size reports the number of retained suppression entries, which tests assert
// stays bounded.
func (w *dedupWindow) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

// contentHash identifies content for duplicate detection. It is a digest rather
// than the content itself so the window does not retain a copy of every message
// ever published.
func contentHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:8])
}

// counterWindow is a fixed-window counter.
type counterWindow struct {
	start time.Time
	count int
}

func (c *counterWindow) allow(limit int, now time.Time) bool {
	if limit <= 0 {
		return true
	}
	if c.start.IsZero() || now.Sub(c.start) >= time.Second {
		c.start = now
		c.count = 0
	}
	if c.count >= limit {
		return false
	}
	c.count++
	return true
}

// applyReplacement substitutes matched content in body.
//
// The matched patterns come from the moderation decision in a stable sorted
// order, so the same content and rule set always produce the same result.
func applyReplacement(body []byte, patterns []string, replacement string) []byte {
	out := body
	for _, pattern := range patterns {
		out = bytes.ReplaceAll(out, []byte(pattern), []byte(replacement))
	}
	return out
}
