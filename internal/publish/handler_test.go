package publish

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/starflow/push/internal/auth"
	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/moderation"
	"github.com/starflow/push/internal/protocol"
)

type handlerFixture struct {
	handler  *Handler
	bus      *bus.MemoryBus
	identity auth.Identity
	resolve  func(*http.Request) (auth.Identity, error)
	failNext bool
}

func newHandlerFixture(t *testing.T) *handlerFixture {
	t.Helper()
	ids, err := protocol.NewIDGen(1)
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	matcher := moderation.NewAutomaton()
	if err := matcher.Reload("t1", nil); err != nil {
		t.Fatalf("Reload matcher: %v", err)
	}

	b := bus.NewMemoryBus()
	t.Cleanup(func() { _ = b.Close() })

	f := &handlerFixture{
		bus: b,
		identity: auth.Identity{
			TenantID: "t1", AppKey: "ak", Subject: "u1",
			Privileges: auth.Privileges(true, true),
		},
	}
	f.resolve = func(*http.Request) (auth.Identity, error) { return f.identity, nil }

	f.handler = NewHandler(New(Deps{
		Privileges:      func(string) bool { return true },
		Matcher:         matcher,
		IDs:             ids,
		MaxMessageBytes: 4096,
		DefaultTTL:      30 * time.Second,
	}), b, func(r *http.Request) (auth.Identity, error) { return f.resolve(r) },
		func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	return f
}

func (f *handlerFixture) post(t *testing.T, body PublishRequest) (*httptest.ResponseRecorder, PublishResponse) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)

	var out PublishResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec, out
}

// The response must carry the admission outcome without waiting for delivery.
// A publisher whose latency depended on every subscriber's socket would couple
// it to the whole fanout path.
func TestPublishReturnsBeforeDelivery(t *testing.T) {
	f := newHandlerFixture(t)

	received := make(chan bus.Envelope, 1)
	f.bus.Subscribe(t.Context(), func(e bus.Envelope) { received <- e })

	rec, out := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
	}
	if !out.Accepted || out.MessageID == 0 {
		t.Fatalf("response = %+v, want an accepted outcome with an identifier", out)
	}

	select {
	case e := <-received:
		if e.Channel != "room:1" || string(e.Body) != "hello" {
			t.Errorf("envelope = %+v, want the published content", e)
		}
		if e.MessageID != out.MessageID {
			t.Errorf("envelope id %d does not match the response id %d", e.MessageID, out.MessageID)
		}
	case <-time.After(time.Second):
		t.Fatal("no envelope reached the bus")
	}
}

func TestPublishAssignsIdentifierAndTimestamp(t *testing.T) {
	f := newHandlerFixture(t)
	_, out := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if out.MessageID == 0 {
		t.Error("no message identifier assigned")
	}
	if out.MessageTimeMs == 0 {
		t.Error("no server timestamp assigned")
	}
}

// The outcome code must be present on every response, so a publisher can
// distinguish a rate limit from a moderation block without parsing prose.
func TestPublishOutcomesAreDistinguishable(t *testing.T) {
	f := newHandlerFixture(t)

	_, ok := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if ok.Outcome != string(OutcomeAccepted) {
		t.Errorf("outcome = %q, want accepted", ok.Outcome)
	}

	// No channel.
	_, missing := f.post(t, PublishRequest{Body: "hello"})
	if missing.Outcome == "" || missing.Outcome == string(OutcomeAccepted) {
		t.Errorf("outcome = %q for a missing channel, want a distinct code", missing.Outcome)
	}

	// Oversized.
	_, big := f.post(t, PublishRequest{Channel: "room:1", Body: strings.Repeat("a", 5000)})
	if big.Outcome != string(OutcomeTooLarge) {
		t.Errorf("outcome = %q, want too_large", big.Outcome)
	}
	if big.Stage != string(StageSize) {
		t.Errorf("stage = %q, want size", big.Stage)
	}
}

func TestPublishRejectsNonPost(t *testing.T) {
	f := newHandlerFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/publish", nil)
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

// A failed credential must not reveal whether the application key exists.
func TestPublishUnauthenticatedResponseIsOpaque(t *testing.T) {
	f := newHandlerFixture(t)
	f.resolve = func(*http.Request) (auth.Identity, error) {
		return auth.Identity{}, auth.ErrTokenSignature
	}

	rec, out := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if out.Outcome != "unauthenticated" {
		t.Errorf("outcome = %q, want a generic unauthenticated code", out.Outcome)
	}
	if strings.Contains(out.Detail, "key") || strings.Contains(out.Detail, "exist") {
		t.Errorf("detail %q may confirm whether a credential exists", out.Detail)
	}
}

func TestPublishWithoutPrivilegeIsRefused(t *testing.T) {
	f := newHandlerFixture(t)
	f.identity.Privileges = auth.Privileges(false, true)

	rec, out := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
	if out.Outcome != string(OutcomeForbidden) {
		t.Errorf("outcome = %q, want forbidden", out.Outcome)
	}
}

// A publisher-requested TTL must reach the envelope so a gateway can drop the
// message once it lapses, without any persisted state.
func TestPublishCarriesRequestedTTL(t *testing.T) {
	f := newHandlerFixture(t)
	received := make(chan bus.Envelope, 1)
	f.bus.Subscribe(t.Context(), func(e bus.Envelope) { received <- e })

	f.post(t, PublishRequest{Channel: "room:1", Body: "hi", TTLMillis: 5000})

	select {
	case e := <-received:
		if e.ExpiresAtMs == 0 {
			t.Error("publisher-requested TTL did not reach the envelope")
		}
		if got := e.ExpiresAtMs - e.TimeMs; got != 5000 {
			t.Errorf("lifetime = %d ms, want 5000", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no envelope reached the bus")
	}
}

func TestPublishAppliesServiceDefaultTTL(t *testing.T) {
	f := newHandlerFixture(t)
	received := make(chan bus.Envelope, 1)
	f.bus.Subscribe(t.Context(), func(e bus.Envelope) { received <- e })

	f.post(t, PublishRequest{Channel: "room:1", Body: "hi"})
	select {
	case e := <-received:
		if e.ExpiresAtMs == 0 {
			t.Error("no expiry applied when the publisher requested none")
		}
	case <-time.After(time.Second):
		t.Fatal("no envelope reached the bus")
	}
}

func TestPublishPriorityIsCarried(t *testing.T) {
	f := newHandlerFixture(t)
	received := make(chan bus.Envelope, 1)
	f.bus.Subscribe(t.Context(), func(e bus.Envelope) { received <- e })

	f.post(t, PublishRequest{Channel: "room:1", Body: "gift", Priority: "system"})
	select {
	case e := <-received:
		if protocol.PriorityClass(e.Class) != protocol.PrioritySystem {
			t.Errorf("class = %d, want system", e.Class)
		}
	case <-time.After(time.Second):
		t.Fatal("no envelope reached the bus")
	}
}

// A transport-level cap must refuse an oversized upload while it is streaming,
// rather than after buffering it whole.
func TestPublishRefusesOversizedTransport(t *testing.T) {
	f := newHandlerFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/publish",
		strings.NewReader(strings.Repeat("a", 200_000)))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "transport limit") {
		t.Errorf("body = %s, want a transport-limit explanation", rec.Body)
	}
}

func TestPublishMalformedJSON(t *testing.T) {
	f := newHandlerFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader("{not json"))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// A bus failure must be reported as the message not being accepted, rather than
// as a success the publisher would rely on.
func TestPublishBusFailureIsReported(t *testing.T) {
	f := newHandlerFixture(t)
	if err := f.bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rec, out := f.post(t, PublishRequest{Channel: "room:1", Body: "hello"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if out.Accepted {
		t.Error("a message that never reached the bus was reported as accepted")
	}
}

// --- Readiness ---

// The gate must stay closed until every tenant's matcher is compiled. An
// instance without its word list is publishing with no moderation at all, and
// must never join the serving set in that state.
func TestReadinessFailsClosedWithoutMatchers(t *testing.T) {
	m := moderation.NewAutomaton()
	r := NewReadiness(m, []string{"t1", "t2"})
	r.MarkBusUp(true)

	ready, reason := r.Check()
	if ready {
		t.Fatalf("readiness reported true with no matchers loaded: %s", reason)
	}

	if err := r.LoadTenants(map[string][]controlplane.ModerationRule{"t1": nil, "t2": nil}); err != nil {
		t.Fatalf("LoadTenants: %v", err)
	}

	ready, reason = r.Check()
	if !ready {
		t.Fatalf("readiness stayed closed after loading: %s", reason)
	}
}

func TestReadinessGatesOnBus(t *testing.T) {
	m := moderation.NewAutomaton()
	r := NewReadiness(m, nil)

	if ready, _ := r.Check(); ready {
		t.Error("readiness reported true with the bus down")
	}
	r.MarkBusUp(true)
	if ready, reason := r.Check(); !ready {
		t.Errorf("readiness stayed closed with the bus up: %s", reason)
	}
}

func TestReadinessDrainDoesNotCloseTheGate(t *testing.T) {
	m := moderation.NewAutomaton()
	r := NewReadiness(m, nil)
	r.MarkBusUp(true)

	r.StartDrain()
	if !r.Draining() {
		t.Error("instance is not reporting as draining")
	}
	// A draining instance stays ready but refuses new connections; closing the
	// gate instead would take the instance out of service abruptly.
	if ready, _ := r.Check(); !ready {
		t.Error("draining closed the readiness gate; it should only stop accepting")
	}
}

// --- Restriction checker ---

func TestRestrictionCheckerCachesAndInvalidates(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	c := NewRestrictionChecker(func() time.Time { return now }, 5*time.Second)

	restricted := true
	calls := 0
	c.SetResolver(func(tenantID, kind, subject string, t time.Time) (string, string, bool) {
		calls++
		if restricted {
			return controlplane.RestrictionBan, "abuse", true
		}
		return "", "", false
	})

	action, reason, isRestricted := c.Check("t1", controlplane.SubjectUser, "u1")
	if !isRestricted || action != controlplane.RestrictionBan || reason != "abuse" {
		t.Fatalf("Check = %q/%q/%v, want ban/abuse/true", action, reason, isRestricted)
	}

	// Cached: the resolver is not consulted again.
	c.Check("t1", controlplane.SubjectUser, "u1")
	if calls != 1 {
		t.Errorf("resolver called %d times within the cache window, want 1", calls)
	}

	// Lifting a ban invalidates explicitly, so an administrative change takes
	// effect on the next handshake rather than after the cache window.
	restricted = false
	c.Invalidate("t1", controlplane.SubjectUser, "u1")
	if _, _, isRestricted := c.Check("t1", controlplane.SubjectUser, "u1"); isRestricted {
		t.Error("restriction still applied after invalidation")
	}

	// The cache expires on its own too.
	restricted = true
	c.Invalidate("t1", controlplane.SubjectUser, "u1")
	now = now.Add(10 * time.Second)
	if _, _, isRestricted := c.Check("t1", controlplane.SubjectUser, "u1"); !isRestricted {
		t.Error("restriction not applied after a cache expiry")
	}
}

func TestRestrictionCheckerScopesByKind(t *testing.T) {
	c := NewRestrictionChecker(time.Now, time.Second)
	c.SetResolver(func(tenantID, kind, subject string, t time.Time) (string, string, bool) {
		if kind == controlplane.SubjectIP {
			return controlplane.RestrictionBan, "blocked range", true
		}
		return "", "", false
	})

	if _, _, isRestricted := c.Check("t1", controlplane.SubjectUser, "10.0.0.1"); isRestricted {
		t.Error("an IP block matched a user subject")
	}
	if _, _, isRestricted := c.Check("t1", controlplane.SubjectIP, "10.0.0.1"); !isRestricted {
		t.Error("an IP block did not match an IP subject")
	}
}

// TestDocumentedOutcomesExistInCode keeps docs/publishing.md honest.
//
// A publisher integrating against a documented outcome code that the service
// never returns has no way to discover the mismatch except by hitting it in
// production, so the check is a test rather than a review step.
func TestDocumentedOutcomesExistInCode(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "publishing.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	doc := string(raw)

	defined := []Outcome{
		OutcomeAccepted, OutcomeDegraded, OutcomeDropped, OutcomeForbidden,
		OutcomeMuted, OutcomeBanned, OutcomeUserLimited, OutcomeTenantLimited,
		OutcomeChannelLimited, OutcomeDuplicate, OutcomeBlocked, OutcomeTooLarge,
	}
	for _, o := range defined {
		if !strings.Contains(doc, "`"+string(o)+"`") {
			t.Errorf("outcome %q is defined in code but not documented", o)
		}
	}

	// Every outcome code the document lists in its table must exist in code, so
	// a documented code cannot drift into a permanent unknown.
	documented := regexp.MustCompile("`([a-z_]+)` \\|\\s*\\d{3}\\s*\\|")
	known := map[Outcome]bool{}
	for _, o := range defined {
		known[o] = true
	}
	known[Outcome("unauthenticated")] = true
	known[Outcome("bad_request")] = true
	known[Outcome("method_not_allowed")] = true
	known[Outcome("bus_unavailable")] = true

	for _, m := range documented.FindAllStringSubmatch(doc, -1) {
		code := Outcome(m[1])
		if !known[code] {
			t.Errorf("outcome %q is documented with a status but is not defined in code", code)
		}
	}

	// The stage codes are documented too.
	for _, s := range []Stage{StagePermission, StageRateLimit, StageDuplicate, StageModeration, StageSize} {
		if !strings.Contains(doc, "`"+string(s)+"`") {
			t.Errorf("stage %q is defined in code but not documented", s)
		}
	}
}

// The status a documented outcome maps to must be the status the handler
// actually returns, or a client branching on status codes breaks silently.
func TestDocumentedStatusesMatchHandler(t *testing.T) {
	cases := map[Outcome]int{
		OutcomeForbidden:      http.StatusForbidden,
		OutcomeMuted:          http.StatusForbidden,
		OutcomeBanned:         http.StatusForbidden,
		OutcomeBlocked:        http.StatusUnprocessableEntity,
		OutcomeTooLarge:       http.StatusRequestEntityTooLarge,
		OutcomeTenantLimited:  http.StatusTooManyRequests,
		OutcomeChannelLimited: http.StatusTooManyRequests,
		OutcomeUserLimited:    http.StatusTooManyRequests,
		OutcomeDuplicate:      http.StatusOK,
		OutcomeDropped:        http.StatusAccepted,
	}
	for outcome, want := range cases {
		if got := statusFor(outcome); got != want {
			t.Errorf("statusFor(%q) = %d, want %d", outcome, got, want)
		}
	}
}
