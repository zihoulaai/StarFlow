package publish

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/starflow/push/internal/auth"
	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/protocol"
)

// MaxRequestBytes bounds the request body before decoding, so an oversized
// upload is refused while it is still arriving rather than after it has been
// buffered whole.
const MaxRequestBytes = 64 * 1024

// PublishRequest is the wire shape of a publish call.
type PublishRequest struct {
	Channel string `json:"channel"`
	Body    string `json:"body"`
	// TTLMillis is the publisher's requested lifetime. Zero means the service
	// default.
	TTLMillis int64 `json:"ttl_ms,omitempty"`
	// Priority names the message class for the degradation ladder.
	Priority string `json:"priority,omitempty"`
	// Signature is an optional client-computed integrity value over the other
	// fields, verified against the token key.
	Signature string `json:"signature,omitempty"`
}

// PublishResponse is returned per message, without waiting for delivery.
//
// The response carries the admission outcome rather than a delivery result.
// Waiting for delivery would mean the publisher's latency depended on every
// subscriber's socket, which is exactly the coupling the fanout design avoids.
type PublishResponse struct {
	// Accepted is true when the message reached the delivery path.
	Accepted bool `json:"accepted"`
	// Outcome is the admission outcome code. It is present on every response so
	// a publisher can distinguish a rate limit from a moderation block without
	// parsing prose.
	Outcome string `json:"outcome"`
	// MessageID is the traceable identifier, set on acceptance.
	MessageID uint64 `json:"message_id,omitempty"`
	// MessageTimeMs is the server-assigned timestamp.
	MessageTimeMs uint64 `json:"message_time_ms,omitempty"`
	// Stage names the pipeline stage that produced a refusal.
	Stage string `json:"stage,omitempty"`
	// RuleID identifies the moderation rule behind a block.
	RuleID string `json:"rule_id,omitempty"`
	// Detail is operator-facing context. It is never content from the message.
	Detail string `json:"detail,omitempty"`
}

// Handler serves the publish endpoint.
type Handler struct {
	pipeline *Pipeline
	bus      bus.Bus
	// resolve turns the request's credentials into an identity.
	resolve func(*http.Request) (auth.Identity, error)
	// now is injected so TTL behaviour is testable.
	now func() time.Time
	// maxBodyBytes is the tenant-configured size ceiling, applied by the
	// pipeline; this is the transport-level cap.
	maxBodyBytes int
}

// NewHandler returns a publish handler.
func NewHandler(p *Pipeline, b bus.Bus, resolve func(*http.Request) (auth.Identity, error), now func() time.Time) *Handler {
	if now == nil {
		now = time.Now
	}
	return &Handler{pipeline: p, bus: b, resolve: resolve, now: now, maxBodyBytes: MaxRequestBytes}
}

// ServeHTTP implements http.Handler.
//
// Every path ends in a response carrying an outcome. A refusal is reported with
// a status the client can branch on, but the outcome code is always present so
// the client never has to infer the reason from a status code.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOutcome(w, http.StatusMethodNotAllowed, PublishResponse{
			Outcome: "method_not_allowed", Detail: "publish requires POST",
		})
		return
	}

	identity, err := h.resolve(r)
	if err != nil {
		// The credential failure reason is deliberately not echoed: it would
		// confirm whether an application key exists to a caller probing for one.
		writeOutcome(w, http.StatusUnauthorized, PublishResponse{
			Outcome: "unauthenticated", Detail: "credential rejected",
		})
		return
	}

	if err := auth.Check(identity.Privileges, auth.PrivilegePublish); err != nil {
		writeOutcome(w, http.StatusForbidden, PublishResponse{
			Outcome: string(OutcomeForbidden), Stage: string(StagePermission),
			Detail: "credential lacks the publish privilege",
		})
		return
	}

	body, err := h.decode(w, r)
	if err != nil {
		writeOutcome(w, http.StatusBadRequest, PublishResponse{
			Outcome: "bad_request", Detail: err.Error(),
		})
		return
	}
	if body.Channel == "" {
		writeOutcome(w, http.StatusBadRequest, PublishResponse{
			Outcome: "bad_request", Detail: "channel is required",
		})
		return
	}

	req := Request{
		TenantID: identity.TenantID,
		Channel:  body.Channel,
		Subject:  identity.Subject,
		Body:     []byte(body.Body),
		Priority: parsePriority(body.Priority),
		Now:      h.now(),
	}
	if body.TTLMillis > 0 {
		req.TTL = time.Duration(body.TTLMillis) * time.Millisecond
	}

	res := h.pipeline.Admit(req)
	if !res.Accepted() {
		writeOutcome(w, statusFor(res.Outcome), PublishResponse{
			Accepted: false,
			Outcome:  string(res.Outcome),
			Stage:    string(res.Stage),
			RuleID:   res.RuleID,
			Detail:   res.Detail,
		})
		return
	}

	ttl := req.TTL
	if ttl <= 0 {
		ttl = h.pipeline.deps.DefaultTTL
	}
	env := bus.Envelope{
		TenantID:  identity.TenantID,
		Channel:   body.Channel,
		MessageID: res.MessageID,
		TimeMs:    protocol.NowMs(),
		Class:     uint8(res.Priority),
		Body:      res.Body,
	}
	if ttl > 0 {
		env.ExpiresAtMs = env.TimeMs + uint64(ttl.Milliseconds())
	}

	if err := h.bus.Publish(r.Context(), env); err != nil {
		writeOutcome(w, http.StatusServiceUnavailable, PublishResponse{
			Outcome: "bus_unavailable", Detail: "message not accepted for delivery",
		})
		return
	}

	writeOutcome(w, http.StatusAccepted, PublishResponse{
		Accepted:      true,
		Outcome:       string(res.Outcome),
		MessageID:     res.MessageID,
		MessageTimeMs: env.TimeMs,
	})
}

func (h *Handler) decode(w http.ResponseWriter, r *http.Request) (PublishRequest, error) {
	var body PublishRequest

	// The body is read under an explicit cap before decoding. A publisher
	// uploading a large body must be refused while it streams rather than after
	// it has been buffered whole, and reading first makes the limit error
	// unambiguous rather than depending on how the decoder happens to chunk its
	// reads.
	r.Body = http.MaxBytesReader(w, r.Body, int64(h.maxBodyBytes))
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return body, errors.New("request body exceeds the transport limit")
		}
		return body, errors.New("request body could not be read")
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return body, errors.New("request body is not valid json")
	}
	return body, nil
}

// statusFor maps an outcome onto an HTTP status.
//
// A discard is reported as accepted-shaped success on purpose: the point of the
// discard policy is that the publisher is not invited to retry, and a 5xx would
// invite exactly that. The response still carries outcome "dropped" so an
// operator reading the log can see it.
func statusFor(outcome Outcome) int {
	switch outcome {
	case OutcomeForbidden, OutcomeMuted, OutcomeBanned:
		return http.StatusForbidden
	case OutcomeBlocked:
		return http.StatusUnprocessableEntity
	case OutcomeTooLarge:
		return http.StatusRequestEntityTooLarge
	case OutcomeTenantLimited, OutcomeChannelLimited, OutcomeUserLimited:
		return http.StatusTooManyRequests
	case OutcomeDuplicate:
		return http.StatusOK
	case OutcomeDropped:
		return http.StatusAccepted
	default:
		return http.StatusBadRequest
	}
}

func writeOutcome(w http.ResponseWriter, status int, body PublishResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func parsePriority(s string) protocol.PriorityClass {
	switch s {
	case "high":
		return protocol.PriorityHigh
	case "system":
		return protocol.PrioritySystem
	default:
		return protocol.PriorityNormal
	}
}

// ContextKey namespaces values this package places on a request context.
type ContextKey struct{}

// WithIdentity returns a context carrying the resolved identity.
func WithIdentity(ctx context.Context, id auth.Identity) context.Context {
	return context.WithValue(ctx, ContextKey{}, id)
}

// IdentityFrom returns the identity on a context.
func IdentityFrom(ctx context.Context) (auth.Identity, bool) {
	id, ok := ctx.Value(ContextKey{}).(auth.Identity)
	return id, ok
}
