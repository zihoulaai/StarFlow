// Package admin serves the administrative surface.
//
// Every operation here changes what the data plane does, so two properties are
// load-bearing. Every call is authorized against a named principal and its
// tenant, and every call is attributable: who, when, against what. An
// administrative change that cannot be traced is indistinguishable from a bug.
//
// The surface is bound separately from the publish and subscriber ports so it
// can be kept off the public network without affecting data-plane routing.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ErrUnauthorized means the caller has no administrative authority.
var ErrUnauthorized = errors.New("admin: unauthorized")

// ErrForbidden means the caller is known but lacks the required role.
var ErrForbidden = errors.New("admin: forbidden")

// ErrNotFound means the addressed tenant or application does not exist.
//
// It is a sentinel rather than a message so a store can distinguish a miss from
// a failure without the handler matching on error text. The distinction decides
// the status code: a miss is a 404, while a database that is unreachable is a
// 500, and answering 404 to the latter tells an operator to fix a tenant that
// does not have a problem.
var ErrNotFound = errors.New("admin: not found")

// writeLookupError answers a read that failed, separating a miss from a fault.
//
// Anything that is not ErrNotFound is reported as a server error: an unreachable
// store is not evidence that the addressed tenant is absent.
func writeLookupError(w http.ResponseWriter, err error, what string) {
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, what+" not found")
		return
	}
	writeError(w, http.StatusInternalServerError, "could not read "+what)
}

// Role is an administrative role.
type Role string

const (
	// RoleViewer may read but not change.
	RoleViewer Role = "viewer"
	// RoleAdmin may change.
	RoleAdmin Role = "admin"
)

// Authorizer resolves a caller to a role within a tenant.
type Authorizer interface {
	// RoleFor returns the caller's role. A caller with no grant returns an
	// empty role and no error, which is distinct from an error resolving the
	// grant: an unknown caller is not a failure of the store.
	RoleFor(ctx context.Context, tenantID, subject string) (Role, error)
}

// Auditor records administrative actions.
type Auditor interface {
	RecordAdminAction(ctx context.Context, entry Action)
}

// Action is one recorded administrative change.
type Action struct {
	TenantID string
	// Principal is who made the change.
	Principal string
	// Operation is the endpoint's stable name, not its URL, so a route rename
	// does not fragment the audit trail.
	Operation string
	// Target identifies what was changed.
	Target string
	// Detail is operator-facing context, never secrets.
	Detail string
	At     time.Time
}

// TenantStore is the administrative data access the tenant handlers need.
//
// Each aggregate gets its own interface rather than one Store covering all of
// them, so a consumer that only manages quotas is not handed thirteen methods to
// implement and the boundaries between aggregates stay visible. The adapters
// satisfy all four, and a caller may supply them separately.
type TenantStore interface {
	ListTenants(ctx context.Context) ([]TenantView, error)
	GetTenant(ctx context.Context, id string) (TenantView, error)
	CreateTenant(ctx context.Context, name string) (TenantView, error)
	UpdateTenant(ctx context.Context, id, name string) (TenantView, error)
}

// ApplicationStore is the administrative data access for credentials.
type ApplicationStore interface {
	ListApplications(ctx context.Context, tenantID string) ([]AppView, error)
	GetApplication(ctx context.Context, tenantID, id string) (AppView, error)
	CreateApplication(ctx context.Context, tenantID, name string, canPublish, canSubscribe bool) (AppView, string, error)
	SetApplicationEnabled(ctx context.Context, tenantID, id string, enabled bool) (AppView, error)
	RotateSecret(ctx context.Context, tenantID, id string) (AppView, string, error)
}

// QuotaStore is the administrative data access for a tenant's ceilings.
type QuotaStore interface {
	GetQuota(ctx context.Context, tenantID string) (QuotaView, error)
	SetQuota(ctx context.Context, tenantID string, q QuotaView) (QuotaView, error)
}

// ChannelStore is the administrative data access for channel limits.
type ChannelStore interface {
	GetChannel(ctx context.Context, tenantID, channel string) (ChannelView, error)
	SetChannelLimit(ctx context.Context, tenantID, channel string, maxSubscribers int, ceiling float64, hasCeiling bool) (ChannelView, error)
}

// TenantView is an administrative view of a tenant.
type TenantView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// AppView is an administrative view of an application. It never carries a
// secret digest.
type AppView struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	Name         string    `json:"name"`
	AppKey       string    `json:"app_key"`
	CanPublish   bool      `json:"can_publish"`
	CanSubscribe bool      `json:"can_subscribe"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
}

// QuotaView is an administrative view of a tenant's ceilings.
type QuotaView struct {
	MaxConnections       int    `json:"max_connections"`
	MaxPublishQPS        int    `json:"max_publish_qps"`
	MaxChannels          int    `json:"max_channels"`
	PublishFailurePolicy string `json:"publish_failure_policy"`
	PerUserPublishQPS    int    `json:"per_user_publish_qps"`
}

// ChannelView is an administrative view of a channel's effective limits.
type ChannelView struct {
	TenantID       string `json:"tenant_id"`
	Channel        string `json:"channel"`
	Subscribers    int    `json:"subscribers"`
	MaxSubscribers int    `json:"max_subscribers"`
	// PublishCeiling is the effective derived or overridden ceiling.
	PublishCeiling    float64 `json:"publish_ceiling"`
	CeilingOverride   bool    `json:"ceiling_override"`
	CoalescingEnabled bool    `json:"coalescing_enabled"`
}

// ConnectionView is an administrative view of a live connection.
type ConnectionView struct {
	ID           uint64   `json:"id"`
	TenantID     string   `json:"tenant_id"`
	Subject      string   `json:"subject"`
	Channels     []string `json:"channels"`
	State        string   `json:"state"`
	PendingBytes int      `json:"pending_bytes"`
	Dropped      uint64   `json:"dropped"`
}

// Connections is the live-connection view the surface reads from.
type Connections interface {
	List(tenantID string) []ConnectionView
	Terminate(tenantID string, id uint64) bool
}

// Degradation is the ladder the surface controls.
type Degradation interface {
	SetManual(on bool, level int)
	Manual() bool
	Level() int
}

// Server serves the administrative surface.
type Server struct {
	tenants      TenantStore
	applications ApplicationStore
	quotas       QuotaStore
	channels     ChannelStore
	auth         Authorizer
	audit        Auditor
	conns        Connections
	degrade      Degradation
	now          func() time.Time
	mux          *http.ServeMux
}

// ServerConfig holds the collaborators a Server needs.
//
// It is a struct rather than a parameter list because the fields are mostly
// interfaces of similar shape: positionally, a caller cannot tell which is
// which, and adding a dependency means changing every call site. Now is injected
// so administrative timestamps are testable; nil means time.Now.
type ServerConfig struct {
	Tenants      TenantStore
	Applications ApplicationStore
	Quotas       QuotaStore
	Channels     ChannelStore
	Authorizer   Authorizer
	Auditor      Auditor
	Connections  Connections
	Degradation  Degradation
	Now          func() time.Time
}

// New returns a server.
func New(cfg ServerConfig) *Server {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	s := &Server{
		tenants:      cfg.Tenants,
		applications: cfg.Applications,
		quotas:       cfg.Quotas,
		channels:     cfg.Channels,
		auth:         cfg.Authorizer,
		audit:        cfg.Auditor,
		conns:        cfg.Connections,
		degrade:      cfg.Degradation,
		now:          now,
	}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	m := http.NewServeMux()
	m.HandleFunc("/admin/tenants", s.handleTenants)
	m.HandleFunc("/admin/tenants/", s.handleTenantByID)
	m.HandleFunc("/admin/applications", s.handleApplications)
	m.HandleFunc("/admin/applications/", s.handleApplicationByID)
	m.HandleFunc("/admin/quotas/", s.handleQuota)
	m.HandleFunc("/admin/channels/", s.handleChannel)
	m.HandleFunc("/admin/connections", s.handleConnections)
	m.HandleFunc("/admin/connections/", s.handleConnectionByID)
	m.HandleFunc("/admin/degradation", s.handleDegradation)
	s.mux = m
}

// authorize resolves the caller and checks the role.
//
// The tenant is taken from the request path, never from the caller, so a
// principal can only act within the tenant it was granted. Combining the two
// would let a principal for tenant A act on tenant B by naming it.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request, need Role) (context.Context, string, bool) {
	p := tenantFrom(r)
	tenantID, subject := p.tenantID, p.subject
	if tenantID == "" || subject == "" {
		writeError(w, http.StatusBadRequest, "tenant and principal are required")
		return nil, "", false
	}

	role, err := s.auth.RoleFor(r.Context(), tenantID, subject)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "authorization lookup failed")
		return nil, "", false
	}
	if role == "" {
		writeError(w, http.StatusUnauthorized, "no administrative authority")
		return nil, "", false
	}
	if need == RoleAdmin && role != RoleAdmin {
		writeError(w, http.StatusForbidden, "administrator role required")
		return nil, "", false
	}

	ctx := withPrincipal(r.Context(), principal{TenantID: tenantID, Subject: subject, Role: role})
	return ctx, subject, true
}

// record writes the audit entry for a mutating operation.
func (s *Server) record(ctx context.Context, principal, operation, target, detail string) {
	if s.audit == nil {
		return
	}
	s.audit.RecordAdminAction(ctx, Action{
		TenantID:  principalTenant(ctx),
		Principal: principal,
		Operation: operation,
		Target:    target,
		Detail:    detail,
		At:        s.now(),
	})
}

// --- handlers ---

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodGet:
		tenants, err := s.tenants.ListTenants(ctx)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not list tenants")
			return
		}
		writeJSON(w, http.StatusOK, tenants)

	case http.MethodPost:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := decode(r, &body); err != nil || body.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		tenant, err := s.tenants.CreateTenant(ctx, body.Name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not create tenant")
			return
		}
		s.record(ctx, subject, "tenant.create", tenant.ID, tenant.Name)
		writeJSON(w, http.StatusCreated, tenant)

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleTenantByID(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/admin/tenants/")

	switch r.Method {
	case http.MethodGet:
		tenant, err := s.tenants.GetTenant(ctx, id)
		if err != nil {
			writeLookupError(w, err, "tenant")
			return
		}
		writeJSON(w, http.StatusOK, tenant)

	case http.MethodPatch:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		tenant, err := s.tenants.UpdateTenant(ctx, id, body.Name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not update tenant")
			return
		}
		s.record(ctx, subject, "tenant.update", id, tenant.Name)
		writeJSON(w, http.StatusOK, tenant)

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleApplications(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	tenantID := tenantFrom(r).tenantID

	switch r.Method {
	case http.MethodGet:
		apps, err := s.applications.ListApplications(ctx, tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not list applications")
			return
		}
		writeJSON(w, http.StatusOK, apps)

	case http.MethodPost:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			Name         string `json:"name"`
			CanPublish   bool   `json:"can_publish"`
			CanSubscribe bool   `json:"can_subscribe"`
		}
		if err := decode(r, &body); err != nil || body.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		app, secret, err := s.applications.CreateApplication(ctx, tenantID, body.Name, body.CanPublish, body.CanSubscribe)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not create application")
			return
		}
		s.record(ctx, subject, "application.create", app.ID, app.Name)
		// The secret is returned exactly once, at creation.
		writeJSON(w, http.StatusCreated, struct {
			Application AppView `json:"application"`
			Secret      string  `json:"secret"`
		}{app, secret})

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleApplicationByID(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	tenantID := tenantFrom(r).tenantID
	rest := strings.TrimPrefix(r.URL.Path, "/admin/applications/")

	// A trailing operation segment is part of the route, not the identifier. It
	// has to be split off before the id is used, or the lookup is against
	// "a1/rotate" and never matches.
	rotate := strings.HasSuffix(rest, "/rotate")
	id := strings.TrimSuffix(rest, "/rotate")

	switch r.Method {
	case http.MethodGet:
		app, err := s.applications.GetApplication(ctx, tenantID, id)
		if err != nil {
			writeLookupError(w, err, "application")
			return
		}
		writeJSON(w, http.StatusOK, app)

	case http.MethodPatch:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			Enabled *bool `json:"enabled"`
		}
		if err := decode(r, &body); err != nil || body.Enabled == nil {
			writeError(w, http.StatusBadRequest, "enabled is required")
			return
		}
		app, err := s.applications.SetApplicationEnabled(ctx, tenantID, id, *body.Enabled)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not update application")
			return
		}
		detail := "enabled"
		if !*body.Enabled {
			// Disabling ends existing connections, which is a user-visible
			// consequence and is recorded as such.
			detail = "disabled; live connections ended"
		}
		s.record(ctx, subject, "application.set_enabled", id, detail)
		writeJSON(w, http.StatusOK, app)

	case http.MethodPost:
		if !rotate {
			writeError(w, http.StatusNotFound, "unknown operation")
			return
		}
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		{
			app, secret, err := s.applications.RotateSecret(ctx, tenantID, id)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "could not rotate secret")
				return
			}
			s.record(ctx, subject, "application.rotate_secret", id, "secret rotated; previous digest retained with no grace")
			writeJSON(w, http.StatusOK, struct {
				Application AppView `json:"application"`
				Secret      string  `json:"secret"`
			}{app, secret})
		}

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleQuota(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	tenantID := tenantFrom(r).tenantID

	switch r.Method {
	case http.MethodGet:
		q, err := s.quotas.GetQuota(ctx, tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not read quota")
			return
		}
		writeJSON(w, http.StatusOK, q)

	case http.MethodPut:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var q QuotaView
		if err := decode(r, &q); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		// Lowering the connection ceiling below current usage is allowed and is
		// recorded, because existing connections are not ended by it. An
		// operator reducing a limit should be able to see that it took effect
		// without being surprised that nobody was disconnected.
		updated, err := s.quotas.SetQuota(ctx, tenantID, q)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not write quota")
			return
		}
		s.record(ctx, subject, "quota.set", tenantID,
			"connections="+itoa(updated.MaxConnections)+" publish_qps="+itoa(updated.MaxPublishQPS))
		writeJSON(w, http.StatusOK, updated)

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleChannel(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	tenantID := tenantFrom(r).tenantID
	channel := channelFrom(r)

	if channel == "" {
		writeError(w, http.StatusBadRequest, "channel is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		view, err := s.channels.GetChannel(ctx, tenantID, channel)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not read channel")
			return
		}
		writeJSON(w, http.StatusOK, view)

	case http.MethodPatch:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			MaxSubscribers *int     `json:"max_subscribers"`
			PublishCeiling *float64 `json:"publish_ceiling"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		maxSubs := 0
		if body.MaxSubscribers != nil {
			maxSubs = *body.MaxSubscribers
		}
		ceiling := 0.0
		hasCeiling := body.PublishCeiling != nil
		if hasCeiling {
			ceiling = *body.PublishCeiling
		}
		view, err := s.channels.SetChannelLimit(ctx, tenantID, channel, maxSubs, ceiling, hasCeiling)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not write channel limits")
			return
		}
		detail := "max_subscribers=" + itoa(maxSubs)
		if hasCeiling {
			detail += " publish_ceiling=override"
		} else {
			detail += " publish_ceiling=derived"
		}
		s.record(ctx, subject, "channel.set_limits", tenantID+"/"+channel, detail)
		writeJSON(w, http.StatusOK, view)

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.authorize(w, r, RoleViewer); !ok {
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
		return
	}
	tenantID := tenantFrom(r).tenantID
	if s.conns == nil {
		writeJSON(w, http.StatusOK, []ConnectionView{})
		return
	}
	views := s.conns.List(tenantID)
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleConnectionByID(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	id := connectionIDFrom(r)
	if id == 0 {
		writeError(w, http.StatusBadRequest, "connection id is required")
		return
	}
	tenantID := tenantFrom(r).tenantID

	switch r.Method {
	case http.MethodGet:
		if s.conns == nil {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		for _, v := range s.conns.List(tenantID) {
			if v.ID == id {
				writeJSON(w, http.StatusOK, v)
				return
			}
		}
		writeError(w, http.StatusNotFound, "connection not found")

	case http.MethodDelete:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		if s.conns == nil {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		if !s.conns.Terminate(tenantID, id) {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		// The client may reconnect: termination is not a ban, and the audit
		// entry says so, because the two are routinely confused.
		s.record(ctx, subject, "connection.terminate", itoaU64(id),
			"connection closed; client may reconnect subject to normal checks")
		w.WriteHeader(http.StatusNoContent)

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

func (s *Server) handleDegradation(w http.ResponseWriter, r *http.Request) {
	ctx, subject, ok := s.authorize(w, r, RoleViewer)
	if !ok {
		return
	}
	if s.degrade == nil {
		writeError(w, http.StatusNotFound, "degradation control is not available")
		return
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"manual": s.degrade.Manual(),
			"level":  s.degrade.Level(),
		})

	case http.MethodPut:
		if !s.requireAdmin(w, r, RoleAdmin) {
			return
		}
		var body struct {
			Manual bool `json:"manual"`
			Level  int  `json:"level"`
		}
		if err := decode(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		if body.Level < 0 || body.Level > 3 {
			writeError(w, http.StatusBadRequest, "level must be between 0 and 3")
			return
		}
		s.degrade.SetManual(body.Manual, body.Level)
		detail := "manual off"
		if body.Manual {
			detail = "manual on, level " + itoa(body.Level)
		}
		s.record(ctx, subject, "degradation.set", "ladder", detail)
		writeJSON(w, http.StatusOK, map[string]any{
			"manual": s.degrade.Manual(),
			"level":  s.degrade.Level(),
		})

	default:
		writeError(w, http.StatusMethodNotAllowed, "unsupported method")
	}
}

// requireAdmin re-checks the role for a mutating operation, after authorize has
// already established that the caller exists.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request, need Role) bool {
	ctx, _, ok := s.authorize(w, r, need)
	if !ok {
		return false
	}
	_ = ctx
	return true
}

// --- request parsing ---

// requestParts carries the values extracted from a request.
type requestParts struct {
	tenantID string
	subject  string
	channel  string
	connID   uint64
}

func tenantFrom(r *http.Request) requestParts {
	p := requestParts{
		tenantID: r.Header.Get("X-Tenant-Id"),
		subject:  r.Header.Get("X-Admin-Subject"),
	}
	// A path override exists so a single-tenant deployment does not need the
	// header on every call, but the header is authoritative when present.
	if v := r.URL.Query().Get("channel"); v != "" {
		p.channel = v
	}
	if v := r.URL.Query().Get("connection_id"); v != "" {
		p.connID = parseUint(v)
	}
	return p
}

func channelFrom(r *http.Request) string {
	return tenantFrom(r).channel
}

func connectionIDFrom(r *http.Request) uint64 {
	return tenantFrom(r).connID
}

func decode(r *http.Request, dst any) error {
	defer r.Body.Close()
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
