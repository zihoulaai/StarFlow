package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/starflow/push/internal/controlplane"
)

// Resolver turns a presented token into an authenticated, authorized identity.
type Resolver struct {
	signer *Signer
	cache  *controlplane.ConfigCache
	// auditor records authentication outcomes. It is optional so the resolver
	// can be exercised without a store.
	auditor auditor
	now     func() time.Time
}

// auditor records authentication decisions.
type auditor interface {
	RecordAuthFailure(ctx context.Context, tenantID, subject, appKey, reason string)
}

// NewResolver returns a Resolver.
func NewResolver(signer *Signer, cache *controlplane.ConfigCache, auditor auditor, now func() time.Time) *Resolver {
	if now == nil {
		now = time.Now
	}
	return &Resolver{signer: signer, cache: cache, auditor: auditor, now: now}
}

// Identity is a resolved caller.
type Identity struct {
	TenantID     string
	AppKey       string
	AppID        string
	Subject      string
	Privileges   Privilege
	ChannelLimit int
	PublishLimit int
	ConnLimit    int
}

// Authenticate verifies token and resolves it to an Identity.
//
// Every rejection is audited with its specific reason, because "token invalid"
// on its own cannot distinguish a client bug from an attack, and the two want
// different responses.
func (r *Resolver) Authenticate(ctx context.Context, token string) (Identity, error) {
	claims, err := r.signer.Verify(token)
	if err != nil {
		r.auditFailure(ctx, "", "", claims.AppKey, err)
		return Identity{}, err
	}

	app, err := r.lookupApplication(claims.AppKey)
	if err != nil {
		r.auditFailure(ctx, claims.TenantID, claims.Subject, claims.AppKey, err)
		return Identity{}, err
	}
	if !app.Enabled {
		err := ErrCredentialDisabled
		r.auditFailure(ctx, app.TenantID, claims.Subject, claims.AppKey, err)
		return Identity{}, err
	}
	// A token is bound to the tenant it was issued for. A token replayed
	// against a different tenant's channels must not carry its issuer's
	// privileges across.
	if app.TenantID != claims.TenantID {
		err := ErrTokenSignature
		r.auditFailure(ctx, claims.TenantID, claims.Subject, claims.AppKey, err)
		return Identity{}, err
	}

	cfg := r.cache.Tenant(app.TenantID)
	return Identity{
		TenantID:     app.TenantID,
		AppKey:       app.AppKey,
		AppID:        app.ID,
		Subject:      claims.Subject,
		Privileges:   Privileges(app.CanPublish, app.CanSubscribe),
		ChannelLimit: cfg.Quota.MaxChannels,
		PublishLimit: cfg.Quota.MaxPublishQPS,
		ConnLimit:    cfg.Quota.MaxConnections,
	}, nil
}

// AuthenticateSecret verifies an app key and plaintext secret directly, for
// callers that publish with long-lived service credentials rather than tokens.
func (r *Resolver) AuthenticateSecret(ctx context.Context, appKey, secret string) (Identity, error) {
	app, err := r.lookupApplication(appKey)
	if err != nil {
		r.auditFailure(ctx, "", "", appKey, err)
		return Identity{}, err
	}
	if !controlplane.SecretAccepted(app, secret, r.now()) {
		err := ErrTokenSignature
		r.auditFailure(ctx, app.TenantID, "", appKey, err)
		return Identity{}, err
	}
	if !app.Enabled {
		err := ErrCredentialDisabled
		r.auditFailure(ctx, app.TenantID, "", appKey, err)
		return Identity{}, err
	}

	cfg := r.cache.Tenant(app.TenantID)
	return Identity{
		TenantID:     app.TenantID,
		AppKey:       app.AppKey,
		AppID:        app.ID,
		Privileges:   Privileges(app.CanPublish, app.CanSubscribe),
		ChannelLimit: cfg.Quota.MaxChannels,
		PublishLimit: cfg.Quota.MaxPublishQPS,
		ConnLimit:    cfg.Quota.MaxConnections,
	}, nil
}

func (r *Resolver) lookupApplication(appKey string) (controlplane.Application, error) {
	if r.cache == nil {
		return controlplane.Application{}, ErrCredentialUnknown
	}
	app, ok := r.cache.FindApplication(appKey)
	if !ok {
		return controlplane.Application{}, ErrCredentialUnknown
	}
	return app, nil
}

func (r *Resolver) auditFailure(ctx context.Context, tenantID, subject, appKey string, err error) {
	if r.auditor != nil {
		r.auditor.RecordAuthFailure(ctx, tenantID, subject, appKey, err.Error())
	}
}

// OriginPolicy enforces the browser origin allowlist.
type OriginPolicy struct {
	// Allowed lists the origins permitted for browser clients.
	Allowed []string
	// DefaultForNonBrowser is applied when a client presents no Origin header.
	// It is a boolean rather than an origin so a non-browser service client is
	// not silently refused by a policy written for browsers.
	DefaultForNonBrowser bool
}

// CheckOrigin evaluates a request's Origin header.
//
// Returning true permits the handshake. A request with no Origin is treated as
// non-browser, which is what lets a backend service connect while still
// refusing a browser page from an unlisted site.
func (p OriginPolicy) CheckOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return p.DefaultForNonBrowser
	}
	// Compare normalized forms so a trailing slash or a default port does not
	// silently refuse a legitimate origin.
	normalized := normalizeOrigin(origin)
	for _, allowed := range p.Allowed {
		if subtle.ConstantTimeCompare([]byte(normalizeOrigin(allowed)), []byte(normalized)) == 1 {
			return true
		}
	}
	return false
}

func normalizeOrigin(origin string) string {
	u, err := url.Parse(strings.TrimSpace(origin))
	if err != nil {
		return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(origin), "/"))
	}
	host := u.Host
	switch {
	case u.Scheme == "https" && strings.HasSuffix(host, ":443"),
		u.Scheme == "http" && strings.HasSuffix(host, ":80"):
		host = strings.TrimSuffix(host, u.Scheme[len(u.Scheme)-3:])
	}
	return strings.ToLower(u.Scheme + "://" + host)
}

// NetworkPolicy decides whether a peer address may connect or publish.
type NetworkPolicy struct {
	// Blocks are the tenant's configured CIDRs.
	Blocks []*net.IPNet
	// PlaintextEnabled reports whether the unencrypted endpoint is open. It is
	// a kill switch: with it false, a ws:// request is refused before any
	// subscription state exists.
	PlaintextEnabled bool
	// TLS reports whether the connection being evaluated is encrypted.
	TLS bool
}

// CheckTransport evaluates the transport requirement.
func (p NetworkPolicy) CheckTransport() error {
	if !p.PlaintextEnabled && !p.TLS {
		return ErrPlaintextRefused
	}
	return nil
}

// CheckPeer evaluates the tenant's address blocks.
func (p NetworkPolicy) CheckPeer(remoteAddr string) error {
	if remoteAddr == "" || len(p.Blocks) == 0 {
		return nil
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// An unparseable peer is not blocked here; transport-level identity is
		// not established yet at this layer.
		return nil
	}
	for _, block := range p.Blocks {
		if block.Contains(ip) {
			return ErrPeerBlocked
		}
	}
	return nil
}

// Transport and peer failures.
var (
	// ErrPlaintextRefused means the unencrypted endpoint is closed.
	ErrPlaintextRefused = errors.New("auth: plaintext transport refused")
	// ErrPeerBlocked means the peer address is blocked for this tenant.
	ErrPeerBlocked = errors.New("auth: peer address blocked")
)

// RandomToken returns an unguessable value, used for session identifiers.
func RandomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
