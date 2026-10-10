// Package auth implements credential authentication and privilege separation.
//
// Authentication happens twice against different things. A subscriber proves
// itself once, at handshake, with a token signed by the application's secret. A
// publisher proves itself per request, because a publish may originate from a
// backend service holding different credentials from the browser clients
// subscribed to the same channels.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Signing failures. Each is distinct because the administrative surface reports
// them differently and the audit trail needs to tell a replay from a forgery.
var (
	// ErrTokenMalformed means the token could not be parsed.
	ErrTokenMalformed = errors.New("auth: malformed token")
	// ErrTokenSignature means the token parsed but was not signed with the
	// expected key.
	ErrTokenSignature = errors.New("auth: token signature does not verify")
	// ErrTokenExpired means the token's expiry has passed.
	ErrTokenExpired = errors.New("auth: token expired")
	// ErrTokenSkew means the token's timestamp is outside the accepted window.
	// It is deliberately distinct from ErrTokenExpired: a token that is valid
	// but from the future is a clock problem or a forgery attempt, whereas an
	// expired token is ordinary.
	ErrTokenSkew = errors.New("auth: token timestamp outside accepted skew")
	// ErrPrivilegeDenied means the credential authenticated but lacks the
	// privilege for the requested operation.
	ErrPrivilegeDenied = errors.New("auth: privilege denied")
	// ErrCredentialUnknown means no application matched the presented key.
	ErrCredentialUnknown = errors.New("auth: unknown credential")
	// ErrCredentialDisabled means the application has been disabled.
	ErrCredentialDisabled = errors.New("auth: credential disabled")
)

// Claims is the payload of a token.
type Claims struct {
	// AppKey identifies the application. The secret is never carried.
	AppKey string
	// Subject is the end user or service on whose behalf the token was issued.
	Subject string
	// TenantID is carried so the gateway does not need a second lookup to
	// scope a request.
	TenantID string
	// IssuedAtMs and ExpiresAtMs bound validity.
	IssuedAtMs  int64
	ExpiresAtMs int64
}

// Signer issues and verifies tokens.
//
// The signing key comes from a secret source rather than from image contents or
// arguments; see the deployment tasks. It is held in memory for the process
// lifetime.
type Signer struct {
	key  []byte
	ttl  time.Duration
	skew time.Duration
	now  func() time.Time
}

// NewSigner returns a Signer over key.
func NewSigner(key []byte, ttl, skew time.Duration, now func() time.Time) *Signer {
	if now == nil {
		now = time.Now
	}
	return &Signer{key: key, ttl: ttl, skew: skew, now: now}
}

// Issue returns a signed token for claims, stamping issue and expiry times.
//
// The token is a compact structure of header, claims and an HMAC over both, so
// verification needs no lookup to check integrity — only to resolve the
// application from the key it names.
func (s *Signer) Issue(appKey, subject, tenantID string) (string, error) {
	if appKey == "" {
		return "", errors.New("auth: issue requires an app key")
	}
	now := s.now()
	claims := Claims{
		AppKey:      appKey,
		Subject:     subject,
		TenantID:    tenantID,
		IssuedAtMs:  now.UnixMilli(),
		ExpiresAtMs: now.Add(s.ttl).UnixMilli(),
	}

	payload := claims.Encode()
	sig := s.sign(payload)
	return "v1." + payload + "." + sig, nil
}

func (s *Signer) sign(payload string) string {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Verify checks a token's integrity and validity window, returning its claims.
//
// The order is deliberate. The signature is checked before the timestamps are
// read, so a forged token cannot be used to probe how the window behaves, and a
// token that does not verify is never reported as merely expired.
func (s *Signer) Verify(token string) (Claims, error) {
	var claims Claims

	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return claims, ErrTokenMalformed
	}
	payload, sig := parts[1], parts[2]

	// Constant-time comparison: the signature is attacker-supplied data, and an
	// early exit would leak how many leading bytes were correct.
	if !hmac.Equal([]byte(s.sign(payload)), []byte(sig)) {
		return claims, ErrTokenSignature
	}

	if err := claims.Decode(payload); err != nil {
		return claims, err
	}

	now := s.now()
	if claims.ExpiresAtMs <= now.UnixMilli() {
		return claims, ErrTokenExpired
	}
	// A token dated too far in the future is not a valid token that happens to
	// be early; it is either a forgery or a badly skewed client, and accepting
	// it would extend the token's life beyond what was issued.
	skewMs := s.skew.Milliseconds()
	if claims.IssuedAtMs > now.UnixMilli()+skewMs {
		return claims, ErrTokenSkew
	}
	if claims.IssuedAtMs < now.UnixMilli()-skewMs {
		return claims, ErrTokenSkew
	}
	return claims, nil
}

// Encode serialises claims for the token payload.
func (c Claims) Encode() string {
	return strings.Join([]string{
		base64.RawURLEncoding.EncodeToString([]byte(c.AppKey)),
		base64.RawURLEncoding.EncodeToString([]byte(c.Subject)),
		base64.RawURLEncoding.EncodeToString([]byte(c.TenantID)),
		strconv.FormatInt(c.IssuedAtMs, 10),
		strconv.FormatInt(c.ExpiresAtMs, 10),
	}, "~")
}

// Decode parses a token payload.
func (c *Claims) Decode(payload string) error {
	parts := strings.Split(payload, "~")
	if len(parts) != 5 {
		return ErrTokenMalformed
	}
	var err error
	if c.AppKey, err = decodeField(parts[0]); err != nil {
		return err
	}
	if c.Subject, err = decodeField(parts[1]); err != nil {
		return err
	}
	if c.TenantID, err = decodeField(parts[2]); err != nil {
		return err
	}
	if c.IssuedAtMs, err = strconv.ParseInt(parts[3], 10, 64); err != nil {
		return ErrTokenMalformed
	}
	if c.ExpiresAtMs, err = strconv.ParseInt(parts[4], 10, 64); err != nil {
		return ErrTokenMalformed
	}
	return nil
}

func decodeField(s string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", ErrTokenMalformed
	}
	return string(b), nil
}

// Privilege is an operation a credential may be granted.
type Privilege uint8

const (
	PrivilegePublish   Privilege = 1 << 0
	PrivilegeSubscribe Privilege = 1 << 1
)

// Privileges converts the stored grant into a bitmask.
func Privileges(canPublish, canSubscribe bool) Privilege {
	var p Privilege
	if canPublish {
		p |= PrivilegePublish
	}
	if canSubscribe {
		p |= PrivilegeSubscribe
	}
	return p
}

// Has reports whether the grant includes p.
func (p Privilege) Has(want Privilege) bool { return p&want == want }

// String names a privilege for error messages and audit records.
func (p Privilege) String() string {
	switch p {
	case PrivilegePublish:
		return "publish"
	case PrivilegeSubscribe:
		return "subscribe"
	default:
		return "unknown"
	}
}

// Check reports whether granted permits want, without any store lookup. The
// caller has already resolved and authenticated the credential by this point.
func Check(granted Privilege, want Privilege) error {
	if !granted.Has(want) {
		return fmt.Errorf("%w: %s", ErrPrivilegeDenied, want)
	}
	return nil
}
