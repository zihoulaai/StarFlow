package controlplane

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"
)

// Store owns the database handle and exposes the repositories.
//
// Every tenant-scoped method takes a tenant identifier as its first argument
// and filters on it in the query itself. Tenant isolation is therefore a
// property of the query, not of the caller's discipline.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// NewStore returns a Store over db. now is injected so retention and
// expiry behaviour is testable without sleeping.
func NewStore(db *sql.DB, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, now: now}
}

// DB exposes the handle for migrations and health checks.
func (s *Store) DB() *sql.DB { return s.db }

// Now returns the store's clock.
func (s *Store) Now() time.Time { return s.now() }

// NewID returns a random identifier suitable for a primary key.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice; a panic here is preferable to
		// silently issuing colliding identifiers.
		panic("controlplane: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// HashSecret returns the stored form of an application secret.
//
// Application secrets are generated with full entropy, so a fast digest is
// sufficient: there is nothing for an attacker to brute-force at the storage
// layer. The secret itself is never persisted.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Tenant is an isolated customer of the service.
type Tenant struct {
	ID                 string
	Name               string
	AuditRetentionDays int
	RedactAuditBodies  bool
	CreatedAt          time.Time
}

// Application is a set of credentials and privileges under a tenant.
type Application struct {
	ID       string
	TenantID string
	Name     string
	// AppKey is the public identifier presented by clients.
	AppKey string
	// SecretHash is the stored digest. The plaintext is returned once at
	// creation and never again.
	SecretHash string
	// SecretPrevHash and SecretPrevValidUntil support rotation with an
	// optional grace window. A zero validity window means the previous secret
	// stops working the moment rotation is recorded.
	SecretPrevHash       string
	SecretPrevValidUntil time.Time
	CanPublish           bool
	CanSubscribe         bool
	Enabled              bool
	CreatedAt            time.Time
}

// Quota holds the ceilings applied to a tenant.
type Quota struct {
	TenantID       string
	MaxConnections int
	MaxPublishQPS  int
	MaxChannels    int
	// PublishFailurePolicy selects behaviour when the publish ceiling is
	// exceeded: discard, degrade, or fail_fast.
	PublishFailurePolicy    string
	PerUserPublishQPS       int
	DuplicateWindowMS       int
	DuplicateContentEnabled bool
	PromoPatternEnabled     bool
	UpdatedAt               time.Time
}

// ChannelConfig is the optional per-channel override set.
//
// A nil override means "derive it", which is distinct from an override of
// zero: the derived publish ceiling depends on subscriber count, which is
// runtime state the gateway owns.
type ChannelConfig struct {
	TenantID                  string
	Channel                   string
	PublishCeilingOverrideQPS *float64
	ReceiveRateTarget         *float64
	MergeRatioOverride        *int
	CoalescingEnabled         bool
	// MaxSubscribers of zero means the channel imposes no subscriber ceiling.
	MaxSubscribers int
	UpdatedAt      time.Time
}

// ModerationRule kinds.
const (
	RuleKindSensitiveWord = "sensitive_word"
	RuleKindPromotional   = "promotional"
)

// ModerationRule actions.
const (
	ActionBlock   = "block"
	ActionReplace = "replace"
	ActionTag     = "tag"
)

// ModerationRule is one content rule applied on the publish path.
type ModerationRule struct {
	ID          string
	TenantID    string
	Kind        string
	Pattern     string
	Action      string
	Replacement string
	Tag         string
	Enabled     bool
	UpdatedAt   time.Time
}

// Publish failure policies.
const (
	PolicyDiscard  = "discard"
	PolicyDegrade  = "degrade"
	PolicyFailFast = "fail_fast"
)

// Restriction action kinds.
const (
	RestrictionMute = "mute"
	RestrictionBan  = "ban"
)

// Subject kinds for restrictions and network blocks.
const (
	SubjectUser = "user"
	SubjectIP   = "ip"
)

// Restriction is a mute or a ban on a subject. A zero ExpiresAt means the
// restriction does not expire, which is how a permanent ban differs from a
// mute.
type Restriction struct {
	ID          string
	TenantID    string
	SubjectKind string
	SubjectID   string
	Action      string
	Reason      string
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

// Active reports whether the restriction is in force at now.
func (r Restriction) Active(now time.Time) bool {
	return r.ExpiresAt.IsZero() || r.ExpiresAt.After(now)
}

// NetworkBlock is a blocked address range.
type NetworkBlock struct {
	ID        string
	TenantID  string
	CIDR      string
	Reason    string
	Enabled   bool
	CreatedAt time.Time
}

// AdminPrincipal is an authorized administrative caller.
type AdminPrincipal struct {
	ID       string
	TenantID string
	Subject  string
	// Role is admin or viewer. Only admin may mutate.
	Role      string
	Enabled   bool
	CreatedAt time.Time
}

// Admin roles.
const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"
)

// Audit entry kinds.
const (
	AuditModeration = "moderation"
	AuditAuth       = "auth"
	AuditAdmin      = "admin"
)

// AuditEntry is one recorded decision. Body is already redacted at write time
// according to the tenant's RedactAuditBodies setting, so a mis-scoped read
// cannot surface content that should not have been retained.
type AuditEntry struct {
	ID        int64
	TenantID  string
	At        time.Time
	Kind      string
	Subject   string
	Channel   string
	MessageID string
	RuleID    string
	Decision  string
	Body      string
}

// ErrNotFound is returned when a tenant-scoped lookup matches no row.
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "controlplane: not found" }
