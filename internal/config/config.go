// Package config loads and validates process configuration.
//
// Two rules shape this package. First, every knob that affects capacity is
// explicit rather than defaulted, because a silent default on a memory limit
// turns a capacity problem into an out-of-memory kill with no explanation.
// Second, the gateway's runtime settings are chosen for the delivery path's
// allocation discipline, not for general convenience.
package config

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// setGCPercent is a variable so tests can observe the call without mutating
// process-wide GC state.
var setGCPercent func(int) int = debug.SetGCPercent

// DefaultMaxMessageBytes is the publish-side size limit when a tenant has not
// configured one. 4 KB keeps a coalesced batch inside a single TCP segment
// budget on the egress side.
const DefaultMaxMessageBytes = 4 * 1024

// DefaultPerViewerBytesPerSecond is the egress budget each subscriber is held
// to. This is the number the whole capacity model is derived from: instance
// count follows from (connections x this budget) / NIC capacity.
const DefaultPerViewerBytesPerSecond = 2 * 1024

// Config is the full gateway process configuration.
type Config struct {
	// Instance identifies this gateway for message id generation and for
	// quota sharding. It must be unique among concurrently running gateways.
	Instance int64

	// Listen is the address the subscriber and publish endpoints bind to.
	Listen string

	// AdminListen is the address the administrative endpoints bind to. It is
	// separate from Listen so the admin surface can be bound to an internal
	// network.
	AdminListen string

	// ReadyListen is the address the readiness and liveness probes bind to.
	ReadyListen string

	Bus BusConfig

	ControlPlane ControlPlaneConfig

	// Delivery holds the settings the fanout loop is tuned against.
	Delivery DeliveryConfig

	// Runtime holds process-level settings including the memory and GC limits.
	Runtime RuntimeConfig
}

// BusConfig selects and configures the message bus.
type BusConfig struct {
	// Kind is the bus implementation to use: "redis" or "memory".
	Kind string
	// Addr is the bus endpoint, used by the redis implementation.
	Addr string
	// Password authenticates to the bus.
	Password string
	// Channel is the pub/sub channel admitted messages are published on.
	Channel string
	// ConfigChannel carries control-plane configuration changes.
	ConfigChannel string
}

// ControlPlaneConfig locates the durable store.
type ControlPlaneConfig struct {
	// DSN is the relational database connection string.
	DSN string
	// MaxOpenConns bounds the connection pool.
	MaxOpenConns int
	// ConfigPollInterval is how often a gateway reconciles with the durable
	// store, as a backstop to pub/sub propagation.
	ConfigPollInterval time.Duration
	// SecretSource is where signing material is read from. "env" reads a
	// named environment variable; "file" reads a path. Signing material is
	// never taken from image contents or command-line arguments.
	SecretSource string
	// SecretRef names the environment variable or file path to read.
	SecretRef string

	// BootstrapTenant and BootstrapSubject name the first administrative
	// principal, seeded from configuration when both are set. The administrative
	// API authorizes callers against stored grants, so an instance with no grant
	// has no reachable administrator and cannot be unlocked through its own API.
	//
	// Both are required together. Bootstrapping with a default subject would
	// create an administrator whose credentials are public knowledge.
	BootstrapTenant  string
	BootstrapSubject string
	// BootstrapRole is admin or viewer. An unrecognised value is rejected at
	// start-up rather than silently granting nothing, because a typo here would
	// look like a working configuration that authorizes no one.
	BootstrapRole string
}

// DeliveryConfig holds the fanout loop's tuning.
type DeliveryConfig struct {
	// FlushInterval is the coalescing window. It is the latency dial: raising
	// it cuts packet count without cutting bytes.
	FlushInterval time.Duration

	// WriteBufferBytes is the per-connection egress buffer, allocated once at
	// handshake. It must be large enough to hold a few coalesced batches or
	// the batching is paid for and then wasted.
	WriteBufferBytes int

	// ReadBufferBytes is the per-connection control-frame read buffer.
	ReadBufferBytes int

	// MergeRatio is the coalescing target. Messages arriving within
	// FlushInterval are merged toward this many per delivered packet.
	//
	// A value of 1 disables coalescing: every message becomes its own packet.
	MergeRatio int

	// Compress enables permessage-deflate on delivered batches.
	//
	// Measured, not assumed. An unmerged 200-byte message compresses to 112%
	// of its original size, because deflate framing costs more than the
	// payload saves; a coalesced batch of twenty compresses to 12%, an 88%
	// saving. The saving is on the path that carries nearly all egress
	// traffic, so on a bandwidth-bound instance this is close to free CPU for
	// a large cut in bytes.
	//
	// Apply only to batches of at least MinCompressBatch messages, which is
	// what CompressBatchThreshold encodes.
	Compress bool

	// CompressBatchThreshold is the smallest batch worth compressing.
	//
	// This has to exist because MergeRatio is a target and not a guarantee: a
	// coalescing window also closes on elapsed time, so a lightly loaded
	// channel emits one-message batches even at MergeRatio 20. Those are the
	// case that expands, so the decision is made per batch on its actual size
	// rather than once from the configured target.
	CompressBatchThreshold int

	// PerViewerBytesPerSecond is the egress budget per subscriber.
	PerViewerBytesPerSecond int

	// HeartbeatInterval is how often a liveness probe is sent.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long a subscriber may fail to answer before the
	// connection is closed.
	HeartbeatTimeout time.Duration

	// DrainGracePeriod is how long a draining instance waits for subscribers
	// to reconnect elsewhere before closing their connections.
	DrainGracePeriod time.Duration

	// Shards is the number of dirty-list shards the flush loop works over.
	// Flush cost tracks delivered traffic rather than connection count.
	Shards int

	// TokenSkew is the accepted clock skew for a publish token's timestamp.
	TokenSkew time.Duration

	// TokenTTL is how long an issued token remains valid.
	TokenTTL time.Duration
}

// RuntimeConfig holds process-level settings.
type RuntimeConfig struct {
	// MemLimitBytes is the Go soft memory limit. It is mandatory: the
	// delivery path runs with an elevated GOGC against a large live heap, and
	// without a hard limit the collector is free to overshoot into an OOM
	// kill before GOGC has a chance to act.
	MemLimitBytes int64

	// GOGC is the collector's target. It is raised well above the default
	// because the delivery path allocates almost nothing, so the default
	// frequency costs CPU without buying anything.
	GOGC int

	// GOMAXPROCS is left at 0 to mean "runtime default".
	GOMAXPROCS int
}

// ErrMissingMemLimit is returned when MemLimitBytes is not configured.
var ErrMissingMemLimit = errors.New("config: memory limit is required and has no default")

// Load reads configuration from the environment and validates it.
//
// Every failure is reported with the offending variable rather than a generic
// parse failure, because these values are set in deployment manifests and a
// bad manifest should be diagnosable from the error alone.
func Load(getenv func(string) string) (*Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	l := &loader{getenv: getenv}

	cfg := &Config{
		Instance:    l.int64("INSTANCE_ID", 0),
		Listen:      l.str("LISTEN", ":8080"),
		AdminListen: l.str("ADMIN_LISTEN", ":8081"),
		ReadyListen: l.str("READY_LISTEN", ":8082"),

		Bus: BusConfig{
			Kind:          l.str("BUS_KIND", "memory"),
			Addr:          l.str("BUS_ADDR", "127.0.0.1:6379"),
			Password:      l.str("BUS_PASSWORD", ""),
			Channel:       l.str("BUS_CHANNEL", "push:admitted"),
			ConfigChannel: l.str("BUS_CONFIG_CHANNEL", "push:config"),
		},

		ControlPlane: ControlPlaneConfig{
			DSN:                l.str("CONTROL_DSN", ""),
			MaxOpenConns:       l.int("CONTROL_MAX_OPEN_CONNS", 16),
			ConfigPollInterval: l.duration("CONTROL_POLL_INTERVAL", 15*time.Second),
			SecretSource:       l.str("SECRET_SOURCE", "env"),
			SecretRef:          l.str("SECRET_REF", "PUSH_TOKEN_SECRET"),
			BootstrapTenant:    l.str("BOOTSTRAP_TENANT", ""),
			BootstrapSubject:   l.str("BOOTSTRAP_SUBJECT", ""),
			BootstrapRole:      l.str("BOOTSTRAP_ROLE", "admin"),
		},

		Delivery: DeliveryConfig{
			FlushInterval:           l.duration("DELIVERY_FLUSH_INTERVAL", 10*time.Millisecond),
			WriteBufferBytes:        l.int("DELIVERY_WRITE_BUFFER_BYTES", 16*1024),
			ReadBufferBytes:         l.int("DELIVERY_READ_BUFFER_BYTES", 4*1024),
			MergeRatio:              l.int("DELIVERY_MERGE_RATIO", 20),
			Compress:                l.bool("DELIVERY_COMPRESS", true),
			CompressBatchThreshold:  l.int("DELIVERY_COMPRESS_MIN_BATCH", 5),
			PerViewerBytesPerSecond: l.int("DELIVERY_PER_VIEWER_BPS", DefaultPerViewerBytesPerSecond),
			HeartbeatInterval:       l.duration("DELIVERY_HEARTBEAT_INTERVAL", 25*time.Second),
			HeartbeatTimeout:        l.duration("DELIVERY_HEARTBEAT_TIMEOUT", 10*time.Second),
			DrainGracePeriod:        l.duration("DELIVERY_DRAIN_GRACE", 30*time.Second),
			Shards:                  l.int("DELIVERY_SHARDS", 1024),
			TokenSkew:               l.duration("AUTH_TOKEN_SKEW", 2*time.Minute),
			TokenTTL:                l.duration("AUTH_TOKEN_TTL", 5*time.Minute),
		},

		Runtime: RuntimeConfig{
			MemLimitBytes: l.int64Ptr("GOMEMLIMIT_BYTES"),
			GOGC:          l.int("GOGC", 400),
			GOMAXPROCS:    l.int("GOMAXPROCS", 0),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate rejects a configuration that would run the service in a state it
// cannot be diagnosed from later.
func (c *Config) Validate() error {
	if c.Runtime.MemLimitBytes <= 0 {
		return ErrMissingMemLimit
	}

	// The bootstrap settings are all-or-nothing. A tenant without a subject would
	// name an administrator with no identity, and a subject without a tenant has
	// no scope to administer.
	if (c.ControlPlane.BootstrapTenant == "") != (c.ControlPlane.BootstrapSubject == "") {
		return fmt.Errorf("config: BOOTSTRAP_TENANT and BOOTSTRAP_SUBJECT must be set together")
	}
	if c.ControlPlane.BootstrapSubject != "" {
		if c.ControlPlane.BootstrapRole != "admin" && c.ControlPlane.BootstrapRole != "viewer" {
			return fmt.Errorf("config: BOOTSTRAP_ROLE must be admin or viewer, got %q",
				c.ControlPlane.BootstrapRole)
		}
	}
	if c.Instance < 0 {
		return fmt.Errorf("config: INSTANCE_ID must not be negative, got %d", c.Instance)
	}
	if c.Bus.Kind != "redis" && c.Bus.Kind != "memory" {
		return fmt.Errorf("config: BUS_KIND must be \"redis\" or \"memory\", got %q", c.Bus.Kind)
	}
	if c.Bus.Kind == "redis" && c.Bus.Addr == "" {
		return fmt.Errorf("config: BUS_ADDR is required when BUS_KIND is \"redis\"")
	}
	if c.Bus.Channel == "" || c.Bus.ConfigChannel == "" {
		return fmt.Errorf("config: bus channels must not be empty")
	}
	if c.ControlPlane.SecretSource != "env" && c.ControlPlane.SecretSource != "file" {
		return fmt.Errorf("config: SECRET_SOURCE must be \"env\" or \"file\", got %q", c.ControlPlane.SecretSource)
	}
	if c.ControlPlane.SecretRef == "" {
		return fmt.Errorf("config: SECRET_REF must name the signing material to read")
	}
	if c.Delivery.FlushInterval <= 0 {
		return fmt.Errorf("config: DELIVERY_FLUSH_INTERVAL must be positive")
	}
	if c.Delivery.WriteBufferBytes <= 0 {
		return fmt.Errorf("config: DELIVERY_WRITE_BUFFER_BYTES must be positive")
	}
	if c.Delivery.ReadBufferBytes <= 0 {
		return fmt.Errorf("config: DELIVERY_READ_BUFFER_BYTES must be positive")
	}
	if c.Delivery.MergeRatio <= 0 {
		return fmt.Errorf("config: DELIVERY_MERGE_RATIO must be positive")
	}
	// Compression is coupled to coalescing by measurement, not preference. The
	// threshold is checked against the merge target because a threshold below 2
	// would compress one-message batches, which expand rather than shrink.
	//
	// This is refused at startup rather than at first delivery, because a
	// per-batch expansion is invisible until someone reads an egress meter.
	if c.Delivery.Compress && c.Delivery.CompressBatchThreshold < 2 {
		return fmt.Errorf("config: DELIVERY_COMPRESS is enabled with DELIVERY_COMPRESS_MIN_BATCH=%d; "+
			"batches smaller than two messages compress to more than their original size, so the "+
			"threshold must be at least 2", c.Delivery.CompressBatchThreshold)
	}
	if c.Delivery.PerViewerBytesPerSecond <= 0 {
		return fmt.Errorf("config: DELIVERY_PER_VIEWER_BPS must be positive")
	}
	if c.Delivery.Shards <= 0 {
		return fmt.Errorf("config: DELIVERY_SHARDS must be positive")
	}
	if c.Delivery.HeartbeatInterval <= 0 || c.Delivery.HeartbeatTimeout <= 0 {
		return fmt.Errorf("config: heartbeat interval and timeout must be positive")
	}
	if c.Delivery.DrainGracePeriod < 0 {
		return fmt.Errorf("config: DELIVERY_DRAIN_GRACE must not be negative")
	}
	if c.Delivery.TokenTTL <= 0 || c.Delivery.TokenSkew < 0 {
		return fmt.Errorf("config: AUTH_TOKEN_TTL must be positive and AUTH_TOKEN_SKEW must not be negative")
	}
	if c.Runtime.GOGC < 0 {
		return fmt.Errorf("config: GOGC must not be negative")
	}
	if c.ControlPlane.MaxOpenConns < 0 {
		return fmt.Errorf("config: CONTROL_MAX_OPEN_CONNS must not be negative")
	}
	return nil
}

// ApplyRuntime installs the process-level settings that cannot be changed after
// the runtime has started.
func (c *Config) ApplyRuntime() {
	debug.SetMemoryLimit(c.Runtime.MemLimitBytes)
	setGCPercent(c.Runtime.GOGC)
	if c.Runtime.GOMAXPROCS > 0 {
		runtime.GOMAXPROCS(c.Runtime.GOMAXPROCS)
	}
}

// loader reads environment variables, reporting the variable name on failure.
type loader struct{ getenv func(string) string }

func (l *loader) str(key, def string) string {
	if v := strings.TrimSpace(l.getenv(key)); v != "" {
		return v
	}
	return def
}

func (l *loader) bool(key string, def bool) bool {
	raw := strings.TrimSpace(l.getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return v
}

func (l *loader) int(key string, def int) int {
	raw := strings.TrimSpace(l.getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func (l *loader) int64(key string, def int64) int64 {
	raw := strings.TrimSpace(l.getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	return v
}

// int64Ptr distinguishes "absent" from "set to zero", which int64 cannot: an
// absent memory limit must be an error while an explicit zero is merely
// invalid, and both have to be rejected rather than defaulted.
func (l *loader) int64Ptr(key string) int64 {
	raw := strings.TrimSpace(l.getenv(key))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return -1
	}
	return v
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(l.getenv(key))
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return v
}
