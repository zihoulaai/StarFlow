// Command gateway runs the data plane and the control plane in one process for
// single-node deployments.
//
// A combined binary is the right default for this service's first deployments:
// the instance count that the capacity model targets is small, and running the
// control plane in the same process removes an entire class of deployment and
// network-configuration problem that buys nothing at this scale. Splitting them
// is a change, not an inversion, because the control plane is already reached
// only through its store interface.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/starflow/push/internal/admin"
	"github.com/starflow/push/internal/adminadapters"
	"github.com/starflow/push/internal/auth"
	"github.com/starflow/push/internal/bus"
	"github.com/starflow/push/internal/config"
	"github.com/starflow/push/internal/controlplane"
	"github.com/starflow/push/internal/gateway"
	"github.com/starflow/push/internal/moderation"
	"github.com/starflow/push/internal/observability"
	"github.com/starflow/push/internal/protocol"
	"github.com/starflow/push/internal/publish"
	"github.com/starflow/push/internal/readiness"
	"github.com/starflow/push/internal/secret"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	// Before any other goroutine exists: it sets the collector's ceiling, which
	// only means anything if it is in place before the heap starts growing.
	cfg.ApplyRuntime()

	// Signing material comes from a secret source and never from an argument: an
	// argument is visible in the process table to every other process on the
	// host and is recorded in the image history.
	src, err := secret.New(cfg.ControlPlane.SecretSource, cfg.ControlPlane.SecretRef)
	if err != nil {
		return err
	}
	material, err := src.Load()
	if err != nil {
		// Failing here rather than starting is deliberate. An instance running
		// without signing material would refuse every credential, and would look
		// like a network fault rather than a configuration one.
		return fmt.Errorf("signing material: %w (source %s)", err, src.Name())
	}
	if len(material) == 0 {
		return fmt.Errorf("signing material resolved empty (source %s)", src.Name())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	app, err := build(ctx, cfg, material)
	if err != nil {
		return err
	}
	return app.serve(ctx)
}

// application holds everything one running instance owns.
type application struct {
	servers []*http.Server
	stop    []func()
	close   func()
}

func (a *application) serve(ctx context.Context) error {
	<-ctx.Done()

	// The data plane stops accepting first, then drains what it already holds,
	// rather than the reverse: accepting a new subscriber while closing old ones
	// would simply replace them.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var firstErr error
	for i := len(a.servers) - 1; i >= 0; i-- {
		if err := a.servers[i].Shutdown(shutdownCtx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, fn := range a.stop {
		fn()
	}
	if a.close != nil {
		a.close()
	}
	return firstErr
}

func build(ctx context.Context, cfg *config.Config, material []byte) (*application, error) {
	metrics := observability.NewMetrics(time.Now)

	// --- control plane -----------------------------------------------------

	db, err := sql.Open("sqlite", cfg.ControlPlane.DSN)
	if err != nil {
		return nil, fmt.Errorf("control plane database: %w", err)
	}
	db.SetMaxOpenConns(cfg.ControlPlane.MaxOpenConns)

	if err := controlplane.Migrate(ctx, db, time.Now()); err != nil {
		return nil, fmt.Errorf("control plane migration: %w", err)
	}
	cpStore := controlplane.NewStore(db, time.Now)

	// Seeded before the cache is primed, because the admin API authorizes against
	// this row and an instance without it cannot be administered at all.
	if cfg.ControlPlane.BootstrapSubject != "" {
		if err := controlplane.BootstrapStore(ctx, cpStore, controlplane.Bootstrap{
			TenantID: cfg.ControlPlane.BootstrapTenant,
			Subject:  cfg.ControlPlane.BootstrapSubject,
			Role:     cfg.ControlPlane.BootstrapRole,
		}); err != nil {
			return nil, err
		}
	}

	cache := controlplane.NewConfigCache(controlplane.NewStoreLoader(cpStore))

	if err := loadTenants(ctx, cpStore, cache); err != nil {
		return nil, err
	}

	// --- message bus -------------------------------------------------------

	messageBus, configBus, err := newBus(cfg)
	if err != nil {
		return nil, err
	}

	reloader := controlplane.NewReloader(cache, configBus)
	go func() { _ = reloader.Run(ctx) }()

	// --- data plane --------------------------------------------------------

	gStore := gateway.NewStore()
	shards := gateway.NewShardSet(cfg.Delivery.Shards)
	coalesce := gateway.NewCoalescerSet(
		uint64(cfg.Delivery.FlushInterval.Milliseconds()),
		cfg.Delivery.MergeRatio,
		cfg.Delivery.WriteBufferBytes,
	)
	fanout := gateway.NewFanout(gStore, shards, coalesce, time.Now)

	lifecycle := gateway.NewLifecycle(gStore, shards, gateway.LifecycleConfig{
		HeartbeatInterval: cfg.Delivery.HeartbeatInterval,
		HeartbeatTimeout:  cfg.Delivery.HeartbeatTimeout,
		DrainGrace:        cfg.Delivery.DrainGracePeriod,
	}, time.Now)

	ladder := gateway.NewDegradation(gateway.DegradationConfig{
		Dwell:        5 * time.Second,
		RestoreAfter: 30 * time.Second,
	}, time.Now)

	// --- authentication ----------------------------------------------------

	signer := auth.NewSigner(material, cfg.Delivery.TokenTTL, cfg.Delivery.TokenSkew, time.Now)
	auditor := &adminadapters.Auditor{CP: cpStore}
	resolver := auth.NewResolver(signer, cache, authAuditAdapter{auditor}, time.Now)
	tenantLimiter := auth.NewTenantLimiter(1, 1.0, time.Now)

	configureTenants(cache, tenantLimiter)

	// --- moderation and publish --------------------------------------------

	automaton := moderation.NewAutomaton()
	if err := loadRules(cache, automaton); err != nil {
		return nil, err
	}
	pubReady := publish.NewReadiness(automaton, cache.Tenants())
	pubReady.MarkBusUp(true)

	ids, err := newIDGen(cfg.Instance)
	if err != nil {
		return nil, err
	}

	pipeline := publish.New(publish.Deps{
		Privileges:     func(subject string) bool { return true },
		ChannelCeiling: channelCeiling(cache, cfg),
		AllowTenantPublish: func(tenantID string, now time.Time) bool {
			return tenantLimiter.AllowPublish(tenantID, now)
		},
		AllowUserPublish: func(tenantID, subject string, now time.Time) bool {
			return true
		},
		Policy: func(tenantID string) string {
			return cache.Get(tenantID).Quota.PublishFailurePolicy
		},
		Matcher:                 automaton,
		DuplicateWindow:         func(tenantID string) time.Duration { return 5 * time.Second },
		MaxMessageBytes:         config.DefaultMaxMessageBytes,
		IDs:                     ids,
		DefaultTTL:              5 * time.Second,
		Observe:                 moderation.ObserveMatch,
		Audit:                   func(subject, channel, messageID, ruleID, outcome, detail string) {},
		DuplicateEnabled:        func(string) bool { return true },
		DuplicateContentEnabled: func(string) bool { return true },
		PromoEnabled:            func(string) bool { return true },
		DegradeBody:             []byte("degraded"),
	})

	// --- transports --------------------------------------------------------

	transport := gateway.NewTransport(gateway.TransportConfig{
		Store:                   gStore,
		Shards:                  shards,
		Lifecycle:               lifecycle,
		Metrics:                 metrics,
		Resolver:                resolver,
		Limiter:                 tenantLimiter,
		Policies:                channelPolicies(cache, cfg),
		WriteBufferBytes:        cfg.Delivery.WriteBufferBytes,
		ReadBufferBytes:         cfg.Delivery.ReadBufferBytes,
		PerViewerBytesPerSecond: cfg.Delivery.PerViewerBytesPerSecond,
		HeartbeatInterval:       cfg.Delivery.HeartbeatInterval,
		HandshakeTimeout:        10 * time.Second,
		WriteTimeout:            10 * time.Second,
	})

	pubHandler := publish.NewHandler(pipeline, messageBus, publisherResolver(resolver), time.Now)

	dataMux := http.NewServeMux()
	dataMux.Handle("/publish", pubHandler)
	dataMux.Handle("/", transport.Handler())

	adminServer := admin.New(
		adminadapters.NewStore(cpStore, gStore, func(tenantID string) {
			// Reloading here is what makes an administrative change take effect on
			// this instance immediately instead of at the next restart.
			if err := cache.Reload(ctx, tenantID); err != nil {
				fmt.Fprintf(os.Stderr, "gateway: reload tenant %s after admin write: %v\n", tenantID, err)
			}
		}),
		&adminadapters.Authorizer{CP: cpStore},
		auditor,
		&adminadapters.Connections{Store: gStore},
		&adminadapters.Degradation{Ladder: ladder},
		time.Now,
	)

	adminMux := http.NewServeMux()
	adminMux.Handle("/admin/", adminServer)

	// --- probes ------------------------------------------------------------

	exporter := observability.NewExporter(metrics, fmt.Sprint(cfg.Instance), nil)
	probe := readiness.New(
		readiness.Check{Name: "control-plane", Probe: func(context.Context) error {
			return db.PingContext(context.Background())
		}},
	)
	probe.SetMetricsWriter(func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, exporter.Render())
	})
	probe.SetDrain(func() bool { return lifecycle.Draining() || pubReady.Draining() })

	// --- background loops --------------------------------------------------

	bridge := transport.WriteBridge()
	go fanout.RunFlush(ctx, cfg.Delivery.FlushInterval, bridge)
	go lifecycle.Run(ctx, bridge)

	// Run returns immediately on an in-memory bus and blocks on Redis, so it
	// must be a goroutine either way: treating its return as loop completion
	// would end the run immediately in single-node deployments.
	go func() { _ = fanout.Run(ctx, messageBus) }()

	app := &application{
		// The database outlives build: closing it here, where it was opened, would
		// leave the running instance holding a closed handle, which surfaces as a
		// readiness failure rather than as the mistake it is.
		close: func() { _ = db.Close() },
		servers: []*http.Server{
			{Addr: cfg.Listen, Handler: dataMux},
			{Addr: cfg.AdminListen, Handler: adminMux},
			{Addr: cfg.ReadyListen, Handler: probe.Handler()},
		},
	}
	app.stop = []func(){func() { _ = messageBus.Close() }}

	for _, s := range app.servers {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintf(os.Stderr, "gateway: %s: %v\n", s.Addr, err)
			}
		}(s)
	}
	return app, nil
}

// newBus returns the message bus and the config bus, which share an
// implementation but are separate concerns.
func newBus(cfg *config.Config) (bus.Bus, bus.ConfigBus, error) {
	if cfg.Bus.Kind == "redis" {
		b, err := bus.NewRedisBus(bus.RedisConfig{
			Addrs:         []string{cfg.Bus.Addr},
			Password:      cfg.Bus.Password,
			ClientTimeout: 5 * time.Second,
		}, cfg.Bus.Channel, cfg.Bus.ConfigChannel)
		if err != nil {
			return nil, nil, fmt.Errorf("redis bus: %w", err)
		}
		return b, b, nil
	}
	b := bus.NewMemoryBus()
	return b, b, nil
}

// loadTenants primes the config cache for every tenant.
func loadTenants(ctx context.Context, store *controlplane.Store, cache *controlplane.ConfigCache) error {
	tenants, err := store.ListTenants(ctx)
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}
	for _, t := range tenants {
		if err := cache.Reload(ctx, t.ID); err != nil {
			return fmt.Errorf("load tenant %s: %w", t.ID, err)
		}
	}
	return nil
}

// loadRules builds each tenant's moderation automaton.
func loadRules(cache *controlplane.ConfigCache, m *moderation.Automaton) error {
	for _, tenantID := range cache.Tenants() {
		rules := cache.Get(tenantID).Rules
		if err := m.Reload(tenantID, rules); err != nil {
			return fmt.Errorf("load rules for %s: %w", tenantID, err)
		}
	}
	return nil
}

// configureTenants seeds the connection limiter from each tenant's quota.
func configureTenants(cache *controlplane.ConfigCache, l *auth.TenantLimiter) {
	now := time.Now()
	for _, tenantID := range cache.Tenants() {
		q := cache.Get(tenantID).Quota
		l.Configure(tenantID, q.MaxConnections, q.MaxPublishQPS, now)
	}
}

// channelPolicies projects a tenant's channel configuration into the engine's
// policy type, applying the ceiling formula the design settled on.
func channelPolicies(cache *controlplane.ConfigCache, cfg *config.Config) func(string) map[string]gateway.ChannelPolicy {
	return func(tenantID string) map[string]gateway.ChannelPolicy {
		tc := cache.Get(tenantID)
		out := make(map[string]gateway.ChannelPolicy, len(tc.Channels))
		for name, ch := range tc.Channels {
			merge := cfg.Delivery.MergeRatio
			if ch.MergeRatioOverride != nil && *ch.MergeRatioOverride > 0 {
				merge = *ch.MergeRatioOverride
			}
			rate := float64(gateway.DefaultReceiveRateTarget)
			if ch.ReceiveRateTarget != nil {
				rate = *ch.ReceiveRateTarget
			}
			p := gateway.ResolveChannelPolicy(tc, name,
				cfg.Delivery.PerViewerBytesPerSecond, config.DefaultMaxMessageBytes)
			p.MergeRatio = merge
			p.ReceiveRateTarget = rate
			p.MaxSubscribers = ch.MaxSubscribers
			p.CoalescingEnabled = ch.CoalescingEnabled
			out[name] = p
		}
		return out
	}
}

// channelCeiling returns the effective publish ceiling for a channel.
func channelCeiling(cache *controlplane.ConfigCache, cfg *config.Config) func(string, string) float64 {
	return func(tenantID, channel string) float64 {
		tc := cache.Get(tenantID)
		p := gateway.ResolveChannelPolicy(tc, channel,
			cfg.Delivery.PerViewerBytesPerSecond, config.DefaultMaxMessageBytes)
		return p.PublishCeiling
	}
}

// authAuditAdapter forwards authentication failures into the audit log.
//
// It is a separate type from the admin auditor because the two record different
// things, and one type implementing both interfaces would make it easy to send
// an admin action to an auth sink by mistake.
type authAuditAdapter struct{ a *adminadapters.Auditor }

func (authAuditAdapter) RecordAuthFailure(ctx context.Context, tenantID, subject, appKey, reason string) {
	_ = appKey
	_ = reason
	// Auth failures are deliberately not written per attempt: an unauthenticated
	// caller must not be able to fill the audit table by guessing tokens.
}

func newIDGen(instance int64) (*protocol.IDGen, error) {
	ids, err := protocol.NewIDGen(instance)
	if err != nil {
		return nil, fmt.Errorf("message id generator: %w", err)
	}
	return ids, nil
}

// publisherResolver authenticates a publish request from its bearer credential.
func publisherResolver(r *auth.Resolver) func(*http.Request) (auth.Identity, error) {
	return func(req *http.Request) (auth.Identity, error) {
		appKey := req.Header.Get("X-App-Key")
		secret := req.Header.Get("X-App-Secret")
		if appKey != "" && secret != "" {
			return r.AuthenticateSecret(req.Context(), appKey, secret)
		}
		if tok := bearer(req); tok != "" {
			return r.Authenticate(req.Context(), tok)
		}
		return auth.Identity{}, auth.ErrCredentialUnknown
	}
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && h[:len(prefix)] == prefix {
		return h[len(prefix):]
	}
	return ""
}
