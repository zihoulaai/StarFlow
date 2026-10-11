package auth

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestShardedLimiterLocalShareDividesByFleetSize(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)

	l := NewShardedLimiter(1000, 4, 1, now)
	if got := l.LocalShare(); got != 250 {
		t.Errorf("LocalShare with 4 instances = %v, want 250", got)
	}

	l.SetInstanceCount(10, now)
	if got := l.LocalShare(); got != 100 {
		t.Errorf("LocalShare with 10 instances = %v, want 100", got)
	}
}

func TestShardedLimiterZeroConfiguredIsUnlimited(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(0, 4, 1, now)

	if got := l.LocalShare(); got != 0 {
		t.Errorf("LocalShare = %v, want 0 for an unlimited ceiling", got)
	}
	for i := 0; i < 10_000; i++ {
		if !l.Allow(1, now) {
			t.Fatalf("unlimited limiter refused at iteration %d", i)
		}
	}
}

// Each instance admits its configured share, so N instances together admit the
// configured total. This is the property that lets the fleet approximate a
// platform-wide ceiling with no shared state.
func TestShardedAggregateStaysWithinConfiguredCeiling(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	const instances = 4
	const configured = 400

	limiters := make([]*ShardedLimiter, instances)
	for i := range limiters {
		limiters[i] = NewShardedLimiter(configured, instances, 1, base)
	}

	// Each instance offers a large number of publishes within one window.
	admitted := 0
	now := base
	for i := 0; i < 10_000; i++ {
		for _, l := range limiters {
			if l.Allow(1, now) {
				admitted++
			}
		}
	}

	if admitted != configured {
		t.Errorf("aggregate admitted %d within one window, want exactly %d", admitted, configured)
	}
}

// The real overshoot is a rollout: a fresh instance starts enforcing the share
// for the new fleet size while an instance that has not yet reloaded is still
// enforcing the larger share for the old one. The aggregate therefore exceeds
// the configured ceiling, and the safety factor is what bounds the overshoot
// ratio.
//
// Four instances each last saw a fleet size of two, so each holds a share sized
// for two instances: with no margin the aggregate reaches twice the configured
// ceiling.
func TestRolloutOvershootIsBoundedBySafetyFactor(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	const configured = 100.0
	const instances = 4
	const staleFleetSize = 2

	admitAll := func(safety float64) int {
		admitted := 0
		for i := 0; i < instances; i++ {
			l := NewShardedLimiter(configured, staleFleetSize, safety, base)
			for j := 0; j < 10_000; j++ {
				if l.Allow(1, base) {
					admitted++
				}
			}
		}
		return admitted
	}

	unmargined := admitAll(1)
	if unmargined <= int(configured) {
		t.Fatalf("with no safety margin the aggregate was %d, expected an overshoot above %d",
			unmargined, int(configured))
	}
	t.Logf("safety 1.0: aggregate %d (configured %.0f)", unmargined, configured)

	// A margin equal to the staleness ratio brings the aggregate back to the
	// configured ceiling: this is the property an operator relies on when
	// choosing the factor.
	compensating := float64(instances) / float64(staleFleetSize)
	if got := admitAll(compensating); got != int(configured) {
		t.Errorf("safety %.1f: aggregate %d, want exactly the configured %d",
			compensating, got, int(configured))
	}

	// A larger margin must strictly reduce throughput during the transient, or
	// the factor is not being applied.
	if got := admitAll(compensating * 2); got >= int(configured) {
		t.Errorf("safety %.1f: aggregate %d did not fall below %d",
			compensating*2, got, int(configured))
	}
}

// A converged fleet admits exactly the configured total, so the safety factor
// costs throughput only where it is needed: during a rollout.
func TestConvergedFleetAdmitsExactlyTheConfiguredTotal(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	const instances = 4
	const configured = 400

	limiters := make([]*ShardedLimiter, instances)
	for i := range limiters {
		limiters[i] = NewShardedLimiter(configured, instances, 1, base)
	}

	admitted := 0
	for i := 0; i < 10_000; i++ {
		for _, l := range limiters {
			if l.Allow(1, base) {
				admitted++
			}
		}
	}
	if admitted != configured {
		t.Errorf("converged aggregate admitted %d, want exactly %d", admitted, configured)
	}
}

// After a scale-out the window resets, so no instance keeps the old allowance
// for a whole window afterwards.
func TestSetInstanceCountResetsTheWindow(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(1000, 2, 1, base)

	for i := 0; i < 500; i++ {
		if !l.Allow(1, base) {
			t.Fatalf("refused at %d before any scale change", i)
		}
	}
	l.SetInstanceCount(2, base)
	if l.Used() != 0 {
		t.Errorf("window used = %d immediately after rebalance, want 0", l.Used())
	}
	if !l.Allow(1, base) {
		t.Error("refused right after rebalance")
	}
}

func TestLimiterWindowRollsOver(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(10, 1, 1, base)

	admitted := 0
	for i := 0; i < 1000; i++ {
		if l.Allow(1, base) {
			admitted++
		}
	}
	if admitted != 10 {
		t.Errorf("first window admitted %d, want 10", admitted)
	}

	// The next second must admit a fresh allowance.
	next := base.Add(time.Second)
	second := 0
	for i := 0; i < 1000; i++ {
		if l.Allow(1, next) {
			second++
		}
	}
	if second != 10 {
		t.Errorf("second window admitted %d, want 10", second)
	}
}

func TestTenantLimiterRefusesAtTheConnectionCeiling(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1, func() time.Time { return base })
	l.Configure("t1", 3, 100, base)

	for i := 0; i < 3; i++ {
		if !l.AllowConnection("t1", base) {
			t.Fatalf("connection %d refused below the ceiling", i)
		}
	}
	if l.AllowConnection("t1", base) {
		t.Error("connection admitted past the ceiling of 3")
	}
	if got := l.LocalConnections("t1"); got != 3 {
		t.Errorf("local connections = %d, want 3", got)
	}
}

// Lowering a ceiling below current usage must refuse new connections without
// ending existing ones: an operator reducing a limit does not mean to disconnect
// everyone already connected.
func TestLoweredConnectionCeilingRefusesNewButKeepsExisting(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1, func() time.Time { return base })
	l.Configure("t1", 5, 100, base)

	for i := 0; i < 5; i++ {
		if !l.AllowConnection("t1", base) {
			t.Fatalf("connection %d refused under the original ceiling", i)
		}
	}

	// Reduce below current usage.
	l.Configure("t1", 2, 100, base)

	if got := l.LocalConnections("t1"); got != 5 {
		t.Errorf("existing connections = %d after lowering the ceiling, want them intact", got)
	}
	if l.AllowConnection("t1", base) {
		t.Error("a new connection was admitted under a ceiling of 2 with 5 already open")
	}
}

func TestTenantLimiterReleasesConnectionSlots(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1, func() time.Time { return base })
	l.Configure("t1", 2, 100, base)

	if !l.AllowConnection("t1", base) || !l.AllowConnection("t1", base) {
		t.Fatal("two connections refused under a ceiling of 2")
	}
	if l.AllowConnection("t1", base) {
		t.Fatal("third connection admitted under a ceiling of 2")
	}

	l.ReleaseConnection("t1")
	if !l.AllowConnection("t1", base) {
		t.Error("a released slot was not reusable")
	}

	// Releasing below zero must not create capacity.
	l.ReleaseConnection("t1")
	l.ReleaseConnection("t1")
	l.ReleaseConnection("t1")
	if got := l.LocalConnections("t1"); got != 0 {
		t.Errorf("connections = %d after over-release, want 0", got)
	}
}

func TestTenantLimiterPublishCeiling(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1, func() time.Time { return base })
	l.Configure("t1", 100, 50, base)

	admitted := 0
	for i := 0; i < 1000; i++ {
		if l.AllowPublish("t1", base) {
			admitted++
		}
	}
	if admitted != 50 {
		t.Errorf("publishes admitted in one window = %d, want 50", admitted)
	}
}

func TestTenantLimiterUnknownTenantIsAdmitted(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1, func() time.Time { return base })

	// Configuration arrival is not the same event as the first connection, so
	// an unconfigured tenant is admitted rather than refused.
	if !l.AllowConnection("nobody", base) {
		t.Error("an unconfigured tenant was refused a connection")
	}
	for i := 0; i < 1000; i++ {
		if !l.AllowPublish("nobody", base) {
			t.Fatalf("an unconfigured tenant was refused publish %d", i)
		}
	}
}

func TestTenantLimiterRebalancesFleetOnInstanceChange(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(2, 1, func() time.Time { return base })
	l.Configure("t1", 100, 400, base)

	if l.byTenant["t1"].publish.InstanceCount() != 2 {
		t.Fatal("limiter did not start at the configured fleet size")
	}
	l.SetInstanceCount(4, base)
	if got := l.byTenant["t1"].publish.InstanceCount(); got != 4 {
		t.Errorf("instance count after rebalance = %d, want 4", got)
	}
	if got := l.byTenant["t1"].publish.LocalShare(); got != 100 {
		t.Errorf("share after rebalance = %v, want 100", got)
	}
}

// --- Transport policy ---

func TestOriginPolicy(t *testing.T) {
	policy := OriginPolicy{
		Allowed:              []string{"https://watch.example.com", "http://localhost:3000"},
		DefaultForNonBrowser: true,
	}

	cases := []struct {
		name   string
		origin string
		want   bool
	}{
		{name: "exact match", origin: "https://watch.example.com", want: true},
		{name: "second entry", origin: "http://localhost:3000", want: true},
		{name: "trailing slash", origin: "https://watch.example.com/", want: true},
		{name: "uppercase host", origin: "https://WATCH.EXAMPLE.COM", want: true},
		{name: "unlisted origin", origin: "https://evil.example.com", want: false},
		{name: "subdomain of an allowed host", origin: "https://sub.watch.example.com", want: false},
		{name: "lookalike host", origin: "https://watch.example.com.evil.test", want: false},
		{name: "absent origin is non-browser", origin: "", want: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "https://gateway/", nil)
			if c.origin != "" {
				r.Header.Set("Origin", c.origin)
			}
			if got := policy.CheckOrigin(r); got != c.want {
				t.Errorf("CheckOrigin(%q) = %v, want %v", c.origin, got, c.want)
			}
		})
	}
}

// A browser page from an unlisted site must be refused even when non-browser
// clients are allowed through, which is what separates the two cases.
func TestOriginPolicyRefusesBrowsersWhenNonBrowserAllowed(t *testing.T) {
	policy := OriginPolicy{Allowed: []string{"https://watch.example.com"}, DefaultForNonBrowser: true}

	r := httptest.NewRequest(http.MethodGet, "https://gateway/", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	if policy.CheckOrigin(r) {
		t.Error("an unlisted browser origin was permitted while non-browser clients are allowed")
	}
}

func TestOriginPolicyWithoutNonBrowserDefault(t *testing.T) {
	policy := OriginPolicy{Allowed: []string{"https://watch.example.com"}, DefaultForNonBrowser: false}
	r := httptest.NewRequest(http.MethodGet, "https://gateway/", nil)
	if policy.CheckOrigin(r) {
		t.Error("a request with no Origin was permitted with the non-browser default off")
	}
}

func TestNetworkPolicyTransport(t *testing.T) {
	t.Run("plaintext refused when disabled", func(t *testing.T) {
		p := NetworkPolicy{PlaintextEnabled: false, TLS: false}
		if err := p.CheckTransport(); !errors.Is(err, ErrPlaintextRefused) {
			t.Errorf("err = %v, want ErrPlaintextRefused", err)
		}
	})
	t.Run("encrypted accepted when plaintext disabled", func(t *testing.T) {
		p := NetworkPolicy{PlaintextEnabled: false, TLS: true}
		if err := p.CheckTransport(); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})
	t.Run("plaintext accepted when enabled", func(t *testing.T) {
		p := NetworkPolicy{PlaintextEnabled: true, TLS: false}
		if err := p.CheckTransport(); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})
}

func TestNetworkPolicyPeerBlocks(t *testing.T) {
	p := NetworkPolicy{
		PlaintextEnabled: true,
		Blocks:           []*net.IPNet{mustCIDR(t, "203.0.113.0/24"), mustCIDR(t, "10.0.0.0/8")},
	}

	blocked := []string{"203.0.113.5:5000", "10.1.2.3:1234"}
	for _, addr := range blocked {
		if err := p.CheckPeer(addr); !errors.Is(err, ErrPeerBlocked) {
			t.Errorf("CheckPeer(%q) = %v, want ErrPeerBlocked", addr, err)
		}
	}

	allowed := []string{"198.51.100.1:443", "192.0.2.7:80", ""}
	for _, addr := range allowed {
		if err := p.CheckPeer(addr); err != nil {
			t.Errorf("CheckPeer(%q) = %v, want nil", addr, err)
		}
	}
}

func TestNetworkPolicyHandlesBareAddress(t *testing.T) {
	p := NetworkPolicy{PlaintextEnabled: true, Blocks: []*net.IPNet{mustCIDR(t, "203.0.113.0/24")}}
	// Some transports report a bare address with no port.
	if err := p.CheckPeer("203.0.113.9"); !errors.Is(err, ErrPeerBlocked) {
		t.Errorf("err = %v, want ErrPeerBlocked", err)
	}
}

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("ParseCIDR(%q): %v", s, err)
	}
	return n
}
