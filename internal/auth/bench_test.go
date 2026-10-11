package auth

import (
	"testing"
	"time"
)

// BenchmarkShardedLimiterAllow measures the hot path: allowing a publish
// within the rate limit.
func BenchmarkShardedLimiterAllow(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	// Use a very high limit so the benchmark doesn't exhaust it
	l := NewShardedLimiter(1_000_000_000, 1, 1.0, now)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !l.Allow(1, now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkShardedLimiterAllowRefused measures the refusal path when the
// limit is exhausted.
func BenchmarkShardedLimiterAllowRefused(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(10, 1, 1.0, now)
	// Exhaust the limit
	for i := 0; i < 10; i++ {
		l.Allow(1, now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = l.Allow(1, now)
	}
}

// BenchmarkShardedLimiterAllowMultiInstance measures the limiter with a
// multi-instance fleet (the common production case).
func BenchmarkShardedLimiterAllowMultiInstance(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(1_000_000_000, 5, 1.0, now) // high limit

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !l.Allow(1, now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkShardedLimiterWindowRollover measures the cost when the time
// window rolls over (resets the counter).
func BenchmarkShardedLimiterWindowRollover(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewShardedLimiter(1000, 1, 1.0, now)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		l.Allow(1, now)
		now = now.Add(time.Second)
	}
}

// BenchmarkTenantLimiterAllowPublish measures the tenant publish allowance.
func BenchmarkTenantLimiterAllowPublish(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	l.Configure("t1", 1_000_000_000, 1_000_000_000, now)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !l.AllowPublish("t1", now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkTenantLimiterAllowPublishMultiTenant measures with multiple tenants.
func BenchmarkTenantLimiterAllowPublishMultiTenant(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	for i := 0; i < 100; i++ {
		l.Configure("t"+string(rune('0'+i)), 1_000_000_000, 1_000_000_000, now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	tenantIdx := 0
	for n := 0; n < b.N; n++ {
		tenant := "t" + string(rune('0'+tenantIdx%100))
		tenantIdx++
		if !l.AllowPublish(tenant, now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkTenantLimiterAllowPublishUnconfigured measures an unconfigured
// tenant (unlimited path).
func BenchmarkTenantLimiterAllowPublishUnconfigured(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	// No Configure call

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !l.AllowPublish("unknown", now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkTenantLimiterAllowConnection measures connection admission.
func BenchmarkTenantLimiterAllowConnection(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	l.Configure("t1", 1_000_000_000, 1000, now)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !l.AllowConnection("t1", now) {
			b.Fatal("unexpected refusal")
		}
	}
}

// BenchmarkTenantLimiterAllowConnectionRefused measures connection refusal
// when ceiling is hit.
func BenchmarkTenantLimiterAllowConnectionRefused(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	l.Configure("t1", 10, 1000, now)
	// Fill up
	for i := 0; i < 10; i++ {
		l.AllowConnection("t1", now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = l.AllowConnection("t1", now)
	}
}

// BenchmarkTenantLimiterReleaseConnection measures connection release.
func BenchmarkTenantLimiterReleaseConnection(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	l.Configure("t1", 10000, 1000, now)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		l.ReleaseConnection("t1")
	}
}

// BenchmarkTenantLimiterSetInstanceCount measures fleet rebalancing.
func BenchmarkTenantLimiterSetInstanceCount(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	for i := 0; i < 100; i++ {
		l.Configure("t"+string(rune('0'+i)), 10000, 1000, now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		l.SetInstanceCount(5, now)
	}
}

// BenchmarkTenantLimiterConcurrent measures concurrent access.
func BenchmarkTenantLimiterConcurrent(b *testing.B) {
	now := time.UnixMilli(1_700_000_000_000)
	l := NewTenantLimiter(1, 1.0, func() time.Time { return now })
	l.Configure("t1", 1_000_000_000, 1_000_000_000, now)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !l.AllowPublish("t1", now) {
				b.Fatal("unexpected refusal")
			}
		}
	})
}