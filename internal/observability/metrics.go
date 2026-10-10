// Package observability collects the metrics and alerts the service is judged on.
//
// One constraint shapes everything here. The delivery path admits and delivers
// at thousands of messages per second per instance, and it must not allocate and
// must not take a lock that could serialise a fanout. So nothing in this package
// is called synchronously from that path. Counters are plain atomics read at
// export time, the latency histogram is a fixed-bucket sketch updated with one
// atomic add, and everything is scraped on an interval.
package observability

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is the in-process metric set.
//
// Scopes are explicit rather than derived from labels at scrape time: a counter
// per tenant would need a map lookup and a lock on the delivery path to find,
// and a map keyed by tenant that grows with traffic is exactly what the path
// must not touch.
type Metrics struct {
	// Platform totals. These are plain atomics so a read never blocks a write.
	connsLive         atomic.Int64
	publishesAccepted atomic.Uint64
	publishesRefused  atomic.Uint64
	deliveriesOut     atomic.Uint64
	packetsOut        atomic.Uint64
	bytesOut          atomic.Uint64
	channelMisses     atomic.Uint64
	expiredDropped    atomic.Uint64

	// Drops, attributed by cause. A single undifferentiated drop count is
	// useless: a slow subscriber, a rate limit, and a degradation decision are
	// three different incidents with three different responses.
	dropsSlowConsumer atomic.Uint64
	dropsRateLimit    atomic.Uint64
	dropsDegradation  atomic.Uint64

	slowSubscribers atomic.Int64
	heartbeatsLost  atomic.Uint64

	// perTenant holds the counters that must be tenant-scoped. It is written
	// under mu, and only on admission rather than per delivery, which is what
	// keeps the lock off the fanout path.
	mu        sync.Mutex
	perTenant map[string]*TenantCounters

	latency *LatencySketch
	now     func() time.Time
}

// TenantCounters is the tenant-scoped view.
type TenantCounters struct {
	ConnsLive         atomic.Int64
	PublishesAccepted atomic.Uint64
	PublishesRefused  atomic.Uint64
	DeliveriesOut     atomic.Uint64
	BytesOut          atomic.Uint64
}

// NewMetrics returns an empty metric set.
func NewMetrics(now func() time.Time) *Metrics {
	if now == nil {
		now = time.Now
	}
	return &Metrics{
		perTenant: map[string]*TenantCounters{},
		latency:   NewLatencySketch(),
		now:       now,
	}
}

func (m *Metrics) tenant(tenantID string) *TenantCounters {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.perTenant[tenantID]
	if !ok {
		tc = &TenantCounters{}
		m.perTenant[tenantID] = tc
	}
	return tc
}

// ObservePublish records an admitted publish.
func (m *Metrics) ObservePublish(tenantID string) {
	m.publishesAccepted.Add(1)
	m.tenant(tenantID).PublishesAccepted.Add(1)
}

// ObservePublishRefused records a publish refused by admission.
func (m *Metrics) ObservePublishRefused(tenantID string) {
	m.publishesRefused.Add(1)
	m.tenant(tenantID).PublishesRefused.Add(1)
}

// ObserveDelivery records messages and bytes delivered to one subscriber.
//
// delivered is the message count for a single recipient, not the batch size, so
// that summing across a fanout yields true deliveries rather than messages
// mentioned once per packet.
func (m *Metrics) ObserveDelivery(tenantID string, delivered, bytesOut int, packets int) {
	m.deliveriesOut.Add(uint64(delivered))
	m.bytesOut.Add(uint64(bytesOut))
	m.packetsOut.Add(uint64(packets))
	tc := m.tenant(tenantID)
	tc.DeliveriesOut.Add(uint64(delivered))
	tc.BytesOut.Add(uint64(bytesOut))
}

// ObserveLatency records a publish-to-delivery duration.
func (m *Metrics) ObserveLatency(d time.Duration) { m.latency.Observe(d) }

// ObserveDrop records a drop attributed to its cause.
func (m *Metrics) ObserveDrop(cause DropCause, n uint64) {
	switch cause {
	case DropSlowConsumer:
		m.dropsSlowConsumer.Add(n)
	case DropRateLimit:
		m.dropsRateLimit.Add(n)
	case DropDegradation:
		m.dropsDegradation.Add(n)
	}
}

// DropCause is why content was lost.
type DropCause int

const (
	DropSlowConsumer DropCause = iota
	DropRateLimit
	DropDegradation
)

func (c DropCause) String() string {
	switch c {
	case DropSlowConsumer:
		return "slow_consumer"
	case DropRateLimit:
		return "rate_limit"
	case DropDegradation:
		return "degradation"
	default:
		return "unknown"
	}
}

// ConnectionOpened and ConnectionClosed track live connections.
func (m *Metrics) ConnectionOpened(tenantID string) {
	m.connsLive.Add(1)
	m.tenant(tenantID).ConnsLive.Add(1)
}

// ConnectionClosed decrements the live count.
func (m *Metrics) ConnectionClosed(tenantID string) {
	m.connsLive.Add(-1)
	m.tenant(tenantID).ConnsLive.Add(-1)
}

// SetSlowSubscribers publishes the current count.
func (m *Metrics) SetSlowSubscribers(n int) { m.slowSubscribers.Store(int64(n)) }

// ObserveHeartbeatLost records a connection lost to liveness.
func (m *Metrics) ObserveHeartbeatLost() { m.heartbeatsLost.Add(1) }

// ObserveChannelMiss records a message for a channel this instance does not
// serve. Under broadcast this is the common case rather than an error, but the
// ratio to admitted messages is what reveals a subscription table problem.
func (m *Metrics) ObserveChannelMiss() { m.channelMisses.Add(1) }

// ObserveExpired records a message dropped because its requested lifetime had
// passed.
func (m *Metrics) ObserveExpired(n uint64) { m.expiredDropped.Add(n) }

// Snapshot is a consistent-enough read of every counter for export.
//
// It is not atomic across counters, and does not need to be: the values are
// counters scraped on an interval, and a snapshot taken over a few microseconds
// of a system delivering thousands of messages per second is exact enough that
// adding a global lock would be the wrong trade.
type Snapshot struct {
	ConnsLive         int64
	PublishesAccepted uint64
	PublishesRefused  uint64
	DeliveriesOut     uint64
	PacketsOut        uint64
	BytesOut          uint64
	ChannelMisses     uint64
	ExpiredDropped    uint64

	DropsSlowConsumer uint64
	DropsRateLimit    uint64
	DropsDegradation  uint64

	SlowSubscribers int64
	HeartbeatsLost  uint64

	// Latency percentiles, in milliseconds.
	LatencyP50Ms float64
	LatencyP95Ms float64
	LatencyP99Ms float64

	// Derived.
	FanoutFactor float64
	DropRate     float64

	TenantCount int
}

// Snapshot returns the current values.
func (m *Metrics) Snapshot() Snapshot {
	p50, p95, p99 := m.latency.Percentiles()
	s := Snapshot{
		ConnsLive:         m.connsLive.Load(),
		PublishesAccepted: m.publishesAccepted.Load(),
		PublishesRefused:  m.publishesRefused.Load(),
		DeliveriesOut:     m.deliveriesOut.Load(),
		PacketsOut:        m.packetsOut.Load(),
		BytesOut:          m.bytesOut.Load(),
		ChannelMisses:     m.channelMisses.Load(),
		ExpiredDropped:    m.expiredDropped.Load(),
		DropsSlowConsumer: m.dropsSlowConsumer.Load(),
		DropsRateLimit:    m.dropsRateLimit.Load(),
		DropsDegradation:  m.dropsDegradation.Load(),
		SlowSubscribers:   m.slowSubscribers.Load(),
		HeartbeatsLost:    m.heartbeatsLost.Load(),
		LatencyP50Ms:      p50,
		LatencyP95Ms:      p95,
		LatencyP99Ms:      p99,
	}
	if s.PublishesAccepted > 0 {
		s.FanoutFactor = float64(s.DeliveriesOut) / float64(s.PublishesAccepted)
	}
	total := s.DropsSlowConsumer + s.DropsRateLimit + s.DropsDegradation
	if s.PublishesAccepted+total > 0 {
		s.DropRate = float64(total) / float64(s.PublishesAccepted+total)
	}

	m.mu.Lock()
	s.TenantCount = len(m.perTenant)
	m.mu.Unlock()
	return s
}

// TenantSnapshot is a copyable view of one tenant's counters.
//
// The counters are read field by field rather than returned by value, because
// copying an atomic copies its no-copy marker and defeats the point of having
// them atomic.
type TenantSnapshot struct {
	ConnsLive         int64
	PublishesAccepted uint64
	PublishesRefused  uint64
	DeliveriesOut     uint64
	BytesOut          uint64
}

// TenantSnapshot returns one tenant's counters.
func (m *Metrics) TenantSnapshot(tenantID string) (TenantSnapshot, bool) {
	m.mu.Lock()
	tc, ok := m.perTenant[tenantID]
	m.mu.Unlock()
	if !ok {
		return TenantSnapshot{}, false
	}
	return TenantSnapshot{
		ConnsLive:         tc.ConnsLive.Load(),
		PublishesAccepted: tc.PublishesAccepted.Load(),
		PublishesRefused:  tc.PublishesRefused.Load(),
		DeliveriesOut:     tc.DeliveriesOut.Load(),
		BytesOut:          tc.BytesOut.Load(),
	}, true
}

// TenantIDs returns the tenants with recorded counters.
func (m *Metrics) TenantIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.perTenant))
	for id := range m.perTenant {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// LatencySketch is a fixed-bucket histogram for publish-to-delivery latency.
//
// An average would hide exactly the problem this exists to find: the delivery
// budget is stated in percentiles, and a distribution that is healthy on average
// can still be breaching P99 badly. The buckets are logarithmic so a single
// fixed-size array covers microseconds to seconds without reallocating, and an
// observation costs one atomic add rather than a lock.
//
// It is deliberately a sketch and not an exact histogram. Exact per-observation
// storage would mean a growing slice on the delivery path, and the precision
// that buys is not the precision anyone acts on.
type LatencySketch struct {
	boundsMs []float64
	counts   []atomic.Uint64
	overflow atomic.Uint64
	total    atomic.Uint64
	sumMs    atomic.Uint64
}

// NewLatencySketch returns a sketch over the delivery latency range.
func NewLatencySketch() *LatencySketch {
	// Bounds chosen to bracket the stated budget: the P50 target is 50 ms, the
	// P99 target is 200 ms, and the buckets continue well past it so a breach is
	// visible rather than saturating a top bucket.
	bounds := []float64{}
	for v := 0.25; v <= 8; v *= 2 {
		bounds = append(bounds, v)
	}
	for v := 10.0; v <= 100; v += 10 {
		bounds = append(bounds, v)
	}
	for v := 150.0; v <= 1000; v += 50 {
		bounds = append(bounds, v)
	}
	bounds = append(bounds, 2000, 5000)

	s := &LatencySketch{boundsMs: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
	return s
}

// Observe records a duration.
func (s *LatencySketch) Observe(d time.Duration) {
	ms := float64(d.Nanoseconds()) / 1e6
	s.total.Add(1)
	s.sumMs.Add(uint64(ms * 1000))

	idx := sort.SearchFloat64s(s.boundsMs, ms)
	s.counts[idx].Add(1)
}

// Percentiles returns the 50th, 95th and 99th percentile in milliseconds.
//
// The percentile is taken as the bucket's upper bound rather than interpolated.
// That is conservative, which is the right direction for a number whose breach
// triggers a page.
func (s *LatencySketch) Percentiles() (p50, p95, p99 float64) {
	total := s.total.Load()
	if total == 0 {
		return 0, 0, 0
	}

	type bucket struct {
		upper float64
		count uint64
	}
	buckets := make([]bucket, len(s.counts))
	var cumulative uint64
	for i := range s.counts {
		cumulative += s.counts[i].Load()
		upper := s.upperBound(i)
		buckets[i] = bucket{upper: upper, count: cumulative}
	}

	quantile := func(q float64) float64 {
		target := uint64(float64(total) * q)
		for _, b := range buckets {
			if b.count >= target {
				return b.upper
			}
		}
		return buckets[len(buckets)-1].upper
	}
	return quantile(0.50), quantile(0.95), quantile(0.99)
}

func (s *LatencySketch) upperBound(i int) float64 {
	if i < len(s.boundsMs) {
		return s.boundsMs[i]
	}
	return s.boundsMs[len(s.boundsMs)-1] * 2
}

// Count returns the number of observations.
func (s *LatencySketch) Count() uint64 { return s.total.Load() }

// Reset clears the sketch, used between load-test runs.
func (s *LatencySketch) Reset() {
	for i := range s.counts {
		s.counts[i].Store(0)
	}
	s.total.Store(0)
	s.sumMs.Store(0)
}
