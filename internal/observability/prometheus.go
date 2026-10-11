package observability

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Exporter renders metrics in the Prometheus text exposition format.
//
// It renders from a Snapshot rather than holding its own registry of collectors,
// for one reason: the metrics package's counters are the single source of truth,
// and a parallel registry would be a second thing to keep in step with the
// delivery path. Export is an interval operation, so building the text on each
// scrape costs nothing that matters.
type Exporter struct {
	mu       sync.RWMutex
	metrics  *Metrics
	instance string
	// extra carries process-level series an instance wants to publish, such as
	// the degradation level or the connection count.
	extra func() []Sample
}

// Sample is one exported series.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
	// Help is emitted once per metric name.
	Help string
}

// NewExporter returns an exporter over metrics.
func NewExporter(m *Metrics, instance string, extra func() []Sample) *Exporter {
	return &Exporter{metrics: m, instance: instance, extra: extra}
}

// Render returns the exposition text for the current snapshot.
func (e *Exporter) Render() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := e.metrics.Snapshot()
	base := map[string]string{"instance": e.instance}

	var b strings.Builder
	writeMetric(&b, "push_connections_live",
		"Connections currently held by this instance.", base,
		[]Sample{{Value: float64(s.ConnsLive)}})
	writeMetric(&b, "push_publishes_accepted_total",
		"Publishes admitted to the delivery path.", base,
		[]Sample{{Value: float64(s.PublishesAccepted)}})
	writeMetric(&b, "push_publishes_refused_total",
		"Publishes refused by admission.", base,
		[]Sample{{Value: float64(s.PublishesRefused)}})
	writeMetric(&b, "push_deliveries_total",
		"Messages delivered, counted once per recipient.", base,
		[]Sample{{Value: float64(s.DeliveriesOut)}})
	writeMetric(&b, "push_packets_total",
		"Delivered packets.", base,
		[]Sample{{Value: float64(s.PacketsOut)}})
	writeMetric(&b, "push_bytes_total",
		"Bytes written to subscriber sockets.", base,
		[]Sample{{Value: float64(s.BytesOut)}})
	writeMetric(&b, "push_fanout_factor",
		"Delivered messages per accepted publish.", base,
		[]Sample{{Value: s.FanoutFactor}})
	writeMetric(&b, "push_channel_misses_total",
		"Messages for a channel this instance does not serve.", base,
		[]Sample{{Value: float64(s.ChannelMisses)}})
	writeMetric(&b, "push_expired_total",
		"Messages dropped because their requested lifetime had passed.", base,
		[]Sample{{Value: float64(s.ExpiredDropped)}})

	writeMetric(&b, "push_drops_total",
		"Content lost, by cause.", base,
		[]Sample{
			{Labels: map[string]string{"cause": DropSlowConsumer.String()}, Value: float64(s.DropsSlowConsumer)},
			{Labels: map[string]string{"cause": DropRateLimit.String()}, Value: float64(s.DropsRateLimit)},
			{Labels: map[string]string{"cause": DropDegradation.String()}, Value: float64(s.DropsDegradation)},
		})
	writeMetric(&b, "push_drop_rate",
		"Fraction of offered content lost.", base,
		[]Sample{{Value: s.DropRate}})
	writeMetric(&b, "push_slow_subscribers",
		"Subscribers currently classified as stalled.", base,
		[]Sample{{Value: float64(s.SlowSubscribers)}})
	writeMetric(&b, "push_heartbeats_lost_total",
		"Connections closed for failing liveness.", base,
		[]Sample{{Value: float64(s.HeartbeatsLost)}})
	writeMetric(&b, "push_connection_attempts_total",
		"Subscriber handshakes attempted.", base,
		[]Sample{{Value: float64(s.ConnAttempts)}})
	writeMetric(&b, "push_connection_failures_total",
		"Subscriber handshakes refused.", base,
		[]Sample{{Value: float64(s.ConnFailures)}})
	writeMetric(&b, "push_connection_failure_rate",
		"Fraction of subscriber handshakes refused.", base,
		[]Sample{{Value: s.ConnFailureRate}})

	writeMetric(&b, "push_delivery_latency_ms",
		"Publish-to-delivery latency, by quantile.", base,
		[]Sample{
			{Labels: map[string]string{"quantile": "0.50"}, Value: s.LatencyP50Ms},
			{Labels: map[string]string{"quantile": "0.95"}, Value: s.LatencyP95Ms},
			{Labels: map[string]string{"quantile": "0.99"}, Value: s.LatencyP99Ms},
		})

	// Per-tenant series are emitted so a dashboard can be scoped without the
	// scrape having to join against something else.
	for _, id := range e.metrics.TenantIDs() {
		ts, ok := e.metrics.TenantSnapshot(id)
		if !ok {
			continue
		}
		labels := map[string]string{"instance": e.instance, "tenant": id}
		writeMetric(&b, "push_tenant_connections_live",
			"Connections held for a tenant.", labels,
			[]Sample{{Value: float64(ts.ConnsLive)}})
		writeMetric(&b, "push_tenant_publishes_accepted_total",
			"Publishes admitted for a tenant.", labels,
			[]Sample{{Value: float64(ts.PublishesAccepted)}})
		writeMetric(&b, "push_tenant_deliveries_total",
			"Deliveries for a tenant.", labels,
			[]Sample{{Value: float64(ts.DeliveriesOut)}})
		writeMetric(&b, "push_tenant_bytes_total",
			"Bytes written for a tenant.", labels,
			[]Sample{{Value: float64(ts.BytesOut)}})
	}

	if e.extra != nil {
		for _, sample := range e.extra() {
			labels := map[string]string{"instance": e.instance}
			for k, v := range sample.Labels {
				labels[k] = v
			}
			writeMetric(&b, sample.Name, sample.Help, labels, []Sample{{Value: sample.Value}})
		}
	}
	return b.String()
}

func writeMetric(b *strings.Builder, name, help string, labels map[string]string, samples []Sample) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)

	// Samples are sorted by their rendered label set so the output is stable
	// regardless of the order the caller assembled them in. Without this, a
	// scrape can reorder series with no counter having moved, which defeats both
	// diffing and any downstream caching.
	ordered := make([]Sample, len(samples))
	copy(ordered, samples)
	sort.Slice(ordered, func(i, j int) bool {
		return renderLabels(mergeLabels(labels, ordered[i].Labels)) <
			renderLabels(mergeLabels(labels, ordered[j].Labels))
	})

	for _, s := range ordered {
		fmt.Fprintf(b, "%s%s %g\n", name, renderLabels(mergeLabels(labels, s.Labels)), s.Value)
	}
}

func mergeLabels(base, extra map[string]string) map[string]string {
	all := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		all[k] = v
	}
	for k, v := range extra {
		all[k] = v
	}
	return all
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	// Sorted so the output is byte-stable between scrapes; a scrape that
	// reshuffles labels defeats diffing and caching.
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
