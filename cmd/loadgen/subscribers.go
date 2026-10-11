package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// subscribers holds the delivery side of the measurement.
//
// The publish side alone cannot measure capacity: it says nothing about how many
// connections the instance holds, what it pushes to their sockets, or how long a
// message takes to reach a viewer. Those are the numbers the capacity model in
// design.md is actually derived from, so this half exists to produce them.
type subscribers struct {
	cfg config

	// sentAt maps an admitted message id to the instant its publish response
	// arrived. A probe subscriber that sees the id computes the delivery latency
	// from it, which is the only way to measure the asynchronous fanout path end
	// to end.
	//
	// It is consulted only by probes. If every subscriber consumed it, the map
	// would hold one sample per message rather than one per admitted message,
	// and the figures that matter — deliveries and bytes leaving the instance —
	// would describe one subscriber instead of the fanout.
	sentAt sync.Map

	dialed      atomic.Int64
	established atomic.Int64
	ready       atomic.Int64
	delivered   atomic.Uint64
	bytesIn     atomic.Uint64
	gapNotices  atomic.Uint64
	dialErrors  atomic.Int64
	readErrors  atomic.Int64
	probes      atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration
}

func newSubscribers(cfg config) *subscribers {
	return &subscribers{cfg: cfg}
}

// dialURL derives the subscriber endpoint from the publish endpoint so an
// operator supplies one address rather than two that can drift apart.
//
// The path is replaced rather than appended: the operator gave a publish path,
// and carrying it into the subscriber URL would produce a path no route serves.
func dialURL(publishURL string) (string, error) {
	switch {
	case strings.HasPrefix(publishURL, "https://"):
		return "wss://" + strings.TrimSuffix(strings.TrimPrefix(publishURL, "https://"), "/publish") + "/sub", nil
	case strings.HasPrefix(publishURL, "http://"):
		return "ws://" + strings.TrimSuffix(strings.TrimPrefix(publishURL, "http://"), "/publish") + "/sub", nil
	default:
		return "", fmt.Errorf("cannot derive a subscriber URL from %q", publishURL)
	}
}

// ramp opens the requested number of connections, spread over a window.
//
// Opening them all at once would measure the dial burst rather than the steady
// state: the instance would spend the window accepting handshakes and holding
// every new connection's buffer, which is the opposite of the condition the
// capacity claim is about. The rate is derived from the connection target so a
// 100k run and a 1k run take comparable time to reach steady state.
func (s *subscribers) ramp(ctx context.Context, rampWindow time.Duration) error {
	url, err := dialURL(s.cfg.publishURL)
	if err != nil {
		return err
	}
	dialer := &websocket.Dialer{
		Subprotocols: []string{"push.v1"},
		// The handshake carries the credential in a frame rather than a header,
		// so the read deadline is the service's handshake budget, not the
		// library's generous default.
		HandshakeTimeout: 10 * time.Second,
	}

	total := s.cfg.connections
	tick := time.Millisecond
	if rampWindow > 0 && total > 0 {
		// One connection per interval, with the interval chosen so the last one
		// lands at the end of the window.
		tick = rampWindow / time.Duration(total)
		if tick <= 0 {
			tick = time.Microsecond
		}
	}

	// dialed counts the dialling phase only. The connections themselves live
	// until ctx is cancelled, so waiting on them here would block the run that
	// is supposed to measure them.
	var dialed sync.WaitGroup
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	// The first connections are probes: they are the ones whose arrival times
	// are matched against the publish side to produce a latency distribution.
	// Every connection after them carries the fanout load without contributing a
	// sample, so raising the connection count changes what the instance pushes
	// rather than how the latency is sampled.
	probes := s.cfg.probeConnections
	if probes > total {
		probes = total
	}

	for i := 0; i < total; i++ {
		select {
		case <-ctx.Done():
			dialed.Wait()
			return ctx.Err()
		case <-ticker.C:
		}
		s.dialed.Add(1)
		isProbe := i < probes
		dialed.Add(1)
		go func() {
			defer dialed.Done()
			s.connect(ctx, dialer, url, isProbe)
		}()
	}
	dialed.Wait()
	return nil
}

// connect dials one subscriber, completes the handshake, and leaves the read
// loop running in the background.
func (s *subscribers) connect(ctx context.Context, dialer *websocket.Dialer, url string, isProbe bool) {
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		s.dialErrors.Add(1)
		return
	}
	s.established.Add(1)

	if err := s.handshake(conn); err != nil {
		_ = conn.Close()
		return
	}
	s.ready.Add(1)
	if isProbe {
		s.probes.Add(1)
	}

	// The read deadline is cleared rather than left at the handshake value: a
	// subscriber in a quiet channel is healthy precisely because nothing
	// arrives, and a deadline would close healthy connections mid-run.
	_ = conn.SetReadDeadline(time.Time{})

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	go func() {
		s.readFrames(conn, isProbe)
		_ = conn.Close()
	}()
}

// handshake sends the hello frame and waits for the handshake to complete.
//
// Two frames come back, in this order: `subscribed` reports which of the
// requested channels were accepted, and `ready` marks the connection as
// admitted. Waiting for only one of them would accept a connection the service
// then refuses to route to anything.
func (s *subscribers) handshake(conn *websocket.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	hello, err := json.Marshal(map[string]any{
		"op":       "hello",
		"app_key":  s.cfg.appKey,
		"token":    s.cfg.secret,
		"channels": s.cfg.channels(),
	})
	if err != nil {
		return err
	}
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		return err
	}

	sawSubscribed, sawReady := false, false
	for !sawReady {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var frame struct {
			Op     string   `json:"op"`
			Reject []string `json:"rejected"`
		}
		if err := json.Unmarshal(data, &frame); err != nil {
			continue
		}
		switch frame.Op {
		case "subscribed":
			// A rejected channel means the connection is up but will receive
			// nothing. Counting it as ready would inflate the connection count
			// with connections that carry no traffic.
			if len(frame.Reject) > 0 {
				return fmt.Errorf("%d of the requested channels were refused", len(frame.Reject))
			}
			sawSubscribed = true
		case "ready":
			sawReady = true
		case "error":
			return fmt.Errorf("subscriber handshake refused: %s", data)
		}
	}
	if !sawSubscribed {
		// Not fatal on its own — a client that named no channel is legitimate —
		// but it means this connection measures nothing.
		return errors.New("the service completed the handshake without reporting subscriptions")
	}
	return nil
}

// readFrames consumes delivery frames until the connection closes.
//
// The service encodes deliveries as newline-delimited JSON inside a single text
// frame, one object per message, rather than as the binary batch the engine
// builds internally. The binary form is an internal encoding: what reaches a
// socket has already been rendered for a client to parse, so matching delivery
// against the wire format means reading the JSON the SDK reads.
func (s *subscribers) readFrames(conn *websocket.Conn, isProbe bool) {
	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if kind == websocket.BinaryMessage {
			// A binary frame on this path is an engine frame that reached a
			// client unrendered. Counting it as delivered would report bytes
			// no subscriber could have parsed, so it is ignored deliberately.
			continue
		}
		s.recordTextFrame(conn, data, isProbe)
	}
}

// deliveredMessage is the wire shape of one delivered message or control frame.
type deliveredMessage struct {
	Op      string `json:"op"`
	ID      string `json:"id"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

// recordTextFrame parses a newline-delimited delivery frame.
func (s *subscribers) recordTextFrame(conn *websocket.Conn, data []byte, isProbe bool) {
	now := time.Now()
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var m deliveredMessage
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		switch m.Op {
		case "message":
			s.recordDelivery(m, now, isProbe)
		case "gap":
			s.gapNotices.Add(1)
		case "error":
			s.readErrors.Add(1)
		case "ping":
			// The service closes a connection that fails to answer a liveness
			// probe, so silence here would end the run early rather than merely
			// looking idle.
			_ = conn.WriteMessage(websocket.TextMessage, pongFrame())
		}
	}
}

// recordDelivery accounts for one delivered message.
//
// Every connection counts, and that is the point: deliveries and bytes are what
// the instance actually pushed to its subscribers, and the capacity model is
// derived from them. Only a probe consults sentAt, because only a probe can
// produce a latency sample — the message it matches was published once and
// arrived N times, and the first arrival is the delivery latency.
//
// The identifier arrives as a decimal string: it uses 63 bits and a JSON number
// holds at most 53, so parsing it as a number would silently round the high ids
// and make distinct messages collide.
func (s *subscribers) recordDelivery(m deliveredMessage, now time.Time, isProbe bool) {
	s.delivered.Add(1)
	s.bytesIn.Add(uint64(len(m.Body)))

	if !isProbe {
		return
	}
	id, err := strconv.ParseUint(m.ID, 10, 64)
	if err != nil {
		return
	}
	// LoadAndDelete: probes see every message too, so only the first of them to
	// arrive records a sample. Every later arrival is the same message reaching a
	// different viewer, not a second delivery to the same one.
	sentAt, ok := s.sentAt.LoadAndDelete(id)
	if !ok {
		return
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, now.Sub(sentAt.(time.Time)))
	s.mu.Unlock()
}

// A subscriber that ignores control frames is a subscriber the service will
// eventually reap, so a pong is sent back on the same text path the service
// writes it on.
func pongFrame() []byte {
	return []byte(`{"op":"pong"}`)
}

// reset drops everything measured before the measurement window.
//
// The ramp and the warmup both publish and deliver, and their traffic belongs to
// neither figure: connections are still landing during the ramp, so a delivery
// latency taken from it describes a connection that had just arrived rather than
// one in steady state.
func (s *subscribers) reset() {
	s.delivered.Store(0)
	s.bytesIn.Store(0)
	s.gapNotices.Store(0)
	s.readErrors.Store(0)
	s.mu.Lock()
	s.latencies = nil
	s.mu.Unlock()
	s.sentAt.Clear()
}

// noteSent records when a message was admitted, so a subscriber can measure the
// gap between admission and arrival.
func (s *subscribers) noteSent(id uint64, at time.Time) {
	s.sentAt.Store(id, at)
}

// snapshot copies the delivery-side counters.
func (s *subscribers) snapshot(elapsed time.Duration) subscriberReport {
	s.mu.Lock()
	lat := append([]time.Duration(nil), s.latencies...)
	s.mu.Unlock()

	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	r := subscriberReport{
		TargetConns:   int64(s.cfg.connections),
		Dialed:        s.dialed.Load(),
		Established:   s.established.Load(),
		Ready:         s.ready.Load(),
		Probes:        s.probes.Load(),
		DialErrors:    s.dialErrors.Load(),
		Delivered:     s.delivered.Load(),
		BytesIn:       s.bytesIn.Load(),
		GapNotices:    s.gapNotices.Load(),
		DeliveryP50Ms: percentileMs(lat, 0.50),
		DeliveryP95Ms: percentileMs(lat, 0.95),
		DeliveryP99Ms: percentileMs(lat, 0.99),
		DeliveryMaxMs: maxMs(lat),
		DurationSec:   secs,
	}
	// Egress is the quantity the whole capacity model is derived from: instance
	// count follows from connections times the per-viewer budget, so a run has to
	// report the bytes that actually left rather than the bytes offered.
	r.EgressBytesPerSec = float64(r.BytesIn) / secs
	if r.Ready > 0 {
		r.EgressPerViewerBps = r.EgressBytesPerSec / float64(r.Ready)
	}
	return r
}

func maxMs(d []time.Duration) float64 {
	var m float64
	for _, v := range d {
		if ms := float64(v) / float64(time.Millisecond); ms > m {
			m = ms
		}
	}
	return m
}

// subscriberReport is the delivery half of the measurement.
//
// The fields are the ones design.md derives instance count from: how many
// connections are held, and how many bytes leave for them. A publish-only run
// leaves every one of them at zero.
type subscriberReport struct {
	TargetConns   int64   `json:"target_connections"`
	Dialed        int64   `json:"dialed"`
	Established   int64   `json:"established"`
	Ready         int64   `json:"ready"`
	Probes        int64   `json:"probes"`
	DialErrors    int64   `json:"dial_errors"`
	Delivered     uint64  `json:"delivered"`
	BytesIn       uint64  `json:"bytes_delivered"`
	GapNotices    uint64  `json:"gap_notices"`
	DeliveryP50Ms float64 `json:"delivery_p50_ms"`
	DeliveryP95Ms float64 `json:"delivery_p95_ms"`
	DeliveryP99Ms float64 `json:"delivery_p99_ms"`
	DeliveryMaxMs float64 `json:"delivery_max_ms"`

	DurationSec float64 `json:"duration_seconds"`
	// EgressBytesPerSec is what the instance actually pushed to its
	// subscribers. The capacity model derives instance count from this figure,
	// not from the offered publish rate.
	EgressBytesPerSec float64 `json:"egress_bytes_per_second"`
	// EgressPerViewerBps is that rate divided by the connections held. The
	// documented budget is 2 KB/s per viewer, so a run that holds connections
	// well below the budget per viewer is evidence the budget, not the instance,
	// is the binding constraint.
	EgressPerViewerBps float64 `json:"egress_per_viewer_bytes_per_second"`
}

// print writes the delivery half so it is visible without opening the JSON.
func (r subscriberReport) print(w io.Writer) {
	f := func(format string, args ...any) { fmt.Fprintf(w, format, args...) }
	f("\n--- delivery ---\n")
	f("connections ready   %d of %d target\n", r.Ready, r.TargetConns)
	if r.Established != r.Ready {
		f("  refused at handshake %d\n", r.Established-r.Ready)
	}
	if r.DialErrors != 0 {
		f("  dial failures      %d\n", r.DialErrors)
	}
	f("messages delivered  %d\n", r.Delivered)
	f("bytes delivered     %d\n", r.BytesIn)
	f("egress              %.0f B/s total, %.0f B/s per viewer over %.0fs\n",
		r.EgressBytesPerSec, r.EgressPerViewerBps, r.DurationSec)
	if r.GapNotices != 0 {
		f("gap notices         %d  (content dropped for a slow subscriber)\n", r.GapNotices)
	}
	if r.Delivered > 0 {
		f("delivery p50        %.1f ms\n", r.DeliveryP50Ms)
		f("delivery p95        %.1f ms\n", r.DeliveryP95Ms)
		f("delivery p99        %.1f ms\n", r.DeliveryP99Ms)
		f("delivery max        %.1f ms\n", r.DeliveryMaxMs)
	}
}

// fanoutFactor is deliveries per admitted message. It is zero when no
// subscriber is connected, which is the honest reading rather than a division to
// hide behind.
func (r subscriberReport) fanoutFactor(accepted uint64) float64 {
	if accepted == 0 {
		return 0
	}
	return float64(r.Delivered) / float64(accepted)
}
