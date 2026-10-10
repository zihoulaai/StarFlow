package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/starflow/push/internal/auth"
	"github.com/starflow/push/internal/protocol"
)

// stubResolver authenticates a fixed credential and nothing else.
type stubResolver struct {
	identity auth.Identity
	err      error
	calls    int
	mu       sync.Mutex
}

func (s *stubResolver) Authenticate(ctx context.Context, token string) (auth.Identity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return auth.Identity{}, s.err
	}
	if token != "good-token" {
		return auth.Identity{}, auth.ErrTokenSignature
	}
	return s.identity, nil
}

func (s *stubResolver) AuthenticateSecret(ctx context.Context, appKey, secret string) (auth.Identity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return auth.Identity{}, s.err
	}
	if appKey != "ak" || secret != "good-secret" {
		return auth.Identity{}, auth.ErrCredentialUnknown
	}
	return s.identity, nil
}

type countingMetrics struct {
	mu     sync.Mutex
	opened int
	closed int
}

func (c *countingMetrics) ConnectionOpened(string) { c.mu.Lock(); c.opened++; c.mu.Unlock() }
func (c *countingMetrics) ConnectionClosed(string) { c.mu.Lock(); c.closed++; c.mu.Unlock() }
func (c *countingMetrics) ObserveHeartbeatLost()   {}

func newTestTransport(t *testing.T, r Authenticator, lim ConnLimiter) (*Transport, *Store, *Lifecycle, *countingMetrics) {
	t.Helper()
	store := NewStore()
	shards := NewShardSet(8)
	life := NewLifecycle(store, shards, LifecycleConfig{
		HeartbeatInterval: time.Hour,
		HeartbeatTimeout:  time.Hour,
		DrainGrace:        time.Second,
	}, time.Now)
	m := &countingMetrics{}
	tr := NewTransport(TransportConfig{
		Store: store, Shards: shards, Lifecycle: life, Metrics: m,
		Resolver: r, Limiter: lim,
		Policies:                func(string) map[string]ChannelPolicy { return nil },
		WriteBufferBytes:        16384,
		ReadBufferBytes:         4096,
		PerViewerBytesPerSecond: 2048,
		HeartbeatInterval:       time.Hour,
		HandshakeTimeout:        5 * time.Second,
		WriteTimeout:            5 * time.Second,
	})
	return tr, store, life, m
}

func dialSub(t *testing.T, tr *Transport) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(tr.Handler())
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/sub"
	d := websocket.Dialer{Subprotocols: []string{Subprotocol}}
	c, resp, err := d.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if resp != nil {
		t.Cleanup(func() { _ = resp.Body.Close() })
	}
	return c
}

func sendJSON(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := c.WriteMessage(websocket.TextMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readJSON(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Frames are newline-delimited; the tests read one at a time.
	line := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0])
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return m
}

func subIdentity() auth.Identity {
	return auth.Identity{
		TenantID:   "t1",
		AppKey:     "ak",
		Subject:    "u1",
		Privileges: auth.Privileges(true, true),
	}
}

// A valid credential completes the handshake and subscribes the channels named
// in it, so a client never needs a second round trip to start receiving.
func TestHandshakeSubscribesInOneRoundTrip(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, store, _, metrics := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"room:1"}})

	// subscribed precedes ready: the handshake subscribes before announcing
	// readiness, so a client can be certain its channels are live the moment it
	// sees ready.
	sub := readJSON(t, c)
	if sub["op"] != "subscribed" {
		t.Fatalf("first frame op = %v, want subscribed", sub["op"])
	}
	if op := readJSON(t, c)["op"]; op != "ready" {
		t.Fatalf("second frame op = %v, want ready", op)
	}
	channels, _ := sub["channels"].([]any)
	if len(channels) != 1 || channels[0] != "room:1" {
		t.Fatalf("channels = %v, want [room:1]", channels)
	}
	if n := store.SubscriberCount(); n != 1 {
		t.Fatalf("SubscriberCount = %d, want 1", n)
	}

	// The connection counter must follow the connection, not the subscription.
	waitFor(t, func() bool {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		return metrics.opened == 1
	})
}

// An application secret is a real credential for a browser client, so a
// handshake carrying one must be accepted on the subscriber transport too.
func TestHandshakeAcceptsApplicationSecret(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, _, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "app_key": "ak", "token": "good-secret"})

	if op := readJSON(t, c)["op"]; op != "ready" {
		t.Fatalf("first frame op = %v, want ready", op)
	}
}

// A bad credential is refused with an error frame rather than a silent close, so
// the SDK can report why instead of timing out.
func TestBadCredentialIsRefusedWithAReason(t *testing.T) {
	res := &stubResolver{identity: subIdentity(), err: auth.ErrTokenSignature}
	tr, store, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "nope"})

	m := readJSON(t, c)
	if m["op"] != "error" {
		t.Fatalf("op = %v, want error", m["op"])
	}
	if m["reason"] != "authentication" {
		t.Errorf("reason = %v, want authentication", m["reason"])
	}
	if n := store.SubscriberCount(); n != 0 {
		t.Errorf("a refused connection created %d subscriptions", n)
	}
}

// A credential without the subscribe privilege must not open a subscriber
// connection at all.
func TestSubscribePrivilegeIsRequired(t *testing.T) {
	res := &stubResolver{identity: auth.Identity{
		TenantID: "t1", AppKey: "ak", Subject: "u1",
		Privileges: auth.Privileges(true, false),
	}}
	tr, _, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token"})

	if op := readJSON(t, c)["op"]; op != "error" {
		t.Fatalf("op = %v, want error for a publish-only credential", op)
	}
}

// The tenant connection ceiling is enforced at the handshake, so a client that
// is over its limit is refused rather than accepted and then dropped.
func TestTenantConnectionLimitIsEnforced(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	lim := &fixedLimiter{allow: false}
	tr, _, _, _ := newTestTransport(t, res, lim)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token"})

	if op := readJSON(t, c)["op"]; op != "error" {
		t.Fatalf("op = %v, want error when over the connection limit", op)
	}
}

type fixedLimiter struct {
	allow    bool
	released int
}

func (f *fixedLimiter) AllowConnection(string, time.Time) bool { return f.allow }
func (f *fixedLimiter) ReleaseConnection(string)               { f.released++ }

// A refused channel is reported by name rather than silently dropped, so an
// application can show "room full" instead of a stream that never arrives.
func TestRefusedChannelIsReported(t *testing.T) {
	store := NewStore()
	shards := NewShardSet(4)
	life := NewLifecycle(store, shards, LifecycleConfig{
		HeartbeatInterval: time.Hour, HeartbeatTimeout: time.Hour,
	}, time.Now)
	tr := NewTransport(TransportConfig{
		Store: store, Shards: shards, Lifecycle: life, Metrics: &countingMetrics{},
		Resolver: &stubResolver{identity: subIdentity()},
		// A ceiling of one lets the first subscriber in and refuses the rest, so
		// the refusal path is exercised the way it happens in production rather
		// than through a policy value that means "unlimited".
		Policies: func(string) map[string]ChannelPolicy {
			return map[string]ChannelPolicy{"full": {Channel: "full", MaxSubscribers: 1}}
		},
		WriteBufferBytes: 16384, ReadBufferBytes: 4096,
		PerViewerBytesPerSecond: 2048, HeartbeatInterval: time.Hour,
	})

	// A first connection takes the only slot, so a second one is refused.
	first := dialSub(t, tr)
	sendJSON(t, first, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"full"}})
	_ = readJSON(t, first)
	_ = readJSON(t, first)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token"})
	_ = readJSON(t, c)

	sendJSON(t, c, map[string]any{"op": "subscribe", "channels": []string{"full"}})

	m := readJSON(t, c)
	if m["op"] != "subscribed" {
		t.Fatalf("op = %v, want subscribed", m["op"])
	}
	rejected, _ := m["rejected"].([]any)
	if len(rejected) != 1 {
		t.Fatalf("rejected = %v, want one entry", rejected)
	}
	rm, _ := rejected[0].(map[string]any)
	if rm["channel"] != "full" {
		t.Errorf("channel = %v, want full", rm["channel"])
	}
	if rm["reason"] == nil || rm["reason"] == "" {
		t.Error("a refusal was reported without a reason")
	}
}

// Unsubscribe must remove the channel, or a client that stops listening keeps
// paying egress for content it no longer wants.
func TestUnsubscribeRemovesTheChannel(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, store, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"room:1"}})
	_ = readJSON(t, c)
	_ = readJSON(t, c)

	sendJSON(t, c, map[string]any{"op": "unsubscribe", "channels": []string{"room:1"}})
	waitFor(t, func() bool { return store.SubscriberCount() == 0 })
}

// A client ping must be answered, or the SDK's liveness check would close a
// healthy connection.
func TestPingIsAnswered(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, _, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token"})
	_ = readJSON(t, c)

	sendJSON(t, c, map[string]any{"op": "ping"})
	if op := readJSON(t, c)["op"]; op != "pong" {
		t.Fatalf("op = %v, want pong", op)
	}
}

// A malformed control frame must not tear down a connection whose delivery path
// is still healthy. QoS0 already tolerates loss.
func TestMalformedFrameDoesNotCloseTheConnection(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, store, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"room:1"}})
	_ = readJSON(t, c)
	_ = readJSON(t, c)

	if err := c.WriteMessage(websocket.TextMessage, []byte("{not json")); err != nil {
		t.Fatalf("write: %v", err)
	}
	sendJSON(t, c, map[string]any{"op": "ping"})

	if op := readJSON(t, c)["op"]; op != "pong" {
		t.Fatalf("op = %v, want pong after a malformed frame", op)
	}
	if n := store.SubscriberCount(); n != 1 {
		t.Errorf("SubscriberCount = %d, want 1", n)
	}
}

// An identifier uses 63 bits and a JSON number holds 53 exactly, so it must
// travel as a string. Emitting a number would round every id above 2^53 and two
// distinct messages could look identical to a client.
func TestMessageIDTravelsAsAString(t *testing.T) {
	// A value well above 2^53 that a float64 cannot represent exactly.
	const big = uint64(384393057212039168)
	batch := &protocol.Batch{
		Channel: "room:1",
		Messages: []protocol.Message{
			{ID: big, TimeMs: 1_700_000_000_000, Body: []byte("x")},
		},
	}
	out := encodeBatch(batch)

	var m map[string]any
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("decode: %v", err)
	}

	id, ok := m["id"].(string)
	if !ok {
		t.Fatalf("id is %T, want string; a JSON number cannot hold a 63-bit value exactly", m["id"])
	}
	if id != "384393057212039168" {
		t.Errorf("id = %q, want 384393057212039168", id)
	}
	// Prove the round trip would have lost the value.
	if float64(big) == float64(384393057212039169) {
		t.Skip("platform float64 happens to distinguish these values")
	}
}

func TestGapNoticeBecomesAJSONGap(t *testing.T) {
	frame := protocol.AppendGapNotice(nil, protocol.GapNotice{
		Dropped: 7, Reason: protocol.GapReasonSlowConsumer, TimeMs: 1_700_000_000_000,
	})
	out := decodeFrame(frame)

	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m["op"] != "gap" || m["dropped"].(float64) != 7 {
		t.Errorf("gap = %v, want op gap with dropped 7", m)
	}
}

// A frame this transport cannot parse must produce no output at all rather than
// a malformed payload, so the write is skipped instead of corrupting the stream.
func TestUnparseableFrameDecodesToNothing(t *testing.T) {
	for name, frame := range map[string][]byte{
		"empty":        nil,
		"bad version":  {2, byte(protocol.FrameTypeMessageBatch)},
		"unknown type": {protocol.Version, 99},
	} {
		if out := decodeFrame(frame); out != nil {
			t.Errorf("%s decoded to %q, want nil", name, out)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition was never met")
}

// The write bridge is what the engine's flush loop calls; it must turn a
// coalesced batch into per-message frames without re-encoding per connection.
func TestWriteBridgeDeliversABatch(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, store, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"room:1"}})
	_ = readJSON(t, c)
	_ = readJSON(t, c)

	subs, served := store.LookupSubscribers("t1", "room:1")
	if !served || len(subs) == 0 {
		t.Fatal("no subscriber registered")
	}
	conn := subs[0].Conn()

	frame := protocol.AppendBatch(nil, &protocol.Batch{
		Channel:  "room:1",
		Messages: []protocol.Message{{ID: 1, TimeMs: 1_700_000_000_000, Body: []byte("one")}},
	})
	tr.WriteBridge()(conn, frame)

	m := readJSON(t, c)
	if m["op"] != "message" || m["body"] != "one" {
		t.Errorf("frame = %v, want the delivered message", m)
	}
}

// Store.Connections underpins the admin connection list and must not double
// count a connection that holds several channels.
func TestConnectionsDeduplicatesAcrossChannels(t *testing.T) {
	store := NewStore()
	conn := NewConn(1, ConnConfig{TenantID: "t1", Subject: "u1", WriteBufBytes: 4096, ReadBufBytes: 1024, PerViewerBps: 2048, Shards: NewShardSet(4)})
	for _, ch := range []string{"a", "b", "c"} {
		if _, err := store.Subscribe("t1", ch, conn); err != nil {
			t.Fatalf("subscribe %s: %v", ch, err)
		}
	}
	if n := len(store.Connections()); n != 1 {
		t.Errorf("Connections() = %d, want 1 for one connection on three channels", n)
	}
}

var _ = errors.New
var _ = http.StatusOK

// The engine's flush loop, its heartbeat loop and the connection's own read
// goroutine all write to the same socket concurrently. Gorilla's writer is not
// safe for that, so writes must be serialised per connection — this test is
// what the race detector runs.
func TestConcurrentWritesAreSerialised(t *testing.T) {
	res := &stubResolver{identity: subIdentity()}
	tr, store, _, _ := newTestTransport(t, res, nil)

	c := dialSub(t, tr)
	sendJSON(t, c, map[string]any{"op": "hello", "token": "good-token", "channels": []string{"room:1"}})
	_ = readJSON(t, c)
	_ = readJSON(t, c)

	subs, _ := store.LookupSubscribers("t1", "room:1")
	conn := subs[0].Conn()
	bridge := tr.WriteBridge()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				frame := protocol.AppendBatch(nil, &protocol.Batch{
					Channel:  "room:1",
					Messages: []protocol.Message{{ID: uint64(i*100 + j), TimeMs: 1, Body: []byte("x")}},
				})
				bridge(conn, frame)
			}
		}(i)
	}

	// The client must still be able to read, and every frame must be intact.
	done := make(chan struct{})
	var got int
	go func() {
		defer close(done)
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		for got < 100 {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if strings.Contains(line, `"op":"message"`) {
					var m map[string]any
					if err := json.Unmarshal([]byte(line), &m); err != nil {
						t.Errorf("a delivered frame was corrupt: %q", line)
						return
					}
					got++
				}
			}
		}
	}()

	wg.Wait()
	<-done

	if got != 100 {
		t.Errorf("received %d messages, want 100", got)
	}
}
