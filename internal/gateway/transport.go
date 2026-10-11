package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/starflow/push/internal/auth"
	"github.com/starflow/push/internal/protocol"
)

// Subprotocol is the value the SDK negotiates.
const Subprotocol = "push.v1"

// authenticator resolves a handshake credential to an identity.
//
// Both forms are accepted because both are real credentials the service issues:
// a signed token for publishers holding signing material, and an application
// secret for a browser client that cannot hold one. The SDK sends whichever it
// was configured with in the same `token` field, so the credential decides the
// form, not the transport.
//
// Both delegate to the same resolver the publish path uses, so a credential
// cannot be valid for one transport and invalid for the other.
type authenticator interface {
	Authenticate(ctx context.Context, token string) (auth.Identity, error)
	AuthenticateSecret(ctx context.Context, appKey, secret string) (auth.Identity, error)
}

// connLimiter gates new connections against a tenant's ceiling. Nil disables the
// check, which is only appropriate for tests.
type connLimiter interface {
	AllowConnection(tenantID string, now time.Time) bool
	ReleaseConnection(tenantID string)
}

// metricsSink is the subset of the metrics registry the transport reports to.
//
// The registry has a much larger surface than the three events here, and the
// transport is on the connection hot path, so naming what it needs keeps it from
// depending on every metric the service collects.
type metricsSink interface {
	ConnectionOpened(tenantID string)
	ConnectionClosed(tenantID string)
	ObserveHeartbeatLost()
}

// TransportConfig holds the subscriber transport's wiring.
type TransportConfig struct {
	Store     *Store
	Shards    *ShardSet
	Lifecycle *Lifecycle
	Metrics   metricsSink
	Resolver  authenticator
	Limiter   connLimiter

	// Policies resolves the per-channel policy for a tenant. A channel with no
	// entry falls back to the engine's own limit.
	Policies func(tenantID string) map[string]ChannelPolicy

	WriteBufferBytes        int
	ReadBufferBytes         int
	PerViewerBytesPerSecond int
	HeartbeatInterval       time.Duration
	HandshakeTimeout        time.Duration
	WriteTimeout            time.Duration

	Now func() time.Time
}

// socket pairs a connection with its transport and owns the write lock.
//
// The lock lives here rather than on Conn because Conn is engine state shared by
// every writer, and a lock there would serialise the whole instance's delivery
// path behind whichever connection wrote slowest.
type socket struct {
	mu sync.Mutex
	ws *websocket.Conn
}

// Transport serves subscriber WebSocket connections on the data-plane listener.
type Transport struct {
	cfg TransportConfig
	up  websocket.Upgrader

	// mu guards the socket map. It is deliberately not held across a write:
	// see writeLock.
	mu      sync.Mutex
	sock    map[*Conn]*socket
	connSeq uint64
}

// NewTransport returns a subscriber transport.
func NewTransport(cfg TransportConfig) *Transport {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Policies == nil {
		cfg.Policies = func(string) map[string]ChannelPolicy { return nil }
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	return &Transport{
		cfg:  cfg,
		sock: make(map[*Conn]*socket),
		up: websocket.Upgrader{
			Subprotocols:      []string{Subprotocol},
			ReadBufferSize:    4096,
			WriteBufferSize:   16384,
			HandshakeTimeout:  cfg.HandshakeTimeout,
			EnableCompression: false,
			CheckOrigin:       func(r *http.Request) bool { return true },
			Error: func(w http.ResponseWriter, r *http.Request, status int, reason error) {
			},
		},
	}
}

// Handler returns the subscriber endpoint.
func (t *Transport) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sub", t.serveSub)
	return mux
}

var (
	errNotHello        = errors.New("gateway: first frame was not hello")
	errConnLimit       = errors.New("gateway: tenant connection limit reached")
	errMissingResolver = errors.New("gateway: transport has no resolver")
)

// serveSub upgrades, authenticates and runs one subscriber connection.
//
// The connection runs on the goroutine that upgraded it. The alternative is a
// reader goroutine plus a writer queue per connection, and the writer queue is
// exactly the structure the engine was built to avoid: every write here is a
// short, bounded socket write made under a per-connection lock.
func (t *Transport) serveSub(w http.ResponseWriter, r *http.Request) {
	if t.cfg.Resolver == nil {
		http.Error(w, errMissingResolver.Error(), http.StatusInternalServerError)
		return
	}

	ws, err := t.up.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	conn, err := t.acceptHello(r, ws)
	if err != nil {
		_ = ws.WriteMessage(websocket.TextMessage, mustJSON(map[string]any{
			"op": "error", "reason": "authentication", "message": err.Error(),
		}))
		_ = ws.Close()
		return
	}

	t.cfg.Lifecycle.Track(conn)
	if t.cfg.Metrics != nil {
		t.cfg.Metrics.ConnectionOpened(conn.TenantID)
	}
	defer func() {
		t.cfg.Lifecycle.Close(conn)
		t.cfg.Lifecycle.Untrack(conn)
		t.cfg.Store.UnsubscribeAll(conn)
		t.forget(conn)
		if t.cfg.Limiter != nil {
			t.cfg.Limiter.ReleaseConnection(conn.TenantID)
		}
		if t.cfg.Metrics != nil {
			t.cfg.Metrics.ConnectionClosed(conn.TenantID)
		}
		_ = ws.Close()
	}()

	t.write(conn, mustJSON(map[string]any{"op": "ready"}))
	t.readLoop(r, ws, conn)
}

// clientFrame is a control frame from the SDK.
//
// The SDK sends JSON for control because those exchanges happen once per
// connection or per channel change, where readability costs nothing measurable.
type clientFrame struct {
	Op string `json:"op"`
	// AppKey and Token carry the credential. Token holds either a signed token
	// or an application secret; AppKey is required only for the secret form.
	AppKey   string   `json:"app_key,omitempty"`
	Token    string   `json:"token,omitempty"`
	Channels []string `json:"channels,omitempty"`
}

// acceptHello authenticates the client and creates its connection.
//
// Authentication happens after the upgrade because the credential is a field in
// the client's first message rather than a header: the SDK sends the token in
// `hello`. The connection is therefore briefly upgraded but unauthenticated,
// which is why the read deadline here is bounded.
func (t *Transport) acceptHello(r *http.Request, ws *websocket.Conn) (*Conn, error) {
	_ = ws.SetReadDeadline(t.cfg.Now().Add(t.cfg.HandshakeTimeout))

	_, data, err := ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	var f clientFrame
	if err := json.Unmarshal(data, &f); err != nil || f.Op != "hello" {
		return nil, errNotHello
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	id, err := t.resolve(ctx, f.AppKey, f.Token)
	if err != nil {
		return nil, err
	}
	if err := auth.Check(id.Privileges, auth.PrivilegeSubscribe); err != nil {
		return nil, err
	}
	if t.cfg.Limiter != nil && !t.cfg.Limiter.AllowConnection(id.TenantID, t.cfg.Now()) {
		return nil, errConnLimit
	}

	t.mu.Lock()
	t.connSeq++
	seq := t.connSeq
	t.mu.Unlock()

	conn := NewConn(seq, ConnConfig{
		TenantID:      id.TenantID,
		Subject:       id.Subject,
		WriteBufBytes: t.cfg.WriteBufferBytes,
		ReadBufBytes:  t.cfg.ReadBufferBytes,
		PerViewerBps:  t.cfg.PerViewerBytesPerSecond,
		Shards:        t.cfg.Shards,
	})

	t.mu.Lock()
	t.sock[conn] = &socket{ws: ws}
	t.mu.Unlock()

	// The handshake asked for channels, so they are subscribed here rather than
	// waiting for a follow-up frame. That removes a round trip from every
	// connection, including every reconnect.
	t.subscribe(conn, f.Channels)
	return conn, nil
}

// resolve authenticates a credential, trying the token form first.
//
// The token is tried first because a value carrying the token structure is
// unambiguous, while an application secret is an opaque string that would only
// fail the token parse.
func (t *Transport) resolve(ctx context.Context, appKey, credential string) (auth.Identity, error) {
	if credential == "" {
		return auth.Identity{}, auth.ErrCredentialUnknown
	}
	id, err := t.cfg.Resolver.Authenticate(ctx, credential)
	if err == nil {
		return id, nil
	}
	if appKey == "" {
		return auth.Identity{}, err
	}
	// Fall back to the application secret the same way the publish path does,
	// so one credential works on both transports.
	return t.cfg.Resolver.AuthenticateSecret(ctx, appKey, credential)
}

// readLoop consumes client frames until the connection ends.
func (t *Transport) readLoop(r *http.Request, ws *websocket.Conn, conn *Conn) {
	timeout := t.readTimeout()
	_ = ws.SetReadDeadline(t.cfg.Now().Add(timeout))

	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		// Refreshed per frame: this deadline is the only thing that reclaims a
		// connection from a peer that stopped talking without closing.
		_ = ws.SetReadDeadline(t.cfg.Now().Add(timeout))

		t.cfg.Lifecycle.Ping(conn)

		var f clientFrame
		if err := json.Unmarshal(data, &f); err != nil {
			// A malformed control frame is dropped rather than closing a
			// connection whose delivery path is still fine, and QoS0 already
			// tolerates loss.
			continue
		}
		switch f.Op {
		case "subscribe":
			t.subscribe(conn, f.Channels)
		case "unsubscribe":
			for _, ch := range f.Channels {
				t.cfg.Store.Unsubscribe(conn.TenantID, ch, conn)
			}
		case "ping":
			t.write(conn, mustJSON(map[string]any{"op": "pong"}))
		}
	}
}

// subscribe adds channels in one batch.
//
// One batch rather than a call per channel is the difference between one round
// trip and one per channel, which on a fifty millisecond path is the difference
// between a usable client and a slow one.
func (t *Transport) subscribe(conn *Conn, channels []string) {
	if len(channels) == 0 {
		return
	}
	results := t.cfg.Store.BatchSubscribe(conn.TenantID, conn, channels, t.cfg.Policies(conn.TenantID))
	t.write(conn, subscribedFrame(results))
}

// write sends one payload to a connection.
//
// The engine's flush and heartbeat loops call this from goroutines other than
// the one reading this socket, so sends are serialised per connection. Gorilla's
// writer is not safe for concurrent use and two writers interleaving would
// corrupt the frame stream.
func (t *Transport) write(conn *Conn, payload []byte) {
	if len(payload) == 0 {
		return
	}
	t.mu.Lock()
	s, ok := t.sock[conn]
	t.mu.Unlock()
	if !ok {
		return
	}

	// Held across the write, not just around the map lookup.
	//
	// The engine's flush loop, its heartbeat loop and this connection's own read
	// goroutine all write here concurrently, and gorilla's writer is not safe
	// for that: two interleaved writes corrupt the frame stream in a way that
	// looks like a client bug. The lock is per connection, so a slow socket
	// cannot block writes to any other subscriber.
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = s.ws.SetWriteDeadline(t.cfg.Now().Add(t.cfg.WriteTimeout))
	if err := s.ws.WriteMessage(websocket.TextMessage, payload); err != nil {
		// A failed write leaves the socket unusable; closing here rather than
		// retrying is what stops a dead socket holding a subscriber slot.
		t.cfg.Lifecycle.Close(conn)
	}
}

// WriteBridge returns the write callback for Fanout.RunFlush and Lifecycle.Run.
//
// The engine encodes once and shares the frame with every local subscriber. It
// arrives here as binary and is decoded to JSON at the socket boundary, because
// decoding earlier would re-encode per connection and undo the property the
// fanout exists to provide.
func (t *Transport) WriteBridge() func(*Conn, []byte) {
	return func(conn *Conn, frame []byte) {
		t.write(conn, decodeFrame(frame))
	}
}

func (t *Transport) forget(conn *Conn) {
	t.mu.Lock()
	delete(t.sock, conn)
	t.mu.Unlock()
}

func (t *Transport) readTimeout() time.Duration {
	if t.cfg.HeartbeatInterval > 0 {
		// Two missed heartbeats is the point at which the connection is
		// unusable, and the read deadline must be the looser of the two so the
		// engine's own liveness check gets to decide, not the transport.
		return t.cfg.HeartbeatInterval*3 + t.cfg.WriteTimeout
	}
	return 90 * time.Second
}

// decodeFrame turns an engine frame into the JSON the SDK parses.
func decodeFrame(frame []byte) []byte {
	if len(frame) < 2 || frame[0] != protocol.Version {
		return nil
	}
	switch protocol.FrameType(frame[1]) {
	case protocol.FrameTypeMessageBatch:
		batch, err := protocol.ParseBatch(frame)
		if err != nil || len(batch.Messages) == 0 {
			return nil
		}
		return encodeBatch(batch)
	case protocol.FrameTypeGapNotice:
		gap, err := protocol.ParseGapNotice(frame)
		if err != nil {
			return nil
		}
		return mustJSON(map[string]any{
			"op": "gap", "dropped": gap.Dropped,
			"reason": gap.Reason, "time_ms": gap.TimeMs,
		})
	default:
		return nil
	}
}

// encodeBatch renders a batch as newline-delimited message frames.
//
// Newline-delimited rather than a single array because the SDK's frame handler
// dispatches one message per parse, and a joined array would need a matching
// client-side change for no gain: the coalescing that made this batch large has
// already happened upstream.
func encodeBatch(batch *protocol.Batch) []byte {
	var out []byte
	for i := range batch.Messages {
		m := &batch.Messages[i]
		// The id is a decimal string, not a JSON number.
		//
		// Identifiers use 63 bits, and a JavaScript number holds at most 53 bits
		// exactly. Emitting one as a number would silently round every id above
		// 2^53, so two distinct messages could reach a client with the same
		// identifier and a client correlating on it would merge them. The SDK
		// parses this field as a string.
		fields := map[string]any{
			"op":       "message",
			"channel":  batch.Channel,
			"id":       strconv.FormatUint(m.ID, 10),
			"time_ms":  m.TimeMs,
			"priority": m.Class,
			"body":     string(m.Body),
		}
		if m.Tagged() {
			fields["tagged"] = true
			fields["tag"] = m.Tag
		}
		out = append(out, mustJSON(fields)...)
		out = append(out, '\n')
	}
	return out
}

func subscribedFrame(results []SubResult) []byte {
	type refused struct {
		Channel string `json:"channel"`
		Reason  string `json:"reason"`
	}
	accepted := AcceptedChannels(results)
	rejected := make([]refused, 0, len(results))
	for _, r := range results {
		if !r.Accepted {
			rejected = append(rejected, refused{Channel: r.Channel, Reason: r.Reason})
		}
	}
	if accepted == nil {
		accepted = []string{}
	}
	return mustJSON(map[string]any{
		"op": "subscribed", "channels": accepted, "rejected": rejected,
	})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}
