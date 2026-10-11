package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestDialURLDerivesTheSubscriberEndpoint(t *testing.T) {
	cases := []struct {
		publishURL string
		want       string
	}{
		{"http://127.0.0.1:8080/publish", "ws://127.0.0.1:8080/sub"},
		{"https://gateway.example.com/publish", "wss://gateway.example.com/sub"},
	}
	for _, c := range cases {
		got, err := dialURL(c.publishURL)
		if err != nil {
			t.Errorf("dialURL(%q): %v", c.publishURL, err)
			continue
		}
		if got != c.want {
			t.Errorf("dialURL(%q) = %q, want %q", c.publishURL, got, c.want)
		}
	}
	if _, err := dialURL("ftp://nope"); err == nil {
		t.Error("dialURL accepted a scheme it cannot derive a websocket from")
	}
}

// A single channel is the default and must keep the name the operator passed,
// because spreading is an opt-in correction for the per-channel ceiling rather
// than a silent rewrite of the target.
func TestChannelsSpansOnlyWhenAsked(t *testing.T) {
	one := config{channel: "room:1", channelSpan: 1}
	if got := one.channels(); len(got) != 1 || got[0] != "room:1" {
		t.Errorf("channels() = %v, want [room:1]", got)
	}
	many := config{channel: "room", channelSpan: 3}
	got := many.channels()
	if len(got) != 3 || got[0] != "room:0" || got[2] != "room:2" {
		t.Errorf("channels() = %v, want room:0..room:2", got)
	}
}

// The delivery latency is the interval from admission to a subscriber seeing
// the message, so it can only be computed by matching the two.
func TestDeliveryLatencyMatchesOnMessageID(t *testing.T) {
	s := newSubscribers(config{})
	sent := time.Now().Add(-30 * time.Millisecond)
	s.noteSent(7, sent)

	s.recordTextFrame(nil, deliveryFrame("room:1", 7, "x"))

	got := s.snapshot()
	if got.Delivered != 1 {
		t.Fatalf("delivered = %d, want 1", got.Delivered)
	}
	if got.DeliveryP50Ms < 29 || got.DeliveryP50Ms > 200 {
		t.Errorf("delivery p50 = %.1fms, want about 30ms", got.DeliveryP50Ms)
	}
}

// With many subscribers on one channel every message arrives N times. Only the
// first arrival is the delivery latency; counting the rest would report the
// last viewer rather than the delivery.
func TestOnlyTheFirstArrivalCounts(t *testing.T) {
	s := newSubscribers(config{})
	s.noteSent(9, time.Now().Add(-20*time.Millisecond))

	frame := deliveryFrame("room:1", 9, "x")
	for i := 0; i < 5; i++ {
		s.recordTextFrame(nil, frame)
	}

	if got := s.snapshot().Delivered; got != 1 {
		t.Errorf("delivered = %d after five arrivals of one message, want 1", got)
	}
}

// The send-time map is the load generator's only unbounded structure. Deleting
// on match is what keeps it bounded by the in-flight window rather than by the
// length of the run.
func TestSentAtIsReleasedOnMatch(t *testing.T) {
	s := newSubscribers(config{})
	s.noteSent(1, time.Now())
	if _, ok := s.sentAt.Load(uint64(1)); !ok {
		t.Fatal("noteSent did not record the id")
	}
	s.recordTextFrame(nil, deliveryFrame("room:1", 1, ""))
	if _, ok := s.sentAt.Load(uint64(1)); ok {
		t.Error("the id survived its match; the map would grow with the run")
	}
}

// A message this generator never published cannot be measured against, and must
// not be counted as delivered.
func TestUnmatchedDeliveryIsNotCounted(t *testing.T) {
	s := newSubscribers(config{})
	s.recordTextFrame(nil, deliveryFrame("room:1", 999, ""))
	if got := s.snapshot().Delivered; got != 0 {
		t.Errorf("delivered = %d for a message that was never published, want 0", got)
	}
}

func TestFanoutFactorIsZeroWithoutAcceptedMessages(t *testing.T) {
	r := subscriberReport{Delivered: 5}
	if got := r.fanoutFactor(0); got != 0 {
		t.Errorf("fanout factor with no admitted messages = %v, want 0", got)
	}
	if got := r.fanoutFactor(10); got != 0.5 {
		t.Errorf("fanout factor = %v, want 0.5", got)
	}
}

// The verdict has to cover the delivery side. A report where every publish
// passed but nothing was delivered is not a capacity result, and passing it
// would be the one outcome worse than a red report.
func TestDeliveryVerdictFailsWhenNothingArrives(t *testing.T) {
	r := report{
		Accepted:    1000,
		TargetConns: 100,
		Delivery:    &subscriberReport{Ready: 100, Delivered: 0},
	}
	r.checkDelivery()

	if r.Conforms {
		t.Error("a run that admitted 1000 messages and delivered none was judged conforming")
	}
	if len(r.Failures) == 0 {
		t.Error("no failure was named for the undelivered messages")
	}
}

func TestDeliveryVerdictFailsOnShortConnections(t *testing.T) {
	r := report{
		Accepted:    10,
		TargetConns: 100,
		Delivery:    &subscriberReport{Ready: 40, Delivered: 10},
	}
	r.checkDelivery()
	if r.Conforms {
		t.Error("40 of 100 connections was judged conforming")
	}
}

func TestDeliveryVerdictPassesWhenEverythingArrives(t *testing.T) {
	r := report{
		Accepted:    100,
		TargetConns: 10,
		Delivery:    &subscriberReport{Ready: 10, Delivered: 1000, DeliveryP99Ms: 12},
	}
	r.checkDelivery()
	if !r.Conforms {
		t.Errorf("a clean delivery run failed: %v", r.Failures)
	}
}

// A gap notice means a subscriber could not keep up and lost content, which is
// the failure mode the per-connection buffer bound exists to prevent.
func TestDeliveryVerdictFailsOnGapNotices(t *testing.T) {
	r := report{
		Accepted:    100,
		TargetConns: 10,
		Delivery:    &subscriberReport{Ready: 10, Delivered: 900, GapNotices: 1},
	}
	r.checkDelivery()
	if r.Conforms {
		t.Error("a run that lost content to a slow subscriber was judged conforming")
	}
}

// The documented budgets are publish-to-delivery, so they belong on the delivery
// side. Applying them to the admission round trip would judge a fast admission
// path against a number describing a different interval.
func TestDeliveryVerdictUsesTheDocumentedBudgets(t *testing.T) {
	over := report{
		Accepted:    100,
		TargetConns: 10,
		Delivery: &subscriberReport{
			Ready: 10, Delivered: 1000,
			DeliveryP50Ms: 60, DeliveryP95Ms: 120, DeliveryP99Ms: 250,
		},
	}
	over.checkDelivery()
	if over.Conforms {
		t.Error("delivery p99 of 250ms passed a 200ms budget")
	}

	under := report{
		Accepted:    100,
		TargetConns: 10,
		Delivery: &subscriberReport{
			Ready: 10, Delivered: 1000,
			DeliveryP50Ms: 10, DeliveryP95Ms: 20, DeliveryP99Ms: 30,
		},
	}
	under.checkDelivery()
	if !under.Conforms {
		t.Errorf("a fast delivery run failed: %v", under.Failures)
	}
}

// deliveryFrame renders one delivered message the way the service does: a JSON
// object in a newline-delimited text frame, with the identifier as a decimal
// string because it uses 63 bits and a JSON number holds 53.
func deliveryFrame(channel string, id uint64, body string) []byte {
	return []byte(fmt.Sprintf(`{"op":"message","channel":%q,"id":"%d","body":%q}`+"\n", channel, id, body))
}

// A gap notice means the connection lost content, which is the failure mode the
// per-connection buffer bound exists to prevent. It is counted even though it
// carries no message id.
func TestGapNoticeIsCounted(t *testing.T) {
	s := newSubscribers(config{})
	s.recordTextFrame(nil, []byte(`{"op":"gap","dropped":5,"reason":"slow_consumer"}`+"\n"))
	if got := s.snapshot().GapNotices; got != 1 {
		t.Errorf("gap notices = %d, want 1", got)
	}
}

// The service sends several messages in one text frame. Reading only the first
// would understate delivery and make the fanout factor wrong.
func TestOneFrameCarryingSeveralMessagesIsFullyCounted(t *testing.T) {
	s := newSubscribers(config{})
	for i := uint64(1); i <= 3; i++ {
		s.noteSent(i, time.Now().Add(-10*time.Millisecond))
	}
	joined := append(deliveryFrame("room:1", 1, "a"),
		append(deliveryFrame("room:1", 2, "b"), deliveryFrame("room:1", 3, "c")...)...)

	s.recordTextFrame(nil, joined)
	if got := s.snapshot().Delivered; got != 3 {
		t.Errorf("delivered = %d from a frame carrying three messages, want 3", got)
	}
}

// Every message must carry distinct content, or the admission path's dedup
// window suppresses it and the run measures suppression instead of delivery.
//
// Uniqueness is asserted after a JSON round trip, not before it. A body travels
// as a JSON string, and an arbitrary byte sequence is not valid UTF-8: the
// encoder replaces invalid bytes with U+FFFD, which collapsed distinct counters
// onto one string and made ~40% of a run report as duplicates.
func TestMessageBodyIsUniquePerMessage(t *testing.T) {
	g := &generator{cfg: config{bodyBytes: 200}, runPrefix: 1}
	seen := map[string]bool{}
	for i := uint64(1); i <= 1000; i++ {
		h := asPublished(t, g.messageBody(i, 200))
		if seen[h] {
			t.Fatalf("message %d reaches the service identical to an earlier one", i)
		}
		seen[h] = true
	}
}

// A prefix near the current time is the case that broke: the high bits made the
// byte sequence invalid UTF-8 often enough to matter.
func TestMessageBodySurvivesJSONEncoding(t *testing.T) {
	g := &generator{cfg: config{bodyBytes: 200}, runPrefix: uint64(time.Now().UnixNano())}
	seen := map[string]bool{}
	for i := uint64(1); i <= 500; i++ {
		h := asPublished(t, g.messageBody(i, 200))
		if seen[h] {
			t.Fatalf("message %d collapsed onto an earlier body in the JSON encoding", i)
		}
		seen[h] = true
	}
}

// asPublished renders a body the way the service receives it: through the JSON
// request body, where invalid UTF-8 would be replaced.
func asPublished(t *testing.T, body []byte) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"channel": "room:1", "body": string(body)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out.Body
}

// A second run against the same instance replays the first run's bodies, and the
// dedup window the first run left behind suppresses them. The run prefix is what
// keeps a repeat run from measuring that instead of the delivery path.
func TestRunPrefixSeparatesRuns(t *testing.T) {
	first := &generator{cfg: config{bodyBytes: 200}, runPrefix: 1}
	second := &generator{cfg: config{bodyBytes: 200}, runPrefix: 2}
	if asPublished(t, first.messageBody(1, 200)) == asPublished(t, second.messageBody(1, 200)) {
		t.Error("two runs produced the same body; the second would be deduped by the first")
	}
}

// A body too small to hold both the prefix and the counter must still vary per
// message rather than slicing out of range or repeating.
func TestMessageBodyHandlesSmallSizes(t *testing.T) {
	g := &generator{cfg: config{bodyBytes: 4}, runPrefix: 1}
	for _, size := range []int{0, 1, 8, 9, 16, 33, 200} {
		body := g.messageBody(7, size)
		if len(body) != size {
			t.Errorf("messageBody(7, %d) returned %d bytes", size, len(body))
		}
	}
	for _, size := range []int{1, 8, 16} {
		small := &generator{cfg: config{bodyBytes: size}}
		a := asPublished(t, small.messageBody(1, size))
		b := asPublished(t, small.messageBody(2, size))
		if a == b {
			t.Errorf("a %d-byte body did not vary per message; every publish would be deduped", size)
		}
	}
}
