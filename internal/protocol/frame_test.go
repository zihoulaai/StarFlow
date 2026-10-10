package protocol

import (
	"bytes"
	"testing"
	"time"
)

func TestBatchRoundTrip(t *testing.T) {
	want := &Batch{
		Channel: "room:42",
		Messages: []Message{
			{ID: 1001, TimeMs: 1_700_000_000_001, Class: PriorityNormal, Body: []byte("hello")},
			{ID: 1002, TimeMs: 1_700_000_000_002, Class: PriorityHigh, Body: []byte("world")},
			{ID: 1003, TimeMs: 1_700_000_000_003, Class: PrioritySystem, Body: nil},
		},
	}

	encoded := AppendBatch(nil, want)
	got, err := ParseBatch(encoded)
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}

	if got.Channel != want.Channel {
		t.Errorf("channel = %q, want %q", got.Channel, want.Channel)
	}
	if len(got.Messages) != len(want.Messages) {
		t.Fatalf("message count = %d, want %d", len(got.Messages), len(want.Messages))
	}
	for i := range want.Messages {
		w, g := want.Messages[i], got.Messages[i]
		if g.ID != w.ID || g.TimeMs != w.TimeMs || g.Class != w.Class {
			t.Errorf("message %d: got {%d %d %d}, want {%d %d %d}",
				i, g.ID, g.TimeMs, g.Class, w.ID, w.TimeMs, w.Class)
		}
		if !bytes.Equal(g.Body, w.Body) && !(len(g.Body) == 0 && len(w.Body) == 0) {
			t.Errorf("message %d body = %q, want %q", i, g.Body, w.Body)
		}
	}
}

func TestBatchPreservesTaggedFlagAndValue(t *testing.T) {
	want := &Batch{
		Channel: "room:1",
		Messages: []Message{{
			ID: 1, TimeMs: 10, Class: PriorityNormal,
			Flags: FlagTagged, Tag: "rule-7", Body: []byte("x"),
		}},
	}

	got, err := ParseBatch(AppendBatch(nil, want))
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	m := got.Messages[0]
	if !m.Tagged() {
		t.Error("tagged flag not preserved")
	}
	if m.Tag != "rule-7" {
		t.Errorf("tag = %q, want %q", m.Tag, "rule-7")
	}
}

// The whole performance model depends on a frame being byte-identical for every
// recipient, so a batch must encode to the same bytes regardless of which
// subscriber it is destined for. There is nowhere in Message to express a
// recipient, and encoding must be a pure function of the batch.
func TestBatchEncodingIsRecipientIndependent(t *testing.T) {
	batch := &Batch{
		Channel: "room:9",
		Messages: []Message{
			{ID: 5, TimeMs: 100, Class: PriorityNormal, Body: []byte("a")},
			{ID: 6, TimeMs: 101, Class: PriorityNormal, Body: []byte("bb")},
		},
	}

	first := AppendBatch(nil, batch)
	second := AppendBatch(nil, batch)
	if !bytes.Equal(first, second) {
		t.Fatal("encoding the same batch twice produced different bytes")
	}

	// Encoding into a buffer that already holds unrelated bytes must append
	// exactly the same frame, so the fanout path can reuse a scratch buffer.
	scratch := make([]byte, 0, 4096)
	scratch = append(scratch, 0xde, 0xad)
	withPrefix := AppendBatch(scratch, batch)
	if !bytes.Equal(withPrefix[2:], first) {
		t.Fatal("appended frame differs from a standalone encode")
	}
}

func TestGapNoticeRoundTrip(t *testing.T) {
	want := GapNotice{Dropped: 42, Reason: GapReasonSlowConsumer, TimeMs: 1_700_000_000_500}
	got, err := ParseGapNotice(AppendGapNotice(nil, want))
	if err != nil {
		t.Fatalf("ParseGapNotice: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestControlRoundTrip(t *testing.T) {
	body := []byte("subscription-result")
	ct, got, err := ParseControl(AppendControl(nil, ControlSubscribeResult, body))
	if err != nil {
		t.Fatalf("ParseControl: %v", err)
	}
	if ct != ControlSubscribeResult {
		t.Errorf("control type = %d, want %d", ct, ControlSubscribeResult)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body = %q, want %q", got, body)
	}
}

func TestParseRejectsWrongVersion(t *testing.T) {
	encoded := AppendBatch(nil, &Batch{Channel: "c"})
	encoded[0] = Version + 1
	if _, err := ParseBatch(encoded); err != ErrVersionMismatch {
		t.Errorf("err = %v, want %v", err, ErrVersionMismatch)
	}
}

func TestParseRejectsTruncatedFrame(t *testing.T) {
	encoded := AppendBatch(nil, &Batch{
		Channel:  "room:1",
		Messages: []Message{{ID: 1, TimeMs: 1, Body: []byte("payload")}},
	})
	for cut := 1; cut < len(encoded); cut++ {
		if _, err := ParseBatch(encoded[:cut]); err == nil {
			t.Fatalf("truncation at %d parsed without error", cut)
		}
	}
}

func TestParseRejectsTruncatedGapAndControl(t *testing.T) {
	gap := AppendGapNotice(nil, GapNotice{Dropped: 1, Reason: GapReasonDegradation, TimeMs: 2})
	for cut := 1; cut < len(gap); cut++ {
		if _, err := ParseGapNotice(gap[:cut]); err == nil {
			t.Fatalf("gap truncation at %d parsed without error", cut)
		}
	}

	ctl := AppendControl(nil, ControlDrain, []byte("x"))
	for cut := 1; cut < len(ctl); cut++ {
		if _, _, err := ParseControl(ctl[:cut]); err == nil {
			t.Fatalf("control truncation at %d parsed without error", cut)
		}
	}
}

func TestParseRejectsMismatchedFrameType(t *testing.T) {
	encoded := AppendBatch(nil, &Batch{Channel: "c"})
	if _, err := ParseGapNotice(encoded); err != ErrUnknownFrameType {
		t.Errorf("ParseGapNotice on a batch: err = %v, want %v", err, ErrUnknownFrameType)
	}
	if _, _, err := ParseControl(encoded); err != ErrUnknownFrameType {
		t.Errorf("ParseControl on a batch: err = %v, want %v", err, ErrUnknownFrameType)
	}
}

// A corrupt message count must not be trusted into an allocation of arbitrary
// size.
func TestParseRejectsAbsurdMessageCount(t *testing.T) {
	// header + uvarint count = varint(1<<40) + minimal channel
	encoded := EncodeHeader(nil, FrameTypeMessageBatch)
	encoded = append(encoded, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01)
	encoded = append(encoded, 0x00, 0x00, 0x00)
	if _, err := ParseBatch(encoded); err == nil {
		t.Fatal("absurd message count parsed without error")
	}
}

func TestIDGenProducesUniqueMonotonicIDs(t *testing.T) {
	g, err := NewIDGen(7)
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	// Pin the clock so the sequence rollover path is exercised deterministically.
	g.now = func() int64 { return idEpochMs + 100 }

	const n = int(maxSequence) + 500
	prev := uint64(0)
	for i := 0; i < n; i++ {
		id := g.Next()
		if i > 0 && id <= prev {
			t.Fatalf("id %d at index %d is not greater than previous %d", id, i, prev)
		}
		prev = id
	}
}

func TestIDGenRejectsOutOfRangeInstance(t *testing.T) {
	if _, err := NewIDGen(-1); err != ErrInvalidInstanceID {
		t.Errorf("err = %v, want %v", err, ErrInvalidInstanceID)
	}
	if _, err := NewIDGen(maxInstance + 1); err != ErrInvalidInstanceID {
		t.Errorf("err = %v, want %v", err, ErrInvalidInstanceID)
	}
	if _, err := NewIDGen(0); err != nil {
		t.Errorf("NewIDGen(0): %v", err)
	}
}

// A backwards clock step must not produce identifiers that sort before ones
// already issued.
func TestIDGenHoldsMonotonicityAcrossClockStepBack(t *testing.T) {
	g, err := NewIDGen(1)
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	ms := idEpochMs + 1000
	g.now = func() int64 { return ms }

	first := g.Next()
	ms -= 500 // step backwards
	second := g.Next()

	if second <= first {
		t.Fatalf("id after clock step back (%d) is not greater than before (%d)", second, first)
	}
}

func TestTimeOfRecoversGenerationTime(t *testing.T) {
	g, err := NewIDGen(3)
	if err != nil {
		t.Fatalf("NewIDGen: %v", err)
	}
	g.now = func() int64 { return idEpochMs + 1234 }
	id := g.Next()

	got := TimeOf(id)
	want := time.UnixMilli(idEpochMs + 1234)
	if !got.Equal(want) {
		t.Errorf("TimeOf = %v, want %v", got, want)
	}
}
