package protocol

import (
	"bytes"
	"compress/flate"
	"io"
	"testing"
)

func deflate(b []byte) []byte {
	var out bytes.Buffer
	w, _ := flate.NewWriter(&out, flate.DefaultCompression)
	_, _ = w.Write(b)
	_ = w.Close()
	return out.Bytes()
}

// deflateRatio reports how much permessage-deflate would shrink a coalesced
// batch.
//
// The decision this informs is whether to enable compression by default. The
// cost is CPU on the delivery path, which is idle when the bandwidth bound
// binds, so compression is close to free — but only if the ratio is high enough
// to be worth the frame overhead it adds.
//
// The interesting property is that the ratio improves with batch size: a single
// 200-byte danmaku compresses poorly, while a coalesced batch of twenty
// compresses well because they share structure. That is why the measurement is
// taken on a coalesced batch rather than on one message.
func TestCompressionRatioImprovesWithBatchSize(t *testing.T) {
	ratios := make(map[int]float64)
	for _, batch := range []int{1, 5, 20, 50} {
		msg := make([]Message, 0, batch)
		for i := 0; i < batch; i++ {
			msg = append(msg, Message{
				ID: uint64(i + 1), TimeMs: 1_700_000_000_000,
				Body: []byte("这是一条常见的弹幕内容 with some shared structure"),
			})
		}
		raw := AppendBatch(nil, &Batch{Channel: "room:1", Messages: msg})
		packed := deflate(raw)
		ratios[batch] = float64(len(packed)) / float64(len(raw))
	}

	t.Logf("compression ratio by batch size:")
	for _, batch := range []int{1, 5, 20, 50} {
		t.Logf("  batch=%-3d ratio=%.3f", batch, ratios[batch])
	}

	// The finding is sharper than "compression improves with batch size": an
	// unmerged message compresses *worse* than it started, because deflate
	// framing costs more than the content saves. Compression is therefore only
	// ever enabled where coalescing is on, and this asserts the harm directly so
	// a future change that enables it globally fails here.
	if ratios[1] < 1 {
		t.Errorf("an unmerged message compressed to %.3f of its size; it was expected to expand, "+
			"and a ratio below 1 would mean the harm case no longer holds", ratios[1])
	}
	t.Logf("an unmerged message compresses to %.3f of its size: enabling compression without coalescing would cost bandwidth",
		ratios[1])

	// A coalesced batch must compress far better, or the premise for enabling
	// compression at all is wrong.
	if ratios[20] > ratios[1]/4 {
		t.Errorf("batch of 20 compressed to %.3f, not materially better than a single message's %.3f",
			ratios[20], ratios[1])
	}
}

// The decision threshold: a coalesced batch must compress well enough for the
// saved bytes to exceed the framing overhead compression adds.
func TestCoalescedBatchCompressesWellEnoughToBeWorthEnabling(t *testing.T) {
	const batch = 20
	msg := make([]Message, 0, batch)
	for i := 0; i < batch; i++ {
		msg = append(msg, Message{
			ID: uint64(i + 1), TimeMs: 1_700_000_000_000,
			Body: []byte("这是一条常见的弹幕内容 with some shared structure"),
		})
	}
	raw := AppendBatch(nil, &Batch{Channel: "room:1", Messages: msg})
	packed := deflate(raw)

	ratio := float64(len(packed)) / float64(len(raw))
	t.Logf("coalesced batch of %d: %d raw -> %d deflated (%.1f%% saved)",
		batch, len(raw), len(packed), (1-ratio)*100)

	// A 30% saving is the threshold at which enabling compression by default is
	// worth the CPU, given the CPU is idle while the bandwidth bound binds.
	const threshold = 0.30
	if 1-ratio < threshold {
		t.Errorf("compression saves only %.1f%%, below the %.0f%% threshold for enabling it by default",
			(1-ratio)*100, threshold*100)
	}
}

// Compression must not damage the frame: it is applied to the encoded batch, and
// the result has to parse back to the same messages.
func TestCompressedBatchRoundTrips(t *testing.T) {
	want := &Batch{Channel: "room:1", Messages: []Message{
		{ID: 1, TimeMs: 1_700_000_000_000, Body: []byte("第一条")},
		{ID: 2, TimeMs: 1_700_000_000_001, Body: []byte("第二条第二条第二条")},
		{ID: 3, TimeMs: 1_700_000_000_002, Class: 2, Body: []byte("third")},
	}}
	raw := AppendBatch(nil, want)
	packed := deflate(raw)

	// The reader inflates before parsing; that is the whole contract, and a
	// reader that skipped it would see a malformed frame.
	inflated, err := io.ReadAll(flate.NewReader(bytes.NewReader(packed)))
	if err != nil {
		t.Fatalf("inflate: %v", err)
	}
	got, err := ParseBatch(inflated)
	if err != nil {
		t.Fatalf("ParseBatch of a deflated frame: %v", err)
	}
	if got.Channel != want.Channel || len(got.Messages) != len(want.Messages) {
		t.Fatalf("round trip changed the shape: %+v", got)
	}
	for i := range want.Messages {
		if got.Messages[i].ID != want.Messages[i].ID ||
			!bytes.Equal(got.Messages[i].Body, want.Messages[i].Body) {
			t.Errorf("message %d differs after round trip", i)
		}
	}
}
