package gateway

import (
	"testing"

	"github.com/starflow/push/internal/protocol"
)

// MergeRatio is a target, not a guarantee.
//
// The window closes on elapsed time as well as on reaching the merge target, so
// a channel carrying light traffic emits batches of one message even with
// MergeRatio at 20. That matters for compression, because a one-message batch is
// the case measured to expand.
func TestMergeRatioIsATargetNotAGuarantee(t *testing.T) {
	c := NewCoalescer("t", "room:1", 10, 20, 16*1024)
	c.Add(protocol.Message{ID: 1, Body: []byte("hi")}, 1000)

	if c.Due(1005) {
		t.Error("batch was due before the window elapsed; the merge target cannot have been met")
	}
	if !c.Due(1015) {
		t.Fatal("window expiry did not flush a one-message batch")
	}

	batch, err := protocol.ParseBatch(c.Take())
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(batch.Messages) != 1 {
		t.Fatalf("flushed %d messages, want 1", len(batch.Messages))
	}
	t.Logf("MergeRatio=20 but window expiry flushed a batch of %d message(s)", len(batch.Messages))
}

// The compression decision follows the batch's real size, not the configured
// merge target, and a window-expiry batch must be left uncompressed.
func TestCompressionFollowsActualBatchSize(t *testing.T) {
	c := NewCoalescer("t", "room:1", 10, 20, 16*1024)
	c.Add(protocol.Message{ID: 1, Body: []byte("hi")}, 1000)
	if !c.Due(1015) {
		t.Fatal("window did not flush")
	}
	batch, err := protocol.ParseBatch(c.Take())
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}

	if protocol.ShouldCompress(len(batch.Messages), true) {
		t.Errorf("a %d-message batch would be compressed; that size expands rather than shrinks",
			len(batch.Messages))
	}
	t.Logf("batch of %d left uncompressed despite compression being enabled", len(batch.Messages))
}

// A batch that does reach the threshold must still be compressed, so the
// per-batch check does not accidentally disable the feature that saves 88%.
func TestFullBatchIsStillCompressed(t *testing.T) {
	const total = 20
	c := NewCoalescer("t", "room:1", 10, 20, 16*1024)
	for i := 0; i < total; i++ {
		c.Add(protocol.Message{ID: uint64(i + 1), Body: []byte("danmaku")}, 1000)
	}
	if !c.Due(1000) {
		t.Fatal("merge target was not met")
	}
	batch, err := protocol.ParseBatch(c.Take())
	if err != nil {
		t.Fatalf("ParseBatch: %v", err)
	}
	if len(batch.Messages) != total {
		t.Fatalf("flushed %d messages, want %d", len(batch.Messages), total)
	}
	if !protocol.ShouldCompress(len(batch.Messages), true) {
		t.Errorf("a %d-message batch was left uncompressed; this is the case the saving depends on",
			len(batch.Messages))
	}
}

// Compression disabled must be honoured regardless of batch size.
func TestDisabledCompressionIsHonouredAtEverySize(t *testing.T) {
	for _, n := range []int{1, 4, 5, 20, 1000} {
		if protocol.ShouldCompress(n, false) {
			t.Errorf("ShouldCompress(%d, false) = true", n)
		}
	}
}
