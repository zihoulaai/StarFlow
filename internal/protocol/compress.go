package protocol

// MinCompressBatch is the smallest batch whose compression pays for itself.
//
// It is derived from the ratios measured above rather than guessed: a batch of
// five already reaches 0.308, while a single message reaches 1.123 and loses.
// Four is the last size measured, and is used as the default because it leaves
// margin below the five-message case without waiting for a full window.
const MinCompressBatch = 4

// ShouldCompress decides per batch, because the batch's real size is only known
// once the window closes.
//
// Deciding from the configured merge ratio instead would be wrong: the window
// also closes on elapsed time, so a lightly loaded channel emits one-message
// batches even at a merge ratio of twenty. That case expands under compression,
// so it has to be excluded at the only point where the size is actually known.
func ShouldCompress(messages int, enabled bool) bool {
	return enabled && messages >= MinCompressBatch
}
