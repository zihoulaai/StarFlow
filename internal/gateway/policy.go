package gateway

import (
	"math"

	"github.com/starflow/push/internal/controlplane"
)

// ChannelPolicy is the effective configuration for one channel, with the
// derived values already resolved.
//
// Deriving the publish ceiling from subscriber count rather than configuring it
// is the point. A single fixed QPS ceiling is wrong at both ends of the size
// range: it starves a large channel of headroom it needs and over-admits a
// small channel it cannot physically serve. Deriving it makes the ceiling the
// same quantity as the per-viewer budget, viewed from the publish side, so the
// two cannot drift apart.
type ChannelPolicy struct {
	// Channel and TenantID identify it.
	TenantID string
	Channel  string

	// PublishCeiling is the derived rate at which publishes are refused. Zero
	// means unlimited.
	PublishCeiling float64
	// ReceiveRateTarget is the packets per second each subscriber is expected
	// to receive after coalescing. It is the R in the ceiling formula.
	ReceiveRateTarget float64
	// MergeRatio is how many raw messages are merged per delivered packet.
	MergeRatio int
	// CoalescingEnabled is false for a channel that must deliver each message
	// individually.
	CoalescingEnabled bool
	// MaxSubscribers of zero means the channel imposes no ceiling.
	MaxSubscribers int
	// CeilingDerived records whether the ceiling was computed or overridden, so
	// an operator reading a channel's effective limits can see which it is.
	CeilingDerived bool
}

// DefaultReceiveRateTarget is the per-subscriber packet rate the system is
// designed around. It is the R term in the ceiling formula and, together with
// the per-viewer byte budget, is what the capacity model is derived from.
const DefaultReceiveRateTarget = 5.0

// DefaultMergeRatio matches the instance default from the delivery config.
const DefaultMergeRatio = 20

// ResolveChannelPolicy computes the effective policy for a channel.
//
// The derived ceiling deliberately does not take subscriber count. Every
// subscriber of a channel receives every publish, so a viewer's experience is
// determined by the channel's publish rate alone, and a ceiling that scaled with
// subscriber count would admit a rate no single viewer could absorb.
func ResolveChannelPolicy(cfg controlplane.TenantConfig, channel string, budgetBytesPerSec, nominalMessageBytes int) ChannelPolicy {
	p := ChannelPolicy{
		TenantID:          cfg.TenantID,
		Channel:           channel,
		ReceiveRateTarget: DefaultReceiveRateTarget,
		MergeRatio:        DefaultMergeRatio,
		CoalescingEnabled: true,
		CeilingDerived:    true,
	}

	if override, ok := cfg.Channels[channel]; ok {
		if override.ReceiveRateTarget != nil && *override.ReceiveRateTarget > 0 {
			p.ReceiveRateTarget = *override.ReceiveRateTarget
		}
		if override.MergeRatioOverride != nil && *override.MergeRatioOverride > 0 {
			p.MergeRatio = *override.MergeRatioOverride
		}
		p.CoalescingEnabled = override.CoalescingEnabled
		p.MaxSubscribers = override.MaxSubscribers

		if override.PublishCeilingOverrideQPS != nil && *override.PublishCeilingOverrideQPS >= 0 {
			// An explicit zero is a deliberate "no ceiling", distinct from the
			// NULL that means "derive it".
			p.PublishCeiling = *override.PublishCeilingOverrideQPS
			p.CeilingDerived = false
		}
	}

	if p.CeilingDerived {
		p.PublishCeiling = DerivePublishCeiling(budgetBytesPerSec, nominalMessageBytes,
			p.ReceiveRateTarget, p.MergeRatio)
	}
	return p
}

// DerivePublishCeiling returns the publish rate a channel can sustain without
// any one of its subscribers exceeding their budget.
//
//	bandwidth bound:   budget_bytes_per_second / nominal_message_bytes
//	packet-rate bound: receive_rate_target x merge_ratio
//	ceiling           = floor(min(bound, bound))
//
// Every subscriber receives every publish, so both bounds are per-viewer
// quantities and neither involves subscriber count. A thousand-subscriber
// channel and a hundred-thousand-subscriber channel configured identically admit
// the same publish rate; the larger one consumes proportionally more aggregate
// bandwidth while giving each viewer an identical experience.
//
// At the design point the two bounds sit an order of magnitude apart, which is
// the same conclusion the coalescing analysis reaches: bandwidth binds, and the
// packet-rate side has roughly tenfold headroom.
//
// It returns zero when no budget is configured, which means unlimited: with
// nothing to breach there is no reason to refuse.
func DerivePublishCeiling(budgetBytesPerSec, nominalMessageBytes int, receiveRateTarget float64, mergeRatio int) float64 {
	if budgetBytesPerSec <= 0 || nominalMessageBytes <= 0 {
		return 0
	}
	if receiveRateTarget <= 0 {
		receiveRateTarget = DefaultReceiveRateTarget
	}
	if mergeRatio < 1 {
		// A non-positive merge ratio is read as "no merging configured", which
		// makes the packet-rate bound bind and the ceiling tighter. Erring tight
		// is the safe direction for a limit: an under-merged channel admits more
		// publishes than each packet-rate target allows.
		mergeRatio = 1
	}

	bandwidthBound := float64(budgetBytesPerSec) / float64(nominalMessageBytes)
	packetBound := receiveRateTarget * float64(mergeRatio)
	return math.Floor(math.Min(bandwidthBound, packetBound))
}

// AllowsSubscriber reports whether one more subscriber may join.
//
// The refusal exists to stop a single oversized room from degrading the
// instance. Once a channel's subscriber count is past what one instance can
// serve, adding more makes every existing subscriber slower rather than helping
// anyone, so the correct action is to refuse with a clear reason rather than
// accept and degrade.
func (p ChannelPolicy) AllowsSubscriber(current int) (bool, int) {
	if p.MaxSubscribers <= 0 {
		return true, 0
	}
	if current+1 > p.MaxSubscribers {
		return false, 0
	}
	return true, p.MaxSubscribers - (current + 1)
}
