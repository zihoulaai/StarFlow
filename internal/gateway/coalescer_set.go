package gateway

import (
	"sync"

	"github.com/starflow/push/internal/protocol"
)

// CoalescerSet holds one coalescer per channel.
//
// Per-channel rather than global, because a window must not let a busy channel
// delay a quiet one. Closing on per-channel timing keeps latency bounded per
// channel rather than per instance: a flood on one channel cannot push another
// channel's delivery behind it.
type CoalescerSet struct {
	mu        sync.Mutex
	byChannel map[channelKey]*Coalescer

	window        uint64
	mergeRatio    int
	maxBatchBytes int

	// disabled holds channels whose configuration turns coalescing off, so a
	// message for one is delivered on its own rather than merged.
	disabled map[channelKey]bool
}

// NewCoalescerSet returns a set with the supplied defaults.
func NewCoalescerSet(windowMs uint64, mergeRatio, maxBatchBytes int) *CoalescerSet {
	return &CoalescerSet{
		byChannel:     map[channelKey]*Coalescer{},
		disabled:      map[channelKey]bool{},
		window:        windowMs,
		mergeRatio:    mergeRatio,
		maxBatchBytes: maxBatchBytes,
	}
}

// Enqueue adds a message to its channel's coalescer, returning false when
// coalescing is disabled for that channel and the caller should deliver the
// message on its own.
func (s *CoalescerSet) Enqueue(tenantID, channel string, m protocol.Message, nowMs uint64) bool {
	key := channelKey{tenant: tenantID, name: channel}

	s.mu.Lock()
	if s.disabled[key] {
		s.mu.Unlock()
		return false
	}
	c, ok := s.byChannel[key]
	if !ok {
		c = NewCoalescer(tenantID, channel, s.window, s.mergeRatio, s.maxBatchBytes)
		s.byChannel[key] = c
	}
	s.mu.Unlock()

	c.Add(m, nowMs)
	return true
}

// FlushDue closes every window that has run out and delivers its batch.
//
// The coalescers are drained under one lock and the delivery happens outside
// it, so a flush pass over a slow channel does not block enqueue for another.
func (s *CoalescerSet) FlushDue(f *Fanout) int {
	nowMs := uint64(f.now().UnixMilli())

	s.mu.Lock()
	type due struct {
		c *Coalescer
	}
	ready := make([]due, 0, len(s.byChannel))
	for _, c := range s.byChannel {
		if c.Due(nowMs) {
			ready = append(ready, due{c: c})
		}
	}
	s.mu.Unlock()

	delivered := 0
	for _, d := range ready {
		frame := d.c.Take()
		if frame == nil {
			continue
		}
		batch, err := protocol.ParseBatch(frame)
		if err != nil {
			continue
		}
		if _, err := f.deliver(d.c.TenantID(), d.c.ChannelName(), *batch, nowMs); err != nil {
			continue
		}
		delivered++
	}
	return delivered
}

// SetDefaultWindow changes the coalescing window for channels created from now
// on. The degradation ladder calls this as it widens the window at each rung.
func (s *CoalescerSet) SetDefaultWindow(windowMs uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if windowMs == 0 {
		windowMs = 1
	}
	s.window = windowMs
}

// SetCoalescing enables or disables coalescing for a channel.
func (s *CoalescerSet) SetCoalescing(tenantID, channel string, enabled bool) {
	key := channelKey{tenant: tenantID, name: channel}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disabled[key] = !enabled
}

// ApplyPolicy reconfigures a channel's coalescing from a resolved policy.
func (s *CoalescerSet) ApplyPolicy(p ChannelPolicy) {
	key := channelKey{tenant: p.TenantID, name: p.Channel}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.disabled[key] = !p.CoalescingEnabled
	if c, ok := s.byChannel[key]; ok {
		c.SetMergeRatio(p.MergeRatio)
	}
}

// Reset drops all per-channel state, for a test or a full configuration reload.
func (s *CoalescerSet) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byChannel = map[channelKey]*Coalescer{}
}
