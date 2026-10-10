package protocol

import (
	"errors"
	"sync"
	"time"
)

// IDGen produces message identifiers. Identifiers must be unique across the
// service and must be monotonically increasing per generator, because
// AppendBatch delta-encodes timestamps against the first message in a batch and
// clients rely on ordering.
//
// Layout (63 usable bits):
//
//	41 bits  unix milliseconds
//	10 bits  instance id
//	12 bits  per-millisecond sequence
const (
	idEpochMs    int64 = 1_700_000_000_000
	instanceBits       = 10
	sequenceBits       = 12
	maxInstance  int64 = -1 ^ (-1 << instanceBits)
	maxSequence  int64 = -1 ^ (-1 << sequenceBits)
)

// ErrInvalidInstanceID is returned when an instance id does not fit the layout.
var ErrInvalidInstanceID = errors.New("protocol: instance id out of range")

// IDGen is a snowflake-style identifier generator.
type IDGen struct {
	mu       sync.Mutex
	instance int64
	lastMs   int64
	seq      int64
	now      func() int64
}

// NewIDGen returns a generator stamped with instance. Instance ids must be
// unique across concurrently running gateways, otherwise generated identifiers
// can collide.
func NewIDGen(instance int64) (*IDGen, error) {
	if instance < 0 || instance > maxInstance {
		return nil, ErrInvalidInstanceID
	}
	return &IDGen{
		instance: instance,
		now:      func() int64 { return time.Now().UnixMilli() },
	}, nil
}

// Next returns the next identifier.
func (g *IDGen) Next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := g.now()
	if ms < g.lastMs {
		// The clock stepped backwards. Hold the previous millisecond rather
		// than issuing identifiers that sort before ones already handed out.
		ms = g.lastMs
	}
	if ms == g.lastMs {
		g.seq++
		if g.seq > maxSequence {
			// More than 4096 identifiers in one millisecond is beyond any
			// realistic per-instance publish rate; roll into the next
			// millisecond instead of wrapping onto already-used values.
			ms++
			g.lastMs = ms
			g.seq = 0
		}
	} else {
		g.lastMs = ms
		g.seq = 0
	}
	return uint64((ms-idEpochMs)<<(instanceBits+sequenceBits)) |
		uint64(g.instance<<sequenceBits) |
		uint64(g.seq)
}

// TimeOf recovers the generation timestamp encoded in an identifier.
func TimeOf(id uint64) time.Time {
	ms := int64(id>>(instanceBits+sequenceBits)) + idEpochMs
	return time.UnixMilli(ms)
}
