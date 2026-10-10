// Package protocol defines the wire format shared by the gateway, the control
// plane and the Web SDK.
//
// The delivery path serializes a message exactly once and memcpy's the
// resulting immutable buffer into every subscriber connection. That only works
// if a frame's bytes are identical for all recipients of a channel, so this
// package deliberately has no per-subscriber variation: no per-recipient
// fields, no subscriber-specific tags, no personalized rendering. Anything that
// would make a frame differ per recipient must be carried out of band.
package protocol

import "errors"

// Version is the wire format version. It is carried in the first byte of every
// frame so a rolling release can reject mismatched clients instead of
// misparsing them.
const Version uint8 = 1

// FrameType identifies the payload shape that follows the two-byte header.
type FrameType uint8

const (
	// FrameTypeMessageBatch carries one or more coalesced messages.
	FrameTypeMessageBatch FrameType = 1
	// FrameTypeGapNotice reports that content was dropped for a connection.
	FrameTypeGapNotice FrameType = 2
	// FrameTypeControl carries a control-plane exchange (liveness, drain
	// notice, subscription acknowledgement).
	FrameTypeControl FrameType = 3
)

// ControlType identifies the operation inside a FrameTypeControl frame.
type ControlType uint8

const (
	ControlPing            ControlType = 1
	ControlPong            ControlType = 2
	ControlDrain           ControlType = 3
	ControlSubscribeResult ControlType = 4
)

// PriorityClass determines how a message behaves when the service is
// degraded. It is part of the message, not a per-tenant setting, because the
// degradation ladder must be able to shed load without changing configuration.
type PriorityClass uint8

const (
	// PriorityNormal is ordinary user content (danmaku). First to be sampled
	// and first to be shed.
	PriorityNormal PriorityClass = 0
	// PriorityHigh is content that must arrive promptly (gifts, reactions,
	// joins) but may still be coalesced.
	PriorityHigh PriorityClass = 1
	// PrioritySystem is control content that must not be coalesced, sampled,
	// or shed (moderation notices, bans, channel announcements).
	PrioritySystem PriorityClass = 2
)

// MessageFlag is a per-message modifier carried in the encoded frame.
type MessageFlag uint8

const (
	// FlagTagged marks a message as matching a moderation rule configured to
	// tag rather than block. The tag value follows the message body.
	FlagTagged MessageFlag = 1 << 0
)

// ErrVersionMismatch is returned when a frame carries a wire version this
// build cannot parse.
var ErrVersionMismatch = errors.New("protocol: wire version mismatch")

// ErrMalformed is returned when a frame is structurally invalid.
var ErrMalformed = errors.New("protocol: malformed frame")

// ErrUnknownFrameType is returned for a frame type this build does not know.
var ErrUnknownFrameType = errors.New("protocol: unknown frame type")

// FrameHeader is the fixed prefix of every encoded frame.
type FrameHeader struct {
	Version uint8
	Type    FrameType
}

// EncodeHeader writes the two-byte frame header onto dst.
func EncodeHeader(dst []byte, t FrameType) []byte {
	return append(dst, Version, byte(t))
}

// ParseHeader reads the frame header from src. src must be at least two bytes.
func ParseHeader(src []byte) (FrameHeader, error) {
	if len(src) < 2 {
		return FrameHeader{}, ErrMalformed
	}
	if src[0] != Version {
		return FrameHeader{}, ErrVersionMismatch
	}
	return FrameHeader{Version: src[0], Type: FrameType(src[1])}, nil
}
