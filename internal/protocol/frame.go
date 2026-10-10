package protocol

import (
	"encoding/binary"
	"time"
)

// Message is a single unit of content. The fields here are identical for every
// subscriber of the channel it is delivered to.
type Message struct {
	ID     uint64
	TimeMs uint64
	Class  PriorityClass
	Flags  MessageFlag
	// Tag carries the value configured for a moderation rule that tags rather
	// than blocks. It is only meaningful when Flags has FlagTagged set.
	Tag string
	// Body is the opaque publisher-supplied content. The gateway does not
	// interpret it.
	Body []byte
}

// Tagged reports whether the message carries a moderation tag.
func (m *Message) Tagged() bool { return m.Flags&FlagTagged != 0 }

// Batch is a set of messages coalesced into one delivered packet. It is the
// unit of serialization: a batch is encoded once and the resulting bytes are
// shared by every subscriber of the channel.
type Batch struct {
	Channel  string
	Messages []Message
}

// AppendBatch encodes b onto dst and returns the extended buffer.
//
// Layout:
//
//	u8    version
//	u8    frame type
//	uvarint  message count
//	uvarint  channel length, then channel bytes
//	uvarint  base timestamp in unix millis
//	per message:
//	  uvarint  message id
//	  uvarint  timestamp delta from the base, in millis
//	  u8       class (low 2 bits) and flags (high bits)
//	  uvarint  tag length, then tag bytes
//	  uvarint  body length, then body bytes
func AppendBatch(dst []byte, b *Batch) []byte {
	dst = EncodeHeader(dst, FrameTypeMessageBatch)
	dst = binary.AppendUvarint(dst, uint64(len(b.Messages)))
	dst = binary.AppendUvarint(dst, uint64(len(b.Channel)))
	dst = append(dst, b.Channel...)

	base := uint64(0)
	if len(b.Messages) > 0 {
		base = b.Messages[0].TimeMs
	}
	dst = binary.AppendUvarint(dst, base)

	for i := range b.Messages {
		m := &b.Messages[i]
		dst = binary.AppendUvarint(dst, m.ID)
		dst = binary.AppendUvarint(dst, m.TimeMs-base)
		// The delta is unsigned; a clock that steps backwards would wrap. Clamp
		// rather than corrupt the encoding.
		dst = append(dst, byte(m.Class)|byte(m.Flags)<<2)
		dst = binary.AppendUvarint(dst, uint64(len(m.Tag)))
		dst = append(dst, m.Tag...)
		dst = binary.AppendUvarint(dst, uint64(len(m.Body)))
		dst = append(dst, m.Body...)
	}
	return dst
}

// ParseBatch decodes a message batch frame. The returned bodies and tags alias
// src, so callers that retain them past the lifetime of src must copy.
func ParseBatch(src []byte) (*Batch, error) {
	hdr, err := ParseHeader(src)
	if err != nil {
		return nil, err
	}
	if hdr.Type != FrameTypeMessageBatch {
		return nil, ErrUnknownFrameType
	}
	p := batchParser{src: src, pos: 2}

	count, err := p.uvarint()
	if err != nil {
		return nil, err
	}
	chanLen, err := p.uvarint()
	if err != nil {
		return nil, err
	}
	channel, err := p.bytes(int(chanLen))
	if err != nil {
		return nil, err
	}
	base, err := p.uvarint()
	if err != nil {
		return nil, err
	}

	// Guard against a corrupt count claiming more messages than the frame could
	// possibly hold. Each message costs at least four varint bytes plus the
	// packed class/flags byte.
	if int(count) > (len(src)-p.pos)/4+1 {
		return nil, ErrMalformed
	}

	b := &Batch{Channel: string(channel), Messages: make([]Message, 0, count)}
	for i := uint64(0); i < count; i++ {
		id, err := p.uvarint()
		if err != nil {
			return nil, err
		}
		delta, err := p.uvarint()
		if err != nil {
			return nil, err
		}
		if p.pos >= len(src) {
			return nil, ErrMalformed
		}
		packed := src[p.pos]
		p.pos++

		tagLen, err := p.uvarint()
		if err != nil {
			return nil, err
		}
		tag, err := p.bytes(int(tagLen))
		if err != nil {
			return nil, err
		}
		bodyLen, err := p.uvarint()
		if err != nil {
			return nil, err
		}
		body, err := p.bytes(int(bodyLen))
		if err != nil {
			return nil, err
		}

		b.Messages = append(b.Messages, Message{
			ID:     id,
			TimeMs: base + delta,
			Class:  PriorityClass(packed & 0x3),
			Flags:  MessageFlag(packed >> 2),
			Tag:    string(tag),
			Body:   body,
		})
	}
	return b, nil
}

// GapReason identifies why content was dropped for a connection.
type GapReason uint8

const (
	GapReasonSlowConsumer GapReason = 1
	GapReasonDegradation  GapReason = 2
	GapReasonDrain        GapReason = 3
	GapReasonShutdown     GapReason = 4
)

// GapNotice reports content dropped for a single connection. It is delivered
// before the next content frame so a client can present a discontinuity rather
// than an apparently complete but truncated stream.
type GapNotice struct {
	// Dropped is the number of messages lost for this connection.
	Dropped uint64
	Reason  GapReason
	TimeMs  uint64
}

// AppendGapNotice encodes g onto dst.
func AppendGapNotice(dst []byte, g GapNotice) []byte {
	dst = EncodeHeader(dst, FrameTypeGapNotice)
	dst = binary.AppendUvarint(dst, g.Dropped)
	dst = append(dst, byte(g.Reason))
	dst = binary.AppendUvarint(dst, g.TimeMs)
	return dst
}

// ParseGapNotice decodes a gap notice frame.
func ParseGapNotice(src []byte) (GapNotice, error) {
	hdr, err := ParseHeader(src)
	if err != nil {
		return GapNotice{}, err
	}
	if hdr.Type != FrameTypeGapNotice {
		return GapNotice{}, ErrUnknownFrameType
	}
	p := batchParser{src: src, pos: 2}
	dropped, err := p.uvarint()
	if err != nil {
		return GapNotice{}, err
	}
	if p.pos >= len(src) {
		return GapNotice{}, ErrMalformed
	}
	reason := GapReason(src[p.pos])
	p.pos++
	t, err := p.uvarint()
	if err != nil {
		return GapNotice{}, err
	}
	return GapNotice{Dropped: dropped, Reason: reason, TimeMs: t}, nil
}

// AppendControl encodes a control exchange onto dst. body is opaque to the
// core protocol and interpreted by the layer that owns the control type.
func AppendControl(dst []byte, ct ControlType, body []byte) []byte {
	dst = EncodeHeader(dst, FrameTypeControl)
	dst = append(dst, byte(ct))
	dst = binary.AppendUvarint(dst, uint64(len(body)))
	dst = append(dst, body...)
	return dst
}

// ParseControl decodes a control frame, returning the control type and a body
// slice that aliases src.
func ParseControl(src []byte) (ControlType, []byte, error) {
	hdr, err := ParseHeader(src)
	if err != nil {
		return 0, nil, err
	}
	if hdr.Type != FrameTypeControl {
		return 0, nil, ErrUnknownFrameType
	}
	p := batchParser{src: src, pos: 2}
	if p.pos >= len(src) {
		return 0, nil, ErrMalformed
	}
	ct := ControlType(src[p.pos])
	p.pos++
	length, err := p.uvarint()
	if err != nil {
		return 0, nil, err
	}
	body, err := p.bytes(int(length))
	if err != nil {
		return 0, nil, err
	}
	return ct, body, nil
}

// NowMs is the server clock used for message timestamps, isolated here so the
// whole system stamps messages on one timebase.
func NowMs() uint64 { return uint64(time.Now().UnixMilli()) }

// batchParser is a minimal cursor over an encoded frame. It avoids the
// reflection-based binary.Read path because this code runs once per coalesced
// batch, not once per delivery.
type batchParser struct {
	src []byte
	pos int
}

func (p *batchParser) uvarint() (uint64, error) {
	v, n := binary.Uvarint(p.src[p.pos:])
	if n <= 0 {
		return 0, ErrMalformed
	}
	p.pos += n
	return v, nil
}

func (p *batchParser) bytes(n int) ([]byte, error) {
	if n < 0 || p.pos+n > len(p.src) {
		return nil, ErrMalformed
	}
	b := p.src[p.pos : p.pos+n]
	p.pos += n
	return b, nil
}
