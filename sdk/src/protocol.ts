/**
 * Wire protocol types shared with the gateway.
 *
 * These mirror `internal/protocol` in the Go service. The frame layout is a
 * binary format, but the SDK's own control exchanges — handshake, subscribe,
 * heartbeat — are JSON, because they happen once per connection or per channel
 * change and readability there costs nothing measurable.
 */

/** Frame types carried in the binary channel. */
export const FrameType = {
  MessageBatch: 1,
  GapNotice: 2,
  Control: 3,
} as const;

export type FrameTypeValue = (typeof FrameType)[keyof typeof FrameType];

/** Control operation inside a control frame. */
export const ControlType = {
  Ping: 1,
  Pong: 2,
  Drain: 3,
  SubscribeResult: 4,
} as const;

export type ControlTypeValue = (typeof ControlType)[keyof typeof ControlType];

/** Why content was dropped for this connection. */
export const GapReason = {
  SlowConsumer: 1,
  Degradation: 2,
  Drain: 3,
  Shutdown: 4,
} as const;

export type GapReasonValue = (typeof GapReason)[keyof typeof GapReason];

/** Priority class a message was published at. */
export const Priority = {
  Normal: 0,
  High: 1,
  System: 2,
} as const;

export type PriorityValue = (typeof Priority)[keyof typeof Priority];

/** One message as delivered to a subscriber. */
export interface Message {
  /**
   * Server-assigned identifier.
   *
   * A bigint rather than a number: the service issues 63-bit identifiers and a
   * JavaScript number holds 53 bits exactly, so a numeric id would round. Two
   * distinct messages would then be indistinguishable to any client correlating
   * on it.
   */
  id: bigint;
  /** Server-assigned publish time, unix milliseconds. */
  timeMs: number;
  priority: PriorityValue;
  /** Set when a moderation rule tagged rather than blocked the content. */
  tagged?: boolean;
  tag?: string;
  /** Opaque publisher content, decoded from UTF-8. */
  body: string;
}

/** Report that content was dropped for this connection. */
export interface GapNotice {
  dropped: number;
  reason: GapReasonValue;
  timeMs: number;
}

/** Connection lifecycle state reported to the application. */
export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'authenticating'
  | 'ready'
  | 'reconnecting'
  | 'closing'
  | 'closed'
  | 'failed';

/** Why a connection attempt or termination failed. */
export type ErrorReason =
  | 'authentication'
  | 'origin'
  | 'transport'
  | 'upgrade-timeout'
  | 'server-refused'
  | 'retry-exhausted'
  | 'protocol'
  | 'gap';

/** A connection error, with the reason the SDK can distinguish. */
export interface PushError extends Error {
  reason: ErrorReason;
}