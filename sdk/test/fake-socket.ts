/**
 * A scriptable WebSocket stand-in.
 *
 * The SDK's behaviour that matters — reconnect timing, jitter spread, resuming
 * subscriptions — is all timing and event ordering, so the tests need to drive
 * both explicitly rather than wait on real network timing.
 */
export class FakeSocket {
  static instances: FakeSocket[] = [];

  /** Set by the client factory so a close can be observed. */
  static lastClose: { code: number; reason: string } | null = null;

  readonly url: string;
  readyState = 0; // CONNECTING
  sent: string[] = [];
  closed = false;
  closeCode = 1000;

  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: ((e: { code: number; reason: string }) => void) | null = null;

  constructor(url: string) {
    this.url = url;
    FakeSocket.instances.push(this);
  }

  /** Completes the upgrade. */
  open(): void {
    this.readyState = 1;
    this.onopen?.();
  }

  /** Delivers a frame from the service. */
  receive(data: unknown): void {
    this.onmessage?.({ data: typeof data === 'string' ? data : JSON.stringify(data) });
  }

  /** Completes the handshake. */
  ready(session = 'session-1'): void {
    this.receive({ op: 'ready', session });
  }

  send(data: string): void {
    this.sent.push(data);
  }

  /** Every frame sent, parsed. */
  frames(): Record<string, unknown>[] {
    return this.sent.map((s) => JSON.parse(s) as Record<string, unknown>);
  }

  /** Frames of a given operation. */
  framesOf(op: string): Record<string, unknown>[] {
    return this.frames().filter((f) => f['op'] === op);
  }

  close(code = 1000, reason = ''): void {
    FakeSocket.lastClose = { code, reason };
    if (this.closed) return;
    this.closed = true;
    this.readyState = 3;
    this.closeCode = code;
    this.onclose?.({ code, reason });
  }

  /** Drops without a close frame, as a proxy would. */
  dropSilently(): void {
    this.closed = true;
    this.readyState = 3;
    // Deliberately no onclose: the client must notice through its liveness
    // probe rather than being told.
  }

  static reset(): void {
    FakeSocket.instances = [];
  }

  static last(): FakeSocket {
    const s = FakeSocket.instances[FakeSocket.instances.length - 1];
    if (!s) throw new Error('no socket was created');
    return s;
  }

  static count(): number {
    return FakeSocket.instances.length;
  }
}