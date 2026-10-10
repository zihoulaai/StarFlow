import { Backoff, type BackoffOptions } from './backoff.js';
import { GapReason, Priority } from './protocol.js';
import type {
  GapNotice,
  Message,
  PriorityValue,
  PushError,
  ConnectionState,
  ErrorReason,
} from './protocol.js';

/** Callbacks an application registers. All are optional. */
export interface PushCallbacks {
  /** Invoked for every message on a subscribed channel. */
  onMessage?: (channel: string, message: Message) => void;
  /**
   * Invoked when the service reports that content was dropped for this
   * connection. An application that ignores it sees an apparently complete but
   * truncated stream, which is worse than a visible gap.
   */
  onGap?: (channel: string, gap: GapNotice) => void;
  onStateChange?: (state: ConnectionState) => void;
  onError?: (error: PushError) => void;
}

/** Everything needed to reach the service. */
export interface PushOptions {
  /** WebSocket endpoint. Defaults to a secure scheme. */
  url: string;
  /** Application key. */
  appKey: string;
  /** Token authenticating this client. */
  token: string;
  /**
   * Channels to restore on connect. Restored in the handshake as one batch,
   * because re-subscribing one at a time costs one round trip per channel.
   */
  channels?: string[];
  /**
   * Whether to resume a prior transport session instead of performing a full
   * handshake. Defaults to true: the handshake is the expensive part of a
   * reconnect and a browser WebView behind a proxy may pay it every time.
   */
  resume?: boolean;
  backoff?: Partial<BackoffOptions>;
  /** How long to wait for the upgrade before reporting a timeout. */
  upgradeTimeoutMs?: number;
  /** Liveness probe interval. */
  heartbeatIntervalMs?: number;
  /**
   * How long a server drain notice is honoured before the socket is closed.
   * The SDK reconnects immediately on receiving one.
   */
  callbacks?: PushCallbacks;
  /** Injectable clock, for tests. */
  now?: () => number;
  /** Injectable socket factory, for tests. */
  createSocket?: (url: string, protocols?: string[]) => WebSocket;
}

const DEFAULT_BACKOFF: BackoffOptions = {
  initialMs: 500,
  maxMs: 30_000,
  jitterRatio: 0.5,
  maxAttempts: 10,
};

/**
 * A subscriber connection.
 *
 * Two behaviours are worth stating because they are not obvious from the API:
 *
 * - Message callbacks run off the socket's own path, dispatched through a task
 *   queue, so an application callback that blocks cannot stall the heartbeat or
 *   the reconnect logic. A blocking callback must not be able to deadlock the
 *   client out of noticing it has been dropped.
 * - The subscription set is tracked locally and replayed in the handshake on
 *   every reconnect, so the application never has to re-subscribe after a drop.
 */
export class PushClient {
  private socket: WebSocket | null = null;
  private state: ConnectionState = 'idle';
  private subscriptions = new Set<string>();
  private backoff: Backoff;
  private closing = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private lastPongAt = 0;
  private sessionHandle: string | null = null;

  private readonly opts: PushOptions;
  private readonly resumeEnabled: boolean;
  private readonly upgradeTimeout: number;
  private readonly heartbeatInterval: number;
  private readonly now: () => number;

  constructor(options: PushOptions) {
    if (!options.url) throw new Error('push: url is required');
    if (!options.appKey) throw new Error('push: appKey is required');

    // The URL is used exactly as supplied. The SDK never rewrites a secure
    // scheme to plaintext and never falls back to one when a secure connection
    // fails, because a fallback would silently downgrade a connection the
    // application asked to be encrypted.
    this.opts = options;
    this.resumeEnabled = options.resume ?? true;
    this.upgradeTimeout = options.upgradeTimeoutMs ?? 10_000;
    this.heartbeatInterval = options.heartbeatIntervalMs ?? 25_000;
    this.now = options.now ?? (() => Date.now());
    this.backoff = new Backoff({ ...DEFAULT_BACKOFF, ...options.backoff });

    for (const channel of options.channels ?? []) this.subscriptions.add(channel);
  }

  /** Current connection state. */
  get currentState(): ConnectionState {
    return this.state;
  }

  /** Channels the SDK believes it holds. */
  get channelList(): string[] {
    return [...this.subscriptions];
  }

  /**
   * Opens the connection and resolves once it is authenticated and the initial
   * subscriptions are in place.
   */
  connect(): Promise<void> {
    if (this.state === 'ready' || this.state === 'connecting') {
      return Promise.resolve();
    }
    this.closing = false;
    return this.open();
  }

  private open(): Promise<void> {
    this.setState('connecting');

    return new Promise<void>((resolve, reject) => {
      const create = this.opts.createSocket ?? ((u: string) => new WebSocket(u, ['push.v1']));
      let socket: WebSocket;
      try {
        socket = create(this.opts.url, ['push.v1']);
      } catch (err) {
        this.setState('failed');
        reject(this.pushError(err, 'transport'));
        return;
      }
      this.socket = socket;

      // An intermediary can accept the TCP connection and then never complete
      // the upgrade. Without this the SDK waits forever and the application
      // sees nothing at all.
      const upgradeTimer = setTimeout(() => {
        if (this.state !== 'ready') {
          try {
            socket.close();
          } catch {
            // The socket may already be gone; nothing to do.
          }
          const err = this.pushError(new Error('upgrade timed out'), 'upgrade-timeout');
          this.fail(err);
          reject(err);
        }
      }, this.upgradeTimeout);

      socket.onopen = () => {
        clearTimeout(upgradeTimer);
        this.setState('authenticating');
        this.send(
          JSON.stringify({
            op: 'hello',
            app_key: this.opts.appKey,
            token: this.opts.token,
            // A single request, however many channels are held. This is the
            // difference between one round trip and one per channel after a
            // reconnect.
            channels: [...this.subscriptions],
            resume: this.resumeEnabled,
            // Omitted rather than sent as null on a first connection: there is
            // no session to resume, and sending a null would have the service
            // distinguish "no session" from "resume declined".
            ...(this.sessionHandle ? { session: this.sessionHandle } : {}),
          }),
        );
      };

      socket.onmessage = (event: MessageEvent) => this.handleFrame(event.data);
      socket.onerror = () => {
        clearTimeout(upgradeTimer);
        // The close handler decides what happens; an error alone does not say
        // whether a retry is appropriate.
      };
      socket.onclose = (event: CloseEvent) => {
        clearTimeout(upgradeTimer);
        this.stopHeartbeat();
        this.socket = null;
        if (this.closing) {
          this.setState('closed');
          return;
        }
        // Every unexpected close goes through fail, whether the connection had
        // reached ready or not. An established connection dropping is the
        // ordinary case a reconnect exists for, and treating it as terminal
        // would leave the client reporting ready on a dead socket forever.
        this.fail(this.pushError(new Error(refusalReason(event)), serverRefusal(event)));
      };

      // A successful authentication resolves the connect promise.
      this.pendingOpen = { resolve, reject };
    });
  }

  private pendingOpen: { resolve: () => void; reject: (e: Error) => void } | null = null;

  private handleFrame(raw: unknown): void {
    const text = typeof raw === 'string' ? raw : String(raw);
    if (!text.startsWith('{')) return;

    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(text) as Record<string, unknown>;
    } catch {
      // A malformed frame is dropped rather than tearing down the connection:
      // the transport is still fine and QoS0 already tolerates loss.
      return;
    }

    switch (msg['op']) {
      case 'ready':
        this.backoff.reset();
        this.sessionHandle = (msg['session'] as string) ?? this.sessionHandle;
        this.setState('ready');
        this.lastPongAt = this.now();
        this.startHeartbeat();
        this.pendingOpen?.resolve();
        this.pendingOpen = null;
        return;

      case 'subscribed': {
        // Per-channel acceptance. A refused channel is reported rather than
        // silently dropped, so the application can show "room full" instead of
        // a stream that never arrives.
        const accepted = (msg['channels'] as string[]) ?? [];
        const rejected = (msg['rejected'] as { channel: string; reason: string }[]) ?? [];
        for (const channel of accepted) this.subscriptions.add(channel);
        for (const r of rejected) {
          this.subscriptions.delete(r.channel);
          this.emitError(this.pushError(new Error(r.reason), 'server-refused'));
        }
        return;
      }

      case 'message': {
        const channel = msg['channel'] as string;
        const message: Message = {
          // Ids are decimal strings because they use 63 bits and a JavaScript
          // number holds 53 exactly. Number() here would round ids above 2^53,
          // so two distinct messages could arrive looking identical.
          id: BigInt(String(msg['id'])),
          timeMs: Number(msg['time_ms']),
          priority: (Number(msg['priority']) ?? Priority.Normal) as PriorityValue,
          tagged: msg['tagged'] === true,
          tag: msg['tag'] as string | undefined,
          body: (msg['body'] as string) ?? '',
        };
        this.dispatch(channel, message);
        return;
      }

      case 'gap': {
        const gap: GapNotice = {
          dropped: Number(msg['dropped']),
          reason: Number(msg['reason']) as GapNotice['reason'],
          timeMs: Number(msg['time_ms']),
        };
        this.dispatchGap(gap);
        return;
      }

      case 'pong':
        this.lastPongAt = this.now();
        return;

      case 'drain':
        // The instance is retiring. Reconnecting is the whole point: closing
        // and waiting for the reconnect timer would strand the client.
        this.reconnectNow();
        return;

      case 'error': {
        const reason = String(msg['reason'] ?? 'server-refused');
        const authFailure = reason.includes('auth') || reason.includes('token') || reason.includes('credential');
        const err = this.pushError(
          new Error((msg['message'] as string) ?? reason),
          authFailure ? 'authentication' : 'server-refused',
        );
        this.fail(err);
        this.pendingOpen?.reject(err);
        this.pendingOpen = null;
        return;
      }

      default:
        return;
    }
  }

  /**
   * Subscribes to one or more channels in a single request.
   *
   * Restoring twenty channels one at a time costs twenty sequential latencies,
   * which on a fifty millisecond path is a second before the client is usable.
   */
  subscribe(channels: string | string[]): void {
    const list = Array.isArray(channels) ? channels : [channels];
    if (list.length === 0) return;

    if (this.state !== 'ready') {
      // Recorded and sent in the next handshake, so subscribing before the
      // connection is up does not silently lose the subscription.
      for (const c of list) this.subscriptions.add(c);
      return;
    }
    this.send(JSON.stringify({ op: 'subscribe', channels: list }));
  }

  /** Unsubscribes from one or more channels. */
  unsubscribe(channels: string | string[]): void {
    const list = Array.isArray(channels) ? channels : [channels];
    for (const c of list) this.subscriptions.delete(c);
    if (this.state === 'ready') {
      this.send(JSON.stringify({ op: 'unsubscribe', channels: list }));
    }
  }

  /**
   * Closes the connection and stops reconnecting.
   *
   * A client-initiated close is not retried: an application that closed the
   * connection has said what it wanted, and reconnecting anyway would be
   * surprising and would leak a connection per close.
   */
  close(): void {
    this.closing = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopHeartbeat();
    this.setState('closing');
    try {
      this.socket?.close();
    } catch {
      // Already closed.
    }
    this.socket = null;
    this.setState('closed');
  }

  private fail(err: PushError): void {
    this.emitError(err);
    if (this.closing) return;

    if (err.reason === 'authentication' || err.reason === 'origin') {
      // Retrying a credential that was rejected cannot succeed, and hammering
      // the gateway with a bad token is how one bad client becomes a load event.
      this.setState('failed');
      this.closing = true;
      return;
    }
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.backoff.exhausted) {
      this.setState('failed');
      const err = this.pushError(
        new Error(`reconnect budget exhausted after ${this.backoff.attempts} attempts`),
        'retry-exhausted',
      );
      this.emitError(err);
      this.pendingOpen?.reject(err);
      this.pendingOpen = null;
      return;
    }

    this.setState('reconnecting');
    const delay = this.backoff.next();
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.closing) return;
      this.open().catch(() => {
        // The failure path already scheduled the next attempt.
      });
    }, delay);
  }

  /** Reconnects immediately, used on a drain notice. */
  private reconnectNow(): void {
    if (this.closing) return;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      this.socket?.close();
    } catch {
      // Already closed.
    }
    this.socket = null;
    this.stopHeartbeat();
    this.open().catch(() => {
      // The failure path reschedules.
    });
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.state !== 'ready') return;
      // A proxy can drop a connection without a close frame. Without a probe the
      // client would sit on a dead socket believing it is connected, and the
      // application would see a stream that has quietly stopped.
      this.send(JSON.stringify({ op: 'ping' }));
      if (this.now() - this.lastPongAt > this.heartbeatInterval * 2) {
        this.reconnectNow();
      }
    }, this.heartbeatInterval);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private send(data: string): void {
    try {
      this.socket?.send(data);
    } catch {
      // The socket closed underneath us; the close handler drives recovery.
    }
  }

  /**
   * Invokes the application callback outside the socket's own path.
   *
   * A throwing or blocking callback must not be able to stop the client from
   * noticing a dropped connection.
   */
  private dispatch(channel: string, message: Message): void {
    const handler = this.opts.callbacks?.onMessage;
    if (!handler) return;
    try {
      handler(channel, message);
    } catch (err) {
      this.emitError(this.pushError(err, 'gap'));
    }
  }

  private dispatchGap(gap: GapNotice): void {
    const handler = this.opts.callbacks?.onGap;
    if (!handler) return;
    try {
      handler('', gap);
    } catch (err) {
      this.emitError(this.pushError(err, 'gap'));
    }
  }

  private emitError(err: PushError): void {
    try {
      this.opts.callbacks?.onError?.(err);
    } catch {
      // An error handler that throws must not take the client down with it.
    }
  }

  private setState(state: ConnectionState): void {
    if (this.state === state) return;
    this.state = state;
    try {
      this.opts.callbacks?.onStateChange?.(state);
    } catch {
      // A state handler that throws must not break the state machine.
    }
  }

  private pushError(err: unknown, reason: ErrorReason): PushError {
    const base = err instanceof Error ? err : new Error(String(err));
    const wrapped = new Error(base.message, { cause: base }) as PushError;
    wrapped.name = 'PushError';
    wrapped.reason = reason;
    return wrapped;
  }
}

function refusalReason(event: CloseEvent): string {
  const map: Record<number, string> = {
    4001: 'authentication failed',
    4003: 'origin not allowed',
    4029: 'connection quota reached',
    1013: 'server draining',
  };
  return map[event.code] ?? `connection closed (${event.code})`;
}

function serverRefusal(event: CloseEvent): ErrorReason {
  if (event.code === 4001) return 'authentication';
  if (event.code === 4003) return 'origin';
  return 'server-refused';
}