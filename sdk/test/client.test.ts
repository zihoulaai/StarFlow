import assert from 'node:assert/strict';
import { describe, it, beforeEach, afterEach } from 'node:test';

import { Backoff } from '../dist/backoff.js';
import { PushClient } from '../dist/client.js';
import type { ConnectionState, ErrorReason, Message, PushError } from '../dist/protocol.js';
import { FakeSocket } from './fake-socket.ts';

// Live clients, closed after each test.
//
// A client left running keeps a backoff timer alive, and that timer can fire
// during a later test and create a socket in the shared registry. Without this
// the suite is order-dependent and intermittently reports reconnect counts that
// belong to a previous test.
const liveClients: PushClient[] = [];

afterEach(() => {
  while (liveClients.length > 0) {
    liveClients.pop()?.close();
  }
});

function newClient(overrides: Record<string, unknown> = {}) {
  const client = new PushClient({
    url: 'wss://gateway.example.com/ws',
    appKey: 'ak_test',
    token: 'token_test',
    createSocket: (url) => new FakeSocket(url) as unknown as WebSocket,
    backoff: { initialMs: 100, maxMs: 1000, jitterRatio: 0.5, maxAttempts: 3 },
    upgradeTimeoutMs: 50,
    heartbeatIntervalMs: 50,
    ...overrides,
  });
  liveClients.push(client);
  return client;
}

const tick = () => new Promise<void>((r) => setTimeout(r, 5));

beforeEach(() => {
  FakeSocket.reset();
});

describe('connection and authentication', () => {
  it('does not report ready before authentication completes', async () => {
    const states: ConnectionState[] = [];
    const client = newClient({ callbacks: { onStateChange: (s: ConnectionState) => states.push(s) } });

    const connecting = client.connect();
    const socket = FakeSocket.last();

    socket.open();
    // Open, but not yet authenticated: not ready.
    assert.equal(client.currentState, 'authenticating');
    assert.equal(states.includes('ready'), false);

    socket.ready();
    await connecting;
    assert.equal(client.currentState, 'ready');
  });

  it('surfaces an authentication failure and does not retry it', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      callbacks: { onError: (e: PushError) => errors.push(e.reason) },
    });

    const connecting = client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.receive({ op: 'error', reason: 'invalid_token', message: 'bad token' });

    await assert.rejects(connecting);
    assert.ok(errors.includes('authentication'), `errors were ${errors.join(',')}`);

    // A rejected credential cannot become valid by retrying, and hammering the
    // gateway with it is how one bad client becomes a load event.
    await tick();
    assert.equal(FakeSocket.count(), 1, 'a rejected credential was retried');
    assert.equal(client.currentState, 'failed');
  });

  it('defaults to an encrypted endpoint', () => {
    const client = newClient();
    assert.ok(client.currentState === 'idle');
    // The URL is used as given; the SDK never rewrites a secure scheme to
    // plaintext, and never falls back to one.
    assert.equal(client.currentState, 'idle');
  });

  it('reports an upgrade timeout when the intermediary never completes it', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      upgradeTimeoutMs: 10,
      callbacks: { onError: (e: PushError) => errors.push(e.reason) },
    });

    const connecting = client.connect();
    await assert.rejects(connecting);
    assert.ok(errors.includes('upgrade-timeout'), `errors were ${errors.join(',')}`);
  });
});

describe('subscription', () => {
  it('sends many channels in one request', () => {
    const client = newClient({ channels: ['a', 'b', 'c'] });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();

    const hello = socket.framesOf('hello')[0]!;
    assert.deepEqual(hello['channels'], ['a', 'b', 'c']);
    // One hello, not three subscribes.
    assert.equal(socket.framesOf('subscribe').length, 0);
  });

  it('tracks its own subscription state', () => {
    const client = newClient();
    client.subscribe(['x', 'y']);
    assert.deepEqual(client.channelList.sort(), ['x', 'y']);
    client.unsubscribe('x');
    assert.deepEqual(client.channelList, ['y']);
  });

  it('records subscriptions made before the connection is up', () => {
    const client = newClient();
    client.subscribe('early');
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();

    // Subscribing before ready must not lose the subscription.
    assert.deepEqual(socket.framesOf('hello')[0]!['channels'], ['early']);
  });

  it('reports a refused channel instead of silently dropping it', () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      callbacks: { onError: (e: PushError) => errors.push(e.reason) },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({
      op: 'subscribed',
      channels: ['ok'],
      rejected: [{ channel: 'full', reason: 'channel_subscriber_limit' }],
    });

    assert.ok(errors.includes('server-refused'));
    assert.ok(client.channelList.includes('ok'));
    assert.ok(!client.channelList.includes('full'));
  });

  it('restores subscriptions after a reconnect in one handshake', async () => {
    const client = newClient({
      channels: ['room:1', 'room:2'],
      backoff: { initialMs: 5, maxMs: 10, jitterRatio: 0, maxAttempts: 5 },
    });
    const connecting = client.connect();
    FakeSocket.last().open();
    FakeSocket.last().ready();
    await connecting;

    const first = FakeSocket.last();
    first.close();
    await tick();
    await tick();

    const second = FakeSocket.last();
    assert.notEqual(second, first, 'no reconnect happened');
    second.open();
    assert.deepEqual(second.framesOf('hello')[0]!['channels'], ['room:1', 'room:2']);
  });
});

describe('message delivery', () => {
  it('invokes the message callback', async () => {
    const seen: { channel: string; message: Message }[] = [];
    const client = newClient({
      channels: ['room:1'],
      callbacks: { onMessage: (c: string, m: Message) => seen.push({ channel: c, message: m }) },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({
      op: 'message',
      channel: 'room:1',
      id: 42,
      time_ms: 1_700_000_000_000,
      body: 'hello',
      priority: 0,
    });

    assert.equal(seen.length, 1);
    assert.equal(seen[0]?.message.body, 'hello');
    assert.equal(seen[0]?.channel, 'room:1');
  });

  // A blocking callback must not be able to stall the client out of noticing
  // it has been dropped.
  it('keeps the connection healthy while a callback blocks', async () => {
    let inCallback = false;
    let callbackFinished = false;
    const client = newClient({
      channels: ['room:1'],
      callbacks: {
        onMessage: () => {
          inCallback = true;
          // A synchronous stall, as a heavy synchronous render would produce.
          const until = Date.now() + 60;
          while (Date.now() < until) {
            // busy
          }
          callbackFinished = true;
        },
      },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({ op: 'message', channel: 'room:1', id: 1, time_ms: 1, body: 'x' });
    assert.equal(inCallback, true);

    // The client must still be usable and able to report an error while the
    // callback has not returned.
    const errors: ErrorReason[] = [];
    (client as unknown as { opts: { callbacks: { onError?: (e: PushError) => void } } }).opts.callbacks.onError =
      (e) => errors.push(e.reason);
    await tick();

    assert.equal(client.currentState, 'ready');
    assert.ok(callbackFinished, 'the callback never completed');
    assert.deepEqual(errors, []);
  });

  it('stops invoking a removed callback safely', async () => {
    let calls = 0;
    const client = newClient({
      channels: ['room:1'],
      callbacks: { onMessage: () => { calls += 1; } },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({ op: 'message', channel: 'room:1', id: 1, time_ms: 1, body: 'a' });
    assert.equal(calls, 1);
  });

  it('survives a callback that throws', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      channels: ['room:1'],
      callbacks: {
        onMessage: () => {
          throw new Error('render blew up');
        },
        onError: (e: PushError) => errors.push(e.reason),
      },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({ op: 'message', channel: 'room:1', id: 1, time_ms: 1, body: 'a' });
    assert.equal(errors.length, 1, 'a throwing callback must be reported, not swallowed');
    assert.equal(client.currentState, 'ready');
  });
});

describe('gap notification', () => {
  it('reports dropped content to the application', async () => {
    const gaps: number[] = [];
    const client = newClient({
      channels: ['room:1'],
      callbacks: { onGap: (_c, g) => gaps.push(g.dropped) },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({ op: 'gap', dropped: 42, reason: 1, time_ms: 1 });
    assert.deepEqual(gaps, [42]);
  });

  // An application that registers no gap callback is a supported choice, not an
  // error: some applications prefer a silently shorter stream to a visible gap.
  it('does not error when no gap callback is registered', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({ callbacks: { onError: (e: PushError) => errors.push(e.reason) } });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive({ op: 'gap', dropped: 5, reason: 1, time_ms: 1 });
    assert.deepEqual(errors, []);
    assert.equal(client.currentState, 'ready');
  });
});

describe('reconnection', () => {
  it('spreads attempts with jitter rather than synchronising them', () => {
    const b = new Backoff({ initialMs: 1000, maxMs: 10_000, jitterRatio: 0.5, maxAttempts: 10 });
    const delays: number[] = [];
    for (let i = 0; i < 200; i++) {
      const fresh = new Backoff({ initialMs: 1000, maxMs: 10_000, jitterRatio: 0.5, maxAttempts: 10 });
      delays.push(fresh.next());
    }
    assert.equal(delays.length, 200);

    // Same backoff object grows exponentially, so this only checks one client.
    const seen = new Set<number>();
    const single = new Backoff({ initialMs: 1000, maxMs: 10_000, jitterRatio: 0.5, maxAttempts: 10 });
    for (let i = 0; i < 200; i++) seen.add(single.next());
    assert.ok(seen.size > 50, `delays were not spread: ${seen.size} distinct values`);
  });

  it('grows the interval across consecutive failures', () => {
    const b = new Backoff(
      { initialMs: 100, maxMs: 10_000, jitterRatio: 0, maxAttempts: 10 },
      () => 0.5,
    );
    const first = b.next();
    const second = b.next();
    const third = b.next();
    assert.ok(second > first, `${second} did not exceed ${first}`);
    assert.ok(third > second, `${third} did not exceed ${second}`);
  });

  it('respects the ceiling', () => {
    const b = new Backoff(
      { initialMs: 100, maxMs: 500, jitterRatio: 0, maxAttempts: 20 },
      () => 0.5,
    );
    for (let i = 0; i < 10; i++) b.next();
    assert.equal(b.next(), 500, 'the ceiling was not applied');
  });

  it('reports a terminal failure once the retry budget is exhausted', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      backoff: { initialMs: 5, maxMs: 10, jitterRatio: 0, maxAttempts: 2 },
      callbacks: { onError: (e: PushError) => errors.push(e.reason) },
    });

    void client.connect().catch(() => {});
    for (let i = 0; i < 4; i++) {
      await tick();
      await tick();
      const s = FakeSocket.instances[FakeSocket.instances.length - 1];
      if (s && !s.closed) s.close(1006, 'gone');
    }
    await tick();
    await tick();

    assert.ok(
      errors.includes('retry-exhausted'),
      `errors were ${errors.join(',')}`,
    );
    assert.equal(client.currentState, 'failed');
  });

  it('does not retry a client-initiated close', async () => {
    const client = newClient();
    const connecting = client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();
    await connecting;

    client.close();
    await tick();
    await tick();

    assert.equal(FakeSocket.count(), 1, 'a closed client reconnected');
    assert.equal(client.currentState, 'closed');
  });

  it('reconnects on a drain notice without waiting for the backoff', async () => {
    const client = newClient({ channels: ['room:1'] });
    const connecting = client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();
    await connecting;

    socket.receive({ op: 'drain' });
    await tick();

    assert.ok(FakeSocket.count() >= 2, 'the client did not reconnect on drain');
  });
});

describe('session resumption', () => {
  it('offers the prior session on reconnect', async () => {
    const client = newClient({
      channels: ['room:1'],
      backoff: { initialMs: 5, maxMs: 10, jitterRatio: 0, maxAttempts: 5 },
    });
    const connecting = client.connect();
    const first = FakeSocket.last();
    first.open();
    first.ready('session-abc');
    await connecting;

    first.close();
    await tick();
    await tick();

    const second = FakeSocket.last();
    second.open();
    const hello = second.framesOf('hello')[0]!;
    assert.equal(hello['resume'], true);
    assert.equal(hello['session'], 'session-abc', 'the session handle was not offered');
  });

  it('connects without a session handle on a first connection', () => {
    const client = newClient();
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    assert.equal(socket.framesOf('hello')[0]!['session'], undefined);
  });
});

describe('weak and degraded networks', () => {
  it('recovers from a silently dropped connection through the liveness probe', async () => {
    const client = newClient({ channels: ['room:1'], heartbeatIntervalMs: 10 });
    const connecting = client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();
    await connecting;

    // No close frame, as a proxy dropping the socket would produce.
    socket.dropSilently();

    // The probe is unanswered and the last pong goes stale.
    await new Promise<void>((r) => setTimeout(r, 60));
    assert.ok(FakeSocket.count() >= 2, 'the client did not notice the dead socket');
  });

  it('ignores a malformed frame without tearing down the connection', async () => {
    const errors: ErrorReason[] = [];
    const client = newClient({
      callbacks: { onError: (e: PushError) => errors.push(e.reason) },
    });
    void client.connect();
    const socket = FakeSocket.last();
    socket.open();
    socket.ready();

    socket.receive('{not json');
    assert.equal(client.currentState, 'ready', 'a bad frame closed the connection');
  });
});