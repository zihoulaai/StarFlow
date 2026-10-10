/**
 * Integration test against a running gateway.
 *
 * The unit suite drives a fake socket, which is what makes the timing and
 * ordering assertions deterministic. This suite exists because a fake socket
 * cannot prove that the *real* protocol interoperates: that the handshake frame
 * the SDK sends is the one the gateway parses, and that a message the gateway
 * encodes reaches an application callback.
 *
 * It is skipped unless PUSH_TEST_URL is set, so the unit suite stays runnable
 * with no server.
 */
import assert from 'node:assert/strict';
import { describe, it, before } from 'node:test';

import { PushClient } from '../dist/client.js';
import type { Message } from '../dist/protocol.js';

const url = process.env['PUSH_TEST_URL'];
const appKey = process.env['PUSH_TEST_APP_KEY'];
const token = process.env['PUSH_TEST_TOKEN'];

describe('browser integration', () => {
  before(() => {
    if (!url || !appKey || !token) {
      console.log('skipping: PUSH_TEST_URL, PUSH_TEST_APP_KEY and PUSH_TEST_TOKEN are required');
    }
  });

  it('delivers a real message and reconnects restoring subscriptions', async (t) => {
    if (!url || !appKey || !token) return t.skip('no gateway configured');

    const received: { channel: string; message: Message }[] = [];
    const client = new PushClient({
      url,
      appKey,
      token,
      channels: ['room:test'],
      backoff: { initialMs: 50, maxMs: 200, jitterRatio: 0.2, maxAttempts: 5 },
      callbacks: {
        onMessage: (channel, message) => received.push({ channel, message }),
      },
    });

    await client.connect();
    assert.equal(client.currentState, 'ready', 'the handshake did not complete against a real gateway');

    // Publish something the gateway will fan out.
    const res = await fetch(new URL('/publish', url.replace(/^ws/, 'http')), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ channel: 'room:test', body: 'integration' }),
    });
    assert.ok(res.ok, `publish failed with ${res.status}`);

    const deadline = Date.now() + 5000;
    while (received.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.ok(received.length > 0, 'no message arrived from a real gateway');
    assert.equal(received[0]?.message.body, 'integration');
    assert.ok((received[0]?.message.id ?? 0n) > 0n, 'the message carried no identifier');

    // A reconnect must restore the subscription without the application doing
    // anything, which is the property that makes a release invisible to a
    // viewer.
    client.close();
    await client.connect();
    assert.equal(client.currentState, 'ready');
    assert.ok(client.channelList.includes('room:test'), 'the subscription was not restored');

    client.close();
  });
});
