import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { HttpTransport, BASE } from './host.js';

async function fixture(t, options = {}) {
  const peerAbort = new AbortController();
  const peer = { id: 'browser-owner', signal: peerAbort.signal };
  const observations = { opened: 0, cleaned: 0, produced: 0, peers: [] };
  const connection = {
    admit(req) {
      if (req.headers.origin && req.headers.origin !== 'https://dsh.test') return { rejection: 403 };
      if (!/dsh-auth-test=(one|two)/.test(req.headers.cookie ?? '')) return { rejection: 401 };
      return { peer };
    },
  };
  const gateway = { wireStream: {
    failure: e => ({ code: e.code ?? 'gateway/internal', message: e.message, details: {} }),
    async open(endpoint, payload, uplink, admitted, signal) {
      observations.opened++;
      observations.peers.push(admitted);
      return (async function* () {
        try {
          if (endpoint === 'test/burst') {
            for (const value of payload.values) yield value;
          } else if (endpoint === 'test/large') {
            yield 'x'.repeat(3 * 1024 * 1024);
          } else if (endpoint === 'test/chunked') {
            for (let n = 0; n < payload.count; n++) {
              observations.produced++;
              yield { chunk: n, text: 'x'.repeat(512 * 1024) };
            }
          } else if (endpoint === 'test/oversize') {
            yield 'x'.repeat(33 * 1024 * 1024);
          } else if (endpoint === 'test/echo') {
            for await (const value of uplink) yield { echo: value };
          } else if (endpoint === 'test/fail') {
            throw Object.assign(new Error('business error'), { code: 'test/rejected' });
          } else if (endpoint === 'test/hold') {
            if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
          } else if (endpoint === 'test/no-uplink') {
            // A slow receiver exercises bounded uplink buffering.
            if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
          } else throw new Error('unknown test endpoint');
        } finally { observations.cleaned++; }
      })();
    },
  } };
  const transport = new HttpTransport(connection, gateway, { pollMs: 45, idleMs: 10_000, ...options });
  const server = http.createServer((req, res) => { void transport.handle(req, res, Buffer.from('/* bootstrap */')); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  async function request(action, body = {}, headers = {}, method = 'POST') {
    const data = JSON.stringify(body);
    return await new Promise((resolve, reject) => {
      const req = http.request({ host: '127.0.0.1', port, path: BASE + action, method,
        headers: { Host: 'dsh.test', Cookie: 'dsh-auth-test=one', 'Content-Type': 'application/json',
          ...headers, ...(method === 'POST' ? { 'Content-Length': Buffer.byteLength(data) } : {}) } }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString();
          let parsed;
          try { parsed = JSON.parse(text); } catch { parsed = text; }
          resolve({ status: res.statusCode, body: parsed, headers: res.headers });
        });
      });
      req.setTimeout(2000, () => req.destroy(new Error('test HTTP timeout')));
      req.on('error', reject);
      req.end(method === 'POST' ? data : undefined);
    });
  }
  async function open() {
    const result = await request('/open');
    assert.equal(result.status, 200);
    return result.body.sessionId;
  }
  async function send(sessionId, frames) {
    return request('/send', { sessionId, frames });
  }
  async function collect(sessionId, count, after = 0) {
    const frames = [];
    while (frames.length < count) {
      const result = await request('/poll', { sessionId, after });
      assert.equal(result.status, 200);
      frames.push(...result.body.frames);
      after = result.body.cursor;
    }
    return { frames, cursor: after };
  }
  t.after(async () => {
    await transport.dispose();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  return { request, open, send, collect, connection, peer, peerAbort, observations, transport };
}

test('admission protects bootstrap and actions, including forged forwarding headers', async t => {
  const f = await fixture(t);
  for (const action of ['/open', '/send', '/poll', '/close']) {
    assert.equal((await f.request(action, {}, { Cookie: '', 'X-Forwarded-For': '127.0.0.1' })).status, 401);
  }
  assert.equal((await f.request('/client.js', {}, { Cookie: '' }, 'GET')).status, 401);
  assert.equal((await f.request('/open', {}, { Origin: 'https://evil.test' })).status, 403);
  assert.equal((await f.request('/open', {}, { 'Content-Type': 'text/plain' })).status, 415);
  assert.equal((await f.request('/open', {}, {}, 'GET')).status, 405);
  assert.equal((await f.request('/client.js', {}, {}, 'GET')).body, '/* bootstrap */');
});

test('sessions bind to authenticated cookie and Host but tolerate unrelated cookie rotation', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  assert.equal((await f.request('/poll', { sessionId, after: 0 }, { Cookie: 'dsh-auth-test=two' })).status, 410);
  assert.equal((await f.request('/poll', { sessionId, after: 0 }, { Host: 'other.test' })).status, 410);
  const result = await f.request('/poll', { sessionId, after: 0 }, { Cookie: 'cf_clearance=changed; dsh-auth-test=one' });
  assert.equal(result.status, 200);
  assert.deepEqual(result.body, { frames: [], cursor: 0 });
});

test('downlink frames replay until acknowledged and old cursors cannot lose messages silently', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  assert.equal((await f.send(sessionId, [{ type: 'open', streamId: 's', endpoint: 'test/burst', payload: { values: ['first', 'second'] } }])).status, 200);
  const first = await f.request('/poll', { sessionId, after: 0 });
  const replay = await f.request('/poll', { sessionId, after: 0 });
  assert.deepEqual(replay.body, first.body);
  assert.deepEqual(first.body.frames.map(x => [x.seq, x.type, x.value]), [[1, 'item', 'first'], [2, 'item', 'second'], [3, 'end', undefined]]);
  assert.equal(first.body.cursor, 3);
  const acknowledged = await f.request('/poll', { sessionId, after: 3 });
  assert.deepEqual(acknowledged.body, { frames: [], cursor: 3 });
  assert.equal((await f.request('/poll', { sessionId, after: 0 })).status, 409);
  assert.equal((await f.request('/poll', { sessionId, after: 4 })).status, 409);
  assert.equal(f.observations.peers[0], f.peer);
});

test('duplex uplink preserves ordering and half-close yields final end', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  assert.equal((await f.send(sessionId, [
    { type: 'open', streamId: 'echo', endpoint: 'test/echo', payload: {} },
    { type: 'item', streamId: 'echo', value: 'one' },
    { type: 'item', streamId: 'echo', value: { two: 2 } },
    { type: 'item', streamId: 'echo' },
    { type: 'end', streamId: 'echo' },
  ])).status, 200);
  const result = await f.collect(sessionId, 4);
  assert.deepEqual(result.frames.map(x => x.type), ['item', 'item', 'item', 'end']);
  assert.deepEqual(result.frames.slice(0, 3).map(x => x.value), [{ echo: 'one' }, { echo: { two: 2 } }, {}]);
});

test('business errors retain code and message across HTTP framing', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'error', endpoint: 'test/fail', payload: {} }]);
  const { frames } = await f.collect(sessionId, 1);
  assert.equal(frames[0].type, 'error');
  assert.deepEqual(frames[0].error, { code: 'test/rejected', message: 'business error', details: {} });
});

test('cancel aborts the stream and closes its uplink without an orphan task', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'held', endpoint: 'test/hold', payload: {} }]);
  assert.equal((await f.send(sessionId, [{ type: 'cancel', streamId: 'held' }])).status, 200);
  const status = await f.request('/status', {}, {}, 'GET');
  assert.equal(status.body.activeStreams, 0);
  assert.equal(f.observations.cleaned, 1);
});

test('an empty poll completes and concurrent polls are rejected without disrupting the first', async t => {
  const f = await fixture(t, { pollMs: 120 });
  const sessionId = await f.open();
  const pending = f.request('/poll', { sessionId, after: 0 });
  for (let n = 0; n < 20; n++) {
    const status = await f.request('/status', {}, {}, 'GET');
    if (status.body.polls === 1) break;
    await delay(2);
  }
  assert.equal((await f.request('/poll', { sessionId, after: 0 })).status, 409);
  const result = await pending;
  assert.equal(result.status, 200);
  assert.deepEqual(result.body, { frames: [], cursor: 0 });
});

test('session and uplink limits fail closed and release capacity', async t => {
  const f = await fixture(t, { maxSessions: 1 });
  const sessionId = await f.open();
  assert.equal((await f.request('/open')).status, 503);
  const result = await f.send(sessionId, [
    { type: 'open', streamId: 'slow', endpoint: 'test/no-uplink', payload: {} },
    { type: 'item', streamId: 'slow', value: 'x'.repeat(256 * 1024) },
  ]);
  assert.equal(result.status, 429);
  assert.equal((await f.request('/poll', { sessionId, after: 0 })).status, 410);
  await f.open();
});

test('large bursts wait for ACK without expiring the session or losing frames', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'burst', endpoint: 'test/chunked', payload: { count: 10 } }]);
  const session = f.transport.sessions.get(sessionId);
  assert.ok(session.writers.size > 0, 'producer should wait when the ACK window is full');
  assert.ok(f.observations.produced < 10, 'must not eagerly read the entire source');
  assert.ok(session.queueBytes <= 2 * 1024 * 1024);
  const { frames } = await f.collect(sessionId, 11);
  assert.deepEqual(frames.filter(x => x.type === 'item').map(x => x.value.chunk), [...Array(10).keys()]);
  assert.deepEqual(frames.map(x => x.seq), [...Array(11)].map((_, n) => n + 1));
  assert.equal(frames.at(-1).type, 'end');
  assert.equal(f.transport.sessions.size, 1);
  assert.deepEqual(f.transport.metrics.closedReasons, {});
  assert.ok(f.transport.metrics.peakQueuedBytes <= 2 * 1024 * 1024);
});

test('one snapshot over 2 MiB is delivered and replayable until acknowledged', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'large', endpoint: 'test/large', payload: {} }]);
  const first = await f.request('/poll', { sessionId, after: 0 });
  assert.equal(first.status, 200);
  assert.equal(first.body.frames.length, 1);
  assert.equal(first.body.frames[0].value.length, 3 * 1024 * 1024);
  const replay = await f.request('/poll', { sessionId, after: 0 });
  assert.deepEqual(replay.body, first.body);
  const last = await f.request('/poll', { sessionId, after: first.body.cursor });
  assert.equal(last.status, 200);
  assert.equal(last.body.frames[0].type, 'end');
  assert.equal(f.transport.sessions.size, 1);
});

test('cancelling a producer blocked on downlink capacity releases its iterator', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'blocked', endpoint: 'test/chunked', payload: { count: 10 } }]);
  const session = f.transport.sessions.get(sessionId);
  assert.ok(session.writers.size > 0);
  await f.send(sessionId, [{ type: 'cancel', streamId: 'blocked' }]);
  assert.equal(session.writers.size, 0);
  assert.equal(f.observations.cleaned, 1);
  assert.equal(session.streams.size, 0);
  assert.equal(f.transport.sessions.size, 1);
});

test('closing and disposing sessions unlock blocked producers without orphan tasks', async t => {
  const f = await fixture(t);
  for (const action of ['close', 'dispose']) {
    const sessionId = await f.open();
    await f.send(sessionId, [{ type: 'open', streamId: 'blocked', endpoint: 'test/chunked', payload: { count: 10 } }]);
    const session = f.transport.sessions.get(sessionId);
    assert.ok(session.writers.size > 0);
    if (action === 'close') await f.request('/close', { sessionId });
    else await f.transport.dispose();
    for (let n = 0; n < 20 && f.transport.tasks.size; n++) await delay(1);
    assert.equal(session.writers.size, 0);
    assert.equal(f.transport.tasks.size, 0);
    assert.equal(session.streams.size, 0);
  }
  assert.equal(f.transport.metrics.closedReasons['caller-close'], 1);
  assert.equal(f.transport.metrics.closedReasons.dispose, 1);
});

test('a frame beyond 32 MiB fails only its logical stream with an explicit error', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'too-large', endpoint: 'test/oversize', payload: {} }]);
  const first = await f.request('/poll', { sessionId, after: 0 });
  assert.equal(first.status, 200);
  assert.equal(first.body.frames[0].type, 'error');
  assert.equal(first.body.frames[0].error.code, 'transport/frame-too-large');
  assert.equal(first.body.frames[0].error.details.maxFrameBytes, 32 * 1024 * 1024);
  await f.send(sessionId, [{ type: 'open', streamId: 'healthy', endpoint: 'test/burst', payload: { values: ['still connected'] } }]);
  const next = await f.collect(sessionId, 2, first.body.cursor);
  assert.equal(next.frames[0].value, 'still connected');
  assert.equal(f.transport.sessions.size, 1);
  assert.deepEqual(f.transport.metrics.closedReasons, {});
});

test('status explains expired-session reasons without returning binding material', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  const wrongOwner = await f.request('/poll', { sessionId, after: 0 }, { Cookie: 'dsh-auth-test=two' });
  assert.equal(wrongOwner.status, 410);
  assert.equal(wrongOwner.body.reason, 'owner-mismatch');
  const missing = await f.request('/poll', { sessionId: 'unknown', after: 0 });
  assert.equal(missing.body.reason, 'missing-session');
  const admit = f.connection.admit;
  f.connection.admit = req => {
    const result = admit(req);
    return 'peer' in result ? { peer: { ...result.peer } } : result;
  };
  const wrongPeer = await f.request('/poll', { sessionId, after: 0 });
  assert.equal(wrongPeer.status, 410);
  assert.equal(wrongPeer.body.reason, 'peer-mismatch');
  f.connection.admit = admit;
  await f.request('/close', { sessionId });
  const status = (await f.request('/status', {}, {}, 'GET')).body;
  assert.deepEqual(status.rejectionReasons, { 'owner-mismatch': 1, 'missing-session': 1, 'peer-mismatch': 1 });
  assert.deepEqual(status.closedReasons, { 'caller-close': 1 });
  assert.deepEqual(status.lastClose, { reason: 'caller-close', queuedBytes: 0, streams: 0 });
  const serialized = JSON.stringify(status);
  assert.equal(serialized.includes(sessionId), false);
  assert.equal(serialized.includes('dsh-auth-test'), false);
  assert.equal(serialized.includes('browser-owner'), false);
});

test('idle cleanup records the reason and wakes a producer waiting for ACK', async t => {
  const f = await fixture(t, { idleMs: 25 });
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'blocked', endpoint: 'test/chunked', payload: { count: 10 } }]);
  const session = f.transport.sessions.get(sessionId);
  assert.ok(session.writers.size > 0);
  for (let n = 0; n < 50 && f.transport.tasks.size; n++) await delay(5);
  assert.equal(f.transport.tasks.size, 0);
  assert.equal(session.writers.size, 0);
  assert.equal(f.observations.cleaned, 1);
  assert.equal(f.transport.metrics.closedReasons.idle, 1);
  assert.equal(f.transport.metrics.lastClose.reason, 'idle');
  assert.ok(f.transport.metrics.lastClose.queuedBytes > 0);
});

test('Peer disposal wakes a producer waiting for ACK', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'blocked', endpoint: 'test/chunked', payload: { count: 10 } }]);
  const session = f.transport.sessions.get(sessionId);
  assert.ok(session.writers.size > 0);
  f.peerAbort.abort();
  for (let n = 0; n < 20 && f.transport.tasks.size; n++) await delay(1);
  assert.equal(f.transport.tasks.size, 0);
  assert.equal(session.writers.size, 0);
  assert.equal(f.observations.cleaned, 1);
  assert.equal(f.transport.metrics.closedReasons['peer-disposed'], 1);
});

test('peer disposal closes pending polls and releases every active stream', async t => {
  const f = await fixture(t, { pollMs: 1000 });
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'held', endpoint: 'test/hold', payload: {} }]);
  const pending = f.request('/poll', { sessionId, after: 0 });
  for (let n = 0; n < 20; n++) {
    if ((await f.request('/status', {}, {}, 'GET')).body.polls === 1) break;
    await delay(2);
  }
  f.peerAbort.abort();
  const lost = await pending;
  assert.equal(lost.status, 410);
  assert.equal(lost.body.reason, 'peer-disposed');
  await f.transport.dispose();
  assert.equal(f.observations.cleaned, 1);
  assert.equal((await f.request('/status', {}, {}, 'GET')).body.activeSessions, 0);
  assert.equal(f.transport.metrics.closedReasons['peer-disposed'], 1);
});
