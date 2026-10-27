import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { HttpTransport, BASE } from './host.js';

async function fixture(t, options = {}) {
  const peerAbort = new AbortController();
  const peer = { id: 'browser-owner', signal: peerAbort.signal };
  const observations = { opened: 0, cleaned: 0, peers: [] };
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
            yield 'x'.repeat(2 * 1024 * 1024);
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
  return { request, open, send, collect, peer, peerAbort, observations, transport };
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

test('downlink overflow expires the session rather than dropping unseen frames', async t => {
  const f = await fixture(t);
  const sessionId = await f.open();
  await f.send(sessionId, [{ type: 'open', streamId: 'huge', endpoint: 'test/large', payload: {} }]);
  assert.equal((await f.request('/poll', { sessionId, after: 0 })).status, 410);
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
  assert.equal((await pending).status, 410);
  await f.transport.dispose();
  assert.equal(f.observations.cleaned, 1);
  assert.equal((await f.request('/status', {}, {}, 'GET')).body.activeSessions, 0);
});
