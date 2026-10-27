import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import vm from 'node:vm';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { HttpTransport, BASE } from './host.js';

test('real HTTP carrier delivers a large snapshot and concurrent burst to slow consumers without 410', { timeout: 10_000 }, async t => {
  const MiB = 1024 * 1024;
  const snapshotText = 'S'.repeat(3 * MiB);
  const chunkText = 'C'.repeat(512 * 1024);
  const chunkCount = 16;
  const peer = { id: 'integration-owner', signal: new AbortController().signal };
  let cleanedStreams = 0;
  const gateway = { wireStream: {
    failure: error => ({ code: 'test/failure', message: error.message, details: {} }),
    async open(endpoint) {
      return (async function* () {
        try {
          if (endpoint === 'test/snapshot') yield { type: 'snapshot', text: snapshotText };
          else if (endpoint === 'test/chunks') {
            for (let index = 0; index < chunkCount; index++) yield { index, text: chunkText };
          } else throw new Error('Unexpected test endpoint');
        } finally { cleanedStreams++; }
      })();
    },
  } };
  const transport = new HttpTransport({ admit: req => req.headers.cookie === 'dsh-auth-integration=fixture'
    ? { peer } : { rejection: 401 } }, gateway, { pollMs: 100 });
  const server = http.createServer((req, res) => { void transport.handle(req, res, Buffer.from('')); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const events = new Map();
  const responses = [];
  let context;
  t.after(async () => {
    events.get('pagehide')?.();
    await transport.dispose();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  context = vm.createContext({
    URL, TextEncoder, AbortController, DOMException, console,
    document: { baseURI: origin + '/', currentScript: { dataset: { base: BASE } } },
    location: { origin },
    addEventListener: (name, listener) => events.set(name, listener),
    fetch: async (url, options) => {
      const response = await fetch(url, { ...options,
        headers: { ...options.headers, Cookie: 'dsh-auth-integration=fixture' } });
      responses.push({ action: new URL(url).pathname.split('/').pop(), status: response.status });
      return response;
    },
  });
  vm.runInContext(await readFile(new URL('./client.js', import.meta.url), 'utf8'), context);
  const snapshot = context.__DSH_TRANSPORT__.openStream('test/snapshot', {}, new AbortController().signal);
  const chunks = context.__DSH_TRANSPORT__.openStream('test/chunks', {}, new AbortController().signal);
  const stats = () => context.__DSH_HTTP_TRANSPORT__.snapshot();
  // Deliberately let the snapshot queue before asking for its first value.
  // This exercises both the large-frame exception and cross-stream backpressure.
  const deadline = Date.now() + 3000;
  while (stats().bufferedBytes < 3 * MiB && Date.now() < deadline) await delay(2);
  assert.ok(stats().bufferedBytes >= 3 * MiB, 'large snapshot should queue without expiring the session');
  let peakClientBytes = stats().bufferedBytes;
  const sampler = setInterval(() => { peakClientBytes = Math.max(peakClientBytes, stats().bufferedBytes); }, 1);
  t.after(() => clearInterval(sampler));
  let snapshotsRead = 0;
  let chunksRead = 0;
  let deliveredBytes = 0;
  await Promise.all([
    (async () => {
      for await (const value of snapshot) {
        assert.equal(value.type, 'snapshot');
        assert.ok(value.text === snapshotText, 'snapshot contents changed');
        snapshotsRead++;
        deliveredBytes += value.text.length;
        await delay(5);
      }
    })(),
    (async () => {
      for await (const value of chunks) {
        assert.equal(value.index, chunksRead, 'chunk order changed');
        assert.ok(value.text === chunkText, 'chunk contents changed');
        chunksRead++;
        deliveredBytes += value.text.length;
        await delay(5);
      }
    })(),
  ]);
  assert.equal(snapshotsRead, 1);
  assert.equal(chunksRead, chunkCount);
  assert.equal(deliveredBytes, 11 * MiB);
  assert.equal(stats().sessions, 1);
  assert.equal(stats().failures, 0);
  assert.equal(stats().bufferedBytes, 0);
  assert.equal(transport.metrics.sessionsOpened, 1);
  assert.deepEqual(transport.metrics.closedReasons, {});
  assert.ok(transport.metrics.peakQueuedBytes <= 3 * MiB + 1024, 'Host queue exceeded one large frame');
  assert.ok(peakClientBytes <= 3 * MiB + 1024, 'Client queue exceeded one large frame');
  assert.equal(cleanedStreams, 2);
  assert.equal(transport.tasks.size, 0);
  assert.equal(transport.sessions.size, 1);
  assert.ok(responses.every(response => response.status === 200), 'HTTP carrier returned an error');
  assert.equal(responses.filter(response => response.action === 'open').length, 1);
  // A normal page exit closes its one transport and aborts the pending poll.
  events.get('pagehide')();
  const closeDeadline = Date.now() + 1000;
  while (transport.sessions.size && Date.now() < closeDeadline) await delay(2);
  assert.equal(transport.sessions.size, 0);
  assert.equal(stats().connected, false);
  assert.equal(stats().streams, 0);
  assert.equal(transport.tasks.size, 0);
  assert.ok(responses.every(response => response.status === 200), 'page exit returned an HTTP error');
});
