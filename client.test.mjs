import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('./client.js', import.meta.url), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(condition) {
  for (let i = 0; i < 100; i++) {
    if (condition()) return;
    await tick();
  }
  assert.fail('HTTP carrier did not reach expected state');
}

function browser() {
  const requests = [];
  const polls = [];
  const listeners = new Map();
  const emit = (event, value = {}) => {
    const entry = listeners.get(event);
    if (!entry) return;
    if (entry.once) listeners.delete(event);
    entry.listener(value);
  };
  let sessionCount = 0;
  const context = vm.createContext({
    URL, TextEncoder, AbortController, DOMException, console,
    document: { baseURI: 'https://dsh.example/', currentScript: { dataset: { base: '/api/http-transport' } } },
    location: { origin: 'https://dsh.example' },
    __DSH_TRANSPORT__: { unrelated: 'kept' },
    addEventListener: (event, listener, options) => listeners.set(event, { listener, once: options?.once }),
    fetch: async (url, options) => {
      const body = JSON.parse(options.body);
      const action = new URL(url).pathname.split('/').pop();
      requests.push({ action, body, options });
      const response = (body, status = 200) => ({ status, json: async () => body });
      if (action === 'open') return response({ sessionId: `session-${++sessionCount}` });
      if (action === 'send' || action === 'close') return response({ ok: true });
      if (action !== 'poll') throw Error('Unexpected action');
      return new Promise((resolve, reject) => {
        const entry = {
          sessionId: body.sessionId,
          after: body.after,
          reply: value => { remove(); resolve(response(value)); },
          httpError: (status, value) => { remove(); resolve(response(value, status)); },
          fail: error => { remove(); reject(error); },
        };
        const remove = () => {
          const at = polls.indexOf(entry);
          if (at !== -1) polls.splice(at, 1);
          options.signal.removeEventListener('abort', abort);
        };
        const abort = () => { remove(); reject(options.signal.reason); };
        options.signal.addEventListener('abort', abort, { once: true });
        polls.push(entry);
      });
    },
  });
  vm.runInContext(source, context);
  return {
    context, requests, polls,
    open: (...args) => context.__DSH_TRANSPORT__.openStream(...args),
    frames: () => requests.filter(r => r.action === 'send').flatMap(r => r.body.frames),
    close: () => emit('pagehide'),
    restore: () => emit('pageshow', { persisted: true }),
  };
}

test('multiplexes logical streams over one authenticated poll and deduplicates replay', async () => {
  const b = browser();
  const first = b.open('$events', { args: {} }, new AbortController().signal);
  const second = b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 2);
  assert.equal(b.requests.filter(r => r.action === 'open').length, 1);
  assert.equal(b.polls.length, 1);
  assert.equal(b.context.__DSH_TRANSPORT__.unrelated, 'kept');
  assert.equal(b.context.__DSH_TRANSPORT__.ownsHost, undefined);
  const [a, c] = b.frames().map(f => f.streamId);
  b.polls[0].reply({ frames: [{ seq: 1, type: 'item', streamId: a, value: 'ready' }], cursor: 1 });
  assert.equal((await first.next()).value, 'ready');
  await until(() => b.polls[0]?.after === 1);
  b.polls[0].reply({ frames: [
    { seq: 1, type: 'item', streamId: a, value: 'duplicate' },
    { seq: 2, type: 'item', streamId: c, value: { part: 1 } },
    { seq: 3, type: 'item', streamId: a, value: 'next' },
    { seq: 4, type: 'end', streamId: a },
    { seq: 5, type: 'end', streamId: c },
  ], cursor: 5 });
  assert.equal((await first.next()).value, 'next');
  assert.equal((await first.next()).done, true);
  assert.deepEqual((await second.next()).value, { part: 1 });
  assert.equal((await second.next()).done, true);
  for (const request of b.requests) {
    assert.equal(request.options.method, 'POST');
    assert.equal(request.options.credentials, 'same-origin');
    assert.equal(request.options.cache, 'no-store');
  }
  assert.equal('sessionId' in b.context.__DSH_HTTP_TRANSPORT__.snapshot(), false);
  b.close();
});

test('sends open, uplink items and half-close in order; abort releases pending input', async () => {
  const b = browser();
  const controller = new AbortController();
  let released = false;
  let pulled = 0;
  const uplink = {
    [Symbol.asyncIterator]() { return this; },
    next() {
      pulled++;
      if (pulled <= 2) return Promise.resolve({ value: pulled, done: false });
      return new Promise(() => {});
    },
    return() { released = true; return Promise.resolve({ done: true }); },
  };
  const stream = b.open('terminal/io', { args: {} }, controller.signal, uplink);
  await until(() => b.frames().length === 3 && pulled === 3);
  assert.deepEqual(b.frames().map(f => f.type), ['open', 'item', 'item']);
  assert.deepEqual(b.frames().slice(1).map(f => f.value), [1, 2]);
  const pending = stream.next();
  controller.abort(new Error('stop input'));
  await assert.rejects(pending, /stop input/);
  await until(() => released && b.frames().at(-1).type === 'cancel');

  const complete = b.open('terminal/io', { args: {} }, new AbortController().signal, (async function* () { yield undefined; })());
  await until(() => b.frames().filter(f => f.type === 'end').length === 1);
  const last = b.frames().slice(-3);
  assert.deepEqual(last.map(f => f.type), ['open', 'item', 'end']);
  assert.equal('value' in last[1], false);
  await complete.return();
  b.close();
});

test('carrier failure ends every active stream and next generation creates a fresh session', async () => {
  const b = browser();
  const first = b.open('$events', { args: {} }, new AbortController().signal);
  const another = b.open('session/follow', { args: {} }, new AbortController().signal);
  const a = first.next();
  const c = another.next();
  const rejectedA = assert.rejects(a, error => error.dshRemoteStreamFailure.kind === 'carrier');
  const rejectedC = assert.rejects(c, error => error.dshRemoteStreamFailure.kind === 'carrier');
  await until(() => b.polls.length && b.frames().length === 2);
  b.polls[0].fail(new Error('proxy dropped response'));
  await Promise.all([rejectedA, rejectedC]);
  const recovered = b.open('$events', { args: {} }, new AbortController().signal);
  await until(() => b.requests.filter(r => r.action === 'open').length === 2 && b.frames().length === 3);
  assert.equal(b.polls.length, 1);
  assert.equal(b.polls[0].sessionId, 'session-2');
  await recovered.return();
  b.close();
});

test('remote failure retains DSH error marker; sequence gaps fail rather than lose frames', async () => {
  const b = browser();
  const stream = b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 1);
  const streamId = b.frames()[0].streamId;
  const errorRead = stream.next();
  const remoteFailure = assert.rejects(errorRead, error => error.dshRemoteStreamFailure.kind === 'remote' && error.dshRemoteStreamFailure.code === 'gateway/test');
  b.polls[0].reply({ frames: [{ seq: 1, type: 'error', streamId, error: { code: 'gateway/test', message: 'remote denied', details: {} } }], cursor: 1 });
  await remoteFailure;
  const next = b.open('$events', { args: {} }, new AbortController().signal);
  const failure = assert.rejects(next.next(), /poll failed/);
  await until(() => b.frames().length === 2 && b.polls[0]?.after === 1);
  b.polls[0].reply({ frames: [{ seq: 3, type: 'item', streamId: b.frames()[1].streamId, value: 'gap' }], cursor: 3 });
  await failure;
  b.close();
});

test('return releases queued bytes even when Host has already ended the stream', async () => {
  const b = browser();
  const stream = b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 1);
  const streamId = b.frames()[0].streamId;
  b.polls[0].reply({ frames: [
    { seq: 1, type: 'item', streamId, value: 'buffered result' },
    { seq: 2, type: 'end', streamId },
  ], cursor: 2 });
  await until(() => b.context.__DSH_HTTP_TRANSPORT__.snapshot().received === 2);
  assert.ok(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes > 0);
  await stream.return();
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes, 0);
  assert.equal((await stream.next()).done, true);
  b.close();
});

test('BFCache restore permits a new generation and every later pagehide still closes it', async () => {
  const b = browser();
  const first = b.open('$events', { args: {} }, new AbortController().signal);
  const lost = assert.rejects(first.next(), /page closed/);
  await until(() => b.frames().length === 1);
  b.close();
  await lost;
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().connected, false);
  b.restore();
  const second = b.open('$events', { args: {} }, new AbortController().signal);
  const lostAgain = assert.rejects(second.next(), /page closed/);
  await until(() => b.requests.filter(r => r.action === 'open').length === 2 && b.frames().length === 2);
  assert.equal(b.polls.length, 1);
  assert.equal(b.polls[0].sessionId, 'session-2');
  b.close();
  await lostAgain;
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().connected, false);
});

test('delivers a multi-megabyte snapshot directly to an already waiting reader', async () => {
  const b = browser();
  const stream = b.open('session/follow', { args: {} }, new AbortController().signal);
  const read = stream.next();
  await until(() => b.frames().length === 1);
  const value = 'x'.repeat(3 * 1024 * 1024);
  b.polls[0].reply({ frames: [{ seq: 1, type: 'item', streamId: b.frames()[0].streamId, value }], cursor: 1 });
  assert.equal((await read).value, value);
  await until(() => b.polls[0]?.after === 1);
  const state = b.context.__DSH_HTTP_TRANSPORT__.snapshot();
  assert.equal(state.failures, 0);
  assert.equal(state.bufferedBytes, 0);
  await stream.return();
  b.close();
});

test('pauses acknowledgement for a slow consumer instead of failing a full downlink queue', async () => {
  const b = browser();
  const stream = b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 1);
  const streamId = b.frames()[0].streamId;
  const values = Array.from({ length: 6 }, (_, i) => `${i}${'x'.repeat(512 * 1024)}`);
  b.polls[0].reply({ frames: [
    ...values.map((value, i) => ({ seq: i + 1, type: 'item', streamId, value })),
    { seq: 7, type: 'end', streamId },
  ], cursor: 7 });
  await until(() => b.context.__DSH_HTTP_TRANSPORT__.snapshot().received === 3);
  assert.equal(b.polls.length, 0, 'a blocked batch must not be acknowledged prematurely');
  assert.equal(b.requests.filter(r => r.action === 'poll').length, 1);
  assert.ok(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes <= 2 * 1024 * 1024);
  for (const value of values) assert.equal((await stream.next()).value, value);
  await until(() => b.polls[0]?.after === 7);
  assert.equal((await stream.next()).done, true);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().failures, 0);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes, 0);
  b.close();
});

test('cancelling a full queue unblocks the poll and delivers other logical streams', async () => {
  const b = browser();
  const slow = b.open('session/follow', { args: {} }, new AbortController().signal);
  const fast = b.open('$events', { args: {} }, new AbortController().signal);
  const read = fast.next();
  await until(() => b.frames().length === 2);
  const [slowId, fastId] = b.frames().map(f => f.streamId);
  b.polls[0].reply({ frames: [
    ...Array.from({ length: 4 }, (_, i) => ({ seq: i + 1, type: 'item', streamId: slowId, value: 'x'.repeat(512 * 1024) })),
    { seq: 5, type: 'item', streamId: fastId, value: 'still connected' },
  ], cursor: 5 });
  await until(() => b.context.__DSH_HTTP_TRANSPORT__.snapshot().received === 3);
  await slow.return();
  assert.equal((await read).value, 'still connected');
  await until(() => b.polls[0]?.after === 5);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes, 0);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().failures, 0);
  await fast.return();
  b.close();
});

test('page shutdown releases a poll waiting for downlink capacity', async () => {
  const b = browser();
  b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 1);
  const streamId = b.frames()[0].streamId;
  b.polls[0].reply({ frames: Array.from({ length: 4 }, (_, i) => ({
    seq: i + 1, type: 'item', streamId, value: 'x'.repeat(512 * 1024),
  })), cursor: 4 });
  await until(() => b.context.__DSH_HTTP_TRANSPORT__.snapshot().received === 3);
  b.close();
  await tick();
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().connected, false);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes, 0);
  assert.equal(b.requests.filter(r => r.action === 'poll').length, 1);
});

test('HTTP 410 preserves only safe diagnostic facts and the next generation recovers', async () => {
  const b = browser();
  const first = b.open('$events', { args: {} }, new AbortController().signal);
  const lost = assert.rejects(first.next(), error => error.dshRemoteStreamFailure.kind === 'carrier');
  await until(() => b.frames().length === 1);
  b.polls[0].httpError(410, { reason: 'downlink-overflow', error: 'transport session expired', cookie: 'never retain this' });
  await lost;
  const state = b.context.__DSH_HTTP_TRANSPORT__.snapshot();
  assert.equal(state.lastFailure.status, 410);
  assert.equal(state.lastFailure.action, 'poll');
  assert.equal(state.lastFailure.reason, 'downlink-overflow');
  assert.equal(JSON.stringify(state).includes('never retain this'), false);
  assert.equal(JSON.stringify(state).includes('session-1'), false);
  const recovered = b.open('$events', { args: {} }, new AbortController().signal);
  const ready = recovered.next();
  await until(() => b.frames().length === 2 && b.polls[0]?.sessionId === 'session-2');
  b.polls[0].reply({ frames: [{ seq: 1, type: 'item', streamId: b.frames()[1].streamId, value: 'ready' }], cursor: 1 });
  assert.equal((await ready).value, 'ready');
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().failures, 1);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().connected, true);
  await recovered.return();
  b.close();
});

test('buffers one indivisible large snapshot in an empty queue and resumes after it is read', async () => {
  const b = browser();
  const stream = b.open('session/follow', { args: {} }, new AbortController().signal);
  await until(() => b.frames().length === 1);
  const streamId = b.frames()[0].streamId;
  const large = 'snapshot'.repeat(400 * 1024);
  b.polls[0].reply({ frames: [{ seq: 1, type: 'item', streamId, value: large }], cursor: 1 });
  await until(() => b.polls[0]?.after === 1);
  assert.ok(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes > 2 * 1024 * 1024);
  b.polls[0].reply({ frames: [{ seq: 2, type: 'item', streamId, value: 'incremental update' }], cursor: 2 });
  await tick();
  assert.equal(b.polls.length, 0);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().received, 1);
  assert.equal((await stream.next()).value, large);
  assert.equal((await stream.next()).value, 'incremental update');
  await until(() => b.polls[0]?.after === 2);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().failures, 0);
  assert.equal(b.context.__DSH_HTTP_TRANSPORT__.snapshot().bufferedBytes, 0);
  await stream.return();
  b.close();
});
