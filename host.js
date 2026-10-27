import { createHash, randomBytes } from 'node:crypto';

export const BASE = '/api/http-transport';
const BODY_LIMIT = 1024 * 1024;
const QUEUE_LIMIT = 2 * 1024 * 1024;
const UPLINK_LIMIT = 256 * 1024;
const fail = (status, message) => Object.assign(new Error(message), { status });
const bytes = value => Buffer.byteLength(JSON.stringify(value) ?? 'null');

function reply(res, status, body, type = 'application/json; charset=utf-8') {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, {
    'content-type': type,
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

async function readJson(req) {
  if (req.headers['content-type']?.split(';', 1)[0].trim().toLowerCase() !== 'application/json') {
    throw fail(415, 'application/json required');
  }
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > BODY_LIMIT) throw fail(413, 'request body too large');
    chunks.push(chunk);
  }
  let body;
  try { body = JSON.parse(Buffer.concat(chunks).toString()); }
  catch { throw fail(400, 'invalid JSON'); }
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw fail(400, 'JSON object required');
  return body;
}

// Ignore unrelated cookies (for example Cloudflare cookies that may rotate).
// Every request is first admitted by DSH; this hash only binds the transport
// session to its original authenticated cookie and authority.
function ownerOf(req, peer) {
  const cookies = (req.headers.cookie ?? '').split(';').map(s => s.trim())
    .filter(s => s.startsWith('dsh-auth-')).sort().join(';');
  return createHash('sha256').update(`${req.headers.host}\n${peer.id}\n${cookies}`).digest('hex');
}

class Uplink {
  items = [];
  size = 0;
  ended = false;
  discarded = false;
  waiter;
  push(value) {
    if (this.discarded) return;
    if (this.ended) throw fail(400, 'uplink item after end');
    const size = bytes(value);
    if (this.size + size > UPLINK_LIMIT) throw fail(429, 'uplink capacity exceeded');
    this.items.push({ value, size });
    this.size += size;
    this.wake();
  }
  end() { this.ended = true; this.wake(); }
  wake() { this.waiter?.(); this.waiter = undefined; }
  [Symbol.asyncIterator]() { return this; }
  async next() {
    while (!this.items.length && !this.ended) await new Promise(resolve => { this.waiter = resolve; });
    const item = this.items.shift();
    if (!item) return { done: true, value: undefined };
    this.size -= item.size;
    return { done: false, value: item.value };
  }
  async return() {
    this.discarded = true;
    this.items.length = 0;
    this.size = 0;
    this.end();
    return { done: true, value: undefined };
  }
}

export class HttpTransport {
  sessions = new Map();
  tasks = new Set();
  metrics = { sessionsOpened: 0, polls: 0, framesReceived: 0, framesSent: 0 };
  constructor(connection, gateway, { pollMs = 20_000, idleMs = 65_000, maxSessions = 32 } = {}) {
    this.connection = connection;
    this.gateway = gateway;
    this.pollMs = pollMs;
    this.idleMs = idleMs;
    this.maxSessions = maxSessions;
    this.sweep = setInterval(() => {
      for (const session of this.sessions.values()) {
        if (Date.now() - session.lastSeen > this.idleMs) this.close(session);
      }
    }, Math.min(15_000, idleMs)).unref();
  }

  async handle(req, res, script) {
    try {
      const admission = this.connection.admit(req);
      if ('rejection' in admission) return reply(res, admission.rejection, { error: 'authentication required or untrusted origin' });
      const action = new URL(req.url, 'http://local').pathname.slice(BASE.length);
      if (req.method === 'GET' && action === '/client.js') {
        return reply(res, 200, script, 'text/javascript; charset=utf-8');
      }
      if (req.method === 'GET' && action === '/status') {
        return reply(res, 200, { mode: 'http-long-poll', version: '0.1.1', ...this.metrics,
          activeSessions: this.sessions.size,
          activeStreams: [...this.sessions.values()].reduce((n, s) => n + s.streams.size, 0) });
      }
      if (req.method !== 'POST') return reply(res, 405, { error: 'POST required' });
      if (!['/open', '/send', '/poll', '/close'].includes(action)) return reply(res, 404, { error: 'unknown action' });
      const body = await readJson(req);
      const owner = ownerOf(req, admission.peer);
      if (action === '/open') {
        if (this.sessions.size >= this.maxSessions) throw fail(503, 'transport session capacity reached');
        const session = {
          id: randomBytes(24).toString('base64url'), owner, peer: admission.peer,
          streams: new Map(), queue: [], queueBytes: 0, seq: 0, acknowledged: 0,
          lastSeen: Date.now(), closed: false, poll: undefined,
        };
        session.onPeerAbort = () => this.close(session);
        admission.peer.signal.addEventListener('abort', session.onPeerAbort, { once: true });
        this.sessions.set(session.id, session);
        this.metrics.sessionsOpened++;
        return reply(res, 200, { sessionId: session.id });
      }
      const session = this.sessions.get(body.sessionId);
      if (!session || session.owner !== owner || session.peer !== admission.peer) throw fail(410, 'transport session expired');
      session.lastSeen = Date.now();
      if (action === '/close') {
        this.close(session);
        return reply(res, 200, { ok: true });
      }
      if (action === '/poll') return this.poll(session, body.after, res);
      this.send(session, body.frames);
      return reply(res, 200, { ok: true });
    } catch (error) {
      reply(res, error.status ?? 500, { error: error.status ? error.message : 'HTTP transport failed' });
    }
  }

  send(session, frames) {
    if (!Array.isArray(frames) || !frames.length || frames.length > 128) throw fail(400, 'invalid frame batch');
    for (const frame of frames) {
      if (!frame || typeof frame !== 'object' || typeof frame.streamId !== 'string' ||
        !/^[A-Za-z0-9_-]{1,128}$/.test(frame.streamId) || !['open', 'item', 'end', 'cancel'].includes(frame.type)) {
        throw fail(400, 'invalid frame');
      }
      if (frame.type === 'open' && (typeof frame.endpoint !== 'string' || frame.endpoint.length > 256 || !Object.hasOwn(frame, 'payload'))) {
        throw fail(400, 'invalid stream open');
      }
    }
    try {
      for (const frame of frames) {
        if (session.closed) throw fail(410, 'transport session expired');
        this.metrics.framesReceived++;
        if (frame.type === 'open') {
          if (session.streams.has(frame.streamId)) throw fail(400, 'duplicate stream id');
          if (session.streams.size >= 128) throw fail(429, 'stream capacity exceeded');
          const stream = { id: frame.streamId, abort: new AbortController(), uplink: new Uplink() };
          session.streams.set(stream.id, stream);
          const task = this.run(session, stream, frame);
          this.tasks.add(task);
          void task.finally(() => this.tasks.delete(task));
          continue;
        }
        const stream = session.streams.get(frame.streamId);
        if (!stream) continue; // A Host-completed stream may still have in-flight uplink frames.
        if (frame.type === 'item') stream.uplink.push(frame.value);
        if (frame.type === 'end') stream.uplink.end();
        if (frame.type === 'cancel') {
          stream.abort.abort();
          void stream.uplink.return();
        }
      }
    } catch (error) {
      this.close(session);
      throw error;
    }
  }

  async run(session, stream, frame) {
    try {
      const source = await this.gateway.wireStream.open(frame.endpoint, frame.payload,
        stream.uplink, session.peer, stream.abort.signal);
      for await (const value of source) {
        if (stream.abort.signal.aborted || session.closed) break;
        this.enqueue(session, { type: 'item', streamId: stream.id, ...(value === undefined ? {} : { value }) });
      }
      if (!stream.abort.signal.aborted && !session.closed) this.enqueue(session, { type: 'end', streamId: stream.id });
    } catch (error) {
      if (!stream.abort.signal.aborted && !session.closed) {
        this.enqueue(session, { type: 'error', streamId: stream.id, error: this.gateway.wireStream.failure(error) });
      }
    } finally {
      session.streams.delete(stream.id);
      await stream.uplink.return();
    }
  }

  enqueue(session, frame) {
    if (session.closed) return;
    const item = { seq: ++session.seq, ...frame };
    const size = bytes(item);
    if (session.queueBytes + size > QUEUE_LIMIT) { this.close(session); return; }
    session.queue.push({ item, size });
    session.queueBytes += size;
    this.metrics.framesSent++;
    session.poll?.flush();
  }

  poll(session, after, res) {
    if (!Number.isSafeInteger(after) || after < session.acknowledged || after > session.seq) throw fail(409, 'invalid poll cursor');
    if (session.poll) throw fail(409, 'only one poll per session is allowed');
    this.metrics.polls++;
    session.acknowledged = after;
    while (session.queue.length && session.queue[0].item.seq <= after) session.queueBytes -= session.queue.shift().size;
    const take = () => {
      const frames = [];
      let size = 0;
      for (const entry of session.queue) {
        if (frames.length && (frames.length >= 256 || size + entry.size > 512 * 1024)) break;
        frames.push(entry.item);
        size += entry.size;
      }
      return { frames, cursor: frames.at(-1)?.seq ?? after };
    };
    if (session.queue.length) return reply(res, 200, take());
    let timer;
    const finish = (status, value) => {
      clearTimeout(timer);
      res.off('close', onClose);
      session.poll = undefined;
      reply(res, status, value);
    };
    const onClose = () => { clearTimeout(timer); session.poll = undefined; };
    session.poll = {
      flush: () => finish(200, take()),
      close: () => finish(410, { error: 'transport session expired' }),
    };
    res.once('close', onClose);
    timer = setTimeout(() => session.poll?.flush(), this.pollMs).unref();
  }

  close(session) {
    if (session.closed) return;
    session.closed = true;
    this.sessions.delete(session.id);
    session.peer.signal.removeEventListener('abort', session.onPeerAbort);
    session.poll?.close();
    for (const stream of session.streams.values()) {
      stream.abort.abort();
      void stream.uplink.return();
    }
    session.queue.length = 0;
    session.queueBytes = 0;
  }

  async dispose() {
    clearInterval(this.sweep);
    for (const session of this.sessions.values()) this.close(session);
    await Promise.allSettled([...this.tasks]);
  }
}
