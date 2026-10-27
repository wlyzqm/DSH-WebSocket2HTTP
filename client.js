/* DSH 0.2.1 HTTP carrier. Loaded before the native Connection plugin. */
(() => {
  'use strict';
  const root = globalThis;
  const script = document.currentScript;
  const base = new URL(script?.dataset.base || 'api/http-transport', document.baseURI).href.replace(/\/$/, '');
  if (new URL(base).origin !== location.origin) throw new Error('HTTP transport must be same-origin');
  const MAX_BUFFER_BYTES = 2 * 1024 * 1024;
  const MAX_FRAME_BYTES = 32 * 1024 * 1024;
  const encoder = new TextEncoder();
  let current;
  let opening;
  let stopped = false;
  let nextId = 0;
  let lastFailure;
  const counters = { sessions: 0, polls: 0, sends: 0, received: 0, failures: 0 };

  function carrierError(message, cause) {
    const error = new Error(`DSH HTTP transport: ${message}`, cause === undefined ? undefined : { cause });
    error.dshRemoteStreamFailure = { kind: 'carrier' };
    if (cause?.httpTransportFailure) error.httpTransportFailure = cause.httpTransportFailure;
    return error;
  }
  function remoteError(value) {
    const error = new Error(value.message);
    error.dshRemoteStreamFailure = { kind: 'remote', code: value.code, details: value.details || {} };
    return error;
  }
  function deferred() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return { promise, resolve };
  }
  async function post(action, body, signal, keepalive = false) {
    const response = await fetch(`${base}/${action}`, {
      method: 'POST', credentials: 'same-origin', cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body), signal, keepalive,
    });
    if (response.status !== 200) {
      let body;
      try { body = await response.json(); } catch {}
      // Record only a short machine reason; never retain response bodies,
      // cookies, session identifiers, or free-form server error messages.
      const reason = typeof body?.reason === 'string' && /^[a-z0-9][a-z0-9._-]{0,63}$/i.test(body.reason)
        ? body.reason : undefined;
      const error = carrierError(`${action} returned HTTP ${response.status}`);
      error.httpTransportFailure = { action, status: response.status, ...reason === undefined ? {} : { reason } };
      throw error;
    }
    return response.json();
  }
  function wakeCapacity(session) {
    const waiting = session.capacity;
    session.capacity = deferred();
    waiting.resolve();
  }
  function closeRemote(session) {
    void post('close', { sessionId: session.id }, undefined, true).catch(() => {});
  }
  function failSession(session, cause) {
    if (session.closed) return;
    session.closed = true;
    if (current === session) current = undefined;
    counters.failures++;
    lastFailure = Object.freeze({ message: cause.message, ...cause.httpTransportFailure });
    session.abort.abort(cause);
    wakeCapacity(session);
    for (const channel of [...session.channels.values()]) channel.finish(cause, true);
    closeRemote(session);
  }
  async function poll(session) {
    try {
      while (!session.closed) {
        counters.polls++;
        const result = await post('poll', { sessionId: session.id, after: session.after }, session.abort.signal);
        if (session.closed) return;
        if (!Array.isArray(result.frames) || !Number.isSafeInteger(result.cursor)) throw carrierError('invalid poll response');
        for (const frame of result.frames) {
          if (!Number.isSafeInteger(frame.seq) || frame.seq < 1) throw carrierError('invalid frame sequence');
          if (frame.seq <= session.after) continue;
          if (frame.seq !== session.after + 1) throw carrierError('frame sequence gap');
          if (!['item', 'end', 'error'].includes(frame.type) || typeof frame.streamId !== 'string') throw carrierError('invalid stream frame');
          if (frame.type === 'error' && (!frame.error || typeof frame.error.code !== 'string' || typeof frame.error.message !== 'string')) throw carrierError('invalid stream error');
          const channel = session.channels.get(frame.streamId);
          if (channel) {
            if (frame.type === 'item') await channel.push(frame.value);
            else channel.finish(frame.type === 'error' ? remoteError(frame.error) : undefined);
          }
          if (session.closed) return;
          session.after = frame.seq;
          counters.received++;
        }
        if (result.cursor !== session.after) throw carrierError('poll cursor skipped undelivered frames');
      }
    } catch (error) {
      if (!session.closed) failSession(session, carrierError('poll failed', error));
    }
  }
  async function sessionForStream() {
    if (stopped) throw carrierError('page is closing');
    if (current && !current.closed) return current;
    if (!opening) {
      opening = (async () => {
        const result = await post('open', {});
        if (typeof result.sessionId !== 'string' || !result.sessionId || result.sessionId.length > 256) throw carrierError('invalid session response');
        const session = {
          id: result.sessionId, after: 0, closed: false, channels: new Map(),
          abort: new AbortController(), sends: Promise.resolve(), queuedBytes: 0, bufferedBytes: 0, capacity: deferred(),
        };
        if (stopped) { closeRemote(session); throw carrierError('page is closing'); }
        current = session;
        counters.sessions++;
        void poll(session);
        return session;
      })();
      opening.finally(() => { opening = undefined; }).catch(() => {});
    }
    return opening;
  }
  function send(session, frame) {
    const size = encoder.encode(JSON.stringify(frame)).byteLength;
    if (session.closed) return Promise.reject(carrierError('session has ended'));
    if (session.queuedBytes + size > MAX_BUFFER_BYTES) {
      const error = carrierError('uplink buffer exceeded');
      failSession(session, error);
      return Promise.reject(error);
    }
    session.queuedBytes += size;
    const task = session.sends.then(async () => {
      if (session.closed) throw carrierError('session has ended');
      counters.sends++;
      const result = await post('send', { sessionId: session.id, frames: [frame] }, session.abort.signal);
      if (result.ok !== true) throw carrierError('invalid send acknowledgement');
    });
    session.sends = task.catch(error => {
      failSession(session, carrierError('send failed', error));
    }).finally(() => { session.queuedBytes -= size; });
    return task;
  }

  function openStream(endpoint, payload, signal, uplink) {
    const streamId = `http-${++nextId}`;
    const queue = [];
    const readers = [];
    const stopPump = deferred();
    let session;
    let opened = false;
    let ended = false;
    let failure;
    let bufferedBytes = 0;
    let input;
    const channel = {
      async push(value) {
        if (ended || session.closed) return;
        // A waiting reader consumes this value directly, without using queue
        // capacity. Large snapshots must not be rejected by the queue target.
        if (readers.length) { readers.shift().resolve({ value, done: false }); return; }
        const size = encoder.encode(JSON.stringify(value) ?? 'null').byteLength;
        if (size > MAX_FRAME_BYTES) throw carrierError('downlink frame exceeds 32 MiB');
        while (!ended && !session.closed) {
          if (readers.length) { readers.shift().resolve({ value, done: false }); return; }
          // One indivisible snapshot can exceed the target only when there
          // are no queued values. Otherwise pause ACKs until consumers drain.
          if (session.bufferedBytes === 0 || session.bufferedBytes + size <= MAX_BUFFER_BYTES) {
            queue.push({ value, size });
            session.bufferedBytes += size;
            bufferedBytes += size;
            return;
          }
          await session.capacity.promise;
        }
      },
      finish(error, discard = false) {
        if (ended) return;
        ended = true;
        failure = error;
        signal?.removeEventListener('abort', aborted);
        stopPump.resolve();
        if (input?.return) void Promise.resolve().then(() => input.return()).catch(() => {});
        if (session) {
          session.channels.delete(streamId);
          if (discard) { session.bufferedBytes -= bufferedBytes; bufferedBytes = 0; queue.length = 0; }
          wakeCapacity(session);
        }
        for (const reader of readers.splice(0)) {
          if (failure) reader.reject(failure);
          else reader.resolve({ value: undefined, done: true });
        }
      },
    };
    function cancel(error) {
      if (ended) {
        // The Host can finish before the consumer drains its last items. An
        // early return must also release those already-completed queue bytes.
        if (session) { session.bufferedBytes -= bufferedBytes; wakeCapacity(session); }
        bufferedBytes = 0;
        queue.length = 0;
        failure = error;
        return;
      }
      if (session && opened && !session.closed) void send(session, { type: 'cancel', streamId }).catch(() => {});
      channel.finish(error, true);
    }
    function aborted() {
      cancel(signal.reason || new DOMException('Stream aborted', 'AbortError'));
    }
    async function pump() {
      input = uplink[Symbol.asyncIterator]();
      try {
        while (!ended) {
          const next = await Promise.race([
            input.next(), stopPump.promise.then(() => ({ done: true })),
          ]);
          if (ended) return;
          if (next.done) { await send(session, { type: 'end', streamId }); return; }
          await send(session, { type: 'item', streamId, ...next.value === undefined ? {} : { value: next.value } });
        }
      } catch (error) {
        if (!ended) cancel(carrierError('uplink failed', error));
      }
    }
    if (signal?.aborted) aborted();
    else {
      signal?.addEventListener('abort', aborted, { once: true });
      void (async () => {
        try {
          session = await sessionForStream();
          if (ended) return;
          if (session.closed) throw carrierError('session ended while opening stream');
          session.channels.set(streamId, channel);
          opened = true;
          await send(session, { type: 'open', streamId, endpoint, payload });
          if (!ended && uplink) void pump();
        } catch (error) {
          if (!ended) channel.finish(error.dshRemoteStreamFailure ? error : carrierError('open failed', error), true);
        }
      })();
    }
    return {
      [Symbol.asyncIterator]() { return this; },
      next() {
        if (queue.length) {
          const item = queue.shift();
          bufferedBytes -= item.size;
          session.bufferedBytes -= item.size;
          wakeCapacity(session);
          return Promise.resolve({ value: item.value, done: false });
        }
        if (ended) return failure ? Promise.reject(failure) : Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve, reject) => {
          readers.push({ resolve, reject });
          if (session) wakeCapacity(session);
        });
      },
      return() {
        cancel();
        return Promise.resolve({ value: undefined, done: true });
      },
      throw(error) {
        cancel(error);
        return Promise.reject(error);
      },
    };
  }
  root.__DSH_TRANSPORT__ = { ...root.__DSH_TRANSPORT__, openStream };
  Object.defineProperty(root, '__DSH_HTTP_TRANSPORT__', {
    configurable: true,
    value: Object.freeze({
      mode: 'http-long-poll',
      snapshot: () => Object.freeze({
        ...counters, ...lastFailure === undefined ? {} : { lastFailure }, connected: Boolean(current && !current.closed),
        streams: current?.channels.size || 0, bufferedBytes: current?.bufferedBytes || 0,
      }),
    }),
  });
  root.addEventListener('pagehide', () => {
    stopped = true;
    if (current) failSession(current, carrierError('page closed'));
  });
  root.addEventListener('pageshow', event => {
    // BFCache restores this realm, including the Connection retry loop. Its
    // previous generation was cancelled on pagehide; allow that loop to open
    // a fresh HTTP session instead of treating the restored page as closing.
    if (event.persisted) stopped = false;
  });
})();
