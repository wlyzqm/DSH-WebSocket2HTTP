import { readFile } from 'node:fs/promises';
import { HttpTransport, BASE } from './host.js';

export const name = 'dsh-websocket2http';
export const inject = ['webServer', 'connection', 'typertGateway'];

export async function apply(ctx) {
  const script = await readFile(new URL('./client.js', import.meta.url));
  const transport = new HttpTransport(ctx.connection, ctx.typertGateway);
  ctx.effect(() => {
    const route = ctx.webServer.register({
      kind: 'prefix', path: BASE,
      handler: (req, res) => transport.handle(req, res, script),
    });
    // A blocking script must install the carrier before the stock Client starts.
    const tap = ctx.webServer.tapIndex(html => html.replace(/<head\b[^>]*>/i,
      match => `${match}<script src="api/http-transport/client.js" data-base="api/http-transport"></script>`));
    return async () => {
      tap();
      route();
      await transport.dispose();
    };
  }, 'dsh-websocket2http.routes');
}
