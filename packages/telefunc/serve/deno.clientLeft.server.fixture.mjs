// The Deno server of serve/deno.clientLeft.spec.ts, which runs it from Telefunc's sources (`--sloppy-imports` takes
// their `.js` specifiers to the `.ts` files, as Vitest does) in either of Deno's signal modes. Each request names the
// entry it goes through (`x-entry`):
// - `adapter`: `telefunc/deno`, as docs/pages/Telefunc shows it;
// - `stream`: `serve({ request })`, answered with `httpResponse.getReadableWebStream()`, as docs/pages/serve shows it;
// - `string`: `serve({ request })`, answered with `httpResponse.body`, as examples/cloudflare-workers and examples/next.
// And on a server of its own, Deno's `node:http`:
// - `node`: `serve({ url, method, readable, headers })`, answered with `httpResponse.body`, as an Express app does under
//   Deno (examples/babel);
// - `framework`: `serve({ request })` with a `Request` a framework built of it (its body `Readable.toWeb(req)`, its
//   signal the framework's own, aborted as the socket closes), answered with `httpResponse.body`.
// `/leave?entry=<entry>&row=<row>` makes a call whose client leaves after the answer, from this process
// (`callAndLeave`). It prints `LISTENING <port> <node:http port>` once both listen.

import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './deno.js'
import { serve } from '../node/server/telefunc.js'
import { config } from '../node/server/serverConfig.js'
import { startCall } from './deno.clientLeft.client.fixture.js'

config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}/deno.clientLeft.telefunc.js`]
config.disableNamingConvention = true
config.channel = { connectTtl: 1000 }

const telefunc = new Telefunc()
const nodeServer = createServer(async (req, res) => {
  const httpResponse =
    req.headers['x-entry'] === 'framework'
      ? await serve({ request: frameworkRequest(req) })
      : await serve({ url: req.url, method: req.method, readable: req, headers: req.headers })
  for (const [key, value] of httpResponse.headers) res.setHeader(key, value)
  res.writeHead(httpResponse.statusCode)
  res.end(httpResponse.body)
})
await new Promise((resolve) => nodeServer.listen(0, '127.0.0.1', resolve))
const nodePort = nodeServer.address().port
const server = Deno.serve(
  { port: 0, hostname: '127.0.0.1', onListen: ({ port }) => console.log(`LISTENING ${port} ${nodePort}`) },
  async (request, info) => {
    const url = new URL(request.url)
    if (url.pathname === '/leave')
      return new Response(await callAndLeave(url.searchParams.get('entry'), url.searchParams.get('row')))
    const entry = request.headers.get('x-entry')
    if (entry === 'adapter') return (await telefunc.serve({ request, info })) ?? new Response(null, { status: 404 })
    const httpResponse = await serve({ request })
    const init = { status: httpResponse.statusCode, headers: httpResponse.headers }
    return new Response(entry === 'string' ? httpResponse.body : httpResponse.getReadableWebStream(), init)
  },
)

/** Makes `row`'s call to `keepsFile` through `entry`, with a File of 8 × 64 KiB read 120 ms after the call, and sends its
 *  first 64 KiB; leaves 160 ms after the answer begins. Its client is this process: where the client is another
 *  process, Deno at times fails the read on its own as the client leaves, which hides a read left pending. */
async function callAndLeave(entry, row) {
  const socket = await startCall(
    entry === 'node' || entry === 'framework' ? nodePort : server.addr.port,
    entry,
    row,
    8,
    120,
  )
  await new Promise((resolve) => setTimeout(resolve, 160))
  socket.destroy()
  return 'left'
}

/** The `Request` a framework builds of Deno's `node:http` request `req`, with its own signal. */
function frameworkRequest(req) {
  const gone = new AbortController()
  req.socket.once('close', () => gone.abort())
  return new Request(`http://127.0.0.1${req.url}`, {
    method: req.method,
    headers: Object.entries(req.headers).flatMap(([name, value]) =>
      value === undefined ? [] : [[name, String(value)]],
    ),
    body: Readable.toWeb(req),
    signal: gone.signal,
    duplex: 'half',
  })
}
