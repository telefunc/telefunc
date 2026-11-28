// The Deno server of serve/deno.clientLeft.spec.ts, which runs it from Telefunc's sources (`--sloppy-imports` takes
// their `.js` specifiers to the `.ts` files, as Vitest does) in either of Deno's signal modes. Each entry an app hands
// Telefunc a request through listens on a port of its own:
// - `adapter`: `telefunc/deno`, as docs/pages/Telefunc shows it;
// - `stream`: `serve({ request })`, answered with `httpResponse.getReadableWebStream()`, as docs/pages/serve shows it;
// - `string`: `serve({ request })`, answered with `httpResponse.body`, as examples/cloudflare-workers and examples/next.
// And on Deno's `node:http`:
// - `node`: `serve({ url, method, readable, headers })`, answered with `httpResponse.body`, as an Express app does under
//   Deno (examples/babel);
// - `framework`: `serve({ request })` with a `Request` a framework built of it (frameworkRequest.fixture.ts, as Hono's
//   and srvx's Node servers build one: its body `Readable.toWeb(req)`, its signal the framework's own), answered with
//   `httpResponse.body`.
// Its control route, `POST /run {entry, row}` on a port of its own, runs a row's call through an entry from this process
// (deno.clientLeft.client.fixture.ts), and answers with how its client's rest went and what its telefunction saw once
// that settled. It prints `READY {"control":<port>,"entries":{<entry>:<port>,…}}` once every entry listens.

import { createServer } from 'node:http'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './deno.js'
import { serve } from '../node/server/telefunc.js'
import { config } from '../node/server/serverConfig.js'
import { TELEFUNC_FILE, WINDOW_MS, sendRest, startCall } from './deno.clientLeft.client.fixture.js'
import { seen } from './deno.clientLeft.telefunc.js'
import { frameworkRequest } from './frameworkRequest.fixture.js'

config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${TELEFUNC_FILE}`]
config.disableNamingConvention = true
config.channel = { connectTtl: WINDOW_MS }

/** How long a row's telefunction may take to settle once its client's rest is done. */
const SETTLE_MS = 5000

const telefunc = new Telefunc()

/** Each entry's port. */
const entries = {
  adapter: await listenDeno(
    async (request, info) => (await telefunc.serve({ request, info })) ?? new Response(null, { status: 404 }),
  ),
  stream: await listenDeno(async (request) => {
    const httpResponse = await serve({ request })
    return new Response(httpResponse.getReadableWebStream(), responseInit(httpResponse))
  }),
  string: await listenDeno(async (request) => {
    const httpResponse = await serve({ request })
    return new Response(httpResponse.body, responseInit(httpResponse))
  }),
  node: await listenNode((req) => serve({ url: req.url, method: req.method, readable: req, headers: req.headers })),
  framework: await listenNode((req) => serve({ request: frameworkRequest(req, `http://127.0.0.1${req.url}`) })),
}
const control = await listenDeno(async (request) => {
  const { entry, row } = await request.json()
  const socket = await startCall(entries[entry], row)
  const sent = await sendRest(socket, row)
  const uploadEndedAt = performance.now()
  const state = await seen(row.row, SETTLE_MS, uploadEndedAt)
  socket.destroy()
  return Response.json({ sent, ...state })
})
console.log(`READY ${JSON.stringify({ control, entries })}`)

/** `Deno.serve` with `handler`; resolves with its port once it listens. */
function listenDeno(handler) {
  return new Promise((resolve) =>
    Deno.serve({ port: 0, hostname: '127.0.0.1', onListen: ({ port }) => resolve(port) }, handler),
  )
}

/** A `node:http` server that answers with the `httpResponse` `answer` gives for a request, as an app answers with its
 *  string body; resolves with its port once it listens. */
async function listenNode(answer) {
  const server = createServer(async (req, res) => {
    const httpResponse = await answer(req)
    for (const [key, value] of httpResponse.headers) res.setHeader(key, value)
    res.writeHead(httpResponse.statusCode)
    res.end(httpResponse.body)
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  return server.address().port
}

/** The status and headers of Telefunc's `httpResponse`, as a `Response` takes them. */
function responseInit(httpResponse) {
  return { status: httpResponse.statusCode, headers: httpResponse.headers }
}
