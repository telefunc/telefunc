import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http'
import { Hono } from 'hono'
import vike from '@vikejs/hono'
import { installRedis } from '@telefunc/redis'
import { config, serve } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { Telefunc as TelefuncBun } from 'telefunc/bun'
import { Telefunc as TelefuncDeno } from 'telefunc/deno'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from './cleanup-state'
import { trackWireSocket, cutWireSockets } from './wire-sockets'
import { runServerAction } from './server-actions'
import { redis } from './redis'

config.channel.pingInterval = 1000
config.shield = true
// A lane reaching a channel limit sets it low, e.g. `{"serverReplayBuffer":65536}`.
if (process.env.CHANNEL_CONFIG) Object.assign(config.channel, JSON.parse(process.env.CHANNEL_CONFIG))

const INST = process.env.INSTANCE_ID ?? '?'

if (redis) {
  installRedis(redis)
  console.log(`[INST=${INST}] Redis broadcast transport installed`)
}

// Translate Ctrl-C / docker-stop into a clean `process.exit(0)`. Without this, Node's
// default SIGINT/SIGTERM handlers tear the process down without flushing the V8 CPU
// profile written by `--cpu-prof`, leaving `profiles/` empty.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => process.exit(0))
}

const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')

// `pnpm preview:bun` and `pnpm preview:deno` serve the same build through Telefunc's Bun and Deno adapters.
const tfBun = 'Bun' in globalThis ? new TelefuncBun() : undefined
const tfDeno = 'Deno' in globalThis ? new TelefuncDeno() : undefined
// On Bun and Deno, fetch() answers /_telefunc before the app's route.
const tf = tfBun || tfDeno ? undefined : new Telefunc()
const app = new Hono()

// Bench toggle: when `TELEFUNC_NATIVE=1` the `/_telefunc` route feeds the raw
// Node `IncomingMessage` / `ServerResponse` into `tf.serve({ req, res })`,
// which skips webstreams on both directions. Default is the Web Request path
// (`tf.serve({ request })`) so the same playground exercises both setups
// without code edits — flip the env var between bench runs to A/B them.
const USE_NATIVE = process.env.TELEFUNC_NATIVE === '1'
// `TELEFUNC_NATIVE=readable` serves `/_telefunc` as the /serve page's Express example does: `serve({ readable: req })`.
const USE_READABLE = process.env.TELEFUNC_NATIVE === 'readable'
console.log(
  `[INST=${INST}] /_telefunc adapter: ${USE_NATIVE ? 'node-native (req/res)' : USE_READABLE ? 'serve({ readable: req })' : 'web request'}`,
)

app.get('/api/cleanup-state', async (c) => c.json(await getCleanupStateSnapshot()))
app.post('/api/cleanup-state/reset', async (c) => {
  await resetCleanupState()
  return c.json({ ok: true })
})

// Forces server-side GC so tests can deterministically exercise GC-driven cleanup (e.g. a
// dropped passed-callback's stub being reclaimed → its channel closing → context.onClose firing).
// Requires the server to run with `--expose-gc` (see this playground's dev/preview scripts).
app.post('/api/gc', async (c) => {
  // Bun has no --expose-gc: it collects with Bun.gc(true).
  const { gc: exposedGc, Bun: bun } = globalThis as { gc?: () => void; Bun?: { gc(force: boolean): void } }
  const gc = exposedGc ?? (bun && (() => bun.gc(true)))
  if (!gc) return c.json({ ok: false, reason: 'gc not exposed (run node with --expose-gc)' }, 500)
  for (let i = 0; i < 5; i++) {
    gc()
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  return c.json({ ok: true })
})

app.post('/api/server-close-trigger', async (c) => {
  const channelId = c.req.query('channelId')
  if (!channelId) return c.json({ ok: false, reason: 'missing channelId' }, 400)
  const store: Map<string, any> | undefined = (globalThis as any)[SERVER_CLOSE_RECONNECT_STORE_KEY]
  const channel = store?.get(channelId)
  if (!channel || channel.isClosed) return c.json({ ok: false, reason: 'channel not found or closed' }, 404)
  cleanupState[`serverClose_${channelId}_ackResult`] = 'pending'
  const ackPromise: Promise<unknown> = channel.send('offline-close', { ack: true })
  const closePromise: Promise<0 | 1> = channel.close({ timeout: 10_000 })
  void ackPromise.then(
    (result) => {
      cleanupState[`serverClose_${channelId}_ackResult`] = String(result)
    },
    () => {
      cleanupState[`serverClose_${channelId}_ackResult`] = 'error'
    },
  )
  void closePromise.then(
    (result: 0 | 1) => {
      cleanupState[`serverClose_${channelId}_closeResult`] = String(result)
    },
    () => {
      cleanupState[`serverClose_${channelId}_closeResult`] = 'error'
    },
  )
  return c.json({ ok: true })
})

// Test-only: lets an e2e test drop the page's connections, or hold or refuse its next SSE POST.
// 'hold-stream': the next POST that opens an SSE wire gets its answer, and its first batch, which rides on it, is held.
// 'hold-open': the next POST that opens an SSE wire is held whole.
// The others: the next batch POST.
let nextSsePost: 'hold' | 'refuse' | 'hold-stream' | 'hold-open' | null = null
let dropNextWebSocket = false
const heldSsePosts: (() => void)[] = []

app.post('/api/server-action', (c) => {
  const key = c.req.query('key') ?? ''
  return runServerAction(key) ? c.json({ ok: true }) : c.json({ ok: false, reason: `no action ${key}` }, 404)
})
app.post('/api/wire/cut', (c) => c.json({ cut: cutWireSockets() }))
app.post('/api/wire/next-websocket', (c) => {
  dropNextWebSocket = true
  return c.json({ ok: true })
})
app.post('/api/wire/next-sse-post', (c) => {
  const action = c.req.query('action')
  if (action !== 'hold' && action !== 'refuse' && action !== 'hold-stream' && action !== 'hold-open') {
    return c.json({ ok: false, reason: 'action is hold, refuse, hold-stream or hold-open' }, 400)
  }
  nextSsePost = action
  return c.json({ ok: true })
})
app.get('/api/wire/held', (c) => c.json({ held: heldSsePosts.length }))
app.post('/api/wire/release', (c) => {
  const released = heldSsePosts.length
  for (const release of heldSsePosts.splice(0)) release()
  return c.json({ released })
})

app.all('/_telefunc', async (c) => {
  const isSse = c.req.query('_telefunc') === 'sse'
  const node = (c.req.raw as unknown as { runtime?: { node?: { req: IncomingMessage } } }).runtime?.node
  if (isSse && node) trackWireSocket(node.req.socket)
  // A batch POST has a length; the long-lived upload streams without one.
  if (isSse && nextSsePost && c.req.header('content-length')) {
    const body = new Uint8Array(await c.req.arrayBuffer())
    const metadataLength = new DataView(body.buffer).getUint32(0)
    const metadata = JSON.parse(new TextDecoder().decode(body.subarray(4, 4 + metadataLength)))
    const opens = nextSsePost === 'hold-stream' || nextSsePost === 'hold-open'
    const action = opens === Boolean(metadata.streamResponse) ? nextSsePost : null
    if (action) nextSsePost = null
    if (action === 'refuse') return c.text('', 400)
    if (action === 'hold' || action === 'hold-open') await new Promise<void>((resolve) => heldSsePosts.push(resolve))
    const held = action === 'hold-stream'
    const header = body.subarray(0, 4 + metadataLength)
    const request = new Request(c.req.url, {
      method: 'POST',
      headers: c.req.raw.headers,
      body: !held
        ? body
        : new ReadableStream<Uint8Array>({
            async start(controller) {
              controller.enqueue(header)
              await new Promise<void>((resolve) => heldSsePosts.push(resolve))
              controller.enqueue(body.subarray(header.byteLength))
              controller.close()
            },
          }),
      // @ts-ignore duplex is not yet in TypeScript's RequestInit
      duplex: 'half',
    })
    const response = await tf!.serve({ request })
    return response ?? c.text('Not found', 404)
  }
  if (USE_NATIVE) {
    console.log(`[INST=${INST}] Handling /_telefunc via node-native adapter`)
    // srvx attaches `runtime.node = { req, res }` to the Request it constructs
    // from each `IncomingMessage`. Reaching through `c.req.raw` to those Node
    // primitives lets `tf.serve({ req, res })` bypass webstreams on both
    // request body parsing and response writing.
    const { req, res } = (c.req.raw as unknown as { runtime: { node: { req: IncomingMessage; res: ServerResponse } } })
      .runtime.node
    await tf!.serve({ req, res })
    return new Response(null)
  }
  if (USE_READABLE) {
    const { req, res } = (c.req.raw as unknown as { runtime: { node: { req: IncomingMessage; res: ServerResponse } } })
      .runtime.node
    const httpResponse = await serve({ url: req.url!, method: req.method!, readable: req, headers: req.headers })
    res.statusCode = httpResponse.statusCode
    for (const [name, value] of httpResponse.headers) res.setHeader(name, value)
    await httpResponse.pipe(res)
    return new Response(null)
  }
  console.log(`[INST=${INST}] Handling /_telefunc via web request adapter`)
  const response = await tf!.serve({ request: c.req.raw })
  return response ?? c.text('Not found', 404)
})

vike(app)

// HTTPS via `certs/localhost.{pem,-key.pem}` next to the playground root (resolved
// from `process.cwd()` — start the server from `test/playground-stream/`). Generate:
//   mkcert -install && cd test/playground-stream && mkdir -p certs && cd certs && mkcert localhost
// Falls back to plain HTTP/1.1 otherwise. Docker compose sets `NO_HTTPS=1` because
// Caddy is the TLS terminator there and the certs are visible via the bind mount.
// srvx negotiates HTTP/2 vs HTTP/1.1 automatically via ALPN when `tls` is set.
const certDir = path.resolve(process.cwd(), 'certs')
const certPath = path.join(certDir, 'localhost.pem')
const keyPath = path.join(certDir, 'localhost-key.pem')
const httpsAvailable = !process.env.NO_HTTPS && fs.existsSync(certPath) && fs.existsSync(keyPath)
const tls = httpsAvailable ? { cert: certPath, key: keyPath } : undefined

type RuntimeRequest = Request & {
  runtime?: {
    bun?: { server: Parameters<TelefuncBun['serve']>[0]['server'] }
    deno?: { info: Parameters<TelefuncDeno['serve']>[0]['info'] }
  }
}

async function fetch(request: RuntimeRequest): Promise<Response | undefined> {
  const { runtime } = request
  if (tfBun && runtime?.bun) {
    const response = await tfBun.serve({ request, server: runtime.bun.server })
    // An upgraded request is answered by Bun.
    if (response || request.headers.get('upgrade') === 'websocket') return response
  }
  if (tfDeno && runtime?.deno) {
    const response = await tfDeno.serve({ request, info: runtime.deno.info })
    if (response) return response
  }
  return app.fetch(request)
}

export default {
  fetch,
  prod: {
    port: Number(process.env.PORT) || (httpsAvailable ? 8443 : 3000),
    ...(tls ? { tls } : {}),
    // Bun.serve refuses a body over 128 MiB by default; the abort page uploads 150 MB.
    ...(tfBun ? { bun: { websocket: tfBun.websocket, maxRequestBodySize: 2 ** 30 } } : {}),
    onCreate(server: { node?: { server?: HttpServer } }) {
      // srvx wraps the raw `http.Server` as `{ node: { server } }`. Unwrap to
      // the underlying Node server so the WebSocket upgrade listener can be
      // installed directly on it. The HTTP request path goes through Hono
      // normally — the `/_telefunc` route reads `c.env.incoming`/`outgoing`
      // to reach the Node primitives when `TELEFUNC_NATIVE=1`.
      const httpServer = server?.node?.server
      if (!httpServer) return
      // Registered first: a socket it destroys is one Telefunc's upgrade listener then leaves alone.
      httpServer.on('upgrade', (req, socket) => {
        if (!req.url?.startsWith('/_telefunc')) return
        if (dropNextWebSocket) {
          dropNextWebSocket = false
          socket.destroy()
          return
        }
        trackWireSocket(socket)
      })
      tf!.installWebSocket(httpServer)
    },
  },
}
