import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http'
import { Hono } from 'hono'
import vike from '@vikejs/hono'
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/node'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from './cleanup-state'

config.channel.pingInterval = 1000_000
config.shield = true

const INST = process.env.INSTANCE_ID ?? '?'

if (process.env.REDIS_URL) {
  installRedis(new IORedis(process.env.REDIS_URL), { instanceId: INST })
  console.log(`[INST=${INST}] Redis substrate installed`)
}

// Translate Ctrl-C / docker-stop into a clean `process.exit(0)`. Without this, Node's
// default SIGINT/SIGTERM handlers tear the process down without flushing the V8 CPU
// profile written by `--cpu-prof`, leaving `profiles/` empty.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => process.exit(0))
}

const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')

const tf = telefunc()
const app = new Hono()

// Bench toggle: when `TELEFUNC_NATIVE=1` the `/_telefunc` route feeds the raw
// Node `IncomingMessage` / `ServerResponse` into `tf.serve({ req, res })`,
// which skips webstreams on both directions. Default is the Web Request path
// (`tf.serve({ request })`) so the same playground exercises both setups
// without code edits — flip the env var between bench runs to A/B them.
const USE_NATIVE = process.env.TELEFUNC_NATIVE === '1'
console.log(`[INST=${INST}] /_telefunc adapter: ${USE_NATIVE ? 'node-native (req/res)' : 'web request'}`)

app.get('/api/cleanup-state', async (c) => c.json(await getCleanupStateSnapshot()))
app.post('/api/cleanup-state/reset', async (c) => {
  await resetCleanupState()
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

app.all('/_telefunc', async (c) => {
  if (USE_NATIVE) {
    console.log(`[INST=${INST}] Handling /_telefunc via node-native adapter`)
    // srvx attaches `runtime.node = { req, res }` to the Request it constructs
    // from each `IncomingMessage`. Reaching through `c.req.raw` to those Node
    // primitives lets `tf.serve({ req, res })` bypass webstreams on both
    // request body parsing and response writing.
    const { req, res } = (c.req.raw as unknown as { runtime: { node: { req: IncomingMessage; res: ServerResponse } } })
      .runtime.node
    await tf.serve({ req, res })
    return new Response(null)
  }
  console.log(`[INST=${INST}] Handling /_telefunc via web request adapter`)
  const response = await tf.serve({ request: c.req.raw })
  return response ?? c.text('Not found', 404)
})

vike(app)

// HTTPS via `certs/localhost.{pem,-key.pem}` next to the playground root (resolved
// from `process.cwd()` — start the server from `test/playground/`). Generate:
//   mkcert -install && cd test/playground && mkdir -p certs && cd certs && mkcert localhost
// Falls back to plain HTTP/1.1 otherwise. Docker compose sets `NO_HTTPS=1` because
// Caddy is the TLS terminator there and the certs are visible via the bind mount.
// srvx negotiates HTTP/2 vs HTTP/1.1 automatically via ALPN when `tls` is set.
const certDir = path.resolve(process.cwd(), 'certs')
const certPath = path.join(certDir, 'localhost.pem')
const keyPath = path.join(certDir, 'localhost-key.pem')
const httpsAvailable = !process.env.NO_HTTPS && fs.existsSync(certPath) && fs.existsSync(keyPath)
const tls = httpsAvailable ? { cert: certPath, key: keyPath } : undefined

export default {
  fetch: app.fetch,
  prod: {
    port: Number(process.env.PORT) || (httpsAvailable ? 8443 : 3000),
    ...(tls ? { tls } : {}),
    onCreate(server: { node?: { server?: HttpServer } }) {
      // srvx wraps the raw `http.Server` as `{ node: { server } }`. Unwrap to
      // the underlying Node server so the WebSocket upgrade listener can be
      // installed directly on it. The HTTP request path goes through Hono
      // normally — the `/_telefunc` route reads `c.env.incoming`/`outgoing`
      // to reach the Node primitives when `TELEFUNC_NATIVE=1`.
      const httpServer = server?.node?.server
      if (httpServer) tf.installWebSocket(httpServer)
    },
  },
}
