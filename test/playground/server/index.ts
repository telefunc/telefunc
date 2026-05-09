import * as fs from 'node:fs'
import * as path from 'node:path'
import * as http2 from 'node:http2'
import { apply, serve } from '@photonjs/hono'
import { Hono } from 'hono'
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/node'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from '../cleanup-state'
config.channel.pingInterval = 1000
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

function startServer() {
  const app = new Hono()

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

  apply(app)

  app.all('/_telefunc', async (c) => {
    const url = new URL(c.req.url)
    const kind = c.req.header('x-telefunc-request') ?? '-'
    const len = c.req.header('content-length') ?? '-'
    const accept = c.req.header('accept') ?? '-'
    console.log(
      `[INST=${INST}] ${c.req.method} ${url.pathname}${url.search} kind=${kind} bytes=${len} accept=${accept}`,
    )
    const response = await tf.serve({ request: c.req.raw })
    if (response) return response
    return c.text('Not found', 404)
  })

  // HTTP/2 + TLS when `certs/localhost.{pem,-key.pem}` exists next to the playground root
  // (resolved from `process.cwd()` — start the server from `test/playground/`). Generate:
  //   mkcert -install && cd test/playground && mkdir -p certs && cd certs && mkcert localhost
  // Falls back to plain HTTP/1.1 otherwise. Docker compose sets `NO_HTTPS=1` because Caddy
  // is the TLS terminator there and the certs are visible via the bind mount.
  const certDir = path.resolve(process.cwd(), 'certs')
  const certPath = path.join(certDir, 'localhost.pem')
  const keyPath = path.join(certDir, 'localhost-key.pem')
  const httpsAvailable = !process.env.NO_HTTPS && fs.existsSync(certPath) && fs.existsSync(keyPath)
  const httpsOpts = httpsAvailable
    ? {
        createServer: http2.createSecureServer,
        serverOptions: {
          cert: fs.readFileSync(certPath),
          key: fs.readFileSync(keyPath),
          allowHTTP1: true,
        },
      }
    : {}
  return serve(app, {
    port: Number(process.env.PORT) || (httpsAvailable ? 8443 : 3000),
    ...httpsOpts,
    onCreate(server) {
      // @ts-expect-error srvx types not exposed
      tf.installWebSocket(server.node.server)
    },
  })
}

export default startServer()
