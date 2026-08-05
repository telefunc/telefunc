import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http'
import { Hono } from 'hono'
import vike from '@vikejs/hono'
import IORedis, { Cluster } from 'ioredis'
import { installRedis, RedisTransport } from '@telefunc/redis'
import { BroadcastChannel, config, Room } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from './cleanup-state'

config.channel.pingInterval = 1000
config.shield = true

const INST = process.env.INSTANCE_ID ?? '?'

if (process.env.REDIS_CLUSTER_NODES) {
  const nodes = process.env.REDIS_CLUSTER_NODES.split(',').map((entry) => {
    const separator = entry.lastIndexOf(':')
    const host = entry.slice(0, separator)
    const port = Number(entry.slice(separator + 1))
    if (!host || !Number.isInteger(port) || port <= 0 || port > 65_535) {
      throw new Error(`Invalid REDIS_CLUSTER_NODES entry: ${entry}`)
    }
    return { host, port }
  })
  installClusterBackend(new Cluster(nodes, { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } }))
  console.log(`[INST=${INST}] Redis Cluster backend installed (${nodes.length} seeds)`)
} else if (process.env.REDIS_URL) {
  installClusterBackend(new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: 0 }))
  console.log(`[INST=${INST}] Redis backend installed`)
}

function installClusterBackend(redis: IORedis | Cluster): void {
  const installTransport = () => {
    config.broadcast.transport = new RedisTransport({ redis })
  }
  if (INST === 'A') {
    installTransport()
    installRedis(redis)
  } else {
    installRedis(redis)
    installTransport()
  }
}

// Exit cleanly on Docker stop so V8 flushes `--cpu-prof` output.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => process.exit(0))
}

const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')

const tf = new Telefunc()
const app = new Hono()

// `TELEFUNC_NATIVE=1` benchmarks raw Node req/res; the default exercises Web Request.
const USE_NATIVE = process.env.TELEFUNC_NATIVE === '1'
console.log(`[INST=${INST}] /_telefunc adapter: ${USE_NATIVE ? 'node-native (req/res)' : 'web request'}`)

app.get('/api/cleanup-state', async (c) => c.json(await getCleanupStateSnapshot()))
app.post('/api/cleanup-state/reset', async (c) => {
  await resetCleanupState()
  return c.json({ ok: true })
})

type CrossInstanceRoomFixture = {
  participant: { publish(data: unknown): Promise<unknown>; leave(): Promise<void> }
  received: unknown[]
  unsubscribe(): void
}
const crossInstanceRooms = new Map<string, CrossInstanceRoomFixture>()
type CrossInstanceBroadcastFixture = {
  channel: BroadcastChannel<unknown>
  received: unknown[]
  unsubscribe(): void
}
const crossInstanceBroadcasts = new Map<string, CrossInstanceBroadcastFixture>()

app.post('/api/room-cross-instance/join', async (c) => {
  const roomId = c.req.query('roomId')
  if (!roomId) return c.json({ ok: false, reason: 'missing roomId' }, 400)
  if (crossInstanceRooms.has(roomId)) return c.json({ ok: true, instance: INST })

  const room = await Room.getOrCreate(roomId, { meta: { purpose: 'cross-instance-e2e' } })
  const participant = await room.join({ meta: { instance: INST } })
  const received: unknown[] = []
  const unsubscribe = room.subscribe((data) => received.push(data))
  crossInstanceRooms.set(roomId, { participant, received, unsubscribe })
  return c.json({ ok: true, instance: INST })
})

app.post('/api/room-cross-instance/publish', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  await fixture.participant.publish(await c.req.json())
  return c.json({ ok: true, instance: INST })
})

app.get('/api/room-cross-instance/received', (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  return c.json({ ok: true, instance: INST, received: fixture.received })
})

app.delete('/api/room-cross-instance/leave', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!roomId || !fixture) return c.json({ ok: true, instance: INST })
  crossInstanceRooms.delete(roomId)
  fixture.unsubscribe()
  await fixture.participant.leave()
  return c.json({ ok: true, instance: INST })
})

app.post('/api/broadcast-cross-instance/subscribe', async (c) => {
  const key = c.req.query('key')
  if (!key) return c.json({ ok: false, reason: 'missing key' }, 400)
  if (crossInstanceBroadcasts.has(key)) return c.json({ ok: true, instance: INST })

  const channel = new BroadcastChannel<unknown>({ key })
  const received: unknown[] = []
  const unsubscribe = channel.subscribe((data) => received.push(data))
  const readiness = { __clusterReadiness: crypto.randomUUID() }
  await channel.publish(readiness)
  await waitUntil(() =>
    received.some(
      (data) =>
        typeof data === 'object' &&
        data !== null &&
        '__clusterReadiness' in data &&
        data.__clusterReadiness === readiness.__clusterReadiness,
    ),
  )
  received.length = 0
  crossInstanceBroadcasts.set(key, { channel, received, unsubscribe })
  return c.json({ ok: true, instance: INST })
})

app.post('/api/broadcast-cross-instance/publish', async (c) => {
  const key = c.req.query('key')
  if (!key) return c.json({ ok: false, reason: 'missing key' }, 400)
  const channel = new BroadcastChannel<unknown>({ key })
  const receipt = await channel.publish(await c.req.json())
  await channel.close()
  return c.json({ ok: true, instance: INST, seq: receipt.seq })
})

app.get('/api/broadcast-cross-instance/received', (c) => {
  const key = c.req.query('key')
  const fixture = key ? crossInstanceBroadcasts.get(key) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'broadcast fixture not subscribed on this instance' }, 404)
  return c.json({ ok: true, instance: INST, received: fixture.received })
})

app.delete('/api/broadcast-cross-instance/unsubscribe', async (c) => {
  const key = c.req.query('key')
  const fixture = key ? crossInstanceBroadcasts.get(key) : undefined
  if (!key || !fixture) return c.json({ ok: true, instance: INST })
  crossInstanceBroadcasts.delete(key)
  fixture.unsubscribe()
  await fixture.channel.close()
  return c.json({ ok: true, instance: INST })
})

// Deterministically exercise GC cleanup; playground scripts enable `--expose-gc`.
app.post('/api/gc', async (c) => {
  const gc = (globalThis as { gc?: () => void }).gc
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

app.all('/_telefunc', async (c) => {
  if (USE_NATIVE) {
    console.log(`[INST=${INST}] Handling /_telefunc via node-native adapter`)
    // srvx exposes raw Node req/res under `c.req.raw.runtime.node` for this adapter.
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

// Use local mkcert files when present; Docker's Caddy terminates TLS when `NO_HTTPS=1`.
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
      // Unwrap srvx's Node server so Telefunc can install its WebSocket upgrade handler.
      const httpServer = server?.node?.server
      if (httpServer) tf.installWebSocket(httpServer)
    },
  },
}

async function waitUntil(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${timeoutMs} ms`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
