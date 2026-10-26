import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http'
import { Hono } from 'hono'
import vike from '@vikejs/hono'
import IORedis, { Cluster, type Redis } from 'ioredis'
import { installRedis } from '@telefunc/redis'
import { Broadcast, BroadcastChannel, config, onBug, Room, serve, type LocalParticipant } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { Telefunc as TelefuncBun } from 'telefunc/bun'
import { Telefunc as TelefuncDeno } from 'telefunc/deno'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from './cleanup-state'
import { trackWireSocket, untrackWireSocket, cutWireSockets } from './wire-sockets'
import { runServerAction } from './server-actions'
import { redis } from './redis'
import { installFailingBackend, addBackendFault } from './failing-backend'

config.channel.pingInterval = 1000
config.shield = true
// A lane reaching a channel limit sets it low, e.g. `{"serverReplayBuffer":65536}`.
if (process.env.CHANNEL_CONFIG) Object.assign(config.channel, JSON.parse(process.env.CHANNEL_CONFIG))

const INST = process.env.INSTANCE_ID ?? '?'

onBug((err) => {
  cleanupState[`bug:${err instanceof Error ? err.message : String(err)}`] = 'reported'
})

// The Redis nodes the instances share, for the test routes below.
let redisNodes = (): Redis[] => (redis ? [redis] : [])

if (process.env.REDIS_CLUSTER_NODES) {
  const nodes = process.env.REDIS_CLUSTER_NODES.split(',').map((entry) => {
    const separator = entry.lastIndexOf(':')
    const host = entry.slice(0, separator)
    const port = Number(entry.slice(separator + 1))
    return { host, port }
  })
  const cluster = new Cluster(nodes)
  installRedis(cluster)
  redisNodes = () => cluster.nodes('master')
  console.log(`[INST=${INST}] Redis Cluster backend installed (${nodes.length} seeds)`)
} else if (redis) {
  installRedis(redis)
  console.log(`[INST=${INST}] Redis backend installed`)
} else {
  installFailingBackend()
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

// Test-only: drops every connection to the shared Redis, as a failover or a network cut does, and reads what it holds.
app.post('/api/redis/drop-connections', async (c) => {
  let dropped = 0
  for (const node of redisNodes()) {
    const own = await node.client('ID')
    const clients = String(await node.client('LIST'))
    for (const [, id] of clients.matchAll(/^id=(\d+)/gm)) {
      if (Number(id) !== own) dropped += Number(await node.client('KILL', 'ID', id!))
    }
  }
  return c.json({ dropped })
})
app.get('/api/redis/channels', async (c) => {
  const channels = new Set<string>()
  for (const node of redisNodes()) {
    for (const channel of await node.pubsub('CHANNELS', c.req.query('pattern') ?? '*')) channels.add(String(channel))
  }
  return c.json({ channels: [...channels] })
})
app.get('/api/redis/keys', async (c) => {
  const keys: { key: string; ttl: number }[] = []
  for (const node of redisNodes()) {
    for await (const batch of node.scanStream({ match: c.req.query('pattern') ?? '*', count: 1000 })) {
      for (const key of batch as string[]) keys.push({ key, ttl: await node.pttl(key) })
    }
  }
  return c.json({ keys })
})

// Redis Cluster: moves the slot of `key` to another master one key at a time, as `redis-cli --cluster reshard` does.
app.post('/api/redis/move-slot', async (c) => {
  const masters = redisNodes()
  const slot = Number(await masters[0]!.call('CLUSTER', 'KEYSLOT', c.req.query('key')!))
  const ids = await Promise.all(masters.map(async (node) => String(await node.call('CLUSTER', 'MYID'))))
  const owners = String(await masters[0]!.call('CLUSTER', 'NODES'))
    .split('\n')
    .map((line) => line.split(' '))
    .filter((fields) => fields[2]?.includes('master'))
  const ownsSlot = (fields: string[]) =>
    fields.slice(8).some((range) => {
      const [first, last = first] = range.split('-').map(Number)
      return first! <= slot && slot <= last!
    })
  const sourceId = owners.find(ownsSlot)![0]!
  const source = masters[ids.indexOf(sourceId)]!
  const targetId = ids.find((id) => id !== sourceId)!
  const target = masters[ids.indexOf(targetId)]!
  const [host, port] = owners
    .find((fields) => fields[0] === targetId)![1]!
    .split('@')[0]!
    .split(':')
  await target.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', sourceId)
  await source.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', targetId)
  let moved = 0
  for (;;) {
    const keys = (await source.call('CLUSTER', 'GETKEYSINSLOT', slot, 1)) as string[]
    if (keys.length === 0) break
    await source.call('MIGRATE', host!, port!, '', 0, 5000, 'KEYS', ...keys)
    moved += keys.length
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  for (const node of masters) await node.call('CLUSTER', 'SETSLOT', slot, 'NODE', targetId)
  return c.json({ moved })
})

// Calls installRedis() again with a client for database `db`, as a module evaluated again does; answers its error.
app.post('/api/redis/install-again', async (c) => {
  const client = new IORedis(`${process.env.REDIS_URL}/${c.req.query('db')}`)
  try {
    installRedis(client)
    return c.json({ error: null })
  } catch (error) {
    return c.json({ error: String(error) })
  } finally {
    client.disconnect()
  }
})

type RedisSnapshot = { node: number; key: string; ttl: number; value: string }[]
// A snapshot of every key; restoring it rewinds Redis as a failover onto a replica that missed the writes since does.
app.get('/api/redis/snapshot', async (c) => {
  const snapshot: RedisSnapshot = []
  for (const [node, client] of redisNodes().entries()) {
    for await (const batch of client.scanStream({ count: 1000 })) {
      for (const key of batch as string[]) {
        const value = await client.dumpBuffer(key)
        if (value)
          snapshot.push({ node, key, ttl: Math.max(0, await client.pttl(key)), value: value.toString('base64') })
      }
    }
  }
  return c.json({ snapshot })
})
app.post('/api/redis/restore', async (c) => {
  const { snapshot } = (await c.req.json()) as { snapshot: RedisSnapshot }
  for (const [node, client] of redisNodes().entries()) {
    const kept = snapshot.filter((entry) => entry.node === node)
    const keys = new Set(kept.map((entry) => entry.key))
    for await (const batch of client.scanStream({ count: 1000 })) {
      for (const key of batch as string[]) if (!keys.has(key)) await client.del(key)
    }
    for (const { key, ttl, value } of kept) await client.restore(key, ttl, Buffer.from(value, 'base64'), 'REPLACE')
  }
  return c.json({ ok: true })
})

type CrossInstanceRoomFixture = {
  room: Room
  participant: LocalParticipant
  /** Members joined with `join-many`. */
  others: LocalParticipant[]
  received: unknown[]
  /** Binary frames of the tracks this instance watches: `<track>:<member>:<first byte>`. */
  frames: string[]
  /** `join:<name>`, `leave:<name>:<cause>` and `close` as this instance's view saw them, `demand:<track>:<wanted>`. */
  events: string[]
  unsubscribe(): void
}
const crossInstanceRooms = new Map<string, CrossInstanceRoomFixture>()
type CrossInstanceBroadcastFixture = {
  channel: BroadcastChannel<unknown>
  received: unknown[]
  unsubscribe(): void
}
const crossInstanceBroadcasts = new Map<string, CrossInstanceBroadcastFixture>()

const nameOf = (member: { meta: unknown }) => String((member.meta as { name?: unknown }).name)

// `name` names this instance's member (default: the instance); `departureTimeout` applies to a room it creates.
app.post('/api/room-cross-instance/join', async (c) => {
  const roomId = c.req.query('roomId')
  if (!roomId) return c.json({ ok: false, reason: 'missing roomId' }, 400)
  if (crossInstanceRooms.has(roomId)) return c.json({ ok: true, instance: INST })

  const departureTimeout = c.req.query('departureTimeout')
  const room = await Room.getOrCreate(roomId, {
    meta: { purpose: 'cross-instance-e2e' },
    ...(departureTimeout ? { departureTimeout: Number(departureTimeout) } : {}),
  })
  const events: string[] = []
  room.onJoin((member) => events.push(`join:${nameOf(member)}`))
  room.onLeave((member, cause) => events.push(`leave:${nameOf(member)}:${cause.type}`))
  room.onClose(() => events.push('close'))
  const participant = await room.join({ meta: { instance: INST, name: c.req.query('name') ?? INST } })
  participant.onDemand((track, wanted) => events.push(`demand:${track}:${wanted}`))
  const received: unknown[] = []
  const readiness = crypto.randomUUID()
  const { promise: subscribed, resolve } = Promise.withResolvers<void>()
  const unsubscribe = room.subscribe((data) => {
    if (data === readiness) resolve()
    else received.push(data)
  })
  // `subscribe()` becomes active asynchronously: once this instance's own publish arrives, other instances' publishes do too.
  await participant.publish(readiness)
  await subscribed
  crossInstanceRooms.set(roomId, { room, participant, others: [], received, frames: [], events, unsubscribe })
  return c.json({ ok: true, instance: INST })
})

app.post('/api/room-cross-instance/publish', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  await fixture.participant.publish(await c.req.json(), { retain: c.req.query('retain') === 'true' })
  return c.json({ ok: true, instance: INST })
})

// Publishes `from` and the `count - 1` numbers after it at once, as text and as one-byte frames on `track`.
app.post('/api/room-cross-instance/publish-many', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const from = Number(c.req.query('from'))
  const commits: Promise<unknown>[] = []
  for (let n = from; n < from + Number(c.req.query('count')); n++) {
    commits.push(fixture.participant.publish(n))
    commits.push(fixture.participant.publishBinary(new Uint8Array([n % 256]), { track: c.req.query('track')! }))
  }
  await Promise.all(commits)
  return c.json({ ok: true, instance: INST })
})

// Publishes a one-byte frame, the body's number, on `track`.
app.post('/api/room-cross-instance/publish-binary', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const track = c.req.query('track')!
  await fixture.participant.publishBinary(new Uint8Array([await c.req.json()]), {
    track,
    retain: c.req.query('retain') === 'true',
  })
  return c.json({ ok: true, instance: INST })
})

// Watches every member's frames on `track`.
app.post('/api/room-cross-instance/watch', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const track = c.req.query('track')!
  fixture.room.subscribeBinary((data, _info, from) => fixture.frames.push(`${track}:${nameOf(from)}:${data[0]}`), {
    track,
  })
  return c.json({ ok: true, instance: INST })
})

// Joins `count` more members at once, named `<name><i>`.
app.post('/api/room-cross-instance/join-many', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const name = c.req.query('name')!
  const joined = await Promise.all(
    Array.from({ length: Number(c.req.query('count')) }, (_, i) =>
      fixture.room.join({ meta: { instance: INST, name: `${name}${i}` } }),
    ),
  )
  fixture.others.push(...joined)
  return c.json({ ok: true, instance: INST, joined: joined.length })
})

// This instance's member leaves; its view stays.
app.post('/api/room-cross-instance/member-leave', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  await fixture.participant.leave()
  return c.json({ ok: true, instance: INST })
})

app.post('/api/room-cross-instance/close', async (c) => {
  const roomId = c.req.query('roomId')
  if (!roomId) return c.json({ ok: false, reason: 'missing roomId' }, 400)
  await Room.close(roomId)
  return c.json({ ok: true, instance: INST })
})

// Merges the body into this instance's member's meta, or into the room's with `target=room`.
app.post('/api/room-cross-instance/meta', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!roomId || !fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const attributes = await c.req.json()
  if (c.req.query('target') === 'room') await Room.setAttributes(roomId, attributes)
  else await fixture.participant.setAttributes(attributes)
  return c.json({ ok: true, instance: INST })
})

app.get('/api/room-cross-instance/received', (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  return c.json({ ok: true, instance: INST, received: fixture.received })
})

// What this instance's view shows, by member name, beside the members the store holds.
app.get('/api/room-cross-instance/view', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const participants = fixture.room.isClosed ? [] : await fixture.room.getParticipants()
  const members = Object.fromEntries(participants.map((member) => [nameOf(member), member.meta]))
  // A room the store closed before this view saw it holds no members.
  const inStore = await Room.getParticipants(roomId!).catch(() => [])
  const stored = Object.fromEntries(inStore.map((member) => [nameOf(member), member.meta]))
  return c.json({
    ok: true,
    instance: INST,
    count: fixture.room.count,
    members,
    stored,
    meta: fixture.room.meta,
    frames: fixture.frames,
    events: fixture.events,
  })
})

app.delete('/api/room-cross-instance/leave', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!roomId || !fixture) return c.json({ ok: true, instance: INST })
  crossInstanceRooms.delete(roomId)
  fixture.unsubscribe()
  await Promise.all([fixture.participant, ...fixture.others].map((participant) => participant.leave()))
  return c.json({ ok: true, instance: INST })
})

app.post('/api/broadcast-cross-instance/subscribe', async (c) => {
  const key = c.req.query('key')
  if (!key) return c.json({ ok: false, reason: 'missing key' }, 400)
  if (crossInstanceBroadcasts.has(key)) return c.json({ ok: true, instance: INST })

  const channel = new BroadcastChannel<unknown>({ key })
  const received: unknown[] = []
  const readiness = crypto.randomUUID()
  const unsubscribeText = channel.subscribe((data) => {
    if (data !== readiness) received.push(data)
  })
  const unsubscribeBinary = channel.subscribeBinary((data) => received.push(`binary:${data[0]}`))
  const unsubscribe = () => {
    unsubscribeText()
    unsubscribeBinary()
  }
  // A publish right after a subscribe waits until the subscription is set up, so other instances' publishes reach it.
  await channel.publish(readiness)
  crossInstanceBroadcasts.set(key, { channel, received, unsubscribe })
  return c.json({ ok: true, instance: INST })
})

app.post('/api/broadcast-cross-instance/publish', async (c) => {
  const key = c.req.query('key')
  if (!key) return c.json({ ok: false, reason: 'missing key' }, 400)
  const body = await c.req.json()
  // `kind=binary`: a one-byte frame, the body's number.
  const receipt =
    c.req.query('kind') === 'binary'
      ? await Broadcast.publishBinary(key, new Uint8Array([body]))
      : await Broadcast.publish(key, body)
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

app.get('/api/heap-used', (c) => c.json({ heapUsed: process.memoryUsage().heapUsed }))

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

// Test-only: the in-memory backend fails a store call, or drops a message, when a test asks (failing-backend.ts).
app.post('/api/backend/fault', async (c) => {
  addBackendFault(await c.req.json())
  return c.json({ ok: true })
})

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
  if (node) {
    if (isSse) trackWireSocket(node.req.socket)
    else untrackWireSocket(node.req.socket)
  }
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
