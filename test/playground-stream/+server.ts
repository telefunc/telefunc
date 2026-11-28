import * as fs from 'node:fs'
import * as path from 'node:path'
import type { IncomingMessage, Server as HttpServer, ServerResponse } from 'node:http'
import { Hono } from 'hono'
import vike from '@vikejs/hono'
import IORedis, { Cluster, type Redis } from 'ioredis'
import { installRedis } from '@telefunc/redis'
import { Broadcast, BroadcastChannel, config, onBug, serve } from 'telefunc'
import { Room, type LocalParticipant } from '@telefunc/room'
import { Telefunc } from 'telefunc/node'
import { Telefunc as TelefuncBun } from 'telefunc/bun'
import { Telefunc as TelefuncDeno } from 'telefunc/deno'
import { cleanupState, resetCleanupState, getCleanupStateSnapshot } from './cleanup-state'
import { trackWireSocket, untrackWireSocket, cutWireSockets } from './wire-sockets'
import { runServerAction } from './server-actions'
import { redis } from './redis'
import { installFailingBackend, addBackendFault } from './failing-backend'
import { StallClock } from './stalls'
import { fieldOf, listOf, numberAt, optionalFieldOf, present, textAt, textListOf } from '../checked'

if (process.env.TEST_ASYNC_CONTEXT === '1') await import('telefunc/async_hooks')

config.channel.pingInterval = 1000
config.shield = true
// A lane reaching a channel limit sets it low, e.g. `{"maxMemory":2097152}`.
if (process.env.CHANNEL_CONFIG) Object.assign(config.channel, JSON.parse(process.env.CHANNEL_CONFIG))

const INST = process.env.INSTANCE_ID ?? '?'

// Vike logs these and keeps the process, which Node's default would end.
process.on('unhandledRejection', (err) => {
  cleanupState[`unhandledRejection:${String(err)}`] = 'reported'
})

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
// Vike's middlewares go before the app's routes, its handlers and pages after them.
vike(app)

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
      if (Number(id) !== own) dropped += Number(await node.client('KILL', 'ID', present(id, 'a client id')))
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
      for (const key of textListOf(batch)) keys.push({ key, ttl: await node.pttl(key) })
    }
  }
  return c.json({ keys })
})

// Redis Cluster: moves the slot of `key` to another master one key at a time, as `redis-cli --cluster reshard` does.
app.post('/api/redis/move-slot', async (c) => {
  const masters = redisNodes()
  const first = present(masters[0], 'a Redis master')
  const slot = Number(await first.call('CLUSTER', 'KEYSLOT', present(c.req.query('key'), 'the key query parameter')))
  const ids = await Promise.all(masters.map(async (node) => String(await node.call('CLUSTER', 'MYID'))))
  const owners = String(await first.call('CLUSTER', 'NODES'))
    .split('\n')
    .map((line) => line.split(' '))
    .filter((fields) => fields[2]?.includes('master'))
  const ownsSlot = (fields: string[]) =>
    fields.slice(8).some((range) => {
      const [from, to] = range.split('-').map(Number)
      const start = present(from, 'a slot range')
      return start <= slot && slot <= (to ?? start)
    })
  const sourceId = present(owners.find(ownsSlot)?.[0], "the slot's master")
  const source = present(masters[ids.indexOf(sourceId)], "the slot's master's client")
  const targetId = present(
    ids.find((id) => id !== sourceId),
    'another master',
  )
  const target = present(masters[ids.indexOf(targetId)], "the other master's client")
  const address = owners.find((fields) => fields[0] === targetId)?.[1]?.split('@')[0]
  const [host, port] = present(address, "the other master's address").split(':')
  await target.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', sourceId)
  await source.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', targetId)
  let moved = 0
  for (;;) {
    const keys = textListOf(await source.call('CLUSTER', 'GETKEYSINSLOT', slot, 1))
    if (keys.length === 0) break
    await source.call('MIGRATE', present(host, 'a host'), present(port, 'a port'), '', 0, 5000, 'KEYS', ...keys)
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
      for (const key of textListOf(batch)) {
        const value = await client.dumpBuffer(key)
        if (value)
          snapshot.push({ node, key, ttl: Math.max(0, await client.pttl(key)), value: value.toString('base64') })
      }
    }
  }
  return c.json({ snapshot })
})
app.post('/api/redis/restore', async (c) => {
  const snapshot: RedisSnapshot = listOf(fieldOf(await c.req.json(), 'snapshot')).map((entry) => ({
    node: numberAt(entry, 'node'),
    key: textAt(entry, 'key'),
    ttl: numberAt(entry, 'ttl'),
    value: textAt(entry, 'value'),
  }))
  for (const [node, client] of redisNodes().entries()) {
    const kept = snapshot.filter((entry) => entry.node === node)
    const keys = new Set(kept.map((entry) => entry.key))
    for await (const batch of client.scanStream({ count: 1000 })) {
      for (const key of textListOf(batch)) if (!keys.has(key)) await client.del(key)
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
  /** `join:<name>`, `leave:<name>:<cause>` and `close` as this instance's view saw them, `demand:<track>:<wanted>` as
   *  a track of its member starts or stops being received. */
  events: string[]
  unsubscribe(): void
  /** Removes this instance's room listeners, or adds its event listeners again: its view stops or starts watching. */
  listen(on: boolean): void
}
const crossInstanceRooms = new Map<string, CrossInstanceRoomFixture>()
type CrossInstanceBroadcastFixture = {
  channel: BroadcastChannel<unknown>
  received: unknown[]
  ordering: { seq: number; timestamp: number }[]
  unsubscribe(): void
}
const crossInstanceBroadcasts = new Map<string, CrossInstanceBroadcastFixture>()

const nameOf = (member: { meta: unknown }) => String(optionalFieldOf(member.meta, 'name'))

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
  const listeners = () => [
    room.onJoin((member) => events.push(`join:${nameOf(member)}`)),
    room.onLeave((member, cause) => events.push(`leave:${nameOf(member)}:${cause.type}`)),
    room.onClose(() => events.push('close')),
  ]
  let removers = listeners()
  const participant = await room.join({ meta: { instance: INST, name: c.req.query('name') ?? INST } })
  let demanded = new Set<string>()
  participant.onChange(() => {
    const now = new Set(participant.demand.map(({ track }) => track))
    for (const track of now) if (!demanded.has(track)) events.push(`demand:${track}:true`)
    for (const track of demanded) if (!now.has(track)) events.push(`demand:${track}:false`)
    demanded = now
  })
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
  removers.push(unsubscribe)
  const listen = (on: boolean) => {
    for (const remove of removers) remove()
    removers = on ? listeners() : []
  }
  crossInstanceRooms.set(roomId, { room, participant, others: [], received, frames: [], events, unsubscribe, listen })
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
    commits.push(
      fixture.participant.publishBinary(new Uint8Array([n % 256]), {
        track: present(c.req.query('track'), 'the track query parameter'),
      }),
    )
  }
  await Promise.all(commits)
  return c.json({ ok: true, instance: INST })
})

// Publishes a one-byte frame, the body's number, on `track`.
app.post('/api/room-cross-instance/publish-binary', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const track = present(c.req.query('track'), 'the track query parameter')
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
  const track = present(c.req.query('track'), 'the track query parameter')
  fixture.room.subscribeBinary((data, { from }) => fixture.frames.push(`${track}:${nameOf(from)}:${data[0]}`), {
    track,
  })
  return c.json({ ok: true, instance: INST })
})

// Joins `count` more members at once, named `<name><i>`.
app.post('/api/room-cross-instance/join-many', async (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  const name = present(c.req.query('name'), 'the name query parameter')
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

// `on=false`: this instance's view stops watching the room (its member left before); `on=true`: it watches again.
app.post('/api/room-cross-instance/listen', (c) => {
  const roomId = c.req.query('roomId')
  const fixture = roomId ? crossInstanceRooms.get(roomId) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'room fixture not joined on this instance' }, 404)
  fixture.listen(c.req.query('on') === 'true')
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
  if (c.req.query('target') === 'room') await Room.updateMeta(roomId, attributes)
  else await fixture.participant.updateMeta(attributes)
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
  const participants = fixture.room.isClosed ? [] : fixture.room.getParticipants()
  const members = Object.fromEntries(participants.map((member) => [nameOf(member), member.meta]))
  // A room the store closed before this view saw it holds no members.
  const inStore = await Room.getParticipants(fixture.room.id).catch(() => [])
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
  const ordering: CrossInstanceBroadcastFixture['ordering'] = []
  const readiness = crypto.randomUUID()
  const unsubscribeText = channel.subscribe((data, { seq, timestamp }) => {
    if (data === readiness) return
    received.push(data)
    ordering.push({ seq, timestamp })
  })
  const unsubscribeBinary = channel.subscribeBinary((data, { seq, timestamp }) => {
    received.push(`binary:${data[0]}`)
    ordering.push({ seq, timestamp })
  })
  const unsubscribe = () => {
    unsubscribeText()
    unsubscribeBinary()
  }
  // A publish right after a subscribe waits until the subscription is set up, so other instances' publishes reach it.
  await channel.publish(readiness)
  crossInstanceBroadcasts.set(key, { channel, received, ordering, unsubscribe })
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
  return c.json({ ok: true, instance: INST, seq: receipt.seq, timestamp: receipt.timestamp })
})

app.get('/api/broadcast-cross-instance/received', (c) => {
  const key = c.req.query('key')
  const fixture = key ? crossInstanceBroadcasts.get(key) : undefined
  if (!fixture) return c.json({ ok: false, reason: 'broadcast fixture not subscribed on this instance' }, 404)
  return c.json({ ok: true, instance: INST, received: fixture.received, ordering: fixture.ordering })
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

// The slow-link cards' clock of this process's stalls (stalls.ts), one at a time.
let stallClock: StallClock | null = null
app.post('/api/stalls/start', (c) => {
  stallClock?.stop()
  stallClock = new StallClock('server', Number(c.req.query('minMs')))
  stallClock.start()
  return c.json({ ok: true })
})
app.post('/api/stalls/stop', (c) => {
  const stalls = stallClock?.stop() ?? []
  stallClock = null
  return c.json(stalls)
})

app.get('/api/heap-used', (c) => c.json({ heapUsed: process.memoryUsage().heapUsed }))
app.get('/api/array-buffers', (c) => c.json({ arrayBuffers: process.memoryUsage().arrayBuffers }))

app.post('/api/server-close-trigger', async (c) => {
  const channelId = c.req.query('channelId')
  if (!channelId) return c.json({ ok: false, reason: 'missing channelId' }, 400)
  const store: Map<string, any> | undefined = (globalThis as any)[SERVER_CLOSE_RECONNECT_STORE_KEY]
  const channel = store?.get(channelId)
  if (!channel || channel.isClosed) return c.json({ ok: false, reason: 'channel not found or closed' }, 404)
  cleanupState[`serverClose_${channelId}_ackResult`] = 'pending'
  const ackPromise: Promise<unknown> = channel.send('offline-close', { ack: true })
  // As long as the server keeps the channel for its page that went away (`reconnectTimeout`, 60 s): the page reconnects
  // once it is back, and a backoff step of up to 5 s later.
  const closePromise: Promise<Error | null> = channel.close({ timeout: 60_000 })
  void ackPromise.then(
    (result) => {
      cleanupState[`serverClose_${channelId}_ackResult`] = String(result)
    },
    () => {
      cleanupState[`serverClose_${channelId}_ackResult`] = 'error'
    },
  )
  void closePromise.then((outcome) => {
    cleanupState[`serverClose_${channelId}_closeResult`] = outcome?.name ?? 'clean'
  })
  return c.json({ ok: true })
})

// Test-only: lets an e2e test drop the page's connections, or hold or refuse its next SSE POST.
// 'hold-stream': the next POST that opens an SSE wire gets its answer, and its first batch, which rides on it, is held.
// 'hold-open': the next POST that opens an SSE wire is held whole.
// The others: the next batch POST.
let nextSsePost: 'hold' | 'refuse' | 'hold-stream' | 'hold-open' | null = null
// 'drop': the server drops the page's next WebSocket upgrade before it opens. 'hold': it never answers it.
let nextWebSocket: 'drop' | 'hold' | null = null
// The Node server, for a test that closes all its connections.
let nodeServer: HttpServer | undefined
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
  const action = c.req.query('action') ?? 'drop'
  if (action !== 'drop' && action !== 'hold') return c.json({ ok: false, reason: 'action is drop or hold' }, 400)
  nextWebSocket = action
  return c.json({ ok: true })
})
// As a shutdown does. After this answer went out, since it closes the connection carrying it too.
app.post('/api/server/close-all-connections', (c) => {
  if (!nodeServer) return c.json({ ok: false, reason: 'not on Node' }, 404)
  const server = nodeServer
  setTimeout(() => server.closeAllConnections(), 100)
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

function testUserContext(request: Request) {
  const user = request.headers.get('x-test-user')
  return user === null ? undefined : { user }
}

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
  const response = await tf?.serve({ request: c.req.raw, context: testUserContext(c.req.raw) })
  return response ?? c.text('Not found', 404)
})

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
    const response = await tfBun.serve({ request, server: runtime.bun.server, context: testUserContext(request) })
    // Telefunc's own WebSocket handshake is answered by Bun, once upgraded; one for another path is the app's.
    const upgraded = request.headers.get('upgrade') === 'websocket' && new URL(request.url).pathname === '/_telefunc'
    if (response || upgraded) return response
  }
  if (tfDeno && runtime?.deno) {
    const response = await tfDeno.serve({ request, info: runtime.deno.info, context: testUserContext(request) })
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
      nodeServer = httpServer
      // Compose's servers behind Caddy: Node's 5 s idle timer destroyed sockets Caddy then wrote to, which is a 502.
      if (process.env.INSTANCE_ID) httpServer.keepAliveTimeout = 60_000
      // The app upgrades nothing of its own: the listener below only watches Telefunc's handshakes, so another request
      // with `Upgrade` goes to its request handler (docs/pages/transport: an app's `upgrade` listener answers or closes
      // what it gets).
      Reflect.set(httpServer, 'shouldUpgradeCallback', () => false)
      // Registered first: a socket it destroys is one Telefunc's upgrade listener then leaves alone.
      httpServer.on('upgrade', (req, socket) => {
        if (!req.url?.startsWith('/_telefunc')) return
        const action = nextWebSocket
        nextWebSocket = null
        if (action === 'drop') {
          socket.destroy()
          return
        }
        trackWireSocket(socket)
        // Nothing the server writes reaches the page, so its upgrade gets no answer, as behind a proxy that black-holes
        // WebSockets.
        if (action === 'hold') socket.write = () => true
      })
      tf!.installWebSocket(httpServer)
    },
  },
}
