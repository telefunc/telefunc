// A join whose `onBeforeJoin` grows its nested meta past the size the store commits, on a Cluster of the oldest and the
// newest Redis and Valkey Telefunc supports: it is refused with nothing stored, and the inbox it opened is unsubscribed.
// The subscriber's UNSUBSCRIBE is held until the row saw the inbox still subscribed on the node the subscriber is on,
// then let go, and the row sees it gone there. A Cluster's PUBSUB NUMSUB counts the subscribers of the node it asks, so
// the row asks each node and adds them up.
import { isDeepStrictEqual } from 'node:util'
import { setTimeout as sleep } from 'node:timers/promises'
import type { Cluster, Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import { appCluster, startCluster, type Forwarder, type Nodes } from '../test/cluster.js'
import { forwarder, removeStores, runs, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'
import { RedisConnection } from './RedisConnection.js'

async function snapshot(admin: Redis): Promise<Map<string, unknown>> {
  const keys = (await admin.keys('tf:*')).sort()
  const held = async (key: string) =>
    (await admin.type(key)) === 'hash'
      ? Object.entries(await admin.hgetall(key)).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      : await admin.dumpBuffer(key)
  return new Map(await Promise.all(keys.map(async (key) => [key, await held(key)] as const)))
}

function changedKeys(before: Map<string, unknown>, after: Map<string, unknown>): string[] {
  const keys = new Set([...before.keys(), ...after.keys()])
  return [...keys].filter((key) => !isDeepStrictEqual(before.get(key), after.get(key))).sort()
}

/** While set, the next UNSUBSCRIBE of the backend's subscriber tells its channels and waits for `release`. */
let unsubscribeHeld: { reached: PromiseWithResolvers<string[]>; release: PromiseWithResolvers<void> } | null = null

let via: [Forwarder, Forwarder] | undefined
let app: Cluster | undefined
beforeAll(() => {
  const open = RedisConnection.prototype.openSubscriberSocket
  // Every subscriber socket the backend opens, its first included, holds an UNSUBSCRIBE while the row asks.
  vi.spyOn(RedisConnection.prototype, 'openSubscriberSocket').mockImplementation(async function (
    this: RedisConnection,
  ) {
    const socket = await open.call(this)
    const unsubscribe = socket.unsubscribe.bind(socket)
    socket.unsubscribe = async (...channels: string[]) => {
      const held = unsubscribeHeld
      if (held !== null) {
        unsubscribeHeld = null
        held.reached.resolve(channels)
        await held.release.promise
      }
      return unsubscribe(...channels)
    }
    return socket
  })
})
afterAll(() => {
  vi.restoreAllMocks()
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
  removeStores()
})
/** The rooms a row created, closed after it, before the next row's Cluster replaces this one. */
const opened: string[] = []
afterEach(async () => {
  unsubscribeHeld = null
  for (const id of opened.splice(0)) await Room.close(id).catch(() => {})
})

/** A new Cluster for the row, and the app's client of it installed as the backend's; reached once a read of a room
 *  that isn't there gets past the store. */
async function reached(store: Store): Promise<Nodes> {
  const proxies = (via ??= [await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  for (const { admin } of [nodes.primary, nodes.replica]) {
    await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  }
  app = appCluster(proxies, nodes)
  installRedis(app)
  const answers = () =>
    Room.get('r-none').then(
      () => true,
      (error: unknown) => !/ECONNRE|Connection is closed|ETIMEDOUT|EPIPE|connect|isn't ready/i.test(String(error)),
    )
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 400) throw new Error(`the backend never reached the ${store.name} Cluster`)
    await sleep(25)
  }
  return nodes
}

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return { name: error.name, outcome: 'outcome' in error ? String(error.outcome) : 'none' }
}

/** Each channel's subscribers, added up over the Cluster's nodes. */
async function subscribers({ primary, replica }: Nodes, channels: readonly string[]): Promise<number[]> {
  const counts = await Promise.all(
    [primary, replica].map(async ({ admin }) => {
      const reply = await admin.call('PUBSUB', 'NUMSUB', ...channels)
      return Array.isArray(reply) ? reply.filter((_, i) => i % 2 === 1).map(Number) : []
    }),
  )
  return channels.map((_, i) => counts.reduce((sum, node) => sum + (node[i] ?? 0), 0))
}

async function until(check: () => Promise<boolean>, ms: number): Promise<boolean> {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(20)) if (await check()) return true
  return check()
}

describe.runIf(runs)('a join whose hook grows its nested meta past the size the store commits, on a Cluster', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: refused with nothing stored, and its inbox subscribed until its UNSUBSCRIBE, then gone`, async () => {
      const nodes = await reached(store)
      const id = `join-cluster-${i}`
      const room = await Room.create(id)
      opened.push(id)
      let growing = false
      let hookRan = false
      Room.guard(room, {
        onBeforeJoin: ({ meta }) => {
          if (!growing) return
          hookRan = true
          const nested = meta.grown
          if (nested instanceof Map) nested.set('big', 'g'.repeat(29 * 1024 * 1024))
        },
      })
      // A first member holds this server's lease on the room and its subscriptions, so what the join leaves is what
      // changes.
      await room.join({ meta: { grown: new Map<string, string>() } })
      await sleep(200)
      // The primary serves every slot, so its keys are the Cluster's.
      const before = await snapshot(nodes.primary.admin)
      growing = true
      const held = { reached: Promise.withResolvers<string[]>(), release: Promise.withResolvers<void>() }
      unsubscribeHeld = held
      const refused = room.join({ meta: { grown: new Map<string, string>() } }).then(
        () => 'resolved',
        (error: unknown) => told(error),
      )
      const unsubscribing = await Promise.race([held.reached.promise, sleep(30_000).then(() => [])])
      const inbox = unsubscribing.filter((channel) => channel.includes(':inbox:'))
      const whileHeld = inbox.length === 0 ? [] : await subscribers(nodes, inbox)
      held.release.resolve()
      const gone =
        inbox.length > 0 &&
        (await until(async () => (await subscribers(nodes, inbox)).every((count) => count === 0), 5_000))
      const answer = await refused
      const changed = changedKeys(before, await snapshot(nodes.primary.admin))
      console.log(JSON.stringify({ store: store.name, hookRan, answer, changed, inbox, whileHeld, gone }))

      expect({ hookRan, answer, changed, inboxes: inbox.length, whileHeld, gone }).toEqual({
        hookRan: true,
        answer: { name: 'ChannelOverflowError', outcome: 'none' },
        changed: [],
        inboxes: 1,
        whileHeld: [1],
        gone: true,
      })
    }, 180_000)
  }
})
