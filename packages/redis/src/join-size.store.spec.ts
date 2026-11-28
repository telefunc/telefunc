// A join whose meta is past the size the store commits, on the oldest and the newest Redis and Valkey Telefunc
// supports: it is refused with nothing stored, and the inbox it opened is unsubscribed. The subscriber's
// UNSUBSCRIBE is held until the row saw the inbox still subscribed, then let go, and the row sees it gone.
import { setTimeout as sleep } from 'node:timers/promises'
import type { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import { changedKeys, endReached, reached, runs, snapshot, STORES, type Store } from '../test/stores.js'
import { RedisConnection } from './RedisConnection.js'

/** While set, the next UNSUBSCRIBE of the backend's subscriber tells its channels and waits for `release`. */
let unsubscribeHeld: { reached: PromiseWithResolvers<string[]>; release: PromiseWithResolvers<void> } | null = null

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
  endReached()
})
afterEach(() => {
  unsubscribeHeld = null
})

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return { name: error.name, outcome: 'outcome' in error ? String(error.outcome) : 'none' }
}

async function subscribers(admin: Redis, channels: readonly string[]): Promise<number[]> {
  const reply = await admin.call('PUBSUB', 'NUMSUB', ...channels)
  const counts = Array.isArray(reply) ? reply.filter((_, i) => i % 2 === 1) : []
  return counts.map(Number)
}

async function until(check: () => Promise<boolean>, ms: number): Promise<boolean> {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(20)) if (await check()) return true
  return check()
}

describe.runIf(runs)('a join whose meta is past the size the store commits', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: refused with nothing stored, and its inbox subscribed until its UNSUBSCRIBE, then gone`, async () => {
      const admin = await reached(store)
      const id = `join-${i}`
      const room = await Room.create(id)
      Room.guard(room, { anyoneCanJoin: true })
      // A first member holds this server's lease on the room and its subscriptions, so what the join leaves is what
      // changes.
      await room.join({ meta: { name: 'first' } })
      await sleep(200)
      const before = await snapshot(admin)
      const held = { reached: Promise.withResolvers<string[]>(), release: Promise.withResolvers<void>() }
      unsubscribeHeld = held
      const refused = room.join({ meta: { big: 'g'.repeat(29 * 1024 * 1024) } }).then(
        () => 'resolved',
        (error: unknown) => told(error),
      )
      const unsubscribing = await Promise.race([held.reached.promise, sleep(30_000).then(() => [])])
      const inbox = unsubscribing.filter((channel) => channel.includes(':inbox:'))
      const whileHeld = inbox.length === 0 ? [] : await subscribers(admin, inbox)
      held.release.resolve()
      const gone =
        inbox.length > 0 &&
        (await until(async () => (await subscribers(admin, inbox)).every((count) => count === 0), 5_000))
      const answer = await refused
      const changed = changedKeys(before, await snapshot(admin))
      console.log(JSON.stringify({ store: store.name, answer, changed, inbox, whileHeld, gone }))
      admin.disconnect()

      expect({ answer, changed, inboxes: inbox.length, whileHeld, gone }).toEqual({
        answer: { name: 'ChannelOverflowError', outcome: 'none' },
        changed: [],
        inboxes: 1,
        whileHeld: [1],
        gone: true,
      })
    }, 180_000)
  }
})
