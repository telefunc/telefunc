// A close by id acts only on the incarnation its first read saw, on the oldest and the newest Redis and Valkey Telefunc
// supports. Once that incarnation is gone, it resolves as a close of a room already closed does, and writes nothing to
// the room that took the id:
// - a close paused after its first read while the room is closed and created again, then let go;
// - a close that lost its lease to another closer, and then finds a new room under the id.
// And, apart: a room closed by two closers, one taking over the other's lapsed lease, commits two close events, and a
// room handle observes one close.
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import {
  getRoomPlane,
  type CommitOptions,
  type CommitResult,
  type CommitTarget,
  type HeadExchangeResult,
  type HeadExpectation,
  type HeadNext,
} from '@telefunc/room/backend'
import { forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
let app: Redis | undefined
afterAll(() => {
  app?.disconnect()
  proxy?.close()
  removeStores()
})
afterEach(() => {
  vi.restoreAllMocks()
})

/** A new store of the row's, which the backend reaches through the forwarder; reached once a read of a room that isn't
 *  there gets past the store. */
async function reached(store: Store): Promise<Redis> {
  if (proxy === undefined) {
    proxy = await forwarder()
    app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
    app.on('error', () => {})
    installRedis(app)
  }
  removeStores()
  const { host, admin } = await startStore(store)
  await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  proxy.to(host)
  const answers = () =>
    Room.get('r-none').then(
      () => true,
      (error: unknown) => !/ECONNRE|Connection is closed|ETIMEDOUT|EPIPE|connect/i.test(String(error)),
    )
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 200) throw new Error(`the backend never reached ${store.name}`)
    await sleep(25)
  }
  return admin
}

/** The close lease of a room, from packages/room/src/server/constants.ts, and a second past it: the store's own clock
 *  decides when it lapsed. */
const LEASE_LAPSED_MS = 15_000 + 1_000

/** A new room Y under `id`, with a member and meta of its own: what a close of the room before must not touch. */
async function recreated(id: string) {
  await Room.create(id, { meta: { name: 'Y' } })
  const member = await Room.join(id, { meta: { name: 'y1' } })
  const head = await getRoomPlane().direct.reads.readHead(id)
  return { inc: head?.currentInc ?? null, member: member.id }
}

/** What is under `id` once the row's close settled: Y as it was, or not. */
async function after(id: string) {
  const head = await getRoomPlane().direct.reads.readHead(id)
  const room = await Room.get(id).then(
    (got) => ({ name: got.meta.name, closed: got.isClosed }),
    (error: unknown) => String(error),
  )
  const participants = await Room.getParticipants(id).then(
    (listed) => listed.map((each) => each.id),
    (error: unknown) => String(error),
  )
  return { state: head?.state ?? 'none', inc: head?.currentInc ?? null, room, participants }
}

describe.runIf(runs)('a close by id, once the incarnation it first read is gone', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: paused after its first read while the room is closed and created again, it leaves the new room as it is`, async () => {
      const admin = await reached(store)
      const id = `close-pin-read-${i}`
      await Room.create(id, { meta: { name: 'X' } })
      const { reads } = getRoomPlane().direct
      const readHead = reads.readHead.bind(reads)
      const firstRead = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      let held = false
      vi.spyOn(reads, 'readHead').mockImplementation(async (roomId) => {
        const head = await readHead(roomId)
        if (roomId === id && !held) {
          held = true
          firstRead.resolve()
          await release.promise
        }
        return head
      })
      const closing = Room.close(id).then(
        () => 'resolved',
        (error: unknown) => String(error),
      )
      await firstRead.promise
      await Room.close(id)
      const y = await recreated(id)
      release.resolve()
      const answer = await closing
      const seen = { answer, ...(await after(id)) }
      console.log(JSON.stringify({ store: store.name, y, ...seen }))
      admin.disconnect()

      expect(seen).toEqual({
        answer: 'resolved',
        state: 'open',
        inc: y.inc,
        room: { name: 'Y', closed: false },
        participants: [y.member],
      })
    }, 120_000)

    it(`${store.name}: having lost its lease to another closer, then finding a new room under the id, it leaves the new room as it is`, async () => {
      const admin = await reached(store)
      const id = `close-pin-lease-${i}`
      await Room.create(id, { meta: { name: 'X' } })
      const commit = RedisRoomDriver.prototype.commitLane
      const held = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      let holding = false
      vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
        this: RedisRoomDriver,
        target: CommitTarget,
        payload: Uint8Array,
        opts?: CommitOptions,
      ): Promise<CommitResult> {
        // The first closer's close event, which it commits under its lease, waits until its lease is another's.
        if (target.roomId === id && opts?.closingLease !== undefined && !holding) {
          holding = true
          held.resolve()
          await release.promise
        }
        return commit.call(this, target, payload, opts)
      })
      const closing = Room.close(id).then(
        () => 'resolved',
        (error: unknown) => String(error),
      )
      await held.promise
      await sleep(LEASE_LAPSED_MS)
      await Room.close(id)
      const y = await recreated(id)
      release.resolve()
      const answer = await closing
      const seen = { answer, ...(await after(id)) }
      console.log(JSON.stringify({ store: store.name, y, ...seen }))
      admin.disconnect()

      expect(seen).toEqual({
        answer: 'resolved',
        state: 'open',
        inc: y.inc,
        room: { name: 'Y', closed: false },
        participants: [y.member],
      })
    }, 120_000)
  }
})

describe.runIf(runs)('a room closed by two closers, one taking over the lapsed lease of the other', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: two close events are committed, and a room handle observes one close`, async () => {
      const admin = await reached(store)
      const id = `close-observed-${i}`
      const room = await Room.create(id)
      const observed: string[] = []
      room.onClose((cause) => void observed.push(cause.type))
      const commit = RedisRoomDriver.prototype.commitLane
      let closeEvents = 0
      vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
        this: RedisRoomDriver,
        target: CommitTarget,
        payload: Uint8Array,
        opts?: CommitOptions,
      ): Promise<CommitResult> {
        const committed = await commit.call(this, target, payload, opts)
        if (target.roomId === id && opts?.closingLease !== undefined && committed.kind === 'accepted') closeEvents++
        return committed
      })
      const exchangeHead = RedisRoomDriver.prototype.compareExchangeHead
      const held = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      let holding = false
      vi.spyOn(RedisRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
        this: RedisRoomDriver,
        roomId: string,
        expected: HeadExpectation,
        next: HeadNext,
      ): Promise<HeadExchangeResult> {
        // The first closer's finalize waits until another closer took its lapsed lease and finished.
        if (roomId === id && expected.form === 'finalize' && !holding) {
          holding = true
          held.resolve()
          await release.promise
        }
        return exchangeHead.call(this, roomId, expected, next)
      })
      const first = Room.close(id)
      await held.promise
      await sleep(LEASE_LAPSED_MS)
      await Room.close(id)
      release.resolve()
      await first
      await vi.waitFor(() => expect(room.isClosed).toBe(true), { timeout: 10_000 })
      console.log(JSON.stringify({ store: store.name, closeEvents, observed }))
      admin.disconnect()

      expect({ closeEvents, observed }).toEqual({ closeEvents: 2, observed: ['closed'] })
    }, 120_000)
  }
})
