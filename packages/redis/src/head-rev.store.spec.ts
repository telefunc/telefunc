// A head's revision on the oldest and the newest Redis and Valkey Telefunc supports: the script stores the revision the
// exchange's caller minted, as the head's `rev`, and an exchange that names none is refused before any write, so a
// head's revision never repeats under its id whatever the store's clock does. Two creates of one id with the head's lapse
// between them carry different revisions.
import { isDeepStrictEqual } from 'node:util'
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { getRoomPlane, type HeadNext } from '@telefunc/room/backend'
import { forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
let app: Redis | undefined
afterEach(() => vi.restoreAllMocks())
afterAll(() => {
  app?.disconnect()
  proxy?.close()
  removeStores()
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
      (error: unknown) => !(error instanceof BackendError),
    )
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 200) throw new Error(`the backend never reached ${store.name}`)
    await sleep(25)
  }
  return admin
}

/** The key of room `id`'s head. */
async function headKey(admin: Redis, id: string): Promise<string> {
  const heads = (await admin.keys(`*{${encodeURIComponent(id)}}*`)).filter((key) => key.endsWith(':head'))
  if (heads.length !== 1) throw new Error(`room ${id} has heads ${heads.join(', ')}`)
  return heads[0] ?? ''
}

/** The `rev` of room `id`'s head as the script stored it. */
async function storedRev(admin: Redis, id: string): Promise<unknown> {
  const stored = await admin.get(await headKey(admin, id))
  if (stored === null) return null
  const head: unknown = JSON.parse(stored)
  return typeof head === 'object' && head !== null && 'rev' in head ? head.rev : 'none'
}

/** Every key the store holds, with what it holds. */
async function snapshot(admin: Redis): Promise<[string, Buffer | null][]> {
  const keys = (await admin.keys('*')).sort()
  return await Promise.all(
    keys.map(async (key): Promise<[string, Buffer | null]> => [key, await admin.dumpBuffer(key)]),
  )
}

describe.runIf(runs)("a head's revision", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: each head a script stores carries the revision its exchange's caller minted`, async () => {
      const admin = await reached(store)
      const id = `rev-minted-${i}`
      // Each exchange of the room's head: the revision its caller minted, and the one the script stored.
      const exchanges: { state: string; minted: unknown; stored: unknown }[] = []
      const { compareExchangeHead } = RedisRoomDriver.prototype
      vi.spyOn(RedisRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
        this: RedisRoomDriver,
        roomId,
        expected,
        next,
      ) {
        const result = await compareExchangeHead.call(this, roomId, expected, next)
        if (roomId === id && result.kind === 'committed') {
          exchanges.push({ state: next.head.state, minted: next.head.rev, stored: await storedRev(admin, id) })
        }
        return result
      })
      await Room.create(id, { emptyTimeout: 600_000, departureTimeout: 600_000 })
      await Room.join(id)
      // The first join's extension of the lapse, which nothing awaits.
      await vi.waitFor(() => expect(exchanges).toHaveLength(2), { timeout: 10_000 })
      await Room.close(id)
      console.log(JSON.stringify({ store: store.name, exchanges }))
      admin.disconnect()

      expect({
        states: exchanges.map(({ state }) => state),
        storedAsMinted: exchanges.every(({ minted, stored }) => typeof minted === 'string' && stored === minted),
        apart: new Set(exchanges.map(({ minted }) => minted)).size === exchanges.length,
      }).toEqual({ states: ['open', 'open', 'closing', 'closed'], storedAsMinted: true, apart: true })
    }, 60_000)

    it(`${store.name}: an exchange whose next head names no revision is refused before any write`, async () => {
      const admin = await reached(store)
      const id = `rev-none-${i}`
      await Room.create(id)
      const head = await getRoomPlane().direct.reads.readHead(id)
      if (head === null) throw new Error(`room ${id} has no head`)
      const next: HeadNext = {
        head: { rev: 'rev-dropped', currentInc: head.currentInc, state: 'open', config: head.config },
      }
      Reflect.deleteProperty(next.head, 'rev')
      const before = await snapshot(admin)
      // The store's check throws before it returns a promise.
      const told = await Promise.resolve()
        .then(() => getRoomPlane().direct.compareExchangeHead(id, { form: 'rev', rev: head.rev }, next))
        .then(
          (result) => result.kind,
          (error: unknown) =>
            String(error).includes('the next head must name the revision its caller minted')
              ? 'refused'
              : String(error),
        )
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      console.log(JSON.stringify({ store: store.name, told, unchanged }))
      admin.disconnect()

      expect({ told, unchanged }).toEqual({ told: 'refused', unchanged: true })
    }, 60_000)

    it(`${store.name}: two creates of one id, with the head's lapse between them, carry different revisions`, async () => {
      const admin = await reached(store)
      const id = `rev-again-${i}`
      const revOf = async () => (await getRoomPlane().direct.reads.readHead(id))?.rev ?? null
      await Room.create(id)
      const x = await revOf()
      await Room.close(id)
      // The tombstone lapses: Redis removes its key.
      await admin.del(await headKey(admin, id))
      await Room.create(id)
      const y = await revOf()
      console.log(JSON.stringify({ store: store.name, x, y }))
      admin.disconnect()

      expect({ x: typeof x, y: typeof y, apart: x !== y }).toEqual({ x: 'string', y: 'string', apart: true })
    }, 60_000)
  }
})
