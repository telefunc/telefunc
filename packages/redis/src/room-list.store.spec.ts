// `Room.list()` whose read the store refuses. A room listed as open needs no repair of its listing, so the call wrote
// nothing, and it says it didn't run; called again once the store allows the read, it lists the room. One that
// cleaned up a lapsed room on its first page and fails on its second may have run: the call's writes are one account.
import { setTimeout as sleep } from 'node:timers/promises'
import { isDeepStrictEqual } from 'node:util'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

/** Every key of Telefunc's in the store, with its value as DUMP gives it. */
async function snapshot(admin: Redis): Promise<Map<string, Buffer | null>> {
  const keys = (await admin.keys('tf:*')).sort()
  return new Map(await Promise.all(keys.map(async (key) => [key, await admin.dumpBuffer(key)] as const)))
}

describe.runIf(runs)('Room.list() whose read the store refuses', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  let app: Redis | undefined
  afterAll(() => {
    app?.disconnect()
    proxy?.close()
    removeStores()
  })

  /** A new store of the row's, which the backend reaches through the forwarder, logging in as `telefunc`. */
  async function reached(store: Store): Promise<Redis> {
    if (proxy === undefined) {
      proxy = await forwarder()
      app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
      app.on('error', () => {})
      installRedis(app)
    }
    const { host, admin } = await startStore(store)
    await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
    proxy.to(host)
    // Pointing the forwarder at this store dropped the backend's connections to the last one: a read gets past the
    // store, to a room that isn't there, once they are back.
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

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is told 'not-run' where nothing needed repair, and lists the room once allowed`, async () => {
      const admin = await reached(store)
      const id = `r-${i}`
      await Room.create(id)

      await admin.call('ACL', 'SETUSER', 'telefunc', '-hget')
      const before = await snapshot(admin)
      const refused = await Room.list().then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      await admin.call('ACL', 'SETUSER', 'telefunc', '+hget')
      const listed = (await Room.list()).map((room) => room.id)
      admin.disconnect()

      expect({ refused, unchanged, listed }).toMatchObject({
        refused: { name: 'BackendError', code: 'denied', outcome: 'not-run' },
        unchanged: true,
        listed: [id],
      })
    }, 60_000)

    it(`${store.name}: may have run where its first page cleaned up a lapsed room and its second page's read fails`, async () => {
      const admin = await reached(store)
      // Sorted by id: the lapsed room and 99 open ones fill the first page of 100, the last open one is the second.
      const lapsed = `a-${i}-lapsed`
      const open = Array.from({ length: 100 }, (_, n) => `b-${i}-${String(n).padStart(3, '0')}`)
      for (const id of [lapsed, ...open]) await Room.create(id)
      // Its head is gone, as once it lapsed: its listing and its incarnation's keys are left for a list to clean up.
      await admin.del(`tf:0:room:{${lapsed}}:head`)
      const lapsedKeys = () => admin.keys(`tf:0:room:{${lapsed}}:*`)
      const left = (await lapsedKeys()).length
      // HGET is refused on the second page's room only: a selector allows it on the directory and every other room.
      await admin.call(
        'ACL',
        'SETUSER',
        'telefunc',
        '-hget',
        `(+hget ~*room-dir:* ~*room:{a-${i}-* ~*room:{b-${i}-0[0-8]?}* ~*room:{b-${i}-09[0-8]}*)`,
      )
      const refused = await Room.list().then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const cleaned = {
        keys: (await lapsedKeys()).length,
        listed: await admin.zscore('tf:0:room-dir:{tf:0:dir}:index', lapsed),
      }
      admin.disconnect()

      expect({ left: left > 0, refused, cleaned }).toMatchObject({
        left: true,
        refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
        cleaned: { keys: 0, listed: null },
      })
    }, 60_000)
  }
})
