// `Room.list()` whose read the store refuses: a room listed as open needs no repair of its listing, so the call wrote
// nothing, and it says it didn't run. Called again once the store allows the read, it lists the room.
import { setTimeout as sleep } from 'node:timers/promises'
import { isDeepStrictEqual } from 'node:util'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { forwarder, removeStores, runs, STORES, startStore } from '../test/stores.js'
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

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is told 'not-run' where nothing needed repair, and lists the room once allowed`, async () => {
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
      const reached = () =>
        Room.get('r-none').then(
          () => true,
          (error: unknown) => !(error instanceof BackendError),
        )
      for (let tries = 0; !(await reached()); tries++) {
        if (tries > 200) throw new Error(`the backend never reached ${store.name}`)
        await sleep(25)
      }
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
  }
})
