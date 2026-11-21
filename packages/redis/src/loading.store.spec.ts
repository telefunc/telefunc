// A store that is loading its data set answers LOADING. Telefunc tells such a call `'not-run'`: here, on the oldest and
// the newest Redis and Valkey Telefunc supports, each in Docker, every public write made while the store loads is
// refused, and once the load is over no key of Telefunc's is in the store. A machine without Docker skips them; CI runs
// them.
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError, Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { installRedis } from './index.js'
import { docker, forwarder, hostOf, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'

/** Keys of the data set the store loads, and the wait per key while loading it: about 6 s of LOADING. */
const LOADED_KEYS = 200_000
const LOAD_DELAY_US = 30

/** A client of the store at `host` that sends its commands while the store loads, as ioredis's ready check would wait
 *  out the load. */
function clientOf(host: string): Redis {
  const redis = new Redis({ host, enableReadyCheck: false, retryStrategy: () => 50, maxRetriesPerRequest: null })
  redis.on('error', () => {})
  return redis
}

async function loading(admin: Redis): Promise<boolean> {
  return /^loading:1\r?$/m.test(await admin.info('persistence'))
}

/** The store, restarted with a data set it loads slowly; its admin client once the store answers LOADING. */
async function loadingStore(store: Store): Promise<{ host: string; admin: Redis }> {
  const { name, admin: started } = await startStore(
    store,
    ...['--enable-debug-command', 'yes', '--key-load-delay', String(LOAD_DELAY_US)],
    // It answers clients while it loads only between such chunks of its data set, by default 2 MB.
    ...['--loading-process-events-interval-bytes', '1024'],
  )
  started.disconnect()
  const host = () => hostOf(name)
  const filler = clientOf(host())
  await filler.call('DEBUG', 'POPULATE', String(LOADED_KEYS), 'loaded')
  await filler.call('SAVE')
  filler.disconnect()
  docker('restart', '--time', '0', name)
  const admin = clientOf(host())
  for (let tries = 0; !(await loading(admin).catch(() => false)); tries++) {
    if (tries > 200) throw new Error(`${store.name} never answered LOADING`)
    await sleep(10)
  }
  return { host: host(), admin }
}

describe.runIf(runs)('a write made while the store loads its data set', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  let app: Redis | undefined
  afterAll(() => {
    app?.disconnect()
    proxy?.close()
    removeStores()
  })

  for (const store of STORES) {
    it(`${store.name}: is told 'not-run', and the store holds no key of Telefunc's after the load`, async () => {
      if (proxy === undefined) {
        proxy = await forwarder()
        app = new Redis({ port: proxy.port, enableReadyCheck: false, retryStrategy: () => 50, lazyConnect: true })
        app.on('error', () => {})
        installRedis(app)
      }
      const { host, admin } = await loadingStore(store)
      proxy.to(host)
      // A read answered LOADING: the backend's command client reached this store, so the writes below are its own.
      for (let tries = 0; ; tries++) {
        const read = await Room.get('r-none').catch((error: unknown) => error)
        if (read instanceof BackendError && read.code === 'unavailable') break
        if (tries > 200) throw new Error(`the backend never reached ${store.name} while it loads`)
        await sleep(10)
      }
      const started = Date.now()
      const told = (call: () => Promise<unknown>) =>
        call().then(
          () => ({ answered: true, ms: Date.now() - started }),
          (error: unknown) =>
            error instanceof BackendError
              ? { code: error.code, outcome: error.outcome, message: error.message, ms: Date.now() - started }
              : { notABackendError: String(error) },
        )
      const calls = {
        publish: await told(async () => Broadcast.publish('k', 'while loading')),
        create: await told(() => Room.create(`r-${store.image}`)),
      }
      const stillLoading = await loading(admin)
      while (await loading(admin)) await sleep(50)
      const keys = await admin.keys('tf:*')
      admin.disconnect()

      const refused = { code: 'unavailable', outcome: 'not-run' }
      expect({ stillLoading, calls, keys }).toMatchObject({
        stillLoading: true,
        calls: { publish: refused, create: refused },
        keys: [],
      })
    }, 120_000)
  }
})
