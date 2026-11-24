// A store that refuses the backend's login, on the oldest and the newest Redis and Valkey Telefunc supports: the backend's
// own client gives up as the app's retry strategy says, and what it ended with is the refusal (the Cluster's rows are in
// cluster-login.store.spec.ts).
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { forwarder, removeStores, runs, STORES, startStore } from '../test/stores.js'
import { installRedis } from './index.js'

describe.runIf(runs)('a store that refuses the login', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  afterAll(() => proxy?.close())

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: after the app's client logged in, the backend's own client gave up before anything waited, and a publish rejects with 'denied'`, async () => {
      removeStores()
      proxy ??= await forwarder()
      const { host, admin } = await startStore(store)
      await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
      proxy.to(host)
      // The app's client connects at once and stays logged in. The password then changes, and the backend's copy of the
      // client, which connects at installRedis(), gives up at its first failed connect, as the app's retry strategy says.
      const app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', retryStrategy: () => null })
      app.on('error', () => {})
      try {
        await new Promise((resolve) => app.once('ready', resolve))
        await admin.call('ACL', 'SETUSER', 'telefunc', 'resetpass', '>another-password')
        proxy.clear()
        installRedis(app)
        // The copy's connection is the one that ends: the app's stays.
        while (!proxy.passages().some(({ ended }) => ended !== undefined)) await sleep(10)

        const published = await Promise.resolve(Broadcast.publish(`login-${i}`, 'x')).then(
          () => 'answered',
          (error: unknown) => error,
        )

        expect({ published, app: app.status }).toMatchObject({
          published: {
            name: 'BackendError',
            code: 'denied',
            outcome: 'not-run',
            message: expect.stringContaining("refused the login of Telefunc's Redis backend (WRONGPASS)"),
          },
          app: 'ready',
        })
      } finally {
        app.disconnect()
        admin.disconnect()
      }
    }, 60_000)
  }
})
