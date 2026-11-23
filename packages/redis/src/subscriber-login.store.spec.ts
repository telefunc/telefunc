// A subscriber whose login the store refuses: the refusal stands until an operator changes the password or the ACL, so
// the subscription ends at once with that error, and the backend doesn't log in again for it.
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { forwarder, runs, STORES, startStore } from '../test/stores.js'
import { installRedis } from './index.js'

describe.runIf(runs)('a subscriber whose login the store refuses', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  let app: Redis | undefined
  afterAll(() => {
    app?.disconnect()
    proxy?.close()
  })

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: its subscription ends once, with 'denied', and the backend doesn't log in again for it`, async () => {
      if (proxy === undefined) {
        proxy = await forwarder()
        app = new Redis({
          port: proxy.port,
          username: 'telefunc',
          password: 'pw',
          lazyConnect: true,
          retryStrategy: () => 50,
        })
        app.on('error', () => {})
        installRedis(app)
      }
      const { host, admin } = await startStore(store)
      // The app's password is no longer the user's.
      await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>another-password', '~*', '&*', '+@all')
      proxy.to(host)

      const subscription = Broadcast.subscribe(`k-${i}`, () => {})
      const ready = await Promise.race([
        subscription.ready.then(
          () => 'resolved',
          (error: unknown) => error,
        ),
        sleep(10_000).then(() => 'still waiting after 10 s'),
      ])
      const loginsAtTheEnd = proxy.connections()
      // Longer than the subscriber's longest wait between two attempts, 2 s.
      await sleep(3_000)
      const loginsAfter = proxy.connections() - loginsAtTheEnd
      subscription()
      admin.disconnect()

      expect({ ready, loginsAfter }).toMatchObject({ ready: { name: 'BackendError', code: 'denied' }, loginsAfter: 0 })
    }, 60_000)
  }
})
