// A subscription through a failover: while the store is a replica it refuses the subscriber's start with READONLY,
// which passes once a primary serves again, so the subscription waits it out and receives what is published after.
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { forwarder, runs, STORES, startStore } from '../test/stores.js'
import { installRedis } from './index.js'
import { broadcastSequenceKey, DEFAULT_PREFIX } from './keys.js'

/** How often the store answered READONLY. */
async function readOnlyReplies(admin: Redis): Promise<number> {
  const count = /^errorstat_READONLY:count=(\d+)/m.exec(await admin.info('errorstats'))?.[1]
  return count === undefined ? 0 : Number(count)
}

describe.runIf(runs)('a subscription while its store is a replica for a moment', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  let app: Redis | undefined
  afterAll(() => {
    app?.disconnect()
    proxy?.close()
  })

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: it waits out READONLY and receives what is published once a primary serves again`, async () => {
      if (proxy === undefined) {
        proxy = await forwarder()
        app = new Redis({ port: proxy.port, lazyConnect: true, retryStrategy: () => 50 })
        app.on('error', () => {})
        installRedis(app)
      }
      const { host, admin } = await startStore(store)
      proxy.to(host)
      const key = `k-${i}`
      const received: unknown[] = []
      const subscription = Broadcast.subscribe(key, (message) => void received.push(message))
      await subscription.ready

      // Its start writes the key's mark again, which a replica refuses.
      await admin.del(broadcastSequenceKey(`${DEFAULT_PREFIX}0:`, key))
      await admin.call('REPLICAOF', '127.0.0.1', '1')
      await admin.call('CLIENT', 'KILL', 'TYPE', 'pubsub')
      for (let tries = 0; (await readOnlyReplies(admin)) === 0; tries++) {
        if (tries > 400) throw new Error(`${store.name} never refused the subscriber's start`)
        await sleep(25)
      }
      await admin.call('REPLICAOF', 'NO', 'ONE')
      for (let tries = 0; !received.includes('after the failover') && tries < 50; tries++) {
        await Promise.resolve(Broadcast.publish(key, 'after the failover')).catch(() => {})
        await sleep(200)
      }
      subscription()
      admin.disconnect()

      expect(received).toContain('after the failover')
    }, 60_000)
  }
})
