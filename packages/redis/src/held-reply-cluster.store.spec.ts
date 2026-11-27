// What a publish and a subscription are told when a Cluster's primary holds the reply, on a Cluster of the oldest and
// the newest Redis and Valkey Telefunc supports, a primary and its replica, through an app Cluster whose nodes'
// `commandTimeout` is 400 ms. The primary holds every write and script (`CLIENT PAUSE ... WRITE`) for 2 s.
// - A publish fails at the deadline and may have run.
// - A subscription whose start read the primary holds reads it again, and is ready once the primary answers.
import type { Cluster } from 'ioredis'
import { afterAll, describe, expect, it, vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { reaches } from '../test/acl.js'
import { appCluster, listed, startCluster, type Forwarder, type Nodes } from '../test/cluster.js'
import { forwarder, removeStores, runs, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

const PAUSE_MS = 2_000
const HELD = "didn't answer a command of Telefunc's Redis backend within 400 ms"

let app: Cluster | undefined
let via: Forwarder[] | undefined
afterAll(() => {
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
})

/** A new Cluster of the row's, whose nodes have a user of `+@all`, which the backend reaches on an app Cluster of that
 *  user whose nodes' deadline is 400 ms. */
async function reached(store: Store): Promise<Nodes> {
  const proxies = (via ??= [await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  for (const { admin } of listed(nodes)) {
    await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  }
  app = appCluster(proxies, listed(nodes), {
    redisOptions: { username: 'telefunc', password: 'pw', commandTimeout: 400 },
  })
  installRedis(app)
  await reaches(`the ${store.name} Cluster`)
  return nodes
}

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return {
    name: error.name,
    code: 'code' in error ? String(error.code) : 'none',
    outcome: 'outcome' in error ? String(error.outcome) : 'none',
    held: error.message.includes(HELD),
  }
}

describe.runIf(runs)("a publish or a subscription whose reply a Cluster's primary holds", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a publish fails at the nodes' commandTimeout, and may have run`, async () => {
      const { primary } = await reached(store)
      await primary.admin.call('CLIENT', 'PAUSE', String(PAUSE_MS), 'WRITE')
      const started = performance.now()
      const settled = await (async () => Broadcast.publish(`held-publish-${i}`, 'hello'))().then(() => 'resolved', told)
      const beforeTheStoreAnswers = performance.now() - started < PAUSE_MS
      await primary.admin.call('CLIENT', 'UNPAUSE')

      expect({ settled, beforeTheStoreAnswers }).toEqual({
        settled: { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run', held: true },
        beforeTheStoreAnswers: true,
      })
    }, 120_000)

    it(`${store.name}: a subscription whose start read is held reads it again, and is ready once the primary answers`, async () => {
      const { primary } = await reached(store)
      const reports: unknown[] = []
      vi.spyOn(console, 'error').mockImplementation((error) => void reports.push(error))
      const key = `held-start-${i}`
      const received: unknown[] = []
      await primary.admin.call('CLIENT', 'PAUSE', String(PAUSE_MS), 'WRITE')
      const subscription = Broadcast.subscribe(key, (message) => void received.push(message))
      try {
        await subscription.opened
        await Broadcast.publish(key, 'after')
        await vi.waitFor(() => expect(received).toEqual(['after']), { timeout: 10_000 })
      } finally {
        subscription()
        vi.restoreAllMocks()
      }

      // The start read sets the key's mark where none is, so it may have run.
      expect(reports.map(told)).toEqual([
        { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run', held: true },
      ])
    }, 120_000)
  }
})
