// What a call and a subscription are told when the store holds its reply, on the oldest and the newest Redis and Valkey
// Telefunc supports, through an app client whose `commandTimeout` is 400 ms. The store holds every write and script
// (`CLIENT PAUSE ... WRITE`), or every command (`... ALL`), for 2 s.
// - A publish fails at the deadline and may have run; a read fails at it and changed nothing.
// - A subscription whose start read the store holds reads it again, and is ready once the store answers.
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it, vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { reaches } from '../test/acl.js'
import { forwarder, removeStores, runs, startStore, STORES, type Forwarder, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

const PAUSE_MS = 2_000
const HELD = "didn't answer a command of Telefunc's Redis backend within 400 ms"

let reach: { proxy: Forwarder; app: Redis } | undefined
afterAll(() => {
  reach?.app.disconnect()
  reach?.proxy.close()
})

/** A new store of the row's, which the backend reaches through the forwarder, on an app client of a 400 ms deadline. */
async function reached(store: Store): Promise<Redis> {
  if (reach === undefined) {
    const proxy = await forwarder()
    const app = new Redis({ port: proxy.port, lazyConnect: true, commandTimeout: 400 })
    app.on('error', () => {})
    installRedis(app)
    reach = { proxy, app }
  }
  removeStores()
  const { host, admin } = await startStore(store)
  reach.proxy.to(host)
  await reaches(store.name)
  return admin
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

/** How a call settled, and whether it did before the store answers again. */
async function whilePaused(admin: Redis, mode: 'WRITE' | 'ALL', call: () => Promise<unknown>) {
  await admin.call('CLIENT', 'PAUSE', String(PAUSE_MS), mode)
  const started = performance.now()
  const settled = await call().then(() => 'resolved', told)
  const beforeTheStoreAnswers = performance.now() - started < PAUSE_MS
  await admin.call('CLIENT', 'UNPAUSE')
  return { settled, beforeTheStoreAnswers }
}

describe.runIf(runs)('a call or a subscription whose reply the store holds', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a publish fails at the client's commandTimeout, and may have run`, async () => {
      const admin = await reached(store)
      const publish = await whilePaused(admin, 'WRITE', async () => Broadcast.publish(`held-publish-${i}`, 'hello'))
      admin.disconnect()

      expect(publish).toEqual({
        settled: { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run', held: true },
        beforeTheStoreAnswers: true,
      })
    }, 60_000)

    it(`${store.name}: a read fails at the client's commandTimeout, and changed nothing`, async () => {
      const admin = await reached(store)
      const read = await whilePaused(admin, 'ALL', () => Room.get(`held-read-${i}`))
      admin.disconnect()

      expect(read).toEqual({
        settled: { name: 'BackendError', code: 'unreachable', outcome: 'not-run', held: true },
        beforeTheStoreAnswers: true,
      })
    }, 60_000)

    it(`${store.name}: a subscription whose start read is held reads it again, and is ready once the store answers`, async () => {
      const admin = await reached(store)
      const reports: unknown[] = []
      vi.spyOn(console, 'error').mockImplementation((error) => void reports.push(error))
      const key = `held-start-${i}`
      const received: unknown[] = []
      await admin.call('CLIENT', 'PAUSE', String(PAUSE_MS), 'WRITE')
      const subscription = Broadcast.subscribe(key, (message) => void received.push(message))
      try {
        await subscription.opened
        await Broadcast.publish(key, 'after')
        await vi.waitFor(() => expect(received).toEqual(['after']), { timeout: 10_000 })
      } finally {
        subscription()
        vi.restoreAllMocks()
        admin.disconnect()
      }

      // The start read sets the key's mark where none is, so it may have run.
      expect(reports.map(told)).toEqual([
        { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run', held: true },
      ])
    }, 60_000)
  }
})
