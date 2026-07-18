// COMPOSED readiness gate. The readiness specs elsewhere call ensureSubscribed() directly, so deleting or
// reordering the `await ensureSubscribed(...)` inside captureAndBuild leaves them green — a false-green the
// review called out. This drives the REAL read pipeline (wrapLiveSelect(...).live()) and asserts the two
// things that must NOT happen before the subscription is proven listening: the graph must not be acquired,
// and the snapshot read must not run. Both are spied; the transport withholds probe delivery until released.

import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ChangeMessage, ChangeTransport } from './changeTransport.js'

const probes = vi.hoisted(() => ({ acquire: vi.fn(), snapshotRead: vi.fn() }))

// Everything except the readiness barrier and the transport is stubbed — this spec is about ORDERING.
vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({
    router: { ingest: vi.fn(), register: vi.fn(), unregister: vi.fn(), watchedTables: () => [] },
    acquire: async (request: unknown) => {
      probes.acquire(request)
      return { graph: {}, token: { redeem: () => ({ release: vi.fn() }), release: vi.fn() } }
    },
  }),
  ingestWrite: vi.fn(),
}))
vi.mock('../binding/database.js', () => ({
  dialectOf: () => 'pg',
  isSingleSession: () => true,
  rlsEnabledOf: async () => false,
  semanticEnvironmentKeyOf: async () => 'env',
  driverOf: () => 'PgliteDatabase',
  entityKindOf: () => undefined,
}))
vi.mock('../extract/queryShape.js', () => ({ extractQueryShape: () => ({ tables: ['accounts'] }) }))
vi.mock('../extract/columns.js', () => ({ schemaFingerprint: () => 'fp', primaryKeyOf: () => ['id'] }))
vi.mock('../extract/identity.js', () => ({ identityOf: () => ({ planKey: 'pk', instanceKey: 'ik' }) }))
vi.mock('../binding/hydrationExecutor.js', () => ({ hydrationExecutorOf: () => async () => [] }))
vi.mock('../binding/drizzleShape.js', () => ({ selectConfigOf: () => null }))
vi.mock('../compile/compile.js', () => ({ compileQuery: () => ({}), coarsePlan: () => ({}) }))
vi.mock('./telefuncHost.js', () => ({
  getTelefuncHost: () => ({ createLive: () => ({ attachSource: vi.fn(), invalidate: vi.fn() }) }),
}))

import { setChangeTransport } from './changeTransport.js'
import { wrapLiveSelect } from './readCapture.js'

/** Delivery is withheld until goLive(), so the readiness probe cannot round-trip before then. */
function gatedTransport() {
  const topics = new Map<string, Set<(m: ChangeMessage) => void>>()
  let open = false
  const transport: ChangeTransport = {
    publish(topic, message) {
      if (!open) return
      const subs = topics.get(topic)
      if (subs) for (const cb of [...subs]) cb(message)
    },
    subscribe(topic, onMessage) {
      let subs = topics.get(topic)
      if (!subs) topics.set(topic, (subs = new Set()))
      subs.add(onMessage)
      return () => topics.get(topic)?.delete(onMessage)
    },
  }
  return { transport, goLive: () => (open = true) }
}

/** A minimal awaitable select builder; awaiting it IS the snapshot read. */
function fakeBuilder() {
  return {
    toSQL: () => ({ sql: 'select 1', params: [] }),
    then(onFulfilled?: (rows: unknown) => unknown) {
      probes.snapshotRead()
      return Promise.resolve([]).then(onFulfilled)
    },
  }
}

afterEach(() => {
  vi.useRealTimers()
  probes.acquire.mockClear()
  probes.snapshotRead.mockClear()
})

describe('captureAndBuild — the readiness barrier gates the whole read', () => {
  it('acquires NO graph and runs NO snapshot read until the subscription is proven listening', async () => {
    vi.useFakeTimers()
    const db = {}
    const { transport, goLive } = gatedTransport()
    setChangeTransport(db, transport)

    const live = (wrapLiveSelect(fakeBuilder(), { mintedTokens: [] }, db) as { live(): Promise<unknown> }).live()
    live.catch(() => {}) // the read must not reject while we hold the probe

    // Several probe attempts fire and are dropped: the barrier is still closed.
    await vi.advanceTimersByTimeAsync(200)
    expect(probes.acquire).not.toHaveBeenCalled() // graph NOT registered/seeded yet
    expect(probes.snapshotRead).not.toHaveBeenCalled() // snapshot NOT read yet

    goLive() // the probe can now round-trip → readiness resolves
    await vi.advanceTimersByTimeAsync(30)
    await live

    expect(probes.acquire).toHaveBeenCalledTimes(1) // only AFTER proven listening
    expect(probes.snapshotRead).toHaveBeenCalledTimes(1)
  })
})
