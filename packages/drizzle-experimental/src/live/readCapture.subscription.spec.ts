// What a live read does with the db's change subscription: it waits for one, and it gives it back.
//
// ORDERING (the composed red-proof). The lifecycle specs elsewhere call acquireSubscription() directly, so
// deleting or reordering the `await acquireSubscription(...)` inside captureAndBuild would leave them green
// — a false-green the review called out. The first case drives the REAL read pipeline
// (wrapLiveSelect(...).live()) and asserts the two things that must NOT happen before the transport has
// admitted the subscription: the graph must not be acquired, and the snapshot read must not run.
//
// OWNERSHIP. The ref is taken before `registry.acquire` and handed on to whoever ends up owning the read —
// the channel lease once the handle is serialized, the finalizer if it never is. Every way out has to give
// it back, or a db stays subscribed for a live query that no longer exists. The paths that resolve WITHOUT
// producing a handle are here, observed as an actual unsubscribe against the transport; the collected-handle
// path needs a real GC and lives in readCapture.spec.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { RlsStatus } from '../ir/types.js'
import type { ChangeSubscription, ChangeTransport } from '../bus/changeTransport.js'

const probes = vi.hoisted(() => ({
  acquire: vi.fn(),
  snapshotRead: vi.fn(),
  acquireFails: false,
  readFails: false,
  tables: ['accounts'],
  rlsStatuses: [false] as RlsStatus[],
  rlsProbeIndex: 0,
}))

// Everything except the subscription and the transport is stubbed — this spec is about ORDER and OWNERSHIP.
vi.mock('../bus/dbRuntime.js', () => ({
  registryFor: () => ({
    router: { ingest: vi.fn(), register: vi.fn(), unregister: vi.fn(), watchedTables: () => [] },
    acquire: async (request: unknown) => {
      probes.acquire(request)
      if (probes.acquireFails) throw new Error('hydrate failed')
      return { graph: {}, token: { redeem: () => ({ release: vi.fn() }), release: vi.fn() } }
    },
  }),
  ingestWrite: vi.fn(),
}))
vi.mock('../binding/database.js', () => ({
  dialectOf: () => 'pg',
  isSingleSession: () => true,
  rlsEnabledOf: async () => probes.rlsStatuses[probes.rlsProbeIndex++] ?? false,
  semanticEnvironmentKeyOf: async () => 'env',
  driverOf: () => 'PgliteDatabase',
  entityKindOf: () => undefined,
}))
vi.mock('../extract/queryShape.js', () => ({ extractQueryShape: () => ({ tables: probes.tables }) }))
vi.mock('../extract/columns.js', () => ({ schemaFingerprint: () => 'fp', primaryKeyOf: () => ['id'] }))
vi.mock('../extract/identity.js', () => ({ identityOf: () => ({ planKey: 'pk', instanceKey: 'ik' }) }))
vi.mock('../binding/hydrationExecutor.js', () => ({ hydrationExecutorOf: () => async () => [] }))
vi.mock('../binding/drizzleShape.js', () => ({ selectConfigOf: () => null }))
vi.mock('../engine/compile/compile.js', () => ({ compileQuery: () => ({}), coarsePlan: () => ({}) }))

/** The Live the engine mints — `attachSource` is captured so a test can drive serialize-time activation
 *  and the channel close that follows it. Stands in for the package's own `LiveCell`; before the boundary
 *  move this mocked telefunc's extension host instead, which is the seam that no longer exists. */
const host = vi.hoisted(() => ({ sources: [] as { subscribe: () => () => void }[] }))
vi.mock('../primitive/live.js', () => ({
  LiveCell: class {
    constructor(readonly data: unknown) {}
    attachSource(source: { subscribe: () => () => void }) {
      host.sources.push(source)
    }
    invalidate() {}
  },
}))

import { setChangeTransport } from '../bus/changeRuntime.js'
import { wrapLiveSelect } from './readCapture.js'

/** A broker that counts what the runtime asked of it. With `autoAdmit` off the SUBSCRIBE stays
 *  unacknowledged until `admit()` — the window the ordering case lives in. */
function brokerTransport(options: { autoAdmit?: boolean } = {}) {
  let admit!: () => void
  let unsubscribes = 0
  const transport: ChangeTransport = {
    publish() {},
    subscribe: () =>
      new Promise<ChangeSubscription>((resolve) => {
        admit = () =>
          resolve({
            unsubscribe() {
              unsubscribes++
            },
          })
        if (options.autoAdmit) admit()
      }),
  }
  return { transport, admit: () => admit(), unsubscribeCount: () => unsubscribes }
}

/** A minimal awaitable select builder; awaiting it IS the snapshot read. */
function fakeBuilder() {
  return {
    toSQL: () => ({ sql: 'select 1', params: [] }),
    then(onFulfilled?: (rows: unknown) => unknown, onRejected?: (reason: unknown) => unknown) {
      probes.snapshotRead()
      if (probes.readFails) return Promise.reject(new Error('snapshot read failed')).then(onFulfilled, onRejected)
      return Promise.resolve([]).then(onFulfilled)
    },
  }
}

const liveRead = (db: object) => (wrapLiveSelect(fakeBuilder(), db) as { live(): Promise<unknown> }).live()

/** Let every already-runnable continuation run, so "has not happened yet" means the code held it back and
 *  not that the assertion simply looked too early. */
const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve()
}

beforeEach(() => {
  probes.acquire.mockClear()
  probes.snapshotRead.mockClear()
  probes.acquireFails = false
  probes.readFails = false
  probes.tables = ['accounts']
  probes.rlsStatuses = [false]
  probes.rlsProbeIndex = 0
  host.sources.length = 0
})

describe('captureAndBuild — the readiness barrier gates the whole read', () => {
  it('acquires NO graph and runs NO snapshot read until the transport admits the subscription', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const live = liveRead(db)
    live.catch(() => {}) // the read must not reject while the subscription is unacknowledged

    await settle()
    expect(probes.acquire).not.toHaveBeenCalled() // graph NOT registered/seeded yet
    expect(probes.snapshotRead).not.toHaveBeenCalled() // snapshot NOT read yet

    broker.admit() // the broker acknowledges → readiness resolves
    await live

    expect(probes.acquire).toHaveBeenCalledTimes(1) // only AFTER the subscription exists
    expect(probes.snapshotRead).toHaveBeenCalledTimes(1)
  })

  it.each([
    { name: 'unknown then true', statuses: ['unknown', true] as RlsStatus[], expected: true as RlsStatus },
    { name: 'true then unknown', statuses: [true, 'unknown'] as RlsStatus[], expected: true as RlsStatus },
    { name: 'false then unknown', statuses: [false, 'unknown'] as RlsStatus[], expected: 'unknown' as RlsStatus },
    { name: 'all false', statuses: [false, false] as RlsStatus[], expected: false as RlsStatus },
  ])('aggregates RLS status: $name → $expected', async ({ statuses, expected }) => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    probes.tables = statuses.map((_, index) => `table_${index}`)
    probes.rlsStatuses = statuses

    await liveRead(db)

    expect(probes.acquire).toHaveBeenCalledWith(expect.objectContaining({ rlsStatus: expected }))
    host.sources[0]!.subscribe()()
  })
})

describe('captureAndBuild — every way out of a read gives the subscription ref back', () => {
  it('a COMPILE/HYDRATE failure releases it (the read never produced a handle to own it)', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    probes.acquireFails = true

    await expect(liveRead(db)).rejects.toThrow('hydrate failed')
    await settle()

    expect(broker.unsubscribeCount()).toBe(1) // nothing is left subscribed for a read that never happened
  })

  it('a SNAPSHOT-READ failure releases it, deterministically — no handle exists to be collected', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    probes.readFails = true

    await expect(liveRead(db)).rejects.toThrow('snapshot read failed')
    await settle()

    expect(broker.unsubscribeCount()).toBe(1) // released on the way out, not left to the finalizer
  })

  it('a SERIALIZED handle keeps it until the last channel closes', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)

    await liveRead(db)
    const teardown = host.sources[0]!.subscribe() // serialize-time activation: the token redeems into a lease
    await settle()
    expect(broker.unsubscribeCount()).toBe(0) // the wire channel owns this read now

    teardown() // the last channel closes
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })
})
