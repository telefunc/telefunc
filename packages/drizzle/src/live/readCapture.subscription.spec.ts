// What a live read does with the db's change subscription: it waits for one, and it gives it back.
//
// ORDERING (the composed red-proof). The lifecycle specs elsewhere call acquireSubscription() directly, so
// deleting or reordering the `await acquireSubscription(...)` inside captureAndBuild would leave them green
// — a false-green the review called out. The first case drives the REAL read pipeline
// (wrapLiveSelect(...).live()) and asserts the two things that must NOT happen before the transport has
// admitted the subscription: the graph must not be acquired, and the snapshot read must not run.
//
// OWNERSHIP. The ref is taken before `registry.acquire` and handed on to whoever ends up owning the read —
// the request sweep for a handle that is never serialized, the channel lease for one that is. Every way out
// has to give it back, or a db stays subscribed for a live query that no longer exists. Each exit path gets
// its own case here, observed as an actual unsubscribe against the transport.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ChangeSubscription, ChangeTransport } from './changeTransport.js'

const probes = vi.hoisted(() => ({
  acquire: vi.fn(),
  snapshotRead: vi.fn(),
  acquireFails: false,
  readFails: false,
}))

// Everything except the subscription and the transport is stubbed — this spec is about ORDER and OWNERSHIP.
vi.mock('./dbRuntime.js', () => ({
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

/** The Live the host mints — `attachSource` is captured so a test can drive serialize-time activation and
 *  the channel close that follows it. */
const host = vi.hoisted(() => ({ sources: [] as { subscribe: () => () => void }[] }))
vi.mock('./telefuncHost.js', () => ({
  getTelefuncHost: () => ({
    createLive: () => ({
      attachSource: (source: { subscribe: () => () => void }) => host.sources.push(source),
      invalidate: vi.fn(),
    }),
  }),
}))

import { setChangeTransport } from './changeTransport.js'
import { type ReadCarrier, disposeUnredeemedReads, wrapLiveSelect } from './readCapture.js'

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

const liveRead = (db: object, carrier: ReadCarrier) =>
  (wrapLiveSelect(fakeBuilder(), carrier, db) as { live(): Promise<unknown> }).live()

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
  host.sources.length = 0
})

describe('captureAndBuild — the readiness barrier gates the whole read', () => {
  it('acquires NO graph and runs NO snapshot read until the transport admits the subscription', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const live = liveRead(db, { mintedTokens: [] })
    live.catch(() => {}) // the read must not reject while the subscription is unacknowledged

    await settle()
    expect(probes.acquire).not.toHaveBeenCalled() // graph NOT registered/seeded yet
    expect(probes.snapshotRead).not.toHaveBeenCalled() // snapshot NOT read yet

    broker.admit() // the broker acknowledges → readiness resolves
    await live

    expect(probes.acquire).toHaveBeenCalledTimes(1) // only AFTER the subscription exists
    expect(probes.snapshotRead).toHaveBeenCalledTimes(1)
  })
})

describe('captureAndBuild — every way out of a read gives the subscription ref back', () => {
  it('a COMPILE/HYDRATE failure releases it (the read never reached the carrier)', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    probes.acquireFails = true

    await expect(liveRead(db, { mintedTokens: [] })).rejects.toThrow('hydrate failed')
    await settle()

    expect(broker.unsubscribeCount()).toBe(1) // nothing is left subscribed for a read that never happened
  })

  it('a SNAPSHOT-READ failure releases it (the request sweep finds the entry the read left behind)', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    probes.readFails = true
    const carrier: ReadCarrier = { mintedTokens: [] }

    await expect(liveRead(db, carrier)).rejects.toThrow('snapshot read failed')
    expect(carrier.mintedTokens).toHaveLength(1) // owned BEFORE the fallible read — which is what makes it sweepable
    expect(broker.unsubscribeCount()).toBe(0) // …so the ref is still held until the sweep runs

    disposeUnredeemedReads(carrier)
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })

  it('a NEVER-SERIALIZED handle releases it at the request sweep', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    const carrier: ReadCarrier = { mintedTokens: [] }

    await liveRead(db, carrier) // the handle is built but never activated
    await settle()
    expect(broker.unsubscribeCount()).toBe(0) // still a live read as far as the runtime knows

    disposeUnredeemedReads(carrier)
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })

  it('a SERIALIZED handle keeps it until the last channel closes, and the sweep does not take it early', async () => {
    const db = {}
    const broker = brokerTransport({ autoAdmit: true })
    setChangeTransport(db, broker.transport)
    const carrier: ReadCarrier = { mintedTokens: [] }

    await liveRead(db, carrier)
    const teardown = host.sources[0]!.subscribe() // serialize-time activation: the token redeems into a lease

    disposeUnredeemedReads(carrier) // the request ends — but the channel is still open
    await settle()
    expect(broker.unsubscribeCount()).toBe(0) // the wire channel owns this read now

    teardown() // the last channel closes
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })
})
