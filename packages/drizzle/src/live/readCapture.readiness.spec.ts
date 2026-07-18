// COMPOSED readiness gate. The readiness specs elsewhere call ensureSubscribed() directly, so deleting or
// reordering the `await ensureSubscribed(...)` inside captureAndBuild leaves them green — a false-green the
// review called out. This drives the REAL read pipeline (wrapLiveSelect(...).live()) and asserts the two
// things that must NOT happen before the transport has admitted the subscription: the graph must not be
// acquired, and the snapshot read must not run. Both are spied; the transport holds its `subscribe()`
// promise open until the test releases it.

import { describe, expect, it, vi } from 'vitest'
import type { ChangeSubscription, ChangeTransport } from './changeTransport.js'

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

/** The subscription is not acknowledged until `admit()` — the window a real broker leaves open between the
 *  SUBSCRIBE command and its ack. */
function brokerTransport() {
  let admit!: () => void
  const transport: ChangeTransport = {
    publish() {},
    subscribe: () => new Promise<ChangeSubscription>((resolve) => (admit = () => resolve({ unsubscribe() {} }))),
  }
  return { transport, admit: () => admit() }
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

/** Let every already-resolvable continuation run, so "not yet called" means the barrier held rather than
 *  that the test simply looked too early. */
const drainMicrotasks = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve()
}

describe('captureAndBuild — the readiness barrier gates the whole read', () => {
  it('acquires NO graph and runs NO snapshot read until the transport admits the subscription', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const live = (wrapLiveSelect(fakeBuilder(), { mintedTokens: [] }, db) as { live(): Promise<unknown> }).live()
    live.catch(() => {}) // the read must not reject while the subscription is unacknowledged

    await drainMicrotasks()
    expect(probes.acquire).not.toHaveBeenCalled() // graph NOT registered/seeded yet
    expect(probes.snapshotRead).not.toHaveBeenCalled() // snapshot NOT read yet

    broker.admit() // the broker acknowledges → readiness resolves
    await live

    expect(probes.acquire).toHaveBeenCalledTimes(1) // only AFTER the subscription exists
    expect(probes.snapshotRead).toHaveBeenCalledTimes(1)
  })
})
