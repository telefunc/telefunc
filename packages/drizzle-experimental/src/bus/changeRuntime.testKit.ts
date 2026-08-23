// Shared fixtures for the change-runtime specs (publish / ordering / isolation / lifecycle).
//
// SHARED rather than copied per file, because all four exercise ONE subject through ONE mocked registry and
// depend on these fixtures identically. Copies of a fixture that large drift, and a drifting fixture is a
// spec that has quietly stopped describing the same system. (The write-capture specs make the opposite call
// for a four-line helper, where drift cannot change what the spec means.)
//
// The mocked registry itself lives in `changeRuntime.registryMock.ts`, which imports nothing from the source
// tree; see the note there for why that separation is load-bearing rather than tidy.

import type { ChangeTransport } from './changeTransport.js'
import { createInMemoryChangeTransport } from './changeTransport.js'
import { acquireSubscription, changeTopicFor, setChangeTransport } from './changeRuntime.js'
import type { RoutableGraph } from './router/changeRouter.js'
import type { TableChange } from './router/events.js'

export { engine, resetEngine } from './changeRuntime.registryMock.js'

/** A registered graph watching one table. Only `tables` matters to `watchedTables()`, but this satisfies the
 *  REAL `RoutableGraph` rather than casting past it — a cast here would erase the registration contract this
 *  shared kit exists to hold, so a change to what the router demands of a graph would not reach these specs
 *  at all. The behavioural members are inert on purpose: the router is mocked here, and what a graph DOES
 *  with a change is the graph specs' subject. */
export const watching = (table: string): RoutableGraph => ({
  tables: [table],
  apply: () => ({ invalidated: false }),
  notifyKey: () => table,
  fault: () => {},
  reseed: () => {},
})

export const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })

/** A fresh db + its own injected transport per test → no cross-test subscriber leakage. The ref that
 *  acquireSubscription returns is deliberately kept for the test's lifetime: these tests are about what a
 *  SUBSCRIBED db does, and the lifecycle that drops the subscription has its own spec. */
export async function freshDb() {
  const db = {}
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  await acquireSubscription(db)
  return { db, transport }
}

/** Two runtimes over ONE logical database, on one transport — the multi-instance path in a single process.
 *  They SHARE a `$client`, which is what makes them the same database: that is how two drizzle objects over
 *  one connection appear, and it is the identity the namespace is derived from. Two dbs with different
 *  clients are different databases and deliberately do NOT exchange changes (see the isolation suite). */
export async function twoInstances() {
  const transport = createInMemoryChangeTransport()
  const $client = { connection: 'shared' }
  const dbA = { $client }
  const dbB = { $client }
  setChangeTransport(dbA, transport)
  setChangeTransport(dbB, transport)
  await acquireSubscription(dbA)
  await acquireSubscription(dbB)
  return { transport, dbA, dbB }
}

/** Everything that reached the wire, as raw payloads — what a broker would actually carry. */
export async function wireTap(transport: ChangeTransport, db: object) {
  const seen: string[] = []
  await transport.subscribe(changeTopicFor(db), (payload) => seen.push(payload))
  return seen
}

/** Publications are CHAINED per db so an async transport receives them in sequence order, which defers the
 *  first one by a microtask. The local graphs were already fed synchronously by ingestWrite; only the
 *  REMOTE hop is deferred, so a test that watches the wire has to let the chain run. */
export const flush = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve()
}
