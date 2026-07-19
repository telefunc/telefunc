// Shared fixtures for the change-runtime specs (publish / ordering / isolation / lifecycle).
//
// These four files were one file, and they exercise ONE subject through ONE mocked registry. That is why this
// is a shared module rather than four copies: unlike the four-line `capturing()` helper the write-capture
// specs each keep their own, these fixtures every file depends on identically, and four copies would drift —
// a drifting fixture is a spec that quietly stops describing the same system.
//
// The mocked registry itself lives in `changeRuntime.registryMock.ts`, which imports nothing from the source
// tree; see the note there for why that separation is load-bearing rather than tidy.

import type { ChangeTransport } from './changeTransport.js'
import { createInMemoryChangeTransport } from './changeTransport.js'
import { acquireSubscription, changeTopicFor, setChangeTransport } from './changeRuntime.js'
import type { TableChange } from '../router/events.js'

export { engine, resetEngine } from './changeRuntime.registryMock.js'

/** A minimal registered graph — only its `tables` matter to watchedTables(). */
export const watching = (table: string) => ({ tables: [table] }) as never

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
