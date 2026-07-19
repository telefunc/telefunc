export { registryFor, ingestWrite, ingestLocal }

import { type Registry, createRegistry } from '../graph/registry.js'
import type { ChangeBatch } from '../router/events.js'
import { publishBatch } from './changeRuntime.js'

// The db-scoped reactive runtime: ONE registry per db instance owns BOTH paths — reads acquire graphs
// from it (`registryFor(db).acquire(...)`), and captured writes feed those same graphs through it
// (`ingestWrite(db, batch)` → `registryFor(db).router.ingest(...)`). Keyed by db object identity via a
// WeakMap, and genuinely weak: the change subscription that a live read establishes is refcounted and
// detached at the last release (changeRuntime.ts), so nothing pins a db whose live queries have all
// closed. This ownership lives here (not inside the read engine) so the write path and the read path share
// the exact same graphs — a write invalidates precisely the live reads that were built against the same db.

/** The per-input state cap: a stateful graph whose shadow exceeds it demotes to coarse (bounded, sound
 *  over-fire). Internal — not a public knob. */
const MAX_STATE_ROWS_PER_INPUT = 50_000

const registries = new WeakMap<object, Registry>()

/** The registry for a db instance — created on first use, shared by every live read AND every captured
 *  write over that db. All live queries over the same db share its graph state. */
function registryFor(db: object): Registry {
  let registry = registries.get(db)
  if (!registry) registries.set(db, (registry = createRegistry({ maxStateRowsPerInput: MAX_STATE_ROWS_PER_INPUT })))
  return registry
}

/** Feed a captured write batch into the db-scoped graphs AND broadcast it cross-instance. The router fans
 *  each `TableChange` to the local graphs watching that table (or coarsens them for a `kind:'coarse'`
 *  change) — the DIRECT local feed, invalidating exactly the live reads this write affects. Then the whole
 *  batch is published ONCE so OTHER instances feed it into their own graphs; the local instance drops its
 *  own echo by origin (see changeRuntime), so there is no double-apply. */
function ingestWrite(db: object, batch: ChangeBatch): void {
  registryFor(db).router.ingest(batch)
  publishBatch(db, batch)
}

/** Feed a batch into THIS db's graphs only, publishing nothing. For a mutation whose reach is announced
 *  SEPARATELY: raw SQL coarsens this db's watched tables locally and tells other instances with ONE
 *  coarse-all announcement (`publishCoarseAll`), which covers every table any receiver watches — including
 *  ones this db has never heard of. Publishing this db's own coarse markers as well would reach the same
 *  remote graphs a second time, costing a redundant refetch. */
function ingestLocal(db: object, batch: ChangeBatch): void {
  registryFor(db).router.ingest(batch)
}
