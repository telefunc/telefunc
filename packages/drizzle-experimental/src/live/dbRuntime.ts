export { registryFor, ingestWrite, ingestLocal, announceCoarse }

import { type Registry, createRegistry } from '../graph/registry.js'
import type { ChangeBatch } from '../router/events.js'
import { publishBatch, publishCoarseAll } from './changeRuntime.js'
import { emitSafely } from './writeChanges.js'

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

/** Feed a batch into THIS db's graphs only, publishing nothing — for a batch whose remote half is announced
 *  separately, and exactly once. Today that is a COMMITTED transaction that ran raw SQL: its captured rows
 *  and its raw statement's coarse markers have to land in ONE local tick, so the transaction flushes them
 *  here and announces its remote half at the same commit. Publishing the batch as well would reach the
 *  remote graphs the coarse-all already covers, costing every remote watcher a second refetch.
 *
 *  Where the local coarsening needs no tick of its own, both halves are `announceCoarse` below. */
function ingestLocal(db: object, batch: ChangeBatch): void {
  registryFor(db).router.ingest(batch)
}

/** Announce a mutation whose touched tables are UNKNOWABLE — raw SQL, `db.batch(…)`. Both halves of that
 *  announcement live here because they are one decision, and it used to be made in two files that each knew
 *  half of it:
 *
 *   - LOCALLY, every table this db watches is coarsened. This db's graphs are fed DIRECTLY, because a
 *     publisher suppresses its own echo (changeRuntime) and would otherwise never hear its own write.
 *   - REMOTELY, ONE coarse-all. It cannot be a batch of this db's own tables: another instance may watch
 *     tables this db has never heard of, and a batch naming only these would never reach them.
 *
 *  Non-throwing on the local half (`emitSafely`), because the statement it describes has already run. */
function announceCoarse(db: object): void {
  const watched = registryFor(db).router.watchedTables()
  if (watched.length > 0) {
    emitSafely(
      (changes) => ingestLocal(db, { changes }),
      watched.map((table) => ({ table, kind: 'coarse' as const })),
    )
  }
  publishCoarseAll(db)
}
