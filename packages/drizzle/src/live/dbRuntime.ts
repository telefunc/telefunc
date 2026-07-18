export { registryFor, ingestWrite }

import { type Registry, createRegistry } from '../graph/registry.js'
import type { ChangeBatch } from '../router/events.js'

// The db-scoped reactive runtime: ONE registry per db instance owns BOTH paths — reads acquire graphs
// from it (`registryFor(db).acquire(...)`), and captured writes feed those same graphs through it
// (`ingestWrite(db, batch)` → `registryFor(db).router.ingest(...)`). Keyed by db object identity via a
// WeakMap, so a discarded db is collectable. This ownership lives here (not inside the read engine) so the
// write path and the read path share the exact same graphs — a write invalidates precisely the live reads
// that were built against the same db.

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

/** Feed a captured write batch into the db-scoped graphs — the router fans each `TableChange` to the
 *  graphs watching that table (or coarsens them for a `kind:'coarse'` change), invalidating exactly the
 *  live reads the write affects. The production caller for `router.ingest`. */
function ingestWrite(db: object, batch: ChangeBatch): void {
  registryFor(db).router.ingest(batch)
}
