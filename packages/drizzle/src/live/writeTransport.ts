export { publishBatch, publishCoarseAll, ensureSubscribed, CHANGE_TOPIC }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { transportFor } from './changeTransport.js'
import { CHANGE_CODEC_VERSION, decodeChangePayload, encodeChangePayload } from './changeCodec.js'
import type { ChangeBatch } from '../router/events.js'

// Cross-instance change transport. A committed batch is published ONCE, to ONE topic per logical database,
// over the db's DEDICATED changeTransport (never the user's app Broadcast — see changeTransport.ts). A
// receiver feeds the whole batch into its own graphs via `router.ingest`; the router already slices it per
// table and notifies each affected graph at most once, so one message is one atomic graph tick.
//
// SINGLE TOPIC, deliberately. An earlier design published one full copy of the batch to each touched
// table's topic, so that a runtime only heard about tables it watched. That fan-out bought receiver
// filtering and paid for it with a correctness protocol: k copies of every k-table commit, an apply-once
// rule to suppress the duplicates it created, per-batch id memory with a time bound, a monotonic clock and
// sweeper to expire it, a 30-second cross-topic skew guarantee demanded of every adapter, and a reserved
// wildcard channel (plus a reserved relation identity) for writes whose tables are unknowable. All of that
// existed to undo the duplication; none of it is needed once the batch is published once.
//
// What remains is the part that was never about fan-out:
//   - ORIGIN self-suppression — a db feeds its own graphs DIRECTLY in `ingestWrite`, before publishing, so
//     it must drop its own echo or apply the same delta twice.
//   - AT-MOST-ONCE per topic, which the adapter still owes: precise row deltas are not idempotent, and
//     single-topic removes the duplicates this layer created, not generic redelivery.
//
// Receiver filtering is now the router's job (it ignores tables no graph watches). If measured broker
// traffic ever makes per-table filtering worth its price again, it returns as a versioned, additive
// transport capability — not as a baseline the runtime has to compensate for.
//
// READINESS BARRIER (ensureSubscribed): a live read is not admitted until this db's subscription is
// ADMITTED by the transport — which is exactly what awaiting `subscribe()` means (changeTransport.ts). The
// readiness question is a CONTROL-PLANE one, so it is asked on the control plane: an earlier design proved
// it on the data plane instead, by publishing a unique token and awaiting it back on a bounded retry pump.
// That probe demanded self-delivery from every adapter, rejected otherwise-usable transports, and still
// proved nothing about the cluster — a Redis namespace isolated from every other instance loops its own
// probe back happily. What the read actually needs is the transport's own assertion that publications made
// from now on can be observed; a rejected (or never-resolving) subscribe fails the read CLOSED.

/** The one change topic for a logical database. Every runtime sharing a changeTransport exchanges changes
 *  on it; scoping several logical databases onto one transport is the adapter's namespacing concern. */
const CHANGE_TOPIC = '__live__:changes'

/** The publishing identity of a db — stable for its lifetime, used to drop our own echo. */
const dbIds = new WeakMap<object, string>()
function dbIdOf(db: object): string {
  let id = dbIds.get(db)
  if (!id) dbIds.set(db, (id = randomUUID()))
  return id
}

/** Publish a committed batch cross-instance: ONE message, carrying the whole batch. No-op for an empty
 *  batch. Receivers slice it per table themselves. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const origin = dbIdOf(db)
  let payload: string
  try {
    payload = encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin, changes: batch.changes })
  } catch (error) {
    // A value the codec cannot carry must not cost the invalidation itself: fall back to the value-free
    // coarse-all envelope, so remote graphs refetch instead of never hearing about the write.
    console.error(
      '[telefunc] live: a change batch could not be encoded for the transport, so a COARSE invalidation was published instead. Remote live queries over-fetch rather than miss the write.',
      error,
    )
    payload = encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin, coarseAll: true })
  }
  publishPayload(db, payload)
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL, batch) to every OTHER instance: each
 *  coarsens its own watched tables, since the publisher cannot know them. Rides the SAME topic — the
 *  separate wildcard channel it used to need was an artefact of per-table fan-out. The publisher's own
 *  graphs are fed directly, and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  publishPayload(db, encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin: dbIdOf(db), coarseAll: true }))
}

/** Hand a payload to the transport. The write that produced it has ALREADY COMMITTED, so a transport
 *  failure — thrown outright, or a rejection from an async client that publishes over a socket — is
 *  reported and dropped. It must never reject the caller's write (which succeeded) and must never surface
 *  as an unhandled rejection. */
function publishPayload(db: object, payload: string): void {
  try {
    const published = transportFor(db).publish(CHANGE_TOPIC, payload)
    if (isThenable(published)) published.then(undefined, reportPublishFailure)
  } catch (error) {
    reportPublishFailure(error)
  }
}

function isThenable(value: void | Promise<void>): value is Promise<void> {
  return typeof (value as Promise<void> | undefined)?.then === 'function'
}

function reportPublishFailure(error: unknown): void {
  console.error(
    '[telefunc] live: publishing a change batch to the changeTransport failed. The write COMMITTED and its result is unaffected; live queries on other instances may be stale until the next write touching those tables.',
    error,
  )
}

/** One readiness promise per db. Subscribes ONCE (the subscription lives for the db, receiving future
 *  batches) and resolves when the transport admits it; later reads await the same already-resolved
 *  promise. Keyed by db, though the live subscription itself pins the db — refcounted teardown lands with
 *  the lifecycle slice. */
const readiness = new WeakMap<object, Promise<void>>()

/** Subscribe this db to the change topic and resolve once the transport has ADMITTED the subscription
 *  (idempotent per db). A live read awaits this before its snapshot read, so a write on another instance
 *  can never land in the window between the read and a not-yet-established subscription. Rejects (fails the
 *  read closed) if the transport refuses the subscription. */
function ensureSubscribed(db: object): Promise<void> {
  let ready = readiness.get(db)
  if (!ready) {
    ready = transportFor(db)
      .subscribe(CHANGE_TOPIC, (payload) => receive(db, payload))
      .then(() => undefined)
    readiness.set(db, ready)
    // Evict a FAILED readiness so a later read re-attempts — a transient transport hiccup must not
    // permanently wedge live reads for this db.
    ready.catch(() => {
      if (readiness.get(db) === ready) readiness.delete(db)
    })
  }
  return ready
}

/** One delivered payload. Decodes, drops our own echo, and feeds the rest into this db's graphs. */
function receive(db: object, payload: string): void {
  const envelope = decodeChangePayload(payload)
  if (!envelope) {
    // Unreadable — an unknown codec version, or a payload the transport mangled. `origin` is unreadable
    // too, so this may even be our own echo: coarsening costs a redundant refetch, while applying a guessed
    // row would be wrong at any price.
    console.error(
      `[telefunc] live: an undecodable payload arrived on "${CHANGE_TOPIC}" (unknown codec version, or a transport that does not deliver the exact string it was given). Coarsening every watched table rather than interpreting it.`,
    )
    coarsenWatched(db)
    return
  }
  if (envelope.origin === dbIdOf(db)) return // our own batch — these graphs were fed directly in ingestWrite
  if ('coarseAll' in envelope) {
    // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table WE
    // watch. Sound over-fire; never a fabricated row.
    coarsenWatched(db)
    return
  }
  // One publication, one ingest — there are no sibling copies to reconcile against.
  registryFor(db).router.ingest({ changes: envelope.changes })
}

/** Coarsen every table this db currently watches — the sound response to a change whose reach we cannot
 *  read. Nothing watched, nothing to do. */
function coarsenWatched(db: object): void {
  const watched = registryFor(db).router.watchedTables()
  if (watched.length === 0) return
  registryFor(db).router.ingest({ changes: watched.map((table) => ({ table, kind: 'coarse' as const })) })
}
