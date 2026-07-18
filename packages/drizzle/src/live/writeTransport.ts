export { publishBatch, publishCoarseAll, ensureSubscribed, CHANGE_TOPIC }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { type ChangeMessage, transportFor } from './changeTransport.js'
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
// READINESS BARRIER (ensureSubscribed): before a live read is admitted, the db's subscription is proven
// LISTENING by a self-probe — publish a unique token and await it back. On a transport whose SUBSCRIBE is
// async (e.g. Redis) a probe sent before the subscribe is acked is dropped, so the probe RETRIES on a
// bounded schedule; if never proven it FAILS CLOSED (rejects the live read) rather than admitting a read
// that could silently miss remote writes.

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
  transportFor(db).publish(CHANGE_TOPIC, { origin: dbIdOf(db), changes: batch.changes })
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL, batch) to every OTHER instance: each
 *  coarsens its own watched tables, since the publisher cannot know them. Rides the SAME topic — the
 *  separate wildcard channel it used to need was an artefact of per-table fan-out. The publisher's own
 *  graphs are fed directly, and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  transportFor(db).publish(CHANGE_TOPIC, { origin: dbIdOf(db), coarseAll: true })
}

// Bounded probe schedule — a proven-listening handshake tops out at ~1s before failing the read closed.
const PROBE_INTERVAL_MS = 25
const PROBE_MAX_ATTEMPTS = 40

/** One readiness promise per db. `subscribeAndProbe` subscribes ONCE (the subscription lives for the db,
 *  receiving future batches) and resolves when the probe proves it listening; later reads await the same
 *  already-resolved promise. Keyed by db, though the live subscription itself pins the db — see the
 *  retention limit in the docs; refcounted teardown is the fix and lands with the awaitable contract. */
const readiness = new WeakMap<object, Promise<void>>()

/** Subscribe this db to the change topic and resolve once the subscription is PROVEN LISTENING (idempotent
 *  per db). A live read awaits this before its snapshot read, so a write on another instance can never land
 *  in the window between the read and a not-yet-listening subscription. Rejects (fails the read closed) if
 *  the subscription cannot be proven listening within the bounded probe schedule. */
function ensureSubscribed(db: object): Promise<void> {
  let ready = readiness.get(db)
  if (!ready) {
    ready = subscribeAndProbe(db)
    readiness.set(db, ready)
    // Evict a FAILED readiness so a later read re-attempts — a transient transport hiccup must not
    // permanently wedge live reads for this db.
    ready.catch(() => {
      if (readiness.get(db) === ready) readiness.delete(db)
    })
  }
  return ready
}

/** Establish the ONE subscription for this db — handling both received batches and the readiness probe —
 *  and resolve when this instance receives its own probe back (proven listening). */
function subscribeAndProbe(db: object): Promise<void> {
  const transport = transportFor(db)
  const probeToken = randomUUID()
  return new Promise<void>((resolve, reject) => {
    let proven = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const unsubscribe = transport.subscribe(CHANGE_TOPIC, (message) => {
      if ('probe' in message) {
        // Our own probe round-tripped → the subscription is proven listening. Foreign probes are ignored.
        if (message.probe === probeToken && !proven) {
          proven = true
          if (timer) clearTimeout(timer)
          resolve()
        }
        return
      }
      if (message.origin === dbIdOf(db)) return // our own batch — these graphs were fed directly in ingestWrite
      if ('coarseAll' in message) {
        // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table
        // WE watch. Sound over-fire; never a fabricated row.
        const watched = registryFor(db).router.watchedTables()
        if (watched.length > 0) {
          registryFor(db).router.ingest({ changes: watched.map((t) => ({ table: t, kind: 'coarse' as const })) })
        }
        return
      }
      // One publication, one ingest — there are no sibling copies to reconcile against.
      registryFor(db).router.ingest({ changes: message.changes })
    })
    let attempts = 0
    const pump = () => {
      if (proven) return
      if (attempts >= PROBE_MAX_ATTEMPTS) {
        proven = true // stop the pump; the read fails closed below
        unsubscribe()
        reject(
          new Error(
            `telefunc live: the change subscription was not proven listening after ${attempts} probes — the changeTransport must deliver a publisher's own messages back to its subscribers`,
          ),
        )
        return
      }
      attempts++
      transport.publish(CHANGE_TOPIC, { probe: probeToken } satisfies ChangeMessage)
      if (proven) return // a synchronous (in-memory) transport already looped the probe back — no timer needed
      timer = setTimeout(pump, PROBE_INTERVAL_MS)
    }
    pump()
  })
}
