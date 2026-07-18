export { publishBatch, ensureSubscribed, batchTopic }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { type ChangeMessage, transportFor } from './changeTransport.js'
import type { ChangeBatch } from '../router/events.js'

// Cross-instance write transport. A committed batch is published (with a unique id) to EACH touched table's
// topic `__live__:{table}` over the db's DEDICATED changeTransport (never the user's app Broadcast — see
// changeTransport.ts); a receiver subscribed to any of those topics feeds the WHOLE batch into its own
// graphs via `router.ingest` (the router slices per table). Deduped by batch id so that (a) a batch spanning
// N of a subscriber's topics applies EXACTLY ONCE, and (b) a local write that round-trips back through this
// instance's own subscription is NOT double-applied — local graphs are fed DIRECTLY (in ingestWrite), never
// via the round-trip. Precise cross-instance because the actual rows travel in the batch.
//
// READINESS BARRIER (ensureSubscribed): before a live read is admitted, its tables' subscriptions are proven
// LISTENING by a self-probe — publish a unique token to the topic and await that same token back. On a
// transport whose SUBSCRIBE is async (e.g. Redis), a probe sent before the subscribe is acked is dropped, so
// the probe RETRIES on a bounded schedule; if it is never proven listening it FAILS CLOSED (rejects the live
// read) rather than admitting a read that could silently miss remote writes. The self-delivery (loopback)
// requirement this leans on is documented on the ChangeTransport contract.

const batchTopic = (table: string): string => `__live__:${table}`

// DE-DUPLICATION IS DETERMINISTIC AND STATELESS — no id memory, so no eviction hole and no unbounded growth.
// (An earlier count-bounded `seen` set was unsound: once an id aged out, a delayed copy of the same batch on
// another subscribed topic applied twice, and precise application is NOT idempotent.)
//
// A batch carries `origin` (the publishing db) and `tables` (every table it touched, stable order). A
// receiver on (db, table) applies it iff BOTH:
//   1. `origin !== dbIdOf(db)` — a db never applies its own batch through the transport (its graphs were fed
//      DIRECTLY in ingestWrite); and
//   2. `table` is the FIRST of the batch's tables that this db is subscribed to — so a db subscribed to
//      several of a batch's tables applies it on exactly one of them, whichever comes first.
// Soundness rests on one invariant: a db's subscription set and its `readiness` entry are established in the
// SAME synchronous step (see ensureSubscribed), and a topic only delivers batches published after it was
// subscribed. So every delivery of a given batch to a given db sees the same subscribed set, and rule (2)
// selects the same single topic for all of them.

/** The publishing identity of a db — stable for the db's lifetime, used to drop our own round-trip. */
const dbIds = new WeakMap<object, string>()
function dbIdOf(db: object): string {
  let id = dbIds.get(db)
  if (!id) dbIds.set(db, (id = randomUUID()))
  return id
}

/** The first of `tables` this db is subscribed to — the ONE topic allowed to apply the batch. */
function owningTable(db: object, tables: readonly string[]): string | undefined {
  const subscribed = readiness.get(db)
  if (!subscribed) return undefined
  for (const table of tables) if (subscribed.has(table)) return table
  return undefined
}

/** Publish a committed batch cross-instance over the db's changeTransport: one message per touched table's
 *  topic, all carrying the same id. The id is pre-marked seen so this instance drops its own round-tripped
 *  copy (it already fed its graphs directly). No-op for an empty batch. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const transport = transportFor(db)
  const tables = [...new Set(batch.changes.map((change) => change.table))] // stable order; drives rule (2)
  const message = { origin: dbIdOf(db), tables, changes: batch.changes }
  for (const table of tables) transport.publish(batchTopic(table), message)
}

// Bounded probe schedule — a proven-listening handshake tops out at ~1s before failing the read closed.
const PROBE_INTERVAL_MS = 25
const PROBE_MAX_ATTEMPTS = 40

// One readiness promise per (db, table). Cached: subscribeAndProbe subscribes ONCE (the subscription lives
// for the db, receiving future batches) and resolves when the probe proves it listening; later reads await
// the same (already-resolved) promise. Keyed by db so a discarded db is collectable.
const readiness = new WeakMap<object, Map<string, Promise<void>>>()

/** Subscribe this db to each table's topic and resolve once every subscription is PROVEN LISTENING (idempotent
 *  per db+table). A live read awaits this before its snapshot read, so a write on another instance can never
 *  land in the window between the read and a not-yet-listening subscription. Rejects (fails the read closed)
 *  if a subscription cannot be proven listening within the bounded probe schedule. */
function ensureSubscribed(db: object, tables: readonly string[]): Promise<void> {
  let perTable = readiness.get(db)
  if (!perTable) readiness.set(db, (perTable = new Map()))
  const waits: Promise<void>[] = []
  for (const table of tables) {
    let ready = perTable.get(table)
    if (!ready) {
      ready = subscribeAndProbe(db, table)
      perTable.set(table, ready)
      // Evict a FAILED readiness so a later read re-attempts — a transient transport hiccup must not
      // permanently wedge live reads for this table.
      const pending = perTable
      ready.catch(() => {
        if (pending.get(table) === ready) pending.delete(table)
      })
    }
    waits.push(ready)
  }
  return Promise.all(waits).then(() => undefined)
}

/** Establish the ONE subscription for (db, table) — handling both received batches (dedupe + ingest) and the
 *  readiness probe — and resolve when this instance receives its own probe back (proven listening). */
function subscribeAndProbe(db: object, table: string): Promise<void> {
  const transport = transportFor(db)
  const topic = batchTopic(table)
  const probeToken = randomUUID()
  return new Promise<void>((resolve, reject) => {
    let proven = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const unsubscribe = transport.subscribe(topic, (message) => {
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
      if (owningTable(db, message.tables) !== table) return // another of our subscribed topics applies it
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
            `telefunc live: change subscription to "${table}" was not proven listening after ${attempts} probes — the changeTransport must deliver a publisher's own messages back to its subscribers`,
          ),
        )
        return
      }
      attempts++
      transport.publish(topic, { probe: probeToken } satisfies ChangeMessage)
      if (proven) return // a synchronous (in-memory) transport already looped the probe back — no timer needed
      timer = setTimeout(pump, PROBE_INTERVAL_MS)
    }
    pump()
  })
}
