export { publishBatch, ensureSubscribed, batchTopic }

import { randomUUID } from 'node:crypto'
import { Broadcast } from 'telefunc'
import { registryFor } from './dbRuntime.js'
import type { ChangeBatch, TableChange } from '../router/events.js'

// Cross-instance write transport. A committed batch is published (with a unique id) to EACH touched table's
// topic `__live__:{table}` over the existing telefunc Broadcast; a receiver subscribed to any of those
// topics feeds the WHOLE batch into its own graphs via `router.ingest` (the router slices per table). Deduped
// by batch id so that (a) a batch spanning N of a subscriber's topics applies EXACTLY ONCE, and (b) a local
// write that round-trips back through this instance's own subscription is NOT double-applied — local graphs
// are fed DIRECTLY (in ingestWrite), never via the round-trip. Precise cross-instance because the actual
// rows travel in the batch.

const batchTopic = (table: string): string => `__live__:${table}`

/** The wire payload: the batch's changes tagged with a globally-unique id for dedupe. */
type BatchMessage = { id: string; changes: TableChange[] }

// Batch ids already applied on THIS instance — the dedupe window. Bounded FIFO: it only needs to outlive
// in-flight redelivery of the same batch across a subscriber's several topics, so an old id aging out is
// harmless (that batch is long applied).
const SEEN_CAP = 4096
const seen = new Set<string>()
function markSeen(id: string): void {
  if (seen.has(id)) return
  seen.add(id)
  if (seen.size > SEEN_CAP) seen.delete(seen.values().next().value as string)
}

/** Publish a committed batch cross-instance: one message per touched table's topic, all carrying the same
 *  id. The id is pre-marked seen so this instance drops its own round-tripped copy (it already fed its
 *  graphs directly). No-op for an empty batch. */
function publishBatch(batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const message: BatchMessage = { id: randomUUID(), changes: batch.changes }
  markSeen(message.id)
  for (const table of new Set(batch.changes.map((change) => change.table))) {
    void Broadcast.publish(batchTopic(table), message)
  }
}

// One subscription per (db, table). Kept for the life of the db (bounded by the table count); a live read
// establishes it so REMOTE writes to that table reach this db's graphs.
const subscriptions = new WeakMap<object, Set<string>>()

/** Subscribe this db to each table's topic (idempotent per db+table). A received batch is deduped by id
 *  (own round-trip / same batch on another subscribed topic) and then `router.ingest`'d ONCE. */
function ensureSubscribed(db: object, tables: readonly string[]): void {
  let subscribed = subscriptions.get(db)
  if (!subscribed) subscriptions.set(db, (subscribed = new Set()))
  for (const table of tables) {
    if (subscribed.has(table)) continue
    subscribed.add(table)
    Broadcast.subscribe(batchTopic(table), (data) => {
      const { id, changes } = data as BatchMessage
      if (seen.has(id)) return // dedupe: our own round-trip, or this batch already applied via another topic
      markSeen(id)
      registryFor(db).router.ingest({ changes })
    })
  }
}
