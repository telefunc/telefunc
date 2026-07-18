export { publishBatch, publishCoarseAll, ensureSubscribed, batchTopic, WILDCARD_TABLE, appliedMarkerCount }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { type ChangeMessage, transportFor } from './changeTransport.js'
import { RESERVED_RELATION_NAMES } from '../ir/relation.js'
import type { ChangeBatch } from '../router/events.js'
import { assertUsage } from '../utils/assert.js'

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

/** The WILDCARD coarse channel (`__live__:*`). Every db subscribes to it alongside its table topics — it
 *  rides the same subscribe + readiness-probe machinery by being treated as a reserved "table". It carries
 *  ONLY coarse-all directives: a mutation whose touched tables are unknowable (raw SQL) coarsens the
 *  publisher's own watched tables locally, but a table watched ONLY on another instance would otherwise
 *  never hear about it — a missed invalidation. A receiver of a coarse-all coarsens ITS OWN watched tables.
 *
 *  Every OTHER topic is keyed by a relation IDENTITY (ir/relation.ts), so this literal must never be one:
 *  `relationIdOf` reserves it (a table actually named `*` takes the framed form instead) and the assertion
 *  below fails the build-time contract if the two ever drift apart. The wildcard itself is deliberately
 *  NOT qualified — it addresses every relation, not one. */
const WILDCARD_TABLE = '*'
assertUsage(
  RESERVED_RELATION_NAMES.includes(WILDCARD_TABLE),
  `the wildcard topic ${WILDCARD_TABLE} must be reserved in ir/relation.ts, or a relation could claim it`,
)

// CROSS-TOPIC DE-DUPLICATION — apply-once-on-first-receipt.
//
// This layer publishes ONE batch to EACH touched table's topic (decision #5: per-table fan-out, and the whole
// batch travels so a multi-table transaction stays ONE atomic tick). A db subscribed to several of those
// topics therefore receives several copies of the same batch and must apply exactly one: precise row
// application is NOT idempotent — a stateful aggregate would count the same delta twice.
//
// A receiver applies a copy iff BOTH:
//   1. `origin !== dbIdOf(db)` — a db never applies its own batch through the transport (its graphs were fed
//      DIRECTLY in ingestWrite); and
//   2. it is the FIRST copy of that batch id this db has seen — recorded in a short-lived APPLIED marker.
//
// SOUNDNESS INVARIANT, and why the previous rule failed it. The earlier rule picked an owner topic — "the
// first of the batch's tables this db is subscribed to" — by consulting the `readiness` map AT DELIVERY TIME.
// That map is mutable: a later live read adds entries. Two consequences, both real:
//   - a PENDING subscription could be named owner while its topic was not yet actually listening, so the copy
//     that would have applied never arrived and every other copy deferred to it — the batch was DROPPED; and
//   - if the subscribed set changed BETWEEN two copies of one batch, the two copies could compute different
//      owners and both apply — a DOUBLE-APPLY.
// The rule's stated invariant ("every delivery of a given batch sees the same subscribed set") was simply not
// true. Ownership must not depend on mutable receiver state, so it now depends on nothing but arrival order.
//
// The marker is time-bounded, which is sound only against an EXPLICIT transport guarantee — see the
// `MAX_FANOUT_SKEW_MS` clause on the ChangeTransport contract: every copy arriving AFTER THE FIRST reaches a
// given subscriber within that window of it. Once the window has passed no further copy can arrive, so
// expiring the marker cannot resurrect a duplicate. Partial fan-out needs no guarantee at all — each copy
// carries the WHOLE batch, so one is sufficient and the rest are pure duplicates.
//
// This is an ARGUED bound tied to a stated contract, not a silent cap: the earlier count-bounded `seen` set
// had no such guarantee — after 4,096 unrelated ids the original aged out and a delayed copy applied twice.
// The window is measured on a MONOTONIC clock, because a wall-clock step would expire a marker whose second
// copy is still moments away in real time (see `monotonicNow`).

/** The publishing identity of a db — stable for the db's lifetime, used to drop our own round-trip. */
const dbIds = new WeakMap<object, string>()
function dbIdOf(db: object): string {
  let id = dbIds.get(db)
  if (!id) dbIds.set(db, (id = randomUUID()))
  return id
}

/** How long after its FIRST copy a batch's marker is kept — the window the transport contract bounds
 *  later copies by. The built-in default transport delivers synchronously inside the publish loop, so its
 *  real skew is zero; the window exists for transport-backed deployments. */
const MAX_FANOUT_SKEW_MS = 30_000

/** Elapsed milliseconds on a MONOTONIC clock. Deliberately NOT `Date.now()`: the wall clock can step
 *  (NTP correction, a VM resuming, an operator setting the time), and a forward step would expire a marker
 *  whose second copy is still moments away in real time — applying the same precise batch twice. Only a
 *  monotonic source makes "all entries share one TTL, so insertion order IS expiry order" actually true. */
const monotonicNow = (): number => performance.now()

/** A db's applied-batch markers plus the timer that sweeps them. The sweep closure captures THIS STATE,
 *  never the db, so the timer alone does not pin one.
 *
 *  RETENTION, stated honestly: the WeakMap does NOT make a db collectable in practice. A db that has served
 *  one live read holds a transport subscription whose callback closes over it (`subscribeAndProbe`), and on
 *  the success path that subscription is never torn down — with the default in-process transport, whose
 *  topic table is a module singleton, that reference lives for the process. So this map and the others keyed
 *  by db (`readiness`, `dbIds`, `registries`) behave as strong maps. Harmless for the usual one-db-per-
 *  process shape; a per-tenant or per-connection db instance leaks its registry and graph state. Closing
 *  that needs a db-disposal seam, which is deliberately not invented here. */
type AppliedState = { markers: Map<string, number>; sweep?: ReturnType<typeof setTimeout> }
const applied = new WeakMap<object, AppliedState>()

/** Drop every marker whose window has passed. Entries share one TTL and the Map preserves insertion order,
 *  so expiry order is insertion order: stop at the first live entry. */
function pruneExpired(state: AppliedState, now: number): void {
  for (const [id, expiresAt] of state.markers) {
    if (expiresAt > now) break
    state.markers.delete(id)
  }
}

/** Keep "short-lived" literally true. Pruning only on the next admission would pin a burst of expired ids
 *  for the db's lifetime if traffic then stopped, so a timer sweeps them out. One timer per db at a time,
 *  rescheduled while markers remain, and `unref`'d so it never holds the process open. */
function scheduleSweep(state: AppliedState): void {
  if (state.sweep) return // a sweep is already pending; it reschedules itself while work remains
  state.sweep = setTimeout(() => {
    state.sweep = undefined
    pruneExpired(state, monotonicNow())
    if (state.markers.size > 0) scheduleSweep(state)
  }, MAX_FANOUT_SKEW_MS)
  state.sweep.unref?.()
}

/** Admit a batch copy for application — true for the FIRST copy this db sees, false for every later one.
 *  This is the whole cross-topic dedupe decision, and it reads no mutable subscription state. */
function admitBatch(db: object, batchId: string): boolean {
  let state = applied.get(db)
  if (!state) applied.set(db, (state = { markers: new Map() }))
  const now = monotonicNow()
  pruneExpired(state, now)
  if (state.markers.has(batchId)) return false
  state.markers.set(batchId, now + MAX_FANOUT_SKEW_MS)
  scheduleSweep(state)
  return true
}

/** How many applied-batch markers this db currently holds. An OBSERVATION SEAM: the claim that markers are
 *  short-lived is otherwise untestable, since nothing else makes their retention visible. */
function appliedMarkerCount(db: object): number {
  return applied.get(db)?.markers.size ?? 0
}

/** Publish a committed batch cross-instance over the db's changeTransport: one message per touched table's
 *  topic, every copy carrying the SAME batch id so a receiver subscribed to several of those topics applies
 *  exactly one of them. No-op for an empty batch. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const transport = transportFor(db)
  const tables = [...new Set(batch.changes.map((change) => change.table))]
  const message = { id: randomUUID(), origin: dbIdOf(db), changes: batch.changes }
  for (const table of tables) transport.publish(batchTopic(table), message)
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL) to every OTHER instance on this
 *  transport: each coarsens its own watched tables. The publisher's own graphs are fed directly (the local
 *  coarse batch), and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  transportFor(db).publish(batchTopic(WILDCARD_TABLE), { origin: dbIdOf(db), coarseAll: true })
}

// Bounded probe schedule — a proven-listening handshake tops out at ~1s before failing the read closed.
const PROBE_INTERVAL_MS = 25
const PROBE_MAX_ATTEMPTS = 40

// One readiness promise per (db, table). Cached: subscribeAndProbe subscribes ONCE (the subscription lives
// for the db, receiving future batches) and resolves when the probe proves it listening; later reads await
// the same (already-resolved) promise. Keyed by db — but see the retention note on `applied`: the live
// subscription itself pins the db, so this is weak in type only.
const readiness = new WeakMap<object, Map<string, Promise<void>>>()

/** Subscribe this db to each table's topic and resolve once every subscription is PROVEN LISTENING (idempotent
 *  per db+table). A live read awaits this before its snapshot read, so a write on another instance can never
 *  land in the window between the read and a not-yet-listening subscription. Rejects (fails the read closed)
 *  if a subscription cannot be proven listening within the bounded probe schedule. */
function ensureSubscribed(db: object, tables: readonly string[]): Promise<void> {
  let perTable = readiness.get(db)
  if (!perTable) readiness.set(db, (perTable = new Map()))
  const waits: Promise<void>[] = []
  // The wildcard coarse channel is subscribed alongside the read's tables and proven by the SAME probe
  // machinery, so a live read is admitted only once it can also hear unknowable-table (raw SQL) writes.
  for (const table of [...tables, WILDCARD_TABLE]) {
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
      if ('coarseAll' in message) {
        // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table
        // WE watch (the publisher can't know them). Sound over-fire; never a fabricated row.
        const watched = registryFor(db).router.watchedTables()
        if (watched.length > 0) {
          registryFor(db).router.ingest({ changes: watched.map((t) => ({ table: t, kind: 'coarse' as const })) })
        }
        return
      }
      if (!admitBatch(db, message.id)) return // an earlier copy of this batch (another topic) already applied
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
