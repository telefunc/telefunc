// The engine read-capture. `reactiveDrizzle(db).select(...)` produces a CHAINABLE
// live-builder (via wrapLiveSelect) whose terminal `.live()` runs the pipeline: extractQueryShape →
// compileQuery → registry.acquire (eager-async hydrate in the prologue) → new LiveCell(rows) +
// attachSource → Live<Row[]>. The cell is this package's OWN primitive (src/primitive/live.ts) — telefunc
// core knows nothing about it. Awaiting the same builder (no `.live()`) forwards to plain rows.
//
// NOTHING HERE IS REQUEST-SCOPED, and that is the point: `reactiveDrizzle(db)` can be called at module
// level and shared by every telefunction. Attachment happens at SERIALIZE time — the wire replacer sees
// the Live in the return value, creates the channel, and activates the handle, synchronously redeeming
// the read token (subscribe-at-redeem + the seqAtRead fence) via the source's `subscribe`; on the last
// owning channel's close it releases the lease. The replacer has the request context it needs, so this
// module never asks for any.
//
// ABANDONMENT — a `.live()` whose handle is never serialized (the telefunction throws, or returns
// something else). Its token and subscription ref are released when the Live is COLLECTED, via the
// FinalizationRegistry below. That is unbounded by spec and in practice the next GC; see the honest limit
// in the docs. It replaces a per-request sweep that required `reactiveDrizzle(db)` to be called before the
// body's first await — a usage rule the owner rejected — and which was in any case a leaky net: a handle
// created after the sweep had run was never swept at all.
//
// What is retained until then is the GRAPH, not a counter: the token is a ref, and the ref is what keeps the
// registry entry (and any hydrated row state) alive. A query sharing that identity keeps it alive regardless;
// a unique one holds it alone. Paths that fail BEFORE producing a handle release immediately instead — there
// is nothing to collect, so there is no reason to wait for a collector.

export { wrapLiveSelect, compilePlanFor }

import { type Table, isTable } from 'drizzle-orm'
import { LiveCell } from '../primitive/live.js'
import type { Live } from '../primitive/live.js'
import { type GraphPlan, coarsePlan, compileQuery } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { selectConfigOf } from '../binding/drizzleShape.js'
import { dialectOf, isSingleSession, rlsEnabledOf, semanticEnvironmentKeyOf } from '../binding/database.js'
import { hydrationExecutorOf } from '../binding/hydrationExecutor.js'
import { schemaFingerprint } from '../extract/columns.js'
import { identityOf } from '../extract/identity.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { parseRelationId } from '../ir/relation.js'
import type { QueryShape } from '../ir/types.js'
import type { ReadToken } from '../graph/registry.js'
import { registryFor } from './dbRuntime.js'
import { acquireSubscription, type SubscriptionRef } from './changeRuntime.js'

/** What one live read holds between `.live()` and either its activation or its collection: the graph's
 *  read token and the db's change-subscription ref. The two have exactly the same owner at every moment,
 *  so they travel together and are dropped by whichever of them ends the read. `redeemed` is flipped by
 *  serialize-time activation, after which the channel lease owns both and the finalizer must not touch
 *  them — `token.release()` on a redeemed token is a usage error, not a no-op. */
type ReadOwnership = { token: ReadToken; subscription: SubscriptionRef; redeemed: boolean }

/** Releases what an ABANDONED live read held, once the handle is collected.
 *
 *  The held value must not reach the Live, or the Live is never collected and this never fires. That is
 *  why the graph's `notify` goes through `invalidationSink()` below: the token closes over `notify`, and
 *  the finalizer holds the token, so a `notify` that closed over the Live strongly would keep every
 *  abandoned handle alive forever — a silently dead net that still looks like a net. */
const abandonedReads = new FinalizationRegistry<ReadOwnership>((owned) => {
  if (owned.redeemed) return // activated: the wire channel's lease owns these and releases them on close
  owned.token.release()
  owned.subscription.release()
})

/** The graph's invalidation sink for one read, holding the Live WEAKLY.
 *
 *  Deliberately its own function so its closure scope contains the WeakRef and nothing else: closures
 *  declared in one scope share a context, so building this inline beside a strong `live` binding could
 *  retain the Live through that shared context and defeat the finalizer. A collected Live simply stops
 *  receiving invalidations, which is correct — nobody is holding it to receive them. */
function invalidationSink(): { notify: () => void; hold: (live: LiveCell<Row[]>) => void } {
  let ref: WeakRef<LiveCell<Row[]>> | undefined
  return {
    notify: () => ref?.deref()?.invalidate(),
    hold: (live) => {
      ref = new WeakRef(live)
    },
  }
}

/** Wrap a live SELECT builder into a CHAINABLE builder: `from`/`where`/… forward to the underlying
 *  drizzle builder and re-wrap the result (so the chain stays live); non-builder returns (`toSQL`,
 *  metadata) pass through untouched; the terminal `.live()` runs the read-capture pipeline and resolves
 *  to `Live<Row[]>`, while `then`/`execute` forward untouched to plain rows (the base builder is itself
 *  a `QueryPromise`). This is the runtime behind reactiveDrizzle's terminal `.live()`. */
function wrapLiveSelect(baseBuilder: unknown, db: object): unknown {
  return new Proxy(baseBuilder as object, {
    get(target, prop, receiver) {
      if (prop === 'live') {
        // The terminal: run the read-capture pipeline and resolve to Live<Row[]>. `.live` is synthesized
        // here — the base drizzle builder has no such member, so nothing leaks onto a plain builder.
        return () => captureAndBuild(target, db)
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isBuilder(next) ? wrapLiveSelect(next, db) : next
      }
    },
  })
}

/** The read-capture pipeline for one live query: compile + acquire (hydrate) + read the initial rows,
 *  then wire a Live whose serialize-time activation redeems the token. The sink forwards the graph's
 *  invalidation to `live.invalidate` (equivalent to the source's onInvalidate); the token subscribes it at
 *  redeem (seam 1), and the seqAtRead fence replays a change that landed during the read window (seam 2). */
async function captureAndBuild(builder: unknown, db: object): Promise<Live<Row[]>> {
  const dialect = dialectOf(db)
  const shape = extractQueryShape(builder, { dialect })
  // READINESS BARRIER: take this read's ref on the db's change subscription AND await the transport's
  // admission of it — before both the graph hydrate (registry.acquire) and the snapshot read below. This
  // closes the window where a write on another instance, landing between our read and a not-yet-established
  // subscription, would be missed. Fails the read closed if the transport refuses. Only live reads reach
  // here — a plain awaited builder or a non-Live telefunction never subscribes. The subscription is per-DB,
  // not per-table: one topic carries every change and the router filters locally.
  const subscription = await acquireSubscription(db)
  // EVERY way out of this read has to give the ref back, or a db stays subscribed for a live query that
  // does not exist. Up to the handoff below that is this flag's job; after it, the channel lease's (on
  // activation) or the finalizer's (on abandonment).
  let handedOff = false
  try {
    const env = {
      dialect,
      semanticEnvironmentKey: await semanticEnvironmentKeyOf(db),
      schemaFingerprint: schemaFingerprint(tableObjectsOf(builder)),
    }
    const { instanceKey } = identityOf(builder, env)
    const rlsEnabled = await anyRlsEnabled(db, shape.tables)

    // The sink forwards graph invalidations to a Live that does not exist yet, and holds it weakly once it
    // does. It is only ever CALLED at redeem-time or later, by which point the Live exists — an un-redeemed
    // token is inert, so no fire reaches it before activation.
    const sink = invalidationSink()
    // Only the token is needed here — the graph drives invalidation through the sink.
    const { token } = await registryFor(db).acquire({
      instanceKey,
      tables: shape.tables,
      rlsEnabled,
      compilePlan: compilePlanFor(db, shape),
      executor: hydrationExecutorOf(db),
      notify: sink.notify,
    })

    let rows: Row[]
    try {
      rows = (await builder) as Row[] // the initial result is a plain read; the graph signals staleness
    } catch (error) {
      token.release() // the σ-read failed, so nothing will ever own this token — release it here
      throw error
    }

    const owned: ReadOwnership = { token, subscription, redeemed: false }
    const live = new LiveCell<Row[]>(rows)
    sink.hold(live)
    // From here the handle owns both refs: its channel lease releases them if it is serialized, and the
    // finalizer releases them if it is collected without ever being.
    abandonedReads.register(live, owned)
    handedOff = true
    live.attachSource({
      subscribe: () => {
        // Serialize-time activation (SYNC): redeem the token — subscribe its notify to the graph's sink
        // and replay the seqAtRead fence — and mark it activated so the finalizer leaves it alone. The
        // returned teardown releases the lease when the last owning channel closes, and with it the read's
        // share of the db's subscription.
        const lease = token.redeem()
        owned.redeemed = true
        return () => {
          lease.release()
          subscription.release()
        }
      },
    })
    // Just return the cell: it IS the `Live<Row[]>` the telefunction hands back, and the wire replacer
    // serializes it. No `.client` re-type — the public type simply doesn't advertise the producer verbs.
    return live
  } finally {
    if (!handedOff) subscription.release()
  }
}

/** The plan compiler for one query, gated on session provability: a single-session connection (a
 *  pinned pg Client / node:sqlite / PGlite) compiles precisely; a POOLED-UNPINNED
 *  connection can't prove the executing session's authority (role/search_path/RLS), so precise state
 *  could hydrate from a mismatched session — force it COARSE (invalidate-on-any-change, sound). */
function compilePlanFor(db: object, shape: QueryShape): () => GraphPlan {
  return isSingleSession(db) ? () => compileQuery(shape) : () => coarsePlan(shape.tables)
}

/** The distinct drizzle table objects a select builder references (from + joins + set-op arms),
 *  for the schema fingerprint. Non-table FROMs (subquery/CTE) contribute nothing here — the shape
 *  already degrades those to coarse. */
function tableObjectsOf(builder: unknown): Table[] {
  const config = selectConfigOf(builder)
  if (!config) return []
  const tables: Table[] = []
  if (isTable(config.table)) tables.push(config.table)
  for (const join of config.joins ?? []) if (isTable(join.table)) tables.push(join.table)
  for (const op of config.setOperators ?? []) tables.push(...tableObjectsOf(op.rightSelect))
  return tables
}

/** Whether any referenced relation has (or may have) row-level security — `true`/`'unknown'` both
 *  gate the graph to born-coarse (never assume off). `tables` carries ROUTING identities, so each is
 *  decoded back to the schema/name pair the catalog probe addresses: a schema-qualified relation is
 *  probed in ITS schema rather than assumed to live in `public`. */
async function anyRlsEnabled(db: object, tables: string[]): Promise<boolean> {
  for (const id of tables) {
    const { name, schema } = parseRelationId(id)
    if ((await rlsEnabledOf(db, name, schema ? { schema } : undefined)) !== false) return true
  }
  return false
}

/** A drizzle query builder, distinguished from a plain method return (metadata, `toSQL()` result). */
function isBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
