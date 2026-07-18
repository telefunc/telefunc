// The engine read-capture. `reactiveDrizzle(db).select(...)` produces a CHAINABLE
// live-builder (via wrapLiveSelect) whose terminal `.live()` runs the pipeline: extractQueryShape →
// compileQuery → registry.acquire (eager-async hydrate in the prologue) → host.createLive(rows) +
// attachSource → Live<Row[]>. The `Live` producer + the request cleanup come from telefunc's extension
// HOST (getTelefuncHost()), never telefunc internals. Awaiting the same builder (no `.live()`) forwards to
// plain rows. The wire replacer activates the handle at SERIALIZE time,
// synchronously redeeming the read token (subscribe-at-redeem + the seqAtRead fence) via the source's
// `subscribe`; on the last owning channel's close it releases the lease. A handle that is never
// serialized never activates, so its token stays un-redeemed and the request's finally-sweep
// (disposeUnredeemedReads) releases it — net-zero, no leak. reactiveDrizzle imports wrapLiveSelect /
// disposeUnredeemedReads from here DIRECTLY (no install seam) — this module IS the engine surface.

export { wrapLiveSelect, disposeUnredeemedReads, compilePlanFor }
export type { ReadCarrier }

import { type Table, isTable } from 'drizzle-orm'
import type { Live, LiveProducer } from 'telefunc'
import { getTelefuncHost } from './telefuncHost.js'
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
import { acquireSubscription, type SubscriptionRef } from './writeTransport.js'

/** A read token minted for a db.live handle in the current request, tracked on the carrier so the
 *  request's finally-sweep can release it if the handle is never serialized (never activated). The
 *  handle's serialize-time `activate` flips `redeemed`, so an activated token is skipped by the sweep
 *  (its lease is owned by the wire channel and released on close). It carries the read's ref on the db's
 *  change subscription too — the two have exactly the same owner at every moment, so they travel together
 *  from token to lease and are dropped by whichever of them ends the read. */
type MintedToken = { token: ReadToken; redeemed: boolean; subscription: SubscriptionRef }

/** What the per-request DbLiveCarrier carries: `wrapLiveSelect` pushes here, `disposeUnredeemedReads`
 *  reads here. */
type ReadCarrier = { mintedTokens: MintedToken[] }

/** Wrap a live SELECT builder into a CHAINABLE builder: `from`/`where`/… forward to the underlying
 *  drizzle builder and re-wrap the result (so the chain stays live); non-builder returns (`toSQL`,
 *  metadata) pass through untouched; the terminal `.live()` runs the read-capture pipeline and resolves
 *  to `Live<Row[]>`, while `then`/`execute` forward untouched to plain rows (the base builder is itself
 *  a `QueryPromise`). This is the runtime behind reactiveDrizzle's terminal `.live()`. */
function wrapLiveSelect(baseBuilder: unknown, carrier: ReadCarrier, db: object): unknown {
  return new Proxy(baseBuilder as object, {
    get(target, prop, receiver) {
      if (prop === 'live') {
        // The terminal: run the read-capture pipeline and resolve to Live<Row[]>. `.live` is synthesized
        // here — the base drizzle builder has no such member, so nothing leaks onto a plain builder.
        return () => captureAndBuild(target, carrier, db)
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isBuilder(next) ? wrapLiveSelect(next, carrier, db) : next
      }
    },
  })
}

/** The read-capture pipeline for one live query. Runs in the db.live prologue (eager-async, before
 *  serialize): compile + acquire (hydrate) + read the initial rows, then wire a Live whose serialize-
 *  time activation redeems the token. `notify` forwards the graph's invalidation to `live.invalidate`
 *  (equivalent to the source's onInvalidate); the token subscribes it at redeem (seam 1), and the
 *  seqAtRead fence replays a change that landed during the read window (seam 2). */
async function captureAndBuild(builder: unknown, carrier: ReadCarrier, db: object): Promise<Live<Row[]>> {
  const dialect = dialectOf(db)
  const shape = extractQueryShape(builder, { dialect })
  // READINESS BARRIER: take this read's ref on the db's change subscription AND await the transport's
  // admission of it — before both the graph hydrate (registry.acquire) and the snapshot read below. This
  // closes the window where a write on another instance, landing between our read and a not-yet-established
  // subscription, would be missed. Fails the read closed if the transport refuses. Only live reads reach
  // here — a plain awaited builder or a non-Live telefunction never subscribes. The subscription is per-DB,
  // not per-table: one topic carries every change and the router filters locally.
  const subscription = await acquireSubscription(db)
  // Held until the carrier owns it below. EVERY way out of the read between here and there — a failed
  // environment probe, compile, hydrate — has to give the ref back, or a db stays subscribed for a live
  // query that never existed.
  let owned = false
  try {
    const env = {
      dialect,
      semanticEnvironmentKey: await semanticEnvironmentKeyOf(db),
      schemaFingerprint: schemaFingerprint(tableObjectsOf(builder)),
    }
    const { instanceKey } = identityOf(builder, env)
    const rlsEnabled = await anyRlsEnabled(db, shape.tables)

    // notify is set to forward to the (not-yet-created) Live; it is only ever CALLED at redeem-time or
    // later (a graph invalidation), by which point `live` exists — an un-redeemed token is inert, so no
    // fire reaches this before activation.
    let live: LiveProducer<Row[]> | undefined
    // Only the token is needed here — the graph drives invalidation through the `notify` callback below.
    const { token } = await registryFor(db).acquire({
      instanceKey,
      tables: shape.tables,
      rlsEnabled,
      compilePlan: compilePlanFor(db, shape),
      executor: hydrationExecutorOf(db),
      notify: () => live?.invalidate(),
    })

    // OWN the token AND the subscription ref on the carrier IMMEDIATELY — BEFORE the fallible σ-read — so a
    // rejecting read still leaves a sweepable (un-redeemed) entry: the request finally-sweep releases both
    // → net-zero, no leak.
    const entry: MintedToken = { token, redeemed: false, subscription }
    carrier.mintedTokens.push(entry)
    owned = true

    const initialRows = (await builder) as Row[] // the initial result is a plain read; the graph signals staleness
    live = getTelefuncHost().createLive<Row[]>(initialRows)
    live.attachSource({
      subscribe: () => {
        // Serialize-time activation (SYNC): redeem the token — subscribe its notify to the graph's sink
        // and replay the seqAtRead fence — and mark it activated so the finally-sweep skips it. The
        // returned teardown releases the lease when the last owning channel closes, and with it the read's
        // share of the db's subscription.
        const lease = token.redeem()
        entry.redeemed = true
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
    if (!owned) subscription.release()
  }
}

/** The plan compiler for one query, gated on session provability: a single-session connection (a
 *  pinned pg Client / node:sqlite / single mysql Connection) compiles precisely; a POOLED-UNPINNED
 *  connection can't prove the executing session's authority (role/search_path/RLS), so precise state
 *  could hydrate from a mismatched session — force it COARSE (invalidate-on-any-change, sound). */
function compilePlanFor(db: object, shape: QueryShape): () => GraphPlan {
  return isSingleSession(db) ? () => compileQuery(shape) : () => coarsePlan(shape.tables)
}

/** Release every read token the request minted but never activated (a handle that was created but
 *  never serialized), and with it that read's share of the db's change subscription. Idempotent:
 *  `token.release()` is a safe no-op path on an un-redeemed token, and activated tokens (`redeemed`) are
 *  skipped — their lease and subscription ref are channel-owned and released on close. */
function disposeUnredeemedReads(carrier: ReadCarrier): void {
  for (const entry of carrier.mintedTokens) {
    if (entry.redeemed) continue
    entry.token.release()
    entry.subscription.release()
  }
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
