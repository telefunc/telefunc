export { acquireCarrier, captureMutation }

import { getTableName, isTable } from 'drizzle-orm'
import { getRawContext } from 'telefunc'
import { getTelefuncHost } from './telefuncHost.js'
import { ingestWrite } from './dbRuntime.js'
import type { DbLiveCarrier } from './reactiveDrizzle.js'
import { disposeUnredeemedReads, type ReadCarrier } from './readCapture.js'

// The per-request carrier every `db.live` read in one request shares. Acquiring it must happen before
// the body's first await, while the request context still exists; the cleanup that follows serialization
// (registered through telefunc's extension host, not its internals) releases anything a read reserved but
// never used.

/** The context symbol under which a request's db.live carrier is stashed — one carrier per request,
 *  reused by every `.live` read in that request. */
const REACTIVE_CARRIER = Symbol.for('telefunc.reactiveDrizzleCarrier')

/** The concrete carrier: the read half (the tokens each live read reserves) plus the opaque brand the
 *  client surface threads through. */
type CarrierImpl = ReadCarrier & { __dbLiveCarrier: true }

/** Acquire the per-request carrier — MUST run before the body's first await, while `getRawContext()`
 *  is still non-null (it nulls at the first macrotask in sync mode, the no-async_hooks reality). Get-or-
 *  create on the context, and register the finally-sweep (via the extension host) so any read token minted
 *  but never activated (a handle created but never serialized) is released post-serialize — net-zero, no
 *  leak. The host binds the cleanup to THIS request's context, so the drain finds it even after context
 *  nulls. */
function acquireCarrier(): DbLiveCarrier {
  const context = getRawContext()
  if (!context) {
    throw new Error('reactiveDrizzle(db) must be called inside a telefunction, before the body’s first await.')
  }
  const existing = context[REACTIVE_CARRIER] as CarrierImpl | undefined
  if (existing) return existing
  const carrier: CarrierImpl = { __dbLiveCarrier: true, mintedTokens: [] }
  context[REACTIVE_CARRIER] = carrier
  getTelefuncHost().onRequestCleanup(() => disposeUnredeemedReads(carrier))
  return carrier
}

/** Capture a write so the live reads it affects refetch. The mutation runs as PLAIN Drizzle; on success
 *  its change is fed into the db's graphs through the shared runtime (`ingestWrite`) — the production
 *  producer for `router.ingest`. Under decision #3's "new + old-PK only" contract, precise capture
 *  (`.returning()` / pre-write SELECT) lands in a later slice; TODAY every write emits a fail-closed
 *  COARSE change for its table (the safe default) — a live query on that table refetches, one on another
 *  table does not. A failed write ingests nothing (a write that didn't happen invalidates nothing). */
function captureMutation(
  _op: 'insert' | 'update' | 'delete',
  baseMethod: (...a: unknown[]) => unknown,
  db: object,
): (...a: unknown[]) => unknown {
  return (...args: unknown[]) => {
    // insert/update/delete all take the target table as their first argument.
    const table = isTable(args[0]) ? getTableName(args[0]) : undefined
    const builder = baseMethod(...args)
    return table === undefined ? builder : wrapMutation(builder, table, db)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and then ingests the
 *  change; chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapMutation(builder: unknown, table: string, db: object): unknown {
  return new Proxy(builder as object, {
    get(target, prop, receiver) {
      if (prop === 'then') {
        return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) =>
          runThenIngest(target, table, db).then(onFulfilled, onRejected)
      }
      if (prop === 'execute') {
        return (...args: unknown[]) => runThenIngest(target, table, db, args)
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isMutationBuilder(next) ? wrapMutation(next, table, db) : next
      }
    },
  })
}

/** Run the underlying mutation to completion, then — ONLY on success — feed a coarse change for its table
 *  into the db's graphs. */
async function runThenIngest(builder: unknown, table: string, db: object, executeArgs?: unknown[]): Promise<unknown> {
  const result = executeArgs
    ? await (builder as { execute: (...a: unknown[]) => Promise<unknown> }).execute(...executeArgs)
    : await (builder as PromiseLike<unknown>)
  ingestWrite(db, { changes: [{ table, kind: 'coarse' }] })
  return result
}

/** A drizzle mutation query builder, distinguished from a plain method return (a `toSQL()` object, a
 *  Promise). */
function isMutationBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
