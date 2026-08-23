export { reactiveDrizzle }
export type { Reactive }

import { type WriteContext, captureMutation, captureRawSql } from './writeCapture.js'
import type { CaptureSink } from './writeChanges.js'
import { announceCoarse, ingestWrite } from '../bus/dbRuntime.js'
import { configureChangeRuntime } from '../bus/changeRuntime.js'
import type { ChangeTransport } from '../bus/changeTransport.js'
import { captureTransactions, isWriteOp } from './writeProxy.js'
import { isCoarseAllSurface } from './writeTerminals.js'
import { wrapLiveSelect } from './readCapture.js'
import { probeOldNewReturning } from './writeCapabilities.js'
import { installLiveReplacer } from '../primitive/wireServer.js'
import { dialectOf } from './binding/database.js'
import { assertUsage } from '../utils/assert.js'
import type { Reactive, ReactiveDatabase } from './reactiveDrizzle.types.js'

/** Reactive-db options. `changeTransport` is the DEDICATED cross-instance transport for THIS feature's
 *  change fan-out — independent of the app's `config.broadcast.transport`; omit it to use the built-in
 *  in-process default (zero-setup dev; inject a transport-backed one for real multi-process fan-out).
 *
 *  `changeNamespace` names the LOGICAL DATABASE these changes belong to, and is REQUIRED alongside an
 *  injected transport: it is what keeps two databases sharing one broker from applying each other's rows.
 *  With the in-process default it is derived from the connection instead, so it is not needed. */
type ReactiveOptions = { changeTransport?: ChangeTransport; changeNamespace?: string }

// The reactive-db RUNTIME: the proxy that routes reads, writes, transactions and raw SQL. The type
// transform that gives the chain its terminal `.live()` lives in reactiveDrizzle.types.ts; the write and
// transaction proxying lives in writeProxy.ts.
//
// `reactiveDrizzle(baseDb)` binds to NOTHING request-scoped: `export const db = reactiveDrizzle(baseDb)`
// at module level is the intended shape, importable anywhere, callable at any point in a telefunction.
// Everything request-bound happens at SERIALIZE time, where the wire replacer sees the returned Live and
// has the request context it needs.

/**
 * Set up reactive queries for a Drizzle `db` (or transaction). Call it anywhere — module level is the
 * intended shape (`export const db = reactiveDrizzle(baseDb)`), and inside a telefunction, before or after
 * an await, works identically. The reactive db's `select()` builds an ordinary
 * Drizzle query that you can either `await` for plain rows or terminate with `.live()` to get a
 * `Live<T[]>`. Everything except `.live()` is the ordinary async builder — `execute`/`prepare` and the
 * dialect runners are preserved. Writes (`insert`/`update`/`delete`) and every other surface run as plain
 * Drizzle, unchanged.
 *
 * ```ts
 * const db = reactiveDrizzle(baseDb)
 * const todos = await db.select().from(todosTable).live() // Live<Todo[]>
 * ```
 *
 * Only chains that start from this db's own `select()` are reactive. A CTE-prefixed read
 * (`db.with(cte).select()…`) goes through Drizzle's ordinary `with()` facade and is NOT reactive — it has
 * no `.live()`. Lift the CTE into the select (a sub-query) if you need the result live.
 *
 * For multi-process deployments, pass `{ changeTransport }` to fan writes across instances over a dedicated
 * Live transport (independent of the app's `config.broadcast.transport`); omitted, an in-process default is
 * used (zero-setup, fans out only within the process).
 */
function reactiveDrizzle<TDb extends ReactiveDatabase>(baseDb: TDb, options?: ReactiveOptions): Reactive<TDb> {
  const db = baseDb as object
  // Reject an unsupported database HERE, at setup, rather than at whichever read or write happens to ask
  // first. `dialectOf` is the single place that decides what this package targets and throws on anything
  // else — calling it for its verdict is what makes MySQL an explicit refusal instead of a db that looks
  // reactive and silently is not. The type surface rejects the same db (reactiveDrizzle.types.ts); this is
  // the half that a JavaScript caller still gets.
  dialectOf(db)
  // Teach telefunc how to put a Live on the wire. Registered HERE rather than at import time, so importing
  // this package registers nothing and the user never has to perform a config step: by the time any
  // telefunction can return a Live, its db has been wrapped, so the replacer is always in place first.
  installLiveReplacer()
  // An injected transport reaches other PROCESSES, where the connection-derived identity a local default
  // uses cannot follow. Without a stable name, two databases on one broker share a topic and apply each
  // other's row deltas — so this fails loudly rather than cross-feeding quietly.
  assertUsage(
    !options?.changeTransport || !!options.changeNamespace,
    'reactiveDrizzle(db, { changeTransport }) also needs `changeNamespace`: a stable id for THIS logical database, identical on every server that shares the transport and different from any other database on it. Without it, two databases on one transport would exchange row changes.',
  )
  // Register the dedicated change transport + namespace for this db (the default transport is the in-process
  // bus). Installed ATOMICALLY — a db must never end up with one and not the other. Reads subscribe over it
  // and writes publish over it — never the user's app Broadcast. The namespace is set-once; the transport is
  // set-once WHILE IN USE; whether this db currently holds a subscription that a rotation would strand is
  // the change runtime's own state, so it decides that itself rather than being told.
  configureChangeRuntime(db, {
    transport: options?.changeTransport,
    namespace: options?.changeNamespace,
  })
  // Ask this connection ONCE, here at setup, whether it can return both images of a changed row. Nothing
  // waits on the answer; it lands long before any request and simply lets later writes be more precise.
  probeOldNewReturning(db)
  // Autocommit writes ingest into THIS db's graphs immediately; a transaction buffers and flushes here once.
  const ingest: CaptureSink = (changes) => ingestWrite(db, { changes })
  const autocommit = { sinkMode: 'autocommit', identityDb: db, sink: ingest } as const satisfies WriteContext
  return new Proxy(db, {
    get(target, prop, receiver) {
      if (prop === 'select') {
        // Wrap ONLY this db's own select builders: the returned chain is live-capable (`.live()` routes
        // to the read-capture engine), while `then`/`execute` forward untouched to plain rows.
        return (...args: unknown[]) => {
          const baseBuilder = (target as { select: (...a: unknown[]) => unknown }).select(...args)
          return wrapLiveSelect(baseBuilder, target)
        }
      }
      if (isWriteOp(prop)) {
        // Writes run as plain Drizzle, then feed their change into this db's graphs (via captureMutation →
        // ingestWrite) so the live reads they affect refetch. Keyed to `db` (the top db) so it reaches the
        // SAME registry the reads acquired from; no sink → autocommit ingest.
        const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
        return captureMutation(prop, base.bind(target), autocommit)
      }
      if (prop === 'transaction') {
        // Writes INSIDE the transaction must capture too (a forwarded raw tx db would bypass capture) — and
        // buffer until the commit boundary, so one committed transaction is one atomic graph tick.
        return captureTransactions(target, db)
      }
      if (isCoarseAllSurface(prop)) {
        // Raw SQL (`db.run(sql`…`)`, `db.execute(sql`…`)`, …) can mutate anything, and its touched tables are
        // unknowable without parsing — so it fails closed by ANNOUNCING itself once it completes
        // (`announceCoarse`, which owns both halves of that) rather than executing silently uncaptured.
        const base = Reflect.get(target, prop, receiver)
        if (typeof base === 'function') {
          return captureRawSql((base as (...a: unknown[]) => unknown).bind(target), () => announceCoarse(db))
        }
      }
      return Reflect.get(target, prop, receiver)
    },
  }) as unknown as Reactive<TDb>
}
