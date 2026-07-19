export { reactiveDrizzle }
export type { Reactive }

import type { Assume, ColumnsSelection } from 'drizzle-orm'
import type { JoinNullability, SelectMode } from 'drizzle-orm/query-builders/select.types'
import type {
  PgSelectBase,
  PgSelectBuilder,
  PgSelectHKTBase,
  SelectedFields as PgSelectedFields,
} from 'drizzle-orm/pg-core'
import type { PgAsyncSelectBase, PgAsyncSelectBuilder } from 'drizzle-orm/pg-core/async/select'
import type {
  SQLiteSelectBase,
  SQLiteSelectBuilder,
  SQLiteSelectHKTBase,
  SelectedFields as SQLiteSelectedFields,
} from 'drizzle-orm/sqlite-core'
import type { SQLiteAsyncSelectBase, SQLiteAsyncSelectBuilder } from 'drizzle-orm/sqlite-core/async/select'
import type {
  MySqlSelectBase,
  MySqlSelectBuilder,
  MySqlSelectHKTBase,
  SelectedFields as MySqlSelectedFields,
} from 'drizzle-orm/mysql-core'
import type { MySqlAsyncSelectBase, MySqlAsyncSelectBuilder } from 'drizzle-orm/mysql-core/async/select'
import type { Live } from 'telefunc'
import { captureMutation, captureRawSql, emitSafely, type CaptureSink } from './writeCapture.js'
import { ingestLocal, ingestWrite, registryFor } from './dbRuntime.js'
import { publishCoarseAll } from './writeTransport.js'
import { type ChangeTransport, configureChanges } from './changeTransport.js'
import { wrapLiveSelect } from './readCapture.js'
import { probeOldNewReturning } from '../binding/database.js'
import type { TableChange } from '../router/events.js'
import { assertUsage } from '../utils/assert.js'

/** Reactive-db options. `changeTransport` is the DEDICATED cross-instance transport for THIS feature's
 *  change fan-out — independent of the app's `config.broadcast.transport`; omit it to use the built-in
 *  in-process default (zero-setup dev; inject a transport-backed one for real multi-process fan-out).
 *
 *  `changeNamespace` names the LOGICAL DATABASE these changes belong to, and is REQUIRED alongside an
 *  injected transport: it is what keeps two databases sharing one broker from applying each other's rows.
 *  With the in-process default it is derived from the connection instead, so it is not needed. */
type ReactiveOptions = { changeTransport?: ChangeTransport; changeNamespace?: string }

/** The driver-terminal surface an async select adds OVER the core query-builder select: the `QueryPromise`
 *  verbs (`then`/`catch`/`finally`/`execute`) plus `prepare` and each dialect's runners (SQLite
 *  `all`/`get`/`run`/`values`, MySQL `iterator`). Picked as exactly the members the async base adds and no
 *  more (`keyof async` minus `keyof core`), so intersecting it onto the reactive chain NEVER clashes with a
 *  chain method — the reactive HKT keeps owning `from`/`where`/joins, and this only re-adds the terminals
 *  the core builder lacks. This is what makes "everything except `.live()` is the ordinary async builder"
 *  literally true, including `execute(placeholderValues)`. */
type AsyncTerminals<TAsyncBase, TCoreBase> = Pick<TAsyncBase, Exclude<keyof TAsyncBase, keyof TCoreBase>>

// The reactive-db surface: the type transform, and the proxy behind it.
//
// `reactiveDrizzle(baseDb)` binds to NOTHING request-scoped: `export const db = reactiveDrizzle(baseDb)`
// at module level is the intended shape, importable anywhere, callable at any point in a telefunction.
// Everything request-bound happens at SERIALIZE time, where the wire replacer sees the returned Live and
// has the request context it needs.
//
// ── Why the terminal `.live()` rides an HKT seam ──────────────────────────────────────────────────
// Drizzle's select builders are parameterized by a Higher-Kinded Type (`PgSelectHKTBase` &c.) that it
// threads through EVERY chain method (`from`/`where`/`innerJoin`/…): each step re-applies the HKT to
// compute the next builder. We define a custom HKT (`PgReactiveSelectHKT` &c.) whose `_type` is the stock
// `PgSelectBase` (threaded with OUR HKT so the chain stays reactive), plus the async driver terminals
// (`AsyncTerminals`: `execute(placeholderValues)`/`prepare`/the dialect runners), plus
// `{ live(): Promise<Live<TResult>> }`. Because Drizzle's own generics keep doing all
// row/selection/nullability inference and our HKT only ADDS `live()`, the terminal rides the entire chain
// with the exact row type intact — no member-remap, nothing to collapse. `await`-ing the builder still
// yields plain rows; `.live()` crosses into `Live<TResult>`; and everything else is the ordinary async
// builder. This is the seam the earlier dotted-namespace transform lacked: a conditional remap of
// Drizzle's generic `from`/`where` collapsed their type parameters to their constraints and lost the row
// type; threading the HKT never remaps them.

// ── PostgreSQL ────────────────────────────────────────────────────────────────────────────────────
/** Our HKT node: identical to Drizzle's `PgSelectBase` at each chain step, but carrying `live()`. */
interface PgReactiveSelectHKT extends PgSelectHKTBase {
  _type: PgReactiveSelect<
    this['tableName'],
    Assume<this['selection'], ColumnsSelection>,
    this['selectMode'],
    Assume<this['nullabilityMap'], Record<string, JoinNullability>>,
    this['dynamic'],
    this['excludedMethods'],
    Assume<this['result'], any[]>,
    Assume<this['selectedFields'], ColumnsSelection>
  >
}
type PgReactiveSelect<
  TTableName extends string | undefined,
  TSelection extends ColumnsSelection | undefined,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability>,
  TDynamic extends boolean,
  TExcludedMethods extends string,
  TResult extends any[],
  TSelectedFields extends ColumnsSelection,
> = PgSelectBase<
  PgReactiveSelectHKT,
  TTableName,
  TSelection,
  TSelectMode,
  TNullabilityMap,
  TDynamic,
  TExcludedMethods,
  TResult,
  TSelectedFields
> &
  AsyncTerminals<
    PgAsyncSelectBase<
      TTableName,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >,
    PgSelectBase<
      PgReactiveSelectHKT,
      TTableName,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >
  > & { live(): Promise<Live<TResult>> }
type ReactivePgDb<TDb> = Omit<TDb, 'select'> & {
  select(): PgSelectBuilder<undefined, PgReactiveSelectHKT>
  select<TSelection extends PgSelectedFields>(fields: TSelection): PgSelectBuilder<TSelection, PgReactiveSelectHKT>
}

// ── SQLite (its select carries extra `TResultType` ('sync'|'async') + `TRunResult` slots) ───────────
// node-sqlite is SYNC (`resultType: 'sync'`), so its run/all/get/values return VALUES, not Promises. Both
// slots are DERIVED from the input db's own select builder and threaded through — hard-coding 'async' here
// would make the reactive terminals lie about a sync driver.
interface SQLiteReactiveSelectHKT extends SQLiteSelectHKTBase {
  _type: SQLiteReactiveSelect<
    this['tableName'],
    Assume<this['resultType'], 'sync' | 'async'>,
    this['runResult'],
    Assume<this['selection'], ColumnsSelection>,
    this['selectMode'],
    Assume<this['nullabilityMap'], Record<string, JoinNullability>>,
    this['dynamic'],
    this['excludedMethods'],
    Assume<this['result'], any[]>,
    Assume<this['selectedFields'], ColumnsSelection>
  >
}
type SQLiteReactiveSelect<
  TTableName extends string | undefined,
  TResultType extends 'sync' | 'async',
  TRunResult,
  TSelection extends ColumnsSelection,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability>,
  TDynamic extends boolean,
  TExcludedMethods extends string,
  TResult extends any[],
  TSelectedFields extends ColumnsSelection,
> = SQLiteSelectBase<
  SQLiteReactiveSelectHKT & { resultType: TResultType },
  TTableName,
  TRunResult,
  TSelection,
  TSelectMode,
  TNullabilityMap,
  TDynamic,
  TExcludedMethods,
  TResult,
  TSelectedFields
> &
  AsyncTerminals<
    SQLiteAsyncSelectBase<
      TTableName,
      TResultType,
      TRunResult,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >,
    SQLiteSelectBase<
      SQLiteReactiveSelectHKT & { resultType: TResultType },
      TTableName,
      TRunResult,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >
  > & { readonly _: { readonly resultType: TResultType } } & { live(): Promise<Live<TResult>> }
/** The run-result the input sqlite db's own select builder carries (node-sqlite: NodeSQLiteRunResult). */
type SqliteRunResultOf<TDb extends { select: (...args: any[]) => any }> = ReturnType<
  TDb['select']
> extends SQLiteSelectBuilder<any, infer TRunResult, any>
  ? TRunResult
  : unknown
/** The result mode ('sync'/'async') the input sqlite db's own select builder carries (node-sqlite: 'sync'). */
type SqliteResultTypeOf<TDb extends { select: (...args: any[]) => any }> = ReturnType<
  TDb['select']
> extends SQLiteSelectBuilder<any, any, infer THKT>
  ? THKT extends { resultType: infer TResultType extends 'sync' | 'async' }
    ? TResultType
    : 'async'
  : 'async'
type ReactiveSQLiteDb<TDb extends { select: (...args: any[]) => any }> = Omit<TDb, 'select'> & {
  select(): SQLiteSelectBuilder<
    undefined,
    SqliteRunResultOf<TDb>,
    SQLiteReactiveSelectHKT & { resultType: SqliteResultTypeOf<TDb> }
  >
  select<TSelection extends SQLiteSelectedFields>(
    fields: TSelection,
  ): SQLiteSelectBuilder<
    TSelection,
    SqliteRunResultOf<TDb>,
    SQLiteReactiveSelectHKT & { resultType: SqliteResultTypeOf<TDb> }
  >
}

// ── MySQL ───────────────────────────────────────────────────────────────────────────────────────
interface MySqlReactiveSelectHKT extends MySqlSelectHKTBase {
  _type: MySqlReactiveSelect<
    this['tableName'],
    Assume<this['selection'], ColumnsSelection>,
    this['selectMode'],
    Assume<this['nullabilityMap'], Record<string, JoinNullability>>,
    this['dynamic'],
    this['excludedMethods'],
    Assume<this['result'], any[]>,
    Assume<this['selectedFields'], ColumnsSelection>
  >
}
type MySqlReactiveSelect<
  TTableName extends string | undefined,
  TSelection extends ColumnsSelection,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability>,
  TDynamic extends boolean,
  TExcludedMethods extends string,
  TResult extends any[],
  TSelectedFields extends ColumnsSelection,
> = MySqlSelectBase<
  MySqlReactiveSelectHKT,
  TTableName,
  TSelection,
  TSelectMode,
  TNullabilityMap,
  TDynamic,
  TExcludedMethods,
  TResult,
  TSelectedFields
> &
  AsyncTerminals<
    MySqlAsyncSelectBase<
      TTableName,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >,
    MySqlSelectBase<
      MySqlReactiveSelectHKT,
      TTableName,
      TSelection,
      TSelectMode,
      TNullabilityMap,
      TDynamic,
      TExcludedMethods,
      TResult,
      TSelectedFields
    >
  > & { live(): Promise<Live<TResult>> }
type ReactiveMySqlDb<TDb> = Omit<TDb, 'select'> & {
  select(): MySqlSelectBuilder<undefined, MySqlReactiveSelectHKT>
  select<TSelection extends MySqlSelectedFields>(
    fields: TSelection,
  ): MySqlSelectBuilder<TSelection, MySqlReactiveSelectHKT>
}

/** The databases reactive queries support: a real, executable Drizzle db or transaction whose `select()`
 *  builds an ASYNC (awaitable, hydratable) query. A bare `QueryBuilder` — which type-checks a query but
 *  has no session to run it against — is deliberately excluded: a live read has nothing to hydrate from,
 *  so accepting one would type a `.live()` that could never resolve. */
type ReactiveDatabase = {
  select: (
    ...args: any[]
  ) => PgAsyncSelectBuilder<any> | MySqlAsyncSelectBuilder<any> | SQLiteAsyncSelectBuilder<any, any, any>
}

/** The reactive db: exactly `TDb` (every write/query surface preserved) with `select()` re-typed to build
 *  a terminal-`.live()` chain, dispatched by the db's dialect. Dialect is read structurally off the db's
 *  own async select BUILDER (the db base classes aren't exported, but the builders are, and each dialect's
 *  is distinct — no cross-match). Scope: only chains from the reactive db's OWN `select()` are live — a
 *  CTE-prefixed chain (`db.with(cte).select()…`) runs through Drizzle's ordinary `with()` facade, which is
 *  neither wrapped nor re-typed, so `.live()` is intentionally absent there (see the `reactiveDrizzle` doc). */
type Reactive<TDb extends ReactiveDatabase> = ReturnType<TDb['select']> extends PgAsyncSelectBuilder<any>
  ? ReactivePgDb<TDb>
  : ReturnType<TDb['select']> extends MySqlAsyncSelectBuilder<any>
    ? ReactiveMySqlDb<TDb>
    : ReturnType<TDb['select']> extends SQLiteAsyncSelectBuilder<any, any, any>
      ? ReactiveSQLiteDb<TDb>
      : never

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
  // An injected transport reaches other PROCESSES, where the connection-derived identity a local default
  // uses cannot follow. Without a stable name, two databases on one broker share a topic and apply each
  // other's row deltas — so this fails loudly rather than cross-feeding quietly.
  assertUsage(
    !options?.changeTransport || !!options.changeNamespace,
    'reactiveDrizzle(db, { changeTransport }) also needs `changeNamespace`: a stable id for THIS logical database, identical on every server that shares the transport and different from any other database on it. Without it, two databases on one transport would exchange row changes.',
  )
  // Register the dedicated change transport + namespace for this db (both set-once; the default transport is
  // the in-process bus). Installed ATOMICALLY — a db must never end up with one and not the other. Reads
  // subscribe over it and writes publish over it — never the user's app Broadcast.
  configureChanges(db, { transport: options?.changeTransport, namespace: options?.changeNamespace })
  // Ask this connection ONCE, here at setup, whether it can return both images of a changed row. Nothing
  // waits on the answer; it lands long before any request and simply lets later writes be more precise.
  probeOldNewReturning(db)
  // Autocommit writes ingest into THIS db's graphs immediately; a transaction buffers and flushes here once.
  const ingest: CaptureSink = (changes) => ingestWrite(db, { changes })
  // Raw SQL's coarse markers stay local — the coarse-all announcement is what reaches other instances.
  const localIngest: CaptureSink = (changes) => ingestLocal(db, { changes })
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
        return captureMutation(prop, base.bind(target), db)
      }
      if (prop === 'transaction') {
        // Writes INSIDE the transaction must capture too (a forwarded raw tx db would bypass capture) — and
        // buffer until the commit boundary, so one committed transaction is one atomic graph tick.
        return wrapTransaction(
          target,
          db,
          ingest,
          localIngest,
          () => publishCoarseAll(db),
          () =>
            registryFor(db)
              .router.watchedTables()
              .map((table) => ({ table, kind: 'coarse' as const })),
        )
      }
      if (isRawSqlOp(prop)) {
        // Raw SQL (`db.run(sql`…`)`, `db.execute(sql`…`)`, …) can mutate anything, and its touched tables are
        // unknowable without parsing — so it fails closed by coarsening every table this db has a graph on,
        // rather than executing silently uncaptured. Its coarse markers are ingested LOCALLY ONLY: other
        // instances hear about it in ONE coarse-all announcement, which reaches every table any of them
        // watches, so publishing this db's own markers as well would just make a remote live query refetch
        // twice.
        const base = Reflect.get(target, prop, receiver)
        if (typeof base === 'function') {
          return captureRawSql((base as (...a: unknown[]) => unknown).bind(target), db, localIngest)
        }
      }
      return Reflect.get(target, prop, receiver)
    },
  }) as unknown as Reactive<TDb>
}

function isWriteOp(prop: string | symbol): prop is 'insert' | 'update' | 'delete' {
  return prop === 'insert' || prop === 'update' || prop === 'delete'
}

/** The db-level RAW SQL execution surfaces — they bypass the builder entirely, so they need their own
 *  fail-closed capture (coarsen this db's watched tables). */
function isRawSqlOp(prop: string | symbol): boolean {
  return (
    prop === 'run' ||
    prop === 'execute' ||
    prop === 'all' ||
    prop === 'get' ||
    prop === 'values' ||
    // Not SQL execution, but a MUTATION with the same problem: it changes what a live query on the view
    // reads, and its effect is unknowable without resolving the view's definition. It used to fall through
    // to plain Drizzle and commit with nothing captured and nothing published — the same silent bypass
    // `tx.execute` had. Coarsening it is sound and it is a rare, deliberate operation.
    prop === 'refreshMaterializedView' ||
    // `db.batch([...])` runs a list of statements that may include writes (libSQL, D1, Neon-HTTP,
    // sqlite-proxy). Reconstructing which rows each item touched would mean re-planning every statement in
    // the list, so it takes the same coarse-all path — AFTER the batch completes, with the caller's result
    // handed back untouched. Rejecting it before execution was the alternative and is worse: batch is a
    // legitimate API, and coarse-all is sound.
    prop === 'batch'
  )
}

/** Wrap `db.transaction(cb)`: the callback gets a proxied tx db whose writes BUFFER; on outer COMMIT the
 *  whole buffer flushes as ONE `ChangeBatch` to `enclosingSink` (the top db's graphs, or a parent tx's
 *  buffer for a savepoint), so a committed transaction is one atomic graph tick. A rollback rejects and
 *  never flushes → its changes are discarded. `topDb` is the session the reads acquired from — capture
 *  reads its dialect/single-session/driver from it (a tx db isn't recognized as its own driver).
 *
 *  `enclosingLocalSink` is the SAME destination minus the remote publication. A transaction that ran raw
 *  SQL announces itself remotely as ONE coarse-all — which reseeds every remotely-watched graph — so also
 *  publishing its batch would make every remote watcher pay twice for one commit. The batch still flushes
 *  locally in one tick; only the remote hop is dropped, because the coarse-all supersedes it. For a
 *  SAVEPOINT both sinks are the parent's buffer, and the top level makes the choice once. */
function wrapTransaction(
  txHost: object,
  topDb: object,
  enclosingSink: CaptureSink,
  enclosingLocalSink: CaptureSink,
  enclosingAnnounce: () => void,
  /** The coarse markers a committed raw statement owes THIS db's own graphs, computed AT COMMIT — the top
   *  level supplies the real thing; a savepoint supplies nothing and promotes intent, so the markers are
   *  computed exactly once, against the watch-set as it stands when the transaction actually lands. */
  commitCoarseMarkers: () => TableChange[] = () => [],
) {
  return (callback: (tx: unknown) => unknown, config?: unknown) => {
    const buffer: TableChange[] = []
    const buffered: CaptureSink = (changes) => {
      for (const change of changes) buffer.push(change)
    }
    // A raw statement inside the transaction cannot be published when it runs: the transaction may still roll
    // back, and a coarse-all already broadcast would have told every other instance to refetch state that
    // never existed. Record the intent and hand it to the enclosing scope only if we COMMIT.
    let announcePending = false
    const bufferedAnnounce = () => {
      announcePending = true
    }
    const baseTransaction = (txHost as { transaction: (cb: unknown, c?: unknown) => unknown }).transaction.bind(txHost)
    return Promise.resolve(
      baseTransaction((tx: object) => callback(txProxy(tx, topDb, buffered, bufferedAnnounce)), config),
    ).then((result) => {
      // Reached ONLY on COMMIT (a rollback / savepoint-rollback rejects and skips this) → flush once.
      // Isolated: the transaction has COMMITTED, so a capture/publish fault must not reject it (it degrades
      // to a coarse ingest and is reported) — the caller's result stays exactly plain Drizzle's.
      //
      // A transaction that ran raw SQL flushes LOCAL-ONLY: its remote announcement is the single coarse-all
      // below, and publishing the batch as well would deliver two messages — and two refetches — for one
      // commit. This used to be defended as the price of atomicity ("splitting the raw markers out would
      // make two local ticks"), which was a false dichotomy: the whole buffer still lands in ONE local tick
      // here; only the redundant remote copy is dropped.
      //
      // The raw statement's coarse markers are computed HERE, at commit, not when the statement ran. The
      // buffer only ever carried INTENT for raw SQL, because statement-time markers snapshot the watch-set
      // of an earlier moment: a graph registering between the raw statement and this commit would be absent
      // from them, unreachable by this local flush — and the remote coarse-all below is its own publisher's
      // echo, origin-suppressed locally — so it would NEVER hear about the committed rows.
      if (announcePending) {
        const merged = [...buffer, ...commitCoarseMarkers()]
        if (merged.length > 0) emitSafely(enclosingLocalSink, merged)
      } else if (buffer.length > 0) {
        emitSafely(enclosingSink, buffer)
      }
      // For a SAVEPOINT this promotes the intent into the parent's; at the top level it publishes.
      if (announcePending) {
        try {
          enclosingAnnounce()
        } catch (error) {
          console.error(
            '[telefunc] live: announcing a committed raw-SQL transaction failed; other instances may hold stale live queries until the next write.',
            error,
          )
        }
      }
      return result
    })
  }
}

/** A raw statement inside a transaction contributes NO markers at statement time — see the raw branch in
 *  txProxy. Named so the call site says what is happening rather than passing an anonymous no-op. */
const DROP_MARKERS: CaptureSink = () => {}

/** The proxy over a transaction db: writes buffer (via `sink`); a nested `transaction` is a SAVEPOINT whose
 *  buffer flushes into THIS one on release (and is discarded on savepoint-rollback); reads + everything else
 *  pass through as plain Drizzle (a live read inside a write transaction is out of scope). */
function txProxy(txDb: object, topDb: object, sink: CaptureSink, announce: () => void): unknown {
  return new Proxy(txDb, {
    get(target, prop, receiver) {
      if (isWriteOp(prop)) {
        const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
        // `target` is the RAW tx db, passed ONLY as an execution handle for the capture-recovery savepoint.
        // Planning and registry keying still read `topDb` — that invariant is what keeps this apart from
        // the separate question of which db owns session identity. Never the PROXY: `execute` on it is
        // intercepted as raw SQL, so a SAVEPOINT through it would coarsen the whole transaction.
        return captureMutation(prop, base.bind(target), topDb, sink, target) // plan on topDb; run on tx; buffer
      }
      if (prop === 'transaction') {
        return wrapTransaction(target, topDb, sink, sink, announce) // nested → flush into THIS tx's buffer on release
      }
      if (isRawSqlOp(prop)) {
        // `tx.execute(sql`…`)` used to pass straight through, so a raw write committed with NOTHING
        // published — the same silent bypass the top-level db already fixed.
        //
        // Inside a transaction, raw SQL records INTENT ONLY (`announce`); its coarse markers are NOT
        // materialized here. Markers name the tables being watched, and the watch-set at statement time is
        // the wrong one — a live read admitted between this statement and the outer COMMIT would be missing
        // from them, unreachable by the commit flush, and origin-suppressed out of the remote coarse-all:
        // it would never hear about the committed rows at all. The outer commit computes the markers
        // against the watch-set that exists when the transaction actually lands, and flushes them with the
        // ORM buffer in one atomic local tick; the single coarse-all carries the remote side.
        const base = Reflect.get(target, prop, receiver)
        if (typeof base === 'function') {
          return captureRawSql((base as (...a: unknown[]) => unknown).bind(target), topDb, DROP_MARKERS, announce)
        }
      }
      return Reflect.get(target, prop, receiver)
    },
  })
}
