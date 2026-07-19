export type { Reactive, ReactiveDatabase }

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
import type { Live } from 'telefunc'

// THE TYPE TRANSFORM, and nothing else. Purely declarative — no value in this module, so it cannot affect
// runtime behaviour and a reader of the entry path never has to descend through ~200 lines of conditional
// generics to reach the proxy. `reactiveDrizzle.ts` re-exports `Reactive` as its public type.

/** The driver-terminal surface an async select adds OVER the core query-builder select: the `QueryPromise`
 *  verbs (`then`/`catch`/`finally`/`execute`) plus `prepare` and each dialect's runners (SQLite
 *  `all`/`get`/`run`/`values`). Picked as exactly the members the async base adds and no
 *  more (`keyof async` minus `keyof core`), so intersecting it onto the reactive chain NEVER clashes with a
 *  chain method — the reactive HKT keeps owning `from`/`where`/joins, and this only re-adds the terminals
 *  the core builder lacks. This is what makes "everything except `.live()` is the ordinary async builder"
 *  literally true, including `execute(placeholderValues)`. */
type AsyncTerminals<TAsyncBase, TCoreBase> = Pick<TAsyncBase, Exclude<keyof TAsyncBase, keyof TCoreBase>>

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

/** The databases reactive queries support: a real, executable Drizzle db or transaction whose `select()`
 *  builds an ASYNC (awaitable, hydratable) query on a SUPPORTED dialect. A bare `QueryBuilder` — which
 *  type-checks a query but has no session to run it against — is deliberately excluded: a live read has
 *  nothing to hydrate from, so accepting one would type a `.live()` that could never resolve. MySQL is
 *  excluded for a different reason: with no `RETURNING` there is no verified row-capture lane for it, and it
 *  is rejected at construction too (see `dialectOf`) rather than merely lacking `.live()`. */
type ReactiveDatabase = {
  select: (...args: any[]) => PgAsyncSelectBuilder<any> | SQLiteAsyncSelectBuilder<any, any, any>
}

/** The reactive db: exactly `TDb` (every write/query surface preserved) with `select()` re-typed to build
 *  a terminal-`.live()` chain, dispatched by the db's dialect. Dialect is read structurally off the db's
 *  own async select BUILDER (the db base classes aren't exported, but the builders are, and each dialect's
 *  is distinct — no cross-match). Scope: only chains from the reactive db's OWN `select()` are live — a
 *  CTE-prefixed chain (`db.with(cte).select()…`) runs through Drizzle's ordinary `with()` facade, which is
 *  neither wrapped nor re-typed, so `.live()` is intentionally absent there (see the `reactiveDrizzle` doc). */
type Reactive<TDb extends ReactiveDatabase> = ReturnType<TDb['select']> extends PgAsyncSelectBuilder<any>
  ? ReactivePgDb<TDb>
  : ReturnType<TDb['select']> extends SQLiteAsyncSelectBuilder<any, any, any>
    ? ReactiveSQLiteDb<TDb>
    : never
