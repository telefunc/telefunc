export type { LiveDrizzle, DrizzleDatabase }

import type { Assume, ColumnsSelection, DrizzleTypeError, SQL, SQLWrapper, Subquery } from 'drizzle-orm'
import type {
  PgColumn,
  PgSelectBase,
  PgSelectBuilder,
  PgSelectHKTBase,
  PgTable,
  SelectedFields,
  TableLikeHasEmptySelection,
} from 'drizzle-orm/pg-core'
import type { PgViewBase } from 'drizzle-orm/pg-core/view-base'
import type {
  SQLiteSelectBase,
  SQLiteSelectBuilder,
  SQLiteSelectHKTBase,
  SQLiteTable,
  SelectedFields as SqliteSelectedFields,
} from 'drizzle-orm/sqlite-core'
import type { SQLiteViewBase } from 'drizzle-orm/sqlite-core/view-base'
import type {
  BuildSubquerySelection,
  GetSelectTableName,
  GetSelectTableSelection,
  JoinNullability,
  SelectMode,
  SelectResult,
} from 'drizzle-orm/query-builders/select.types'
import type { Live } from './live.js'

// Drizzle builds each step of a select chain (`from`, `where`, joins, …) by applying the higher-kinded type in the
// builder's `_.hkt` to the step's generics. A chain started from a select of `liveDrizzle(db)` carries an HKT whose
// result is Drizzle's own `PgSelectBase` plus `live()`, so every step keeps `live()` and Drizzle still infers the rows.

/** A Drizzle database: Postgres (node-postgres, postgres.js, PGlite) or SQLite (better-sqlite3, bun:sqlite, libSQL, D1). */
type DrizzleDatabase = {
  select: (...args: any[]) => PgSelectBuilder<any> | SQLiteSelectBuilder<any, any, any>
  $client: unknown
}

/** The database `liveDrizzle()` returns: the same, with `.live()` at the end of its select chains. */
type LiveDrizzle<TDb extends DrizzleDatabase> = TDb extends {
  select: (...args: any[]) => SQLiteSelectBuilder<any, infer Kind, infer RunResult>
}
  ? WithLiveSqliteSelects<TDb, Kind, RunResult>
  : WithLiveSelects<TDb>

/** A database, a transaction or a `with(…)`: with `.live()` at the end of every select chain it starts, its
 *  transactions' and its `with(…)`'s included. */
type WithLiveSelects<T> = Omit<T, 'select' | 'selectDistinct' | 'selectDistinctOn' | 'with' | 'transaction'> &
  LiveSelectStarts &
  (T extends { with(...queries: infer Queries): infer With }
    ? { with(...queries: Queries): WithLiveSelects<With> }
    : {}) &
  (T extends { transaction(run: (tx: infer Tx) => Promise<any>, ...rest: infer Rest): Promise<any> }
    ? { transaction<R>(run: (tx: WithLiveSelects<Tx>) => Promise<R>, ...rest: Rest): Promise<R> }
    : {})

type LiveSelectStarts = {
  select(): LiveSelectBuilder<undefined>
  select<TSelection extends SelectedFields>(fields: TSelection): LiveSelectBuilder<TSelection>
  selectDistinct(): LiveSelectBuilder<undefined>
  selectDistinct<TSelection extends SelectedFields>(fields: TSelection): LiveSelectBuilder<TSelection>
  selectDistinctOn(on: (PgColumn | SQLWrapper)[]): LiveSelectBuilder<undefined>
  selectDistinctOn<TSelection extends SelectedFields>(
    on: (PgColumn | SQLWrapper)[],
    fields: TSelection,
  ): LiveSelectBuilder<TSelection>
}

type LiveSelectBuilder<TSelection extends SelectedFields | undefined> = {
  from<TFrom extends PgTable | Subquery | PgViewBase | SQL>(
    source: TableLikeHasEmptySelection<TFrom> extends true
      ? DrizzleTypeError<"Cannot reference a data-modifying statement subquery if it doesn't contain a `returning` clause">
      : TFrom,
  ): LiveSelect<
    GetSelectTableName<TFrom>,
    TSelection extends undefined ? GetSelectTableSelection<TFrom> : TSelection,
    TSelection extends undefined ? 'single' : 'partial'
  >
}

interface LiveSelectHKT extends PgSelectHKTBase {
  _type: LiveSelect<
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

type LiveSelect<
  TTableName extends string | undefined,
  TSelection extends ColumnsSelection,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability> = TTableName extends string
    ? Record<TTableName, 'not-null'>
    : {},
  TDynamic extends boolean = false,
  TExcludedMethods extends string = never,
  TResult extends any[] = SelectResult<TSelection, TSelectMode, TNullabilityMap>[],
  TSelectedFields extends ColumnsSelection = BuildSubquerySelection<TSelection, TNullabilityMap>,
> = PgSelectBase<
  TTableName,
  TSelection,
  TSelectMode,
  TNullabilityMap,
  TDynamic,
  TExcludedMethods,
  TResult,
  TSelectedFields
> & {
  readonly _: { readonly hkt: LiveSelectHKT }
  /** Runs the query like `await` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live(): Promise<Live<TResult>>
}

// ── SQLite: the same, on Drizzle's SQLite select, whose steps also carry the driver's result kind ──

/** What a SQLite driver's calls return: at once (better-sqlite3, bun:sqlite) or a promise (libSQL, D1). */
type Result<Kind extends 'sync' | 'async', T> = Kind extends 'sync' ? T : Promise<T>

type WithLiveSqliteSelects<T, Kind extends 'sync' | 'async', RunResult> = Omit<
  T,
  'select' | 'selectDistinct' | 'with' | 'transaction'
> & {
  select(): LiveSqliteSelectBuilder<undefined, Kind, RunResult>
  select<TSelection extends SqliteSelectedFields>(
    fields: TSelection,
  ): LiveSqliteSelectBuilder<TSelection, Kind, RunResult>
  selectDistinct(): LiveSqliteSelectBuilder<undefined, Kind, RunResult>
  selectDistinct<TSelection extends SqliteSelectedFields>(
    fields: TSelection,
  ): LiveSqliteSelectBuilder<TSelection, Kind, RunResult>
} & (T extends { with(...queries: infer Queries): infer With }
    ? { with(...queries: Queries): WithLiveSqliteSelects<With, Kind, RunResult> }
    : {}) &
  (T extends { transaction(run: (tx: infer Tx) => any, ...rest: infer Rest): any }
    ? {
        transaction<R>(
          run: (tx: WithLiveSqliteSelects<Tx, Kind, RunResult>) => Result<Kind, R>,
          ...rest: Rest
        ): Result<Kind, R>
      }
    : {})

type LiveSqliteSelectBuilder<
  TSelection extends SqliteSelectedFields | undefined,
  Kind extends 'sync' | 'async',
  RunResult,
> = {
  from<TFrom extends SQLiteTable | Subquery | SQLiteViewBase | SQL>(
    source: TFrom,
  ): LiveSqliteSelect<
    GetSelectTableName<TFrom>,
    Kind,
    RunResult,
    TSelection extends undefined ? GetSelectTableSelection<TFrom> : TSelection,
    TSelection extends undefined ? 'single' : 'partial'
  >
}

interface LiveSqliteSelectHKT extends SQLiteSelectHKTBase {
  _type: LiveSqliteSelect<
    this['tableName'],
    this['resultType'],
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

type LiveSqliteSelect<
  TTableName extends string | undefined,
  Kind extends 'sync' | 'async',
  RunResult,
  TSelection extends ColumnsSelection,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability> = TTableName extends string
    ? Record<TTableName, 'not-null'>
    : {},
  TDynamic extends boolean = false,
  TExcludedMethods extends string = never,
  TResult extends any[] = SelectResult<TSelection, TSelectMode, TNullabilityMap>[],
  TSelectedFields extends ColumnsSelection = BuildSubquerySelection<TSelection, TNullabilityMap>,
> = SQLiteSelectBase<
  TTableName,
  Kind,
  RunResult,
  TSelection,
  TSelectMode,
  TNullabilityMap,
  TDynamic,
  TExcludedMethods,
  TResult,
  TSelectedFields
> & {
  readonly _: { readonly hkt: LiveSqliteSelectHKT }
  /** Runs the query like `await` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live(): Promise<Live<TResult>>
}
