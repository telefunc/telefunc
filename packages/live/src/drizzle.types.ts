export type { LiveDrizzle, DrizzleDatabase }

import type { Assume, ColumnsSelection, DrizzleTypeError, SQL, Subquery } from 'drizzle-orm'
import type {
  PgSelectBase,
  PgSelectBuilder,
  PgSelectHKTBase,
  PgTable,
  SelectedFields,
  TableLikeHasEmptySelection,
} from 'drizzle-orm/pg-core'
import type { PgViewBase } from 'drizzle-orm/pg-core/view-base'
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
// builder's `_.hkt` to the step's generics. A chain started from `liveDrizzle(db).select()` carries an HKT whose
// result is Drizzle's own `PgSelectBase` plus `live()`, so every step keeps `live()` and Drizzle still infers the rows.

/** A Drizzle Postgres database (node-postgres, postgres.js or PGlite) or transaction. */
type DrizzleDatabase = { select: (...args: any[]) => PgSelectBuilder<any>; $client: unknown }

/** The database `liveDrizzle()` returns: the same, with `.live()` at the end of its `select()` chains. */
type LiveDrizzle<TDb extends DrizzleDatabase> = Omit<TDb, 'select'> & {
  select(): LiveSelectBuilder<undefined>
  select<TSelection extends SelectedFields>(fields: TSelection): LiveSelectBuilder<TSelection>
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
