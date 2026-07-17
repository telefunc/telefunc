export { reactiveDrizzle }
export type { Reactive, DbLiveCarrier }

import type { Assume, ColumnsSelection, QueryPromise } from 'drizzle-orm'
import type { JoinNullability, SelectMode } from 'drizzle-orm/query-builders/select.types'
import type {
  PgSelectBase,
  PgSelectBuilder,
  PgSelectHKTBase,
  SelectedFields as PgSelectedFields,
} from 'drizzle-orm/pg-core'
import type {
  SQLiteSelectBase,
  SQLiteSelectBuilder,
  SQLiteSelectHKTBase,
  SelectedFields as SQLiteSelectedFields,
} from 'drizzle-orm/sqlite-core'
import type {
  MySqlSelectBase,
  MySqlSelectBuilder,
  MySqlSelectHKTBase,
  SelectedFields as MySqlSelectedFields,
} from 'drizzle-orm/mysql-core'
import type { Live } from 'telefunc'
import { acquireCarrier, captureMutation } from './dbLiveRuntime.js'
import { wrapLiveSelect, type ReadCarrier } from './readCapture.js'

// The reactive-db surface: the type transform, and the per-request proxy behind it.
//
// `const db = reactiveDrizzle(baseDb)` is called at the TOP of a telefunction (before the first await),
// and the db it returns CLOSES OVER that request's carrier. That is what lets `db.select()…live()` work
// after an await: it uses the captured carrier rather than looking for the ambient request context,
// which by then may be gone.
//
// ── Why the terminal `.live()` rides an HKT seam ──────────────────────────────────────────────────
// Drizzle's select builders are parameterized by a Higher-Kinded Type (`PgSelectHKTBase` &c.) that it
// threads through EVERY chain method (`from`/`where`/`innerJoin`/…): each step re-applies the HKT to
// compute the next builder. We define a custom HKT (`PgReactiveSelectHKT` &c.) whose `_type` is the
// stock `PgSelectBase` intersected with `{ live(): Promise<Live<TResult>> }`. Because Drizzle's own
// generics keep doing all row/selection/nullability inference and our HKT only ADDS `live()`, the
// terminal rides the entire chain with the exact row type intact — no member-remap, nothing to
// collapse. `await`-ing the builder still yields plain rows (`QueryPromise<TResult>`); only `.live()`
// crosses into `Live<TResult>`. This is the seam the earlier dotted-namespace transform lacked: a
// conditional remap of Drizzle's generic `from`/`where` collapsed their type parameters to their
// constraints and lost the row type; threading the HKT never remaps them.

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
  QueryPromise<TResult> & { live(): Promise<Live<TResult>> }
type ReactivePgDb<TDb> = Omit<TDb, 'select'> & {
  select(): PgSelectBuilder<undefined, PgReactiveSelectHKT>
  select<TSelection extends PgSelectedFields>(fields: TSelection): PgSelectBuilder<TSelection, PgReactiveSelectHKT>
}

// ── SQLite (its select HKT carries the extra `TRunResult` slot) ─────────────────────────────────────
interface SQLiteReactiveSelectHKT extends SQLiteSelectHKTBase {
  _type: SQLiteReactiveSelect<
    this['tableName'],
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
  TRunResult,
  TSelection extends ColumnsSelection,
  TSelectMode extends SelectMode,
  TNullabilityMap extends Record<string, JoinNullability>,
  TDynamic extends boolean,
  TExcludedMethods extends string,
  TResult extends any[],
  TSelectedFields extends ColumnsSelection,
> = SQLiteSelectBase<
  SQLiteReactiveSelectHKT,
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
  QueryPromise<TResult> & { live(): Promise<Live<TResult>> }
type ReactiveSQLiteDb<TDb> = Omit<TDb, 'select'> & {
  select(): SQLiteSelectBuilder<undefined, unknown, SQLiteReactiveSelectHKT>
  select<TSelection extends SQLiteSelectedFields>(
    fields: TSelection,
  ): SQLiteSelectBuilder<TSelection, unknown, SQLiteReactiveSelectHKT>
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
  QueryPromise<TResult> & { live(): Promise<Live<TResult>> }
type ReactiveMySqlDb<TDb> = Omit<TDb, 'select'> & {
  select(): MySqlSelectBuilder<undefined, MySqlReactiveSelectHKT>
  select<TSelection extends MySqlSelectedFields>(
    fields: TSelection,
  ): MySqlSelectBuilder<TSelection, MySqlReactiveSelectHKT>
}

/** The reactive db: exactly `TDb` (every write/query surface preserved) with `select()` re-typed to
 *  build a terminal-`.live()` chain, dispatched by the db's dialect. Dialect is read structurally off
 *  the db's own `select()` return — the db base classes aren't exported, but the select BUILDERS are,
 *  and each dialect's builder is a distinct type (no cross-match). A non-drizzle object falls through
 *  to `TDb` unchanged. */
type Reactive<TDb extends { select: (...args: any[]) => any }> = ReturnType<TDb['select']> extends PgSelectBuilder<
  any,
  any
>
  ? ReactivePgDb<TDb>
  : ReturnType<TDb['select']> extends MySqlSelectBuilder<any, any>
    ? ReactiveMySqlDb<TDb>
    : ReturnType<TDb['select']> extends SQLiteSelectBuilder<any, any, any>
      ? ReactiveSQLiteDb<TDb>
      : TDb

/** Opaque per-request carrier, acquired eagerly. Here it is nothing but a brand: its concrete shape
 *  belongs to the runtime units, and this surface only threads it. */
type DbLiveCarrier = { readonly __dbLiveCarrier: true }

/**
 * Set up reactive queries for a Drizzle `db`. Call it at the TOP of a telefunction (before the body's
 * first await) to get a per-request reactive db: its `select()` builds an ordinary Drizzle query that
 * you can either `await` for plain rows or terminate with `.live()` to get a `Live<T[]>`. Writes
 * (`insert`/`update`/`delete`) and every other surface run as plain Drizzle, unchanged.
 *
 * ```ts
 * const db = reactiveDrizzle(baseDb)
 * const todos = await db.select().from(todosTable).live() // Live<Todo[]>
 * ```
 */
function reactiveDrizzle<TDb extends { select: (...args: any[]) => any }>(baseDb: TDb): Reactive<TDb> {
  // Capture the per-request carrier NOW (context-bearing, before the body's first await). The returned
  // proxy closes over it, so `db.select()…live()` even post-await uses the CAPTURED carrier.
  const carrier = acquireCarrier()
  return new Proxy(baseDb as object, {
    get(target, prop, receiver) {
      if (prop === 'select') {
        // Wrap ONLY this db's own select builders: the returned chain is live-capable (`.live()` routes
        // to the read-capture engine), while `then`/`execute` forward untouched to plain rows.
        return (...args: unknown[]) => {
          const baseBuilder = (target as { select: (...a: unknown[]) => unknown }).select(...args)
          return wrapLiveSelect(baseBuilder, carrier as unknown as ReadCarrier, target)
        }
      }
      if (prop === 'insert' || prop === 'update' || prop === 'delete') {
        // Writes run as plain Drizzle today; the seam exists so T4 can route capture through one place.
        const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
        return captureMutation(prop, base.bind(target), carrier)
      }
      return Reflect.get(target, prop, receiver)
    },
  }) as unknown as Reactive<TDb>
}
