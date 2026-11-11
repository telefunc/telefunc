export { livePrisma }
export type { LivePrisma }

import type * as OnPostgres from '@prisma/orm-postgres/orm-client'
import type * as OnSqlite from '@prisma/orm-sqlite/orm-client'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, liveDatabaseOfFile, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

type LiveMethod<Target> = {
  /** Runs the query like `all()` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live<Self extends HasRow<Target>>(this: Self): Promise<Live<RowOf<Self, Target>[]>>
}

// Prisma's collection class isn't exported, so its type can't take a method, and an interface it implements can't
// either: a class's type doesn't get what is merged into such an interface. `livePrisma()` gives each model's
// collection `live()`; the steps that return the collection they're called on (`where()`, `limit()`, `include()`, …)
// keep it, and `select()`, which returns a new collection type, is retyped to keep it too. `variant()` isn't: the rows
// it returns are typed by Prisma types the package doesn't export.

type PrismaClient = {
  orm: object
  contract: { target: string; roots: Record<string, { model: string; namespace: string }> }
}

/** The client `livePrisma()` returns: the same, with `.live()` at the end of its models' collections, its
 *  transactions' too. */
type LivePrisma<TDb extends PrismaClient> = WithLiveModels<TDb, TDb['contract']['target']>
type WithLiveModels<T, Target> = T extends { orm: infer Orm }
  ? Omit<T, 'orm' | 'transaction'> & { readonly orm: LiveOrm<Orm, Target> } & (T extends {
        transaction(run: (tx: infer Tx) => PromiseLike<any>): Promise<any>
      }
        ? { transaction<R>(run: (tx: WithLiveModels<Tx, Target>) => PromiseLike<R>): Promise<R> }
        : {})
  : T
/** `db.orm`: by namespace on Postgres, the models themselves on SQLite. */
type LiveOrm<Orm, Target> = {
  [K in keyof Orm]: Orm[K] extends HasRow<Target> ? LiveCollection<Orm[K], Target> : LiveModels<Orm[K], Target>
}
type LiveModels<Models, Target> = Models extends (...args: any[]) => any
  ? Models
  : {
      [Model in keyof Models]: Models[Model] extends HasRow<Target>
        ? LiveCollection<Models[Model], Target>
        : Models[Model]
    }

// Prisma's types, from the package of the client's target: each installed copy of Prisma has symbols of its own.
type HasRow<Target> = Target extends 'sqlite' ? OnSqlite.HasRow : OnPostgres.HasRow
type RowOf<C, Target> = Target extends 'sqlite'
  ? C extends OnSqlite.HasRow
    ? OnSqlite.CollectionRowOf<C>
    : never
  : C extends OnPostgres.HasRow
    ? OnPostgres.CollectionRowOf<C>
    : never

/** A collection whose steps keep `live()`. */
type LiveCollection<C, Target> = Omit<C, 'select'> &
  LiveMethod<Target> & {
    // Typed by the collection it's called on, as Prisma's own `select()` is: the steps before it (`include()`,
    // `orderBy()`, …) typed its rows and its state.
    select<Self extends HasRow<Target>, Fields extends FieldsOf<C>>(
      this: Self,
      ...fields: Fields
    ): LiveCollection<Picked<Self, Fields[number], Target>, Target>
  }
type FieldsOf<C> = C extends { select(...fields: infer Fields): unknown } ? Fields : never

/** What `select(...fields)` on `C` returns, as Prisma types it: the collection with the model's `fields` and its
 *  included relations. Spelled out because a generic method's result can't be read off its type for given arguments,
 *  once per target. */
type Picked<C, Field, Target> = Target extends 'sqlite'
  ? C extends OnSqlite.HasTypeState & {
      ctx: OnSqlite.CollectionContext<infer Contract>
      modelName: infer Model extends string
    }
    ? OnSqlite.Collection<
        Contract,
        Model,
        Selected<RowOf<C, Target>, OnSqlite.DefaultModelRow<Contract, Model>, Field>,
        OnSqlite.CollectionTypeStateOf<C>
      >
    : never
  : C extends OnPostgres.HasTypeState & {
        ctx: OnPostgres.CollectionContext<infer Contract>
        modelName: infer Model extends string
      }
    ? OnPostgres.Collection<
        Contract,
        Model,
        Selected<RowOf<C, Target>, OnPostgres.DefaultModelRow<Contract, Model>, Field>,
        OnPostgres.CollectionTypeStateOf<C>
      >
    : never
/** `Row` with only the model's `Field`s: its included relations stay. */
type Selected<Row, ModelRow, Field> = SimplifyDeep<Pick<ModelRow, Field & keyof ModelRow> & Omit<Row, keyof ModelRow>>
/** Prisma's own, which its packages don't export. */
type SimplifyDeep<T> = T extends readonly (infer Element)[]
  ? T extends unknown[]
    ? SimplifyDeep<Element>[]
    : readonly SimplifyDeep<Element>[]
  : T extends string | number | boolean | bigint | symbol | Date | RegExp | Uint8Array | ((...args: never[]) => unknown)
    ? T
    : T extends object
      ? { [K in keyof T]: SimplifyDeep<T[K]> }
      : T

/**
 * Lets a collection read of `db` end with `.live()`: the client fetches it again when a committed write changes what
 * it read. Writes, and reads without `.live()`, run as before. Prisma doesn't expose its connections: pass what it
 * connects through, the pool on Postgres, the database file on SQLite.
 *
 * ```ts
 * export const db = livePrisma(postgres({ contract, pg: pool }), pool)
 * export const db = livePrisma(sqlite({ contract, path: './app.db' }), './app.db')
 * // in a telefunction
 * return db.orm.public.Todo.where({ userId }).live() // Promise<Live<Todo[]>>, `db.orm.Todo` on SQLite
 * ```
 */
function livePrisma<TDb extends PrismaClient>(
  db: TDb,
  connectsThrough: DatabaseClient | string,
  options: LiveOptions = {},
): LivePrisma<TDb> {
  if (typeof connectsThrough === 'string') liveDatabaseOfFile(connectsThrough, options)
  else {
    assertUsage(
      isDatabaseClient(connectsThrough),
      'livePrisma(db, pool) takes the pool Prisma uses: the pg.Pool given to postgres({ contract, pg: pool }), or on SQLite the path given to sqlite({ contract, path }).',
    )
    liveDatabaseOf(connectsThrough, options)
  }
  // The class isn't exported: any model's collection gives its prototype.
  const [root] = Object.values(db.contract.roots)
  assertUsage(root, 'livePrisma() needs a contract with at least one model.')
  const orm = db.orm as Record<string, Record<string, object>>
  // On SQLite, `db.orm` holds the models themselves.
  const models = orm[root.namespace] ?? orm
  const collection = Object.getPrototypeOf(models[root.model]!)
  if (!('live' in collection))
    Object.defineProperty(collection, 'live', {
      value: function live(this: { all(): PromiseLike<unknown[]> }) {
        return readLive(async () => await this.all(), 'livePrisma()')
      },
    })
  return db as LivePrisma<TDb>
}
