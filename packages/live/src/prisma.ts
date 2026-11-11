export { livePrisma }
export type { LivePrisma }

import type {
  Collection,
  CollectionContext,
  CollectionRowOf,
  CollectionTypeStateOf,
  DefaultModelRow,
  HasRow,
  HasTypeState,
} from '@prisma/orm-postgres/orm-client'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

type LiveMethod = {
  /** Runs the query like `all()` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live<Self extends HasRow>(this: Self): Promise<Live<CollectionRowOf<Self>[]>>
}

// Prisma's collection class isn't exported, so its type can't take a method, and an interface it implements can't
// either: a class's type doesn't get what is merged into such an interface. `livePrisma()` gives each model's
// collection `live()`; the steps that return the collection they're called on (`where()`, `limit()`, `include()`, …)
// keep it, and `select()`, which returns a new collection type, is retyped to keep it too. `variant()` isn't: the rows
// it returns are typed by Prisma types the package doesn't export.

type PrismaClient = { orm: object; contract: { roots: Record<string, { model: string; namespace: string }> } }

/** The client `livePrisma()` returns: the same, with `.live()` at the end of its models' collections, its
 *  transactions' too. */
type LivePrisma<TDb extends PrismaClient> = WithLiveModels<TDb>
type WithLiveModels<T> = T extends { orm: infer Orm }
  ? Omit<T, 'orm' | 'transaction'> & { readonly orm: { [Ns in keyof Orm]: LiveModels<Orm[Ns]> } } & (T extends {
        transaction(run: (tx: infer Tx) => PromiseLike<any>): Promise<any>
      }
        ? { transaction<R>(run: (tx: WithLiveModels<Tx>) => PromiseLike<R>): Promise<R> }
        : {})
  : T
type LiveModels<Models> = Models extends (...args: any[]) => any
  ? Models
  : { [Model in keyof Models]: Models[Model] extends HasRow ? LiveCollection<Models[Model]> : Models[Model] }

/** A collection whose steps keep `live()`. */
type LiveCollection<C> = Omit<C, 'select'> &
  LiveMethod & {
    select<Fields extends FieldsOf<C>>(...fields: Fields): LiveCollection<Picked<C, Fields[number]>>
  }
type FieldsOf<C> = C extends { select(...fields: infer Fields): unknown } ? Fields : never

/** What `select(...fields)` returns, as Prisma types it: the collection with the model's `fields` and its included
 *  relations. Spelled out because a generic method's result can't be read off its type for given arguments. */
type Picked<C, Field> = C extends HasRow &
  HasTypeState & { ctx: CollectionContext<infer Contract>; modelName: infer Model extends string }
  ? Collection<
      Contract,
      Model,
      Simplify<
        Pick<DefaultModelRow<Contract, Model>, Field & keyof DefaultModelRow<Contract, Model>> &
          Omit<CollectionRowOf<C>, keyof DefaultModelRow<Contract, Model>>
      >,
      CollectionTypeStateOf<C>
    >
  : never
type Simplify<T> = { [K in keyof T]: T[K] }

/**
 * Lets a collection read of `db` end with `.live()`: the client fetches it again when a committed write changes what
 * it read. Writes, and reads without `.live()`, run as before. Prisma doesn't expose its pool: pass the one it uses.
 *
 * ```ts
 * export const db = livePrisma(postgres({ contract, pg: pool }), pool)
 * // in a telefunction
 * return db.orm.public.Todo.where({ userId }).live() // Promise<Live<Todo[]>>
 * ```
 */
function livePrisma<TDb extends PrismaClient>(
  db: TDb,
  pool: DatabaseClient,
  options: LiveOptions = {},
): LivePrisma<TDb> {
  assertUsage(
    isDatabaseClient(pool),
    'livePrisma(db, pool) takes the pool Prisma uses: the pg.Pool given to postgres({ contract, pg: pool }).',
  )
  liveDatabaseOf(pool, options)
  // The class isn't exported: any model's collection gives its prototype.
  const [root] = Object.values(db.contract.roots)
  assertUsage(root, 'livePrisma() needs a contract with at least one model.')
  const orm = db.orm as Record<string, Record<string, object>>
  const collection = Object.getPrototypeOf(orm[root.namespace]![root.model]!)
  if (!('live' in collection))
    Object.defineProperty(collection, 'live', {
      value: function live(this: { all(): PromiseLike<unknown[]> }) {
        return readLive(async () => await this.all(), 'livePrisma()')
      },
    })
  return db as LivePrisma<TDb>
}
