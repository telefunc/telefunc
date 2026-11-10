export { livePrisma }
export type { LivePrisma }

import type { CollectionContext, CollectionRowOf, HasRow } from '@prisma/orm-postgres/orm-client'
import type { PostgresClient } from '@prisma/orm-postgres/runtime'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

type LiveMethod = {
  /** Runs the query like `all()` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live<Self extends HasRow>(this: Self & Wrapped<Self>): Promise<Live<CollectionRowOf<Self>[]>>
}

// Prisma's collection class isn't exported, so its type can't take a method. `livePrisma()` adds `live()` to each
// model's collection, and the steps that return it (`limit()`, `include()`, …) keep it. `select()` returns a new
// collection type; `where()` and `orderBy()` mark theirs with `HasWhere` and `HasOrderBy`, which carry `live()`.
// Those are Prisma's own, on every client: what every collection does carry is its contract's type, and
// `livePrisma()` adds a mark to it, so `live()` type-checks only on the collections of a wrapped client.

declare const LIVE_CONTRACT: unique symbol
/** The mark on the contract of a client wrapped by `livePrisma()`. It exists in types only. */
type LiveContract = { readonly [LIVE_CONTRACT]: true }
type Wrapped<Self> = Self extends { readonly ctx: CollectionContext<infer TContract> }
  ? TContract extends LiveContract
    ? unknown
    : 'live() needs a client wrapped by livePrisma(): livePrisma(db, pool)'
  : unknown

declare module '@prisma/orm-postgres/orm-client' {
  interface HasWhere extends LiveMethod {}
  interface HasOrderBy extends LiveMethod {}
}

type PrismaClient = { orm: object; contract: { roots: Record<string, { model: string; namespace: string }> } }

/** The client `livePrisma()` returns: the same, with its contract marked and `.live()` on each model's collection. */
type LivePrisma<TDb extends PrismaClient> = WithLiveModels<
  TDb extends PostgresClient<infer TContract, infer TCollections>
    ? PostgresClient<TContract & LiveContract, TCollections>
    : TDb
>
type WithLiveModels<TDb> = TDb extends { orm: infer Orm }
  ? TDb & { orm: { [Ns in keyof Orm]: { [Model in keyof Orm[Ns]]: Orm[Ns][Model] & LiveMethod } } }
  : TDb

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
