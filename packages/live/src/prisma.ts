export { livePrisma }
export type { LivePrisma }

import type { CollectionRowOf, HasRow } from '@prisma/orm-postgres/orm-client'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

type LiveMethod = {
  /** Runs the query like `all()` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. Needs a client wrapped by `livePrisma()`. */
  live<Self extends HasRow>(this: Self): Promise<Live<CollectionRowOf<Self>[]>>
}

// Prisma's collection class isn't exported, so its type can't take a method. `livePrisma()` adds `live()` to each
// model's collection, and the steps that return it (`limit()`, `include()`, …) keep it. `select()` returns a new
// collection type; `where()` and `orderBy()` mark theirs with `HasWhere` and `HasOrderBy`, which carry `live()`.
declare module '@prisma/orm-postgres/orm-client' {
  interface HasWhere extends LiveMethod {}
  interface HasOrderBy extends LiveMethod {}
}

type PrismaClient = { orm: object; contract: { roots: Record<string, { model: string; namespace: string }> } }

/** The client `livePrisma()` returns: the same, with `.live()` on each model's collection. */
type LivePrisma<TDb extends PrismaClient> = TDb & {
  orm: { [Ns in keyof TDb['orm']]: { [Model in keyof TDb['orm'][Ns]]: TDb['orm'][Ns][Model] & LiveMethod } }
}

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
