export { livePrisma }
export type { LivePrisma }

import type * as Prisma from '@prisma/orm-postgres/orm-client'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { LiveOptions } from './liveDatabase.js'
import { withLiveCollections, type PrismaClient, type Selected, type WithLiveModels } from './prismaLive.js'

declare module './prismaLive.js' {
  interface PrismaTypes<C, Field> {
    postgres: {
      hasRow: Prisma.HasRow
      row: C extends Prisma.HasRow ? Prisma.CollectionRowOf<C> : never
      // Spelled out because a generic method's result can't be read off its type for given arguments.
      picked: C extends Prisma.HasRow &
        Prisma.HasTypeState & {
          ctx: Prisma.CollectionContext<infer Contract>
          modelName: infer Model extends string
        }
        ? Prisma.Collection<
            Contract,
            Model,
            Selected<Prisma.CollectionRowOf<C>, Prisma.DefaultModelRow<Contract, Model>, Field>,
            Prisma.CollectionTypeStateOf<C>
          >
        : never
    }
  }
}

/** The client `livePrisma()` returns: the same, with `.live()` at the end of its models' collections, its
 *  transactions' too. */
type LivePrisma<TDb extends PrismaClient> = WithLiveModels<TDb, 'postgres'>

/**
 * Lets a collection read of `db` end with `.live()`: the client fetches it again when a committed write changes what
 * it read. Writes, and reads without `.live()`, run as before. Prisma doesn't expose its connections: pass the pool it
 * connects through. On SQLite, import `livePrisma` from `@telefunc/live/prisma-sqlite`.
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
    "livePrisma(db, pool) takes the pg.Pool given to postgres({ contract, pg: pool }). On SQLite, import livePrisma from '@telefunc/live/prisma-sqlite'.",
  )
  liveDatabaseOf(pool, options)
  withLiveCollections(db)
  return db as LivePrisma<TDb>
}
