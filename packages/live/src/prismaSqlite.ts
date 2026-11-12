export { livePrisma }
export type { LivePrisma }

import type * as Prisma from '@prisma/orm-sqlite/orm-client'
import { assertUsage } from './assert.js'
import { liveDatabaseOfFile } from './database.js'
import type { LiveOptions } from './liveDatabase.js'
import { withLiveCollections, type PrismaClient, type Selected, type WithLiveModels } from './prismaLive.js'

declare module './prismaLive.js' {
  interface PrismaTypes<C, Field> {
    sqlite: {
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
type LivePrisma<TDb extends PrismaClient> = WithLiveModels<TDb, 'sqlite'>

/**
 * Lets a collection read of `db` end with `.live()`: the client fetches it again when a committed write changes what
 * it read. Writes, and reads without `.live()`, run as before. Prisma opens its own connections to the database file:
 * pass the file's path.
 *
 * ```ts
 * export const db = livePrisma(sqlite({ contract, path: './app.db' }), './app.db')
 * // in a telefunction
 * return db.orm.Todo.where({ userId }).live() // Promise<Live<Todo[]>>
 * ```
 */
function livePrisma<TDb extends PrismaClient>(db: TDb, path: string, options: LiveOptions = {}): LivePrisma<TDb> {
  assertUsage(
    typeof path === 'string',
    "livePrisma(db, path) takes the path given to sqlite({ contract, path }). On Postgres, import livePrisma from '@telefunc/live/prisma'.",
  )
  liveDatabaseOfFile(path, options)
  withLiveCollections(db)
  return db as LivePrisma<TDb>
}
