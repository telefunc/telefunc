export { liveKysely }
export type { LiveTables }

import type { Kysely, Simplify } from 'kysely'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

// Kysely's chain methods return `SelectQueryBuilder<…>` by name, with no type parameter to carry an extra method:
// merging `live()` into that interface is the one way it stays on every step. What every step does carry is the
// database's tables, `DB`: `liveKysely()` adds a mark to them, and `live()` type-checks only on a builder that has it.

declare const LIVE_TABLES: unique symbol
/** The mark on the tables of a `Kysely` wrapped by `liveKysely()`. It exists in types only. */
type LiveTables = { readonly [LIVE_TABLES]: true }

declare module 'kysely' {
  interface SelectQueryBuilder<DB, TB extends keyof DB, O> {
    /** Runs the query like `execute()` does, and also makes its result live: the client fetches it again when a
     *  committed write changes what it read. */
    live(
      this: DB extends LiveTables ? unknown : 'live() needs a Kysely wrapped by liveKysely(): liveKysely(db, pool)',
    ): Promise<Live<Simplify<O>[]>>
  }
}

/**
 * Lets a read of `db` end with `.live()`: the client fetches it again when a committed write changes what it read.
 * Writes, and reads without `.live()`, run as before. Kysely doesn't expose its pool: pass the one its dialect uses.
 *
 * ```ts
 * export const db = liveKysely(new Kysely<DB>({ dialect: new PostgresDialect({ pool }) }), pool)
 * // in a telefunction
 * return db.selectFrom('todos').selectAll().where('userId', '=', userId).live() // Promise<Live<Todo[]>>
 * ```
 */
function liveKysely<DB>(db: Kysely<DB>, pool: DatabaseClient, options: LiveOptions = {}): Kysely<DB & LiveTables> {
  assertUsage(
    isDatabaseClient(pool),
    'liveKysely(db, pool) takes the pool its dialect uses: a pg.Pool, a postgres.js `sql` or a PGlite instance.',
  )
  liveDatabaseOf(pool, options)
  // The class isn't exported: a builder that is never run gives its prototype.
  const builder = Object.getPrototypeOf(db.selectFrom('_' as never))
  if (!('live' in builder))
    Object.defineProperty(builder, 'live', {
      value: function live(this: { execute(): Promise<unknown> }) {
        return readLive(() => this.execute(), 'liveKysely()')
      },
    })
  return db as Kysely<DB & LiveTables>
}
