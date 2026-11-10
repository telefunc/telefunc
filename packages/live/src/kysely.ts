export { liveKysely }

import type { Kysely, Simplify } from 'kysely'
import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf, type DatabaseClient } from './database.js'
import type { Live } from './live.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

// Kysely's chain methods return `SelectQueryBuilder<…>` by name, with no type parameter to carry an extra method:
// merging `live()` into that interface is the one way it stays on every step.
declare module 'kysely' {
  interface SelectQueryBuilder<DB, TB extends keyof DB, O> {
    /** Runs the query like `execute()` does, and also makes its result live: the client fetches it again when a
     *  committed write changes what it read. Needs a `Kysely` wrapped by `liveKysely()`. */
    live(): Promise<Live<Simplify<O>[]>>
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
function liveKysely<DB>(db: Kysely<DB>, pool: DatabaseClient, options: LiveOptions = {}): Kysely<DB> {
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
  return db
}
