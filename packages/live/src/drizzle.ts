export { liveDrizzle }
export type { LiveDrizzle }

import { assertUsage } from './assert.js'
import { isDatabaseClient, liveDatabaseOf } from './database.js'
import type { DrizzleDatabase, LiveDrizzle } from './drizzle.types.js'
import type { LiveOptions } from './liveDatabase.js'
import { readLive } from './liveQuery.js'

/**
 * Lets a read of `db` end with `.live()`: the client fetches it again when a committed write changes what it read.
 * Writes, and reads without `.live()`, run as before.
 *
 * ```ts
 * export const db = liveDrizzle(drizzle({ client: pool }))
 * // in a telefunction
 * return db.select().from(todos).where(eq(todos.userId, userId)).live() // Promise<Live<Todo[]>>
 * ```
 */
function liveDrizzle<TDb extends DrizzleDatabase>(db: TDb, options: LiveOptions = {}): LiveDrizzle<TDb> {
  assertUsage(
    isDatabaseClient(db.$client),
    'liveDrizzle() takes a Drizzle database on node-postgres, postgres.js or PGlite, e.g. liveDrizzle(drizzle({ client: pool })).',
  )
  liveDatabaseOf(db.$client, options)
  // From the app's own copy of Drizzle (a package manager may install several): a select that is never run gives it.
  const select = Object.getPrototypeOf(db.select({}).from({} as never))
  if (!('live' in select))
    Object.defineProperty(select, 'live', {
      value: function live(this: { execute(): Promise<unknown> }) {
        return readLive(() => this.execute(), 'liveDrizzle()')
      },
    })
  return db as unknown as LiveDrizzle<TDb>
}
