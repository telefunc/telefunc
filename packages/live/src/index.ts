export { withLiveQueries }
export type { LiveQueriesOptions }

// A telefunction's reads are recorded on its call's context, which has to survive `await`.
import 'telefunc/async_hooks'
import './extension.js'
import { assertUsage } from './assert.js'
import { capturePg, type PgClient, type PgPool } from './capture/pg.js'
import { capturePglite, type Pglite } from './capture/pglite.js'
import { LiveDatabase, type LiveQueriesOptions } from './liveDatabase.js'

/** Makes the queries your telefunctions run through `database` live: a client watching a telefunction's result
 *  refetches it when a committed write changes what it read. Wrap the database before creating the ORM on it. */
function withLiveQueries<T extends PgPool | PgClient | Pglite>(database: T, options: LiveQueriesOptions = {}): T {
  const live = new LiveDatabase(options)
  if ('transaction' in database && 'exec' in database) capturePglite(database as Pglite, live)
  else {
    assertUsage(
      !('totalCount' in database) || database.totalCount === 0,
      'Call withLiveQueries() on the pg.Pool before running any query through it.',
    )
    capturePg(database as PgPool | PgClient, live)
  }
  return database
}
