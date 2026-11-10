export { liveDatabaseOf, isDatabaseClient }
export type { DatabaseClient }

import { assertUsage } from './assert.js'
import { capturePg, type PgClient, type PgPool } from './capture/pg.js'
import { capturePglite, type Pglite } from './capture/pglite.js'
import { capturePostgresJs, isPostgresJs, type PostgresJs } from './capture/postgresJs.js'
import { LiveDatabase, type LiveOptions } from './liveDatabase.js'
import { processWide } from './processWide.js'

/** What an ORM sends its SQL through: a `pg.Pool` or `pg.Client`, a postgres.js `sql`, a PGlite instance. */
type DatabaseClient = PgPool | PgClient | Pglite | PostgresJs

/** By client: a client one copy of this package wraps is captured once, whichever copies wrap it. */
const databases = processWide('databases', () => new WeakMap<object, { live: LiveDatabase; options: LiveOptions }>())

/** The live database of `client`, which captures its statements from now on. Several ORMs over one pool share it. */
function liveDatabaseOf(client: DatabaseClient, options: LiveOptions): LiveDatabase {
  const known = databases.get(client)
  if (known) {
    assertUsage(
      options.transport === undefined || options.transport === known.options.transport,
      'Pass the same `transport` to every live*() wrapper of one database client.',
    )
    return known.live
  }
  const live = new LiveDatabase(options)
  if (isPostgresJs(client)) capturePostgresJs(client, live)
  else if (isPglite(client)) capturePglite(client, live)
  else {
    assertUsage(
      !('totalCount' in client) || client.totalCount === 0,
      'Wrap the database before running any query through its pg.Pool: the connections it already opened would not be captured.',
    )
    capturePg(client, live)
  }
  databases.set(client, { live, options })
  return live
}

function isDatabaseClient(value: unknown): value is DatabaseClient {
  if (typeof value === 'function') return isPostgresJs(value as never)
  return typeof value === 'object' && value !== null && typeof (value as PgClient).query === 'function'
}

function isPglite(client: DatabaseClient): client is Pglite {
  return 'transaction' in client && 'exec' in client
}
