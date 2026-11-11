export { liveDatabaseOf, isDatabaseClient }
export type { DatabaseClient }

import { assertUsage } from './assert.js'
import { captureBetterSqlite3, isBetterSqlite3, type BetterSqlite3 } from './capture/betterSqlite3.js'
import { captureBunSqlite, isBunSqlite, type BunSqlite } from './capture/bunSqlite.js'
import { capturePg, type PgClient, type PgPool } from './capture/pg.js'
import { capturePglite, type Pglite } from './capture/pglite.js'
import { capturePostgresJs, isPostgresJs, type PostgresJs } from './capture/postgresJs.js'
import { planStatement } from './capture/statement.js'
import { loadCatalog } from './catalog.js'
import { LiveDatabase, type Engine, type LiveOptions } from './liveDatabase.js'
import { processWide } from './processWide.js'
import { loadParser } from './sql.js'
import { LOGGED_SQLITE_ENGINE } from './sqlite/engine.js'

/** What an ORM sends its SQL through: a `pg.Pool` or `pg.Client`, a postgres.js `sql`, a PGlite instance, a
 *  better-sqlite3 or bun:sqlite database. */
type DatabaseClient = PgPool | PgClient | Pglite | PostgresJs | BetterSqlite3 | BunSqlite

const POSTGRES_ENGINE: Engine = {
  loadCatalog: async (query) => (await Promise.all([loadCatalog(query), loadParser()]))[0],
  plan: planStatement,
  failureAbortsTransaction: true,
}

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
  if (isBetterSqlite3(client) || isBunSqlite(client)) {
    const live = new LiveDatabase(options, LOGGED_SQLITE_ENGINE)
    if (isBunSqlite(client)) captureBunSqlite(client, live)
    else captureBetterSqlite3(client, live)
    databases.set(client, { live, options })
    return live
  }
  const live = new LiveDatabase(options, POSTGRES_ENGINE)
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
  if (typeof value !== 'object' || value === null) return false
  return typeof (value as PgClient).query === 'function' || isBetterSqlite3(value) || isBunSqlite(value)
}

function isPglite(client: Exclude<DatabaseClient, BetterSqlite3 | BunSqlite>): client is Pglite {
  return 'transaction' in client && 'exec' in client
}
