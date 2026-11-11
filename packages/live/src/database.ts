export { liveDatabaseOf, liveDatabaseOfFile, isDatabaseClient }
export type { DatabaseClient }

import { assertUsage } from './assert.js'
import { captureBetterSqlite3, isBetterSqlite3, type BetterSqlite3 } from './capture/betterSqlite3.js'
import { captureBunSqlite, isBunSqlite, type BunSqlite } from './capture/bunSqlite.js'
import { captureD1, isD1, type D1 } from './capture/d1.js'
import { captureLibsql, isLibsql, type Libsql } from './capture/libsql.js'
import { captureNodeSqliteFile, wrappedFile } from './capture/nodeSqlite.js'
import { capturePg, type PgClient, type PgPool } from './capture/pg.js'
import { capturePglite, type Pglite } from './capture/pglite.js'
import { capturePostgresJs, isPostgresJs, type PostgresJs } from './capture/postgresJs.js'
import { LiveDatabase, type Engine, type LiveOptions } from './liveDatabase.js'
import { processWide } from './processWide.js'
import { LOGGED_SQLITE_ENGINE, RETURNING_SQLITE_ENGINE } from './sqlite/engine.js'

/** What an ORM sends its SQL through: a `pg.Pool` or `pg.Client`, a postgres.js `sql`, a PGlite instance, a
 *  better-sqlite3 or bun:sqlite database, a local libSQL client, a D1 binding. */
type DatabaseClient = PgPool | PgClient | Pglite | PostgresJs | BetterSqlite3 | BunSqlite | Libsql | D1

/** Postgres's planner, catalog and parser (libpg-query), imported once a Postgres client loads its catalog: an app on
 *  SQLite doesn't load them. */
let postgres: typeof import('./postgres/engine.js') | undefined
const POSTGRES_ENGINE: Engine = {
  loadCatalog: async (query) => (postgres ??= await import('./postgres/engine.js')).loadCatalog(query),
  plan: (text, catalog) => postgres!.plan(text, catalog),
  failureAbortsTransaction: true,
}

/** By client: a client one copy of this package wraps is captured once, whichever copies wrap it. */
const databases = processWide('databases', () => new WeakMap<object, { live: LiveDatabase; options: LiveOptions }>())

/** The live database of `client`, which captures its statements from now on. Several ORMs over one pool share it. */
function liveDatabaseOf(client: DatabaseClient, options: LiveOptions): LiveDatabase {
  const known = databases.get(client)
  if (known) return sameDatabase(known, options)
  const live = new LiveDatabase(options, engineOf(client))
  if (isBetterSqlite3(client)) captureBetterSqlite3(client, live)
  else if (isBunSqlite(client)) captureBunSqlite(client, live)
  else if (isD1(client)) captureD1(client, live)
  else if (isLibsql(client)) captureLibsql(client, live)
  else if (isPostgresJs(client)) capturePostgresJs(client, live)
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

/** The live database of the SQLite file at `path`, which captures the statements of every node:sqlite connection on
 *  it from now on, whoever opens it. */
function liveDatabaseOfFile(path: string, options: LiveOptions): LiveDatabase {
  const known = wrappedFile(path)
  if (known) return sameDatabase(known, options)
  const live = new LiveDatabase(options, LOGGED_SQLITE_ENGINE)
  captureNodeSqliteFile(path, { live, options })
  return live
}

function sameDatabase(known: { live: LiveDatabase; options: LiveOptions }, options: LiveOptions): LiveDatabase {
  assertUsage(
    options.transport === undefined || options.transport === known.options.transport,
    'Pass the same `transport` to every live*() wrapper of one database.',
  )
  return known.live
}

function engineOf(client: DatabaseClient): Engine {
  if (isBetterSqlite3(client) || isBunSqlite(client)) return LOGGED_SQLITE_ENGINE
  return isD1(client) || isLibsql(client) ? RETURNING_SQLITE_ENGINE : POSTGRES_ENGINE
}

function isDatabaseClient(value: unknown): value is DatabaseClient {
  if (typeof value === 'function') return isPostgresJs(value as never)
  if (typeof value !== 'object' || value === null) return false
  const sqlite = isBetterSqlite3(value) || isBunSqlite(value) || isLibsql(value) || isD1(value)
  return sqlite || typeof (value as PgClient).query === 'function'
}

function isPglite(client: Exclude<DatabaseClient, BetterSqlite3 | BunSqlite | Libsql | D1>): client is Pglite {
  return 'transaction' in client && 'exec' in client
}
