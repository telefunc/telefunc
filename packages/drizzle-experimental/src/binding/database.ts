export { dialectOf, driverOf, semanticEnvironmentKeyOf, rlsEnabledOf, executeSql, entityKindOf, isSingleSession }
export { rowRunnerFor }
export type { RlsStatus, RowRunner }

import { type SQL, entityKind, sql } from 'drizzle-orm'
import type { Dialect, RlsStatus } from '../ir/types.js'
import { assertUsage } from '../utils/assert.js'

// Facts read off a live drizzle database instance. Dialect/driver come from drizzle's
// `[entityKind]` discriminators (stable across the v1 line). Connection *authority*
// (role, search_path, database) is a runtime property of the executing session — it
// cannot be proven from static `$client` config, which SET ROLE / SET search_path can
// change — so it is discovered by probing the connection, and fails closed when it can't.

type AnyDb = { dialect?: unknown; $client?: unknown }

/** Runs raw SQL against the executing connection and returns the result rows. Injected
 *  in tests; derived from the db by default. */
type RowRunner = (query: SQL) => Promise<Record<string, unknown>[]>

/** The registered `[entityKind]` discriminator of any drizzle value, or undefined. */
function entityKindOf(value: unknown): string | undefined {
  return (value as { constructor?: { [entityKind]?: string } } | null)?.constructor?.[entityKind]
}

/** The SQL dialect, from the db's dialect object — and the ONE place that decides what this package
 *  supports. MySQL is REJECTED by name rather than falling into the unrecognized-dialect message: it is a
 *  dialect drizzle fully supports and this package deliberately does not, and a caller deserves to be told
 *  which of the two it is. */
function dialectOf(db: AnyDb): Dialect {
  const kind = entityKindOf(db.dialect)
  if (kind === 'PgDialect') return 'pg'
  if (kind === 'SQLiteDialect') return 'sqlite'
  assertUsage(
    kind !== 'MySqlDialect',
    'reactiveDrizzle does not support MySQL: with no RETURNING there is no verified row-capture lane for it, so every write would invalidate every live query on the table. Supported databases: PostgreSQL, PGlite, SQLite.',
  )
  assertUsage(
    false,
    `Unrecognized drizzle dialect (${kind ?? 'unknown'}); reactiveDrizzle supports PostgreSQL, PGlite and SQLite.`,
  )
}

/** The concrete driver, e.g. `NodePgDatabase`, `PostgresJsDatabase`, `NodeSQLiteDatabase`. */
function driverOf(db: AnyDb): string {
  const kind = entityKindOf(db)
  assertUsage(kind, 'Expected a drizzle database instance (no entityKind found).')
  return kind
}

// ── Semantic authority ──────────────────────────────────────────────

/** A key for the connection's proven semantic authority — dialect, driver, database,
 *  role and search_path as they actually are on the executing session. Feeds planKey.
 *
 *  A pooled client can satisfy the probe on one connection and the user's query on
 *  another, so its probed authority is unprovable — such clients FAIL CLOSED to a
 *  per-call unique key, so two sessions never share a graph on an unproven assumption.
 *  Only a provably single-session client (node:sqlite, a single pg `Client`, a pinned
 *  connection) yields a shareable probed key. */
async function semanticEnvironmentKeyOf(db: AnyDb, opts?: { run?: RowRunner }): Promise<string> {
  const dialect = dialectOf(db)
  const driver = driverOf(db)
  if (!isSingleSession(db)) return failClosedKey(dialect, driver)
  const run = opts?.run ?? rowRunnerFor(db)
  try {
    const authority = await probeAuthority(dialect, run)
    return `env|${dialect}|${driver}|db=${authority.database}|role=${authority.role}|sp=${authority.searchPath}`
  } catch {
    return failClosedKey(dialect, driver)
  }
}

/** Whether the connection is provably one session. sqlite is a single connection; PGlite is an
 *  in-process single connection; a node-postgres `Client` is single (a `Pool`'s bound client is
 *  `BoundPool`). Pools and postgres.js are treated as pooled. PGlite is classified by its STABLE
 *  drizzle entity kind, NOT the raw client's constructor.name — PGlite's bundled client minifies
 *  (e.g. to `'O'`), which a name check misreads as pooled. */
function isSingleSession(db: AnyDb): boolean {
  if (dialectOf(db) === 'sqlite') return true
  if (entityKindOf(db) === 'PgliteDatabase') return true // single in-process connection ⇒ precise
  return (db.$client as { constructor?: { name?: string } } | null)?.constructor?.name === 'Client'
}

async function probeAuthority(
  dialect: Dialect,
  run: RowRunner,
): Promise<{ database: string; role: string; searchPath: string }> {
  if (dialect === 'pg') {
    const row = one(
      await run(
        sql.raw(
          `select current_database() as database, current_user as role, current_setting('search_path') as search_path`,
        ),
      ),
    )
    return { database: str(row.database), role: str(row.role), searchPath: str(row.search_path) }
  }
  // sqlite has no roles/search_path; authority is the attached database file(s)
  const rows = await run(sql.raw('pragma database_list'))
  return { database: rows.map((r) => `${str(r.name)}:${str(r.file)}`).join(';'), role: '', searchPath: '' }
}

let failCounter = 0

/** Authority couldn't be proven — never share. Unique per call so identical queries do
 *  not dedupe under an unproven assumption. */
function failClosedKey(dialect: Dialect, driver: string): string {
  return `env-failclosed|${dialect}|${driver}|#${failCounter++}`
}

// ── Row-level security ──────────────────────────────────────────────

/** Whether a table has row-level security. sqlite has no per-table RLS (false);
 *  Postgres is read from `pg_class.relrowsecurity` — `true`/`false` when the relation is
 *  found, `'unknown'` when the catalog row is missing or the query fails. Never assumes off. */
async function rlsEnabledOf(db: AnyDb, table: string, opts?: { run?: RowRunner; schema?: string }): Promise<RlsStatus> {
  assertUsage(typeof table === 'string' && table.length > 0, 'rlsEnabledOf requires a table name.')
  const dialect = dialectOf(db)
  if (dialect !== 'pg') return false
  const run = opts?.run ?? rowRunnerFor(db)
  const schema = opts?.schema ?? 'public'
  try {
    const rows = await run(
      sql`select c.relrowsecurity as rls from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = ${table} and n.nspname = ${schema}`,
    )
    if (rows.length === 0) return 'unknown'
    const value = rows[0]!.rls
    if (typeof value === 'boolean') return value
    if (value === 't' || value === 'f') return value === 't' // pg boolean rendered as a string
    return 'unknown'
  } catch {
    return 'unknown'
  }
}

// ── Default row runner ──────────────────────────────────────────────

/** A row runner derived from a drizzle db. sqlite runs through `db.all`; pg through
 *  `db.execute`. Result shapes differ per driver, so rows are normalized. */
function rowRunnerFor(db: AnyDb): RowRunner {
  return (query) => queryRows(db, query)
}

/** Execute a built drizzle `SQL` (not raw text) against the connection and normalize the
 *  result rows — the hydration executor's read primitive. */
async function executeSql(db: AnyDb, query: SQL): Promise<Record<string, unknown>[]> {
  return queryRows(db, query)
}

/** The one dialect dispatch for built and raw SQL alike. */
async function queryRows(db: AnyDb, query: SQL): Promise<Record<string, unknown>[]> {
  const runner = db as { execute?: (q: SQL) => unknown; all?: (q: SQL) => unknown }
  const raw = dialectOf(db) === 'sqlite' ? await runner.all!(query) : await runner.execute!(query)
  return normalizeRows(raw)
}

function normalizeRows(raw: unknown): Record<string, unknown>[] {
  if (Array.isArray(raw)) return raw as Record<string, unknown>[] // postgres.js and sqlite return rows directly
  const rows = (raw as { rows?: unknown })?.rows // node-postgres wraps rows in a Result
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : []
}

function one(rows: Record<string, unknown>[]): Record<string, unknown> {
  assertUsage(rows.length > 0, 'authority probe returned no rows')
  return rows[0]!
}

function str(value: unknown): string {
  return value == null ? '' : String(value)
}
