export { dialectOf, driverOf, semanticEnvironmentKeyOf, rlsEnabledOf, executeSql, entityKindOf, isSingleSession }
export { oldNewReturningOf, probeOldNewReturning }
export type { RlsStatus, RowRunner }

import { type SQL, entityKind, sql } from 'drizzle-orm'
import type { Dialect } from '../ir/types.js'
import { assertUsage } from '../utils/assert.js'

// Facts read off a live drizzle database instance. Dialect/driver come from drizzle's
// `[entityKind]` discriminators (stable across the v1 line). Connection *authority*
// (role, search_path, database) is a runtime property of the executing session — it
// cannot be proven from static `$client` config, which SET ROLE / SET search_path can
// change — so it is discovered by probing the connection, and fails closed when it can't.

type AnyDb = { dialect?: unknown; $client?: unknown }

/** true / false when known; `'unknown'` when discovery isn't possible — consumers
 *  treat unknown as coarse and must never read it as "off". */
type RlsStatus = boolean | 'unknown'

/** Runs raw SQL against the executing connection and returns the result rows. Injected
 *  in tests; derived from the db by default. */
type RowRunner = (sqlText: string) => Promise<Record<string, unknown>[]>

/** The registered `[entityKind]` discriminator of any drizzle value, or undefined. */
function entityKindOf(value: unknown): string | undefined {
  return (value as { constructor?: { [entityKind]?: string } } | null)?.constructor?.[entityKind]
}

/** The SQL dialect, from the db's dialect object. */
function dialectOf(db: AnyDb): Dialect {
  const kind = entityKindOf(db.dialect)
  if (kind === 'PgDialect') return 'pg'
  if (kind === 'MySqlDialect') return 'mysql'
  if (kind === 'SQLiteDialect') return 'sqlite'
  assertUsage(
    false,
    `Unrecognized drizzle dialect (${kind ?? 'unknown'}); reactiveDrizzle targets the drizzle v1 pg/mysql/sqlite dialects.`,
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
 *  `BoundPool`), as is a single mysql `Connection`. Pools and postgres.js are treated as pooled.
 *  PGlite is classified by its STABLE drizzle entity kind, NOT the raw client's constructor.name —
 *  PGlite's bundled client minifies (e.g. to `'O'`), which a name check misreads as pooled. */
function isSingleSession(db: AnyDb): boolean {
  const dialect = dialectOf(db)
  if (dialect === 'sqlite') return true
  if (entityKindOf(db) === 'PgliteDatabase') return true // single in-process connection ⇒ precise
  const clientKind = (db.$client as { constructor?: { name?: string } } | null)?.constructor?.name
  if (dialect === 'pg') return clientKind === 'Client'
  return clientKind === 'Connection' || clientKind === 'PromiseConnection'
}

async function probeAuthority(
  dialect: Dialect,
  run: RowRunner,
): Promise<{ database: string; role: string; searchPath: string }> {
  if (dialect === 'pg') {
    const row = one(
      await run(
        `select current_database() as database, current_user as role, current_setting('search_path') as search_path`,
      ),
    )
    return { database: str(row.database), role: str(row.role), searchPath: str(row.search_path) }
  }
  if (dialect === 'mysql') {
    const row = one(await run('select database() as `database`, current_user() as role'))
    return { database: str(row.database), role: str(row.role), searchPath: '' }
  }
  // sqlite has no roles/search_path; authority is the attached database file(s)
  const rows = await run('pragma database_list')
  return { database: rows.map((r) => `${str(r.name)}:${str(r.file)}`).join(';'), role: '', searchPath: '' }
}

let failCounter = 0

/** Authority couldn't be proven — never share. Unique per call so identical queries do
 *  not dedupe under an unproven assumption. */
function failClosedKey(dialect: Dialect, driver: string): string {
  return `env-failclosed|${dialect}|${driver}|#${failCounter++}`
}

// ── Returned-image capability ───────────────────────────────────────

// Whether this connection can return BOTH images of a changed row in the write statement itself
// (`RETURNING old.*, new.*`, PostgreSQL 18 and up). With it, an update that moves a primary key, and an
// update a stateless live query has to decide membership for, are exact with no extra round trip.
//
// This is a CAPABILITY probe, not a version check — a version number is a claim about a server, and the
// question here is what THIS connection accepts. So the statement is actually run: against a temp table,
// inside a transaction that is ALWAYS rolled back, which is what makes it safe to run against a live
// database. It leaves nothing behind (verified: no `pg_class` row survives), and a server that rejects the
// syntax aborts only the probe's own transaction.
//
// The answer is cached per db object and read SYNCHRONOUSLY by write planning. Until the probe lands the
// answer is unknown, and unknown reads as NOT supported — so capture only ever gains precision from this,
// and never waits on it or assumes it.

const oldNewSupport = new WeakMap<object, boolean>()
const probeInFlight = new WeakMap<object, Promise<boolean>>()
const PROBE_TABLE = 'telefunc_old_new_probe'
/** Thrown to roll the probe back. A sentinel, so a genuine failure is never mistaken for the rollback. */
const PROBE_ROLLBACK = Symbol('telefunc: capability probe rollback')

/** Whether `RETURNING old.*, new.*` is known to work on this db. Never `true` before the probe resolves. */
function oldNewReturningOf(db: AnyDb): boolean {
  return oldNewSupport.get(db as object) === true
}

/** Start the capability probe for this db, at most once, and return what it settles on. Production calls
 *  this and does NOT await it: nothing waits on the probe, so a slow or failing one costs a write nothing
 *  but the extra precision it would have unlocked. The promise is returned so a caller that genuinely needs
 *  the settled answer (a test asserting the capability) can have it without a test-only seam. */
function probeOldNewReturning(db: AnyDb): Promise<boolean> {
  const key = db as object
  const started = probeInFlight.get(key)
  if (started) return started
  const probe = runOldNewProbe(db).then(
    (supported) => {
      oldNewSupport.set(key, supported)
      return supported
    },
    () => {
      oldNewSupport.set(key, false)
      return false
    },
  )
  probeInFlight.set(key, probe)
  return probe
}

async function runOldNewProbe(db: AnyDb): Promise<boolean> {
  if (dialectOf(db) !== 'pg') return false
  const host = db as { transaction?: (cb: (tx: unknown) => Promise<unknown>) => Promise<unknown> }
  if (typeof host.transaction !== 'function') return false
  let supported = false
  try {
    await host.transaction(async (tx) => {
      const run = (text: string) => (tx as { execute: (query: SQL) => Promise<unknown> }).execute(sql.raw(text))
      await run(`create temp table ${PROBE_TABLE} (x int)`)
      await run(`insert into ${PROBE_TABLE} values (1)`)
      await run(`update ${PROBE_TABLE} set x = 2 returning old.x, new.x`)
      supported = true
      throw PROBE_ROLLBACK // never commit — the probe must leave the database exactly as it found it
    })
  } catch (error) {
    if (error !== PROBE_ROLLBACK) return false
  }
  return supported
}

// ── Row-level security ──────────────────────────────────────────────

/** Whether a table has row-level security. sqlite/mysql have no per-table RLS (false);
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
      `select c.relrowsecurity as rls from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = '${lit(table)}' and n.nspname = '${lit(schema)}'`,
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

/** A row runner derived from a drizzle db. sqlite runs through `db.all`; pg/mysql through
 *  `db.execute`. Result shapes differ per driver, so rows are normalized. */
function rowRunnerFor(db: AnyDb): RowRunner {
  const dialect = dialectOf(db)
  const runner = db as { execute?: (q: SQL) => unknown; all?: (q: SQL) => unknown }
  return async (text) => {
    const query = sql.raw(text)
    const raw = dialect === 'sqlite' ? await runner.all!(query) : await runner.execute!(query)
    return normalizeRows(raw)
  }
}

/** Execute a built drizzle `SQL` (not raw text) against the connection and normalize the
 *  result rows — the hydration executor's read primitive. */
async function executeSql(db: AnyDb, query: SQL): Promise<Record<string, unknown>[]> {
  const runner = db as { execute?: (q: SQL) => unknown; all?: (q: SQL) => unknown }
  const raw = dialectOf(db) === 'sqlite' ? await runner.all!(query) : await runner.execute!(query)
  return normalizeRows(raw)
}

function normalizeRows(raw: unknown): Record<string, unknown>[] {
  if (Array.isArray(raw)) {
    // mysql2 returns [rows, fields]; postgres.js and sqlite return the rows array directly
    const [head, second] = raw
    if (Array.isArray(head) && Array.isArray(second)) return head as Record<string, unknown>[]
    return raw as Record<string, unknown>[]
  }
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

function lit(value: string): string {
  return value.replace(/'/g, "''")
}
