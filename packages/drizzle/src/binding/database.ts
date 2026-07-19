export { dialectOf, driverOf, semanticEnvironmentKeyOf, rlsEnabledOf, executeSql, entityKindOf, isSingleSession }
export { oldNewReturningOf, oldNewProvenOf, markOldNewProven, demoteOldNewReturning, probeOldNewReturning }
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
// question here is what THIS connection accepts. Version alone is exactly what PostgreSQL-compatible forks
// break. So the statement is actually run: against a temp table, inside a transaction that is ALWAYS rolled
// back, which is what makes it safe to run against a live database.
//
// WHAT AN OPERATOR WILL SEE IN THEIR QUERY LOG, once per database, at `reactiveDrizzle()` setup:
//
//   BEGIN; CREATE TEMP TABLE telefunc_old_new_probe (x int); INSERT …; UPDATE … RETURNING old.x, new.x;
//   ROLLBACK;
//
// It is fire-and-forget, always rolled back, and has no persistent effect — no table, no row, no sequence
// (verified: nothing survives in `pg_class`). A server that rejects the syntax aborts only this transaction.
//
// `CREATE TEMP TABLE` needs the TEMP privilege, which hardened least-privilege roles revoke. That failure
// says nothing about OLD/NEW, so it is told apart from a refusal of the SYNTAX: on permission-denied ONLY,
// the answer falls back to the privilege-free `server_version_num` (18+ ⇒ supported). A version is weaker
// evidence than a statement, so a capability derived that way is marked UNPROVEN, and the first write that
// actually relies on it is guarded — see `oldNewProvenOf` and its use in writeCapture's `runWrite`.
//
// The answer is cached per db object and read SYNCHRONOUSLY by write planning. Until the probe lands the
// answer is unknown, and unknown reads as NOT supported — so capture only ever gains precision from this,
// and never waits on it or assumes it.

/** `supported`: whether to use OLD/NEW at all. `proven`: whether a statement — rather than a version
 *  number — is what established it. An unsupported answer is always proven: there is nothing to retry. */
type OldNewCapability = { supported: boolean; proven: boolean }

const oldNewSupport = new WeakMap<object, OldNewCapability>()
const probeInFlight = new WeakMap<object, Promise<boolean>>()
const PROBE_TABLE = 'telefunc_old_new_probe'
/** Thrown to roll the probe back. A sentinel, so a genuine failure is never mistaken for the rollback. */
const PROBE_ROLLBACK = Symbol('telefunc: capability probe rollback')
const UNSUPPORTED: OldNewCapability = { supported: false, proven: true }

/** Whether `RETURNING old.*, new.*` is known to work on this db. Never `true` before the probe resolves. */
function oldNewReturningOf(db: AnyDb): boolean {
  return oldNewSupport.get(db as object)?.supported === true
}

/** Whether that answer came from a statement that RAN, rather than from the server's version number. An
 *  unproven capability is believed only until the first statement that depends on it says otherwise. */
function oldNewProvenOf(db: AnyDb): boolean {
  return oldNewSupport.get(db as object)?.proven === true
}

/** The first OLD/NEW statement went through: the version was telling the truth, and there is nothing left
 *  to guard. */
function markOldNewProven(db: AnyDb): void {
  const capability = oldNewSupport.get(db as object)
  if (capability?.supported) capability.proven = true
}

/** Believe the statement over the version number, permanently for this db. Called when a write that relied
 *  on an UNPROVEN capability was rejected by the server. */
function demoteOldNewReturning(db: AnyDb): void {
  oldNewSupport.set(db as object, { supported: false, proven: true })
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
    (capability) => {
      oldNewSupport.set(key, capability)
      return capability.supported
    },
    () => {
      oldNewSupport.set(key, { ...UNSUPPORTED })
      return false
    },
  )
  probeInFlight.set(key, probe)
  return probe
}

async function runOldNewProbe(db: AnyDb): Promise<OldNewCapability> {
  if (dialectOf(db) !== 'pg') return UNSUPPORTED
  const host = db as { transaction?: (cb: (tx: unknown) => Promise<unknown>) => Promise<unknown> }
  if (typeof host.transaction !== 'function') return UNSUPPORTED
  let supported = false
  let failure: unknown
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
    if (error !== PROBE_ROLLBACK) failure = error
  }
  if (supported) return { supported: true, proven: true }
  // The probe could not RUN, which is a different answer from the server refusing the syntax. A role
  // without the TEMP privilege — routine in a hardened deployment — tells us nothing about OLD/NEW, and
  // treating it as "unsupported" would cost such a database the precision forever.
  if (!isPermissionDenied(failure)) return UNSUPPORTED
  const version = await serverVersionNum(db)
  return version >= 180000 ? { supported: true, proven: false } : UNSUPPORTED
}

/** PostgreSQL's `insufficient_privilege` (42501) — "permission denied to create temporary tables". Matched
 *  by SQLSTATE first, since that is the driver-independent fact; the message is only a fallback for drivers
 *  that do not surface the code.
 *
 *  Walks the CAUSE chain, because drizzle re-throws driver errors wrapped ("Failed query: …") and the
 *  SQLSTATE lives on the original. Reading only the outer error classified every refusal as a syntax
 *  refusal, which silently disabled the fallback this function exists to enable. */
function isPermissionDenied(error: unknown): boolean {
  for (let current = error, depth = 0; current != null && depth < 5; depth++) {
    if ((current as { code?: unknown }).code === '42501') return true
    const message = current instanceof Error ? current.message.toLowerCase() : ''
    if (message.includes('permission denied') || message.includes('must be owner')) return true
    current = (current as { cause?: unknown }).cause
  }
  return false
}

/** `server_version_num` as an integer (180000 = PostgreSQL 18), or 0 when it cannot be read. `SHOW` needs
 *  no privileges, which is the whole point of using it as the fallback. */
async function serverVersionNum(db: AnyDb): Promise<number> {
  try {
    const rows = await rowRunnerFor(db)('show server_version_num')
    const value = rows[0] ? Object.values(rows[0])[0] : undefined
    const version = Number.parseInt(str(value), 10)
    return Number.isFinite(version) ? version : 0
  } catch {
    return 0
  }
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
