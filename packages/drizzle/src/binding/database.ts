export { dialectOf, driverOf, clientOf, semanticEnvironmentKeyOf, rlsEnabledOf, entityKindOf }
export type { RlsStatus }

import { entityKind } from 'drizzle-orm'
import type { Dialect } from '../ir/types.js'
import { assertUsage } from '../utils/assert.js'

// Facts read off a live drizzle database instance. Dialect/driver come from drizzle's
// `[entityKind]` discriminators (stable across the v1 line); connection authority is
// derived defensively from `$client` and, where it can't be proven, fails closed.

type AnyDb = { dialect?: unknown; $client?: unknown }

/** true / false when known; `'unknown'` when discovery isn't possible — consumers
 *  treat unknown as coarse and must never read it as "off". */
type RlsStatus = boolean | 'unknown'

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

/** The raw driver client (`Pool`, `postgres` sql, `DatabaseSync`, …). */
function clientOf(db: AnyDb): unknown {
  return db.$client
}

// A connection's real authority — role, search_path — is set at runtime (SET ROLE /
// SET search_path) and can't be proven from static `$client` config, so that config is
// only a hint. We fail closed: each session gets its own key, so two sessions never
// share a graph unless a later layer proves their authority equal. Uniqueness comes
// from the per-instance token; the static hint is folded in only for debuggability.
const sessionKeys = new WeakMap<object, string>()
let sessionCounter = 0

/** A key for the connection's semantic authority. Feeds planKey. Non-shareable across
 *  db instances by construction — never collapses to a shared empty/default. */
function semanticEnvironmentKeyOf(db: AnyDb): string {
  const key = db as object
  let value = sessionKeys.get(key)
  if (value === undefined) {
    value = `sess:${sessionCounter++}|${dialectOf(db)}|${driverOf(db)}|${connectionHint(db.$client)}`
    sessionKeys.set(key, value)
  }
  return value
}

/** Best-effort static connection coordinates (host/port/database/user), across pg
 *  (`Pool.options`), postgres.js (`options`, array host/port), mysql2 (`config`) and
 *  sqlite. Only a hint — the session token is what guarantees non-sharing. */
function connectionHint(client: unknown): string {
  if (typeof client !== 'object' || client === null) return ''
  const c = client as Record<string, unknown>
  const opts = (c.options ?? c.connectionParameters ?? c.config ?? {}) as Record<string, unknown>
  const conn = (opts.connectionConfig ?? opts) as Record<string, unknown>
  const firstOf = (value: unknown) => (Array.isArray(value) ? value[0] : value)
  return [firstOf(conn.host), firstOf(conn.port), conn.database ?? conn.db, conn.user ?? conn.username]
    .filter((value) => value !== undefined && value !== null)
    .map(String)
    .join(':')
}

/** Whether a table has row-level security. RLS changes what a subscription may observe,
 *  so the graph layer folds it into authority. sqlite/mysql have no per-table RLS, so
 *  it's definitively disabled; Postgres RLS lives in `pg_class.relrowsecurity` and needs
 *  a live catalog query wired by the CDC layer — until then it is UNKNOWN, never assumed off. */
function rlsEnabledOf(db: AnyDb, table: string): Promise<RlsStatus> {
  assertUsage(typeof table === 'string' && table.length > 0, 'rlsEnabledOf requires a table name.')
  const dialect = dialectOf(db)
  if (dialect === 'sqlite' || dialect === 'mysql') return Promise.resolve(false)
  return Promise.resolve('unknown')
}
