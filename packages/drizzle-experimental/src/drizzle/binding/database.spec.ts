import { MySqlDialect } from 'drizzle-orm/mysql-core'
import { type SQL, sql } from 'drizzle-orm'
import { drizzle as pgDrizzle } from 'drizzle-orm/node-postgres'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import { drizzle as pjDrizzle } from 'drizzle-orm/postgres-js'
import { Client, Pool } from 'pg'
import postgres from 'postgres'
import { afterAll, describe, expect, it } from 'vitest'
import { StatementSync } from 'node:sqlite'
import {
  type RowRunner,
  dialectOf,
  driverOf,
  executeSql,
  rlsEnabledOf,
  rowRunnerFor,
  semanticEnvironmentKeyOf,
} from './database.js'

// drizzle-orm rc.4 node-sqlite calls StatementSync.setReturnArrays() (added Node 22.16 / 24.0; ABSENT
// in 23.x). Where it is missing, the real sqlite authority probe fail-closes (production-correct) and
// the stable-shareable-key assertion below no longer holds — skip just that case there. Requires Node
// >= 22.16 or >= 24.
//
// Deliberately gated on the runtime CAPABILITY rather than on the probe's outcome, unlike the SQLite lane
// in writeCapture.terminals.spec.ts which runs the real path. Gating on the outcome would be circular here:
// the assertion below IS "the key is not fail-closed", so a probe-succeeded gate would make it vacuous.
// This is an environmental precondition, independent of what the test asserts.
const SQLITE_PROBE_SUPPORTED =
  typeof (StatementSync.prototype as { setReturnArrays?: unknown }).setReturnArrays === 'function'
const sqliteProbeNote = SQLITE_PROBE_SUPPORTED
  ? ''
  : ' — SKIPPED: this runtime lacks StatementSync.setReturnArrays (needs Node >= 22.16 or >= 24)'

// Real driver db instances (never connected). Authority discovery runs through injected
// fake runners; only provably single-session clients (sqlite, a single pg Client) probe.
const cleanups: Array<() => Promise<unknown>> = []

const sqliteDb = sqliteDrizzle(':memory:')

const pgPool = new Pool({ host: 'pg.example', port: 5433, database: 'app', user: 'svc' })
const pgPoolDb = pgDrizzle({ client: pgPool })
cleanups.push(() => pgPool.end())

const pgClient = new Client({ host: 'pg.example', port: 5433, database: 'app', user: 'svc' })
const pgClientDb = pgDrizzle({ client: pgClient })
cleanups.push(() => pgClient.end().catch(() => {}))

const pjSql = postgres({ host: 'pj.example', port: 6000, database: 'pjdb', user: 'pjuser' })
const pjDb = pjDrizzle({ client: pjSql })
cleanups.push(() => pjSql.end())

// A db carrying drizzle's REAL `MySqlDialect` — the exact object `dialectOf` reads its discriminator off,
// built from driver-free `mysql-core` so the rejection stays testable without a mysql2 dependency.
const myDb = { dialect: new MySqlDialect() }

afterAll(async () => {
  await Promise.allSettled(cleanups.map((c) => c()))
})

const authority =
  (role: string, sp = '"$user", public'): RowRunner =>
  async () => [{ database: 'app', role, search_path: sp }]
const runReturning =
  (rows: Record<string, unknown>[]): RowRunner =>
  async () =>
    rows
const runThrows: RowRunner = async () => {
  throw new Error('probe failed')
}

describe('dialect & driver detection', () => {
  it('reads the dialect off each driver', () => {
    expect(dialectOf(sqliteDb)).toBe('sqlite')
    expect(dialectOf(pgPoolDb)).toBe('pg')
    expect(dialectOf(pjDb)).toBe('pg')
  })

  it('reads the concrete driver entityKind', () => {
    expect(driverOf(sqliteDb)).toBe('NodeSQLiteDatabase')
    expect(driverOf(pgPoolDb)).toBe('NodePgDatabase')
    expect(driverOf(pjDb)).toBe('PostgresJsDatabase')
  })

  it('REJECTS MySQL by name, naming the supported set — not the generic unrecognized-dialect message', () => {
    // The fixture carries a real MySqlDialect, so this is the discriminator a real mysql2 db would present.
    expect(() => dialectOf(myDb)).toThrow(/does not support MySQL/)
    expect(() => dialectOf(myDb)).toThrow(/PostgreSQL, PGlite, SQLite/)
    // …and it is told apart from a dialect we simply do not recognize, which gets the other message.
    expect(() => dialectOf({ dialect: {} })).toThrow(/Unrecognized drizzle dialect/)
  })
})

describe('one SQL execution dispatch', () => {
  it('routes raw probes and built hydration SQL through the dialect-specific runner', async () => {
    const query = sql.raw('select 1')
    const sqliteCalls: SQL[] = []
    const sqlite = {
      dialect: (sqliteDb as unknown as { dialect: unknown }).dialect,
      all: async (received: SQL) => {
        sqliteCalls.push(received)
        return [{ value: 'sqlite' }]
      },
    }
    const pgCalls: SQL[] = []
    const pg = {
      dialect: (pgPoolDb as unknown as { dialect: unknown }).dialect,
      execute: async (received: SQL) => {
        pgCalls.push(received)
        return { rows: [{ value: 'pg' }] }
      },
    }

    await expect(rowRunnerFor(sqlite)(query)).resolves.toEqual([{ value: 'sqlite' }])
    await expect(executeSql(pg, query)).resolves.toEqual([{ value: 'pg' }])
    expect(sqliteCalls).toEqual([query])
    expect(pgCalls).toEqual([query])
  })
})

describe('semanticEnvironmentKeyOf — pinned to a provable session', () => {
  it('a single-session client (pg Client) probes and reflects the actual authority', async () => {
    const asSvc = await semanticEnvironmentKeyOf(pgClientDb, { run: authority('svc') })
    const asAdmin = await semanticEnvironmentKeyOf(pgClientDb, { run: authority('admin') })
    expect(asSvc).not.toBe(asAdmin) // role change → different key
    expect(await semanticEnvironmentKeyOf(pgClientDb, { run: authority('svc', 'app, public') })).not.toBe(asSvc)
    expect(asSvc).toContain('role=svc')
  })

  it('a single-session client shares a key when the proven authority is identical', async () => {
    const a = await semanticEnvironmentKeyOf(pgClientDb, { run: authority('svc') })
    const b = await semanticEnvironmentKeyOf(pgClientDb, { run: authority('svc') })
    expect(a).toBe(b)
  })

  it('a POOLED client fails closed to a unique key — even with an injected runner', async () => {
    const a = await semanticEnvironmentKeyOf(pgPoolDb, { run: authority('svc') })
    const b = await semanticEnvironmentKeyOf(pgPoolDb, { run: authority('svc') })
    expect(a).not.toBe(b) // the probe cannot be proven to share the query's connection
    expect(a).toContain('failclosed')
    // postgres.js is likewise pooled
    expect(await semanticEnvironmentKeyOf(pjDb, { run: authority('svc') })).toContain('failclosed')
  })

  it.skipIf(!SQLITE_PROBE_SUPPORTED)(
    `probes a real single-session sqlite connection for a stable shareable key${sqliteProbeNote}`,
    async () => {
      const a = await semanticEnvironmentKeyOf(sqliteDb)
      const b = await semanticEnvironmentKeyOf(sqliteDb)
      expect(a).toBe(b)
      expect(a).toContain('sqlite')
      expect(a).not.toContain('failclosed')
    },
  )
})

describe('rlsEnabledOf — real catalog path, never assumes off', () => {
  it('reads pg_class.relrowsecurity: true / false', async () => {
    await expect(rlsEnabledOf(pgPoolDb, 'users', { run: runReturning([{ rls: true }]) })).resolves.toBe(true)
    await expect(rlsEnabledOf(pgPoolDb, 'users', { run: runReturning([{ rls: false }]) })).resolves.toBe(false)
    await expect(rlsEnabledOf(pgPoolDb, 'users', { run: runReturning([{ rls: 't' }]) })).resolves.toBe(true)
    await expect(rlsEnabledOf(pgPoolDb, 'users', { run: runReturning([{ rls: 'f' }]) })).resolves.toBe(false)
  })

  it('reports unknown when the catalog row is missing or the query fails', async () => {
    await expect(rlsEnabledOf(pgPoolDb, 'ghost', { run: runReturning([]) })).resolves.toBe('unknown')
    await expect(rlsEnabledOf(pgPoolDb, 'users', { run: runThrows })).resolves.toBe('unknown')
  })

  it('binds the relation name and schema as parameters', async () => {
    const table = "users' or true --"
    const schema = "tenant' or true --"
    let captured: SQL | undefined
    await rlsEnabledOf(pgPoolDb, table, {
      schema,
      run: async (query) => {
        captured = query
        return [{ rls: false }]
      },
    })
    const dialect = (pgPoolDb as unknown as { dialect: { sqlToQuery(query: SQL): { sql: string; params: unknown[] } } })
      .dialect
    const rendered = dialect.sqlToQuery(captured!)
    expect(rendered.sql).not.toContain(table)
    expect(rendered.sql).not.toContain(schema)
    expect(rendered.params).toEqual([table, schema])
  })

  it('reports false for engines without per-table RLS', async () => {
    await expect(rlsEnabledOf(sqliteDb, 'todos')).resolves.toBe(false)
  })
})
