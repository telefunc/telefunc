import { PGlite } from '@electric-sql/pglite'
import { drizzle as myDrizzle } from 'drizzle-orm/mysql2'
import { drizzle as pgDrizzle } from 'drizzle-orm/node-postgres'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import { drizzle as pgliteDrizzle } from 'drizzle-orm/pglite'
import { drizzle as pjDrizzle } from 'drizzle-orm/postgres-js'
import { createPool } from 'mysql2'
import { Client, Pool } from 'pg'
import postgres from 'postgres'
import { afterAll, describe, expect, it } from 'vitest'
import { StatementSync } from 'node:sqlite'
import {
  type RowRunner,
  dialectOf,
  driverOf,
  oldNewReturningOf,
  probeOldNewReturning,
  rlsEnabledOf,
  semanticEnvironmentKeyOf,
} from './database.js'

// drizzle-orm rc.4 node-sqlite calls StatementSync.setReturnArrays() (added Node 22.16 / 24.0; ABSENT
// in 23.x). Where it is missing, the real sqlite authority probe fail-closes (production-correct) and
// the stable-shareable-key assertion below no longer holds — skip just that case there. Requires Node
// >= 22.16 or >= 24.
//
// Deliberately gated on the runtime CAPABILITY rather than on the probe's outcome, unlike the SQLite lane
// in writeCapture.capture.spec.ts which runs the real path. Gating on the outcome would be circular here:
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

const myPool = createPool({ host: 'my.example', port: 3307, database: 'mydb', user: 'myuser' })
const myDb = myDrizzle({ client: myPool })
cleanups.push(() => new Promise((resolve) => myPool.end(() => resolve(undefined))))

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
    expect(dialectOf(myDb)).toBe('mysql')
  })

  it('reads the concrete driver entityKind', () => {
    expect(driverOf(sqliteDb)).toBe('NodeSQLiteDatabase')
    expect(driverOf(pgPoolDb)).toBe('NodePgDatabase')
    expect(driverOf(pjDb)).toBe('PostgresJsDatabase')
    expect(driverOf(myDb)).toBe('MySql2Database')
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

  it('reports false for engines without per-table RLS', async () => {
    await expect(rlsEnabledOf(sqliteDb, 'todos')).resolves.toBe(false)
    await expect(rlsEnabledOf(myDb, 'users')).resolves.toBe(false)
  })
})

// ── the OLD/NEW returned-image capability ───────────────────────────
//
// A capability probe rather than a version check, so the test is the same question: run it against a real
// PostgreSQL 18 (PGlite) and against connections that cannot do it, and check that it never guesses.

describe('oldNewReturningOf — probed against the real connection, never assumed', () => {
  it('is FALSE before the probe settles — an unknown capability is never read as present', () => {
    const client = new PGlite()
    const db = pgliteDrizzle({ client })
    expect(oldNewReturningOf(db)).toBe(false) // no probe started at all
    return client.close()
  })

  it('a real PostgreSQL 18 (PGlite) connection probes TRUE, and the probe leaves nothing behind', async () => {
    const client = new PGlite()
    const db = pgliteDrizzle({ client })
    await expect(probeOldNewReturning(db)).resolves.toBe(true)
    expect(oldNewReturningOf(db)).toBe(true) // …and is readable synchronously from then on

    // The probe ran real DDL and real DML. Both were rolled back: no temp table, and — the assertion that
    // would catch a probe that committed — no row anywhere it could have written one.
    const leftovers = await client.query(`select relname from pg_class where relname like 'telefunc%'`)
    expect(leftovers.rows).toEqual([])
    await client.close()
  })

  it('a connection that REJECTS the syntax probes false — the failure is contained, not thrown', async () => {
    // What a PostgreSQL 17 does: the `returning old.x, new.x` statement is a syntax error. The probe must
    // absorb that and answer false, rather than rejecting and taking the caller's setup down with it.
    const rejected: string[] = []
    const preEighteen = {
      // A REAL PgDialect, so `dialectOf` classifies this the way it classifies any PostgreSQL db — only the
      // connection underneath is the fake. (`dialect` is internal on drizzle's public db type.)
      dialect: (pgPoolDb as unknown as { dialect: unknown }).dialect,
      transaction: async (run: (tx: unknown) => Promise<unknown>) =>
        run({
          execute: async (query: { queryChunks?: { value?: unknown }[] }) => {
            // `sql.raw(text)` carries the text inside a StringChunk, not as a plain string — reading it the
            // naive way yielded "[object Object]", so the fake matched nothing, never rejected, and the
            // case passed by reporting a capability it had not actually tested.
            const text = (query.queryChunks ?? []).flatMap((chunk) => chunk.value ?? []).join('')
            rejected.push(text)
            if (text.includes('old.x')) throw new Error('syntax error at or near "old"')
            return []
          },
        }),
    }
    await expect(probeOldNewReturning(preEighteen)).resolves.toBe(false)
    expect(oldNewReturningOf(preEighteen)).toBe(false)
    // …and it really did get as far as the OLD/NEW statement, rather than answering false for some earlier
    // reason. Without this the case would pass even if the probe never ran.
    expect(rejected.some((text) => text.includes('old.x'))).toBe(true)
  })

  it('a non-PostgreSQL connection probes false without running anything', async () => {
    await expect(probeOldNewReturning(sqliteDb)).resolves.toBe(false)
    await expect(probeOldNewReturning(myDb)).resolves.toBe(false)
  })

  it('the probe runs ONCE per db — later calls get the same settled answer', async () => {
    const client = new PGlite()
    const db = pgliteDrizzle({ client })
    const first = probeOldNewReturning(db)
    expect(probeOldNewReturning(db)).toBe(first) // the same promise, not a second probe
    await first
    await client.close()
  })
})
