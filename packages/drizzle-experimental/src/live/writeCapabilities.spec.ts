// THE OLD/NEW RETURNED-IMAGE CAPABILITY, probed rather than assumed.
//
// It lives beside write capture rather than with the database binding because it has exactly one consumer:
// write PLANNING asks whether this connection can return both images of a changed row. Read authority, RLS
// and dialect identity — the binding's other facts — have entirely different consumers and change for
// entirely different reasons.
//
// A capability probe rather than a version check, so the test is the same question: run it against a real
// PostgreSQL 18 (PGlite) and against connections that cannot do it, and check that it never guesses.

import { PGlite } from '@electric-sql/pglite'
import { drizzle as pgDrizzle } from 'drizzle-orm/node-postgres'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import { drizzle as pgliteDrizzle } from 'drizzle-orm/pglite'
import { Pool } from 'pg'
import { afterAll, describe, expect, it } from 'vitest'
import { oldNewProvenOf, oldNewReturningOf, probeOldNewReturning } from './writeCapabilities.js'

// Connections that CANNOT do OLD/NEW, each a real driver rather than a stand-in — the probe answers a
// question about a live connection, so a fake one would be answering a different question. They never
// connect: every assertion below is decided before a socket is needed.
const cleanups: (() => Promise<unknown>)[] = []

const sqliteDb = sqliteDrizzle(':memory:')

const pgPool = new Pool({ host: 'pg.example', port: 5433, database: 'app', user: 'svc' })
const pgPoolDb = pgDrizzle({ client: pgPool })
cleanups.push(() => pgPool.end())

afterAll(async () => {
  await Promise.allSettled(cleanups.map((c) => c()))
})

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

describe('oldNewReturningOf — a role without TEMP privilege falls back to the version, unproven', () => {
  /** A PostgreSQL connection whose statements are answered by `respond`. Real dialect, fake wire. */
  const fakePg = (respond: (text: string) => unknown) => ({
    dialect: (pgPoolDb as unknown as { dialect: unknown }).dialect,
    execute: async (query: { queryChunks?: { value?: unknown }[] }) =>
      respond((query.queryChunks ?? []).flatMap((chunk) => chunk.value ?? []).join('')),
    transaction: async (run: (tx: unknown) => Promise<unknown>) =>
      run({
        execute: async (query: { queryChunks?: { value?: unknown }[] }) =>
          respond((query.queryChunks ?? []).flatMap((chunk) => chunk.value ?? []).join('')),
      }),
  })

  /** What PostgreSQL raises for `CREATE TEMP TABLE` without the TEMP privilege. */
  const denied = () => Object.assign(new Error('permission denied to create temporary tables'), { code: '42501' })

  it('permission-denied on the temp table + version 18 → supported, but NOT proven', async () => {
    const seen: string[] = []
    const db = fakePg((text) => {
      seen.push(text)
      if (text.startsWith('create temp table')) throw denied()
      if (text.includes('server_version_num')) return { rows: [{ server_version_num: '180003' }] }
      return { rows: [] }
    })
    await expect(probeOldNewReturning(db)).resolves.toBe(true)
    expect(oldNewReturningOf(db)).toBe(true)
    expect(oldNewProvenOf(db)).toBe(false) // a version is weaker evidence than a statement
    expect(seen.some((text) => text.includes('server_version_num'))).toBe(true) // it really did fall back
  })

  it('permission-denied + version 17 → unsupported', async () => {
    const db = fakePg((text) => {
      if (text.startsWith('create temp table')) throw denied()
      if (text.includes('server_version_num')) return { rows: [{ server_version_num: '170004' }] }
      return { rows: [] }
    })
    await expect(probeOldNewReturning(db)).resolves.toBe(false)
  })

  it('a SYNTAX refusal does NOT fall back to the version — the server already answered', async () => {
    // The distinction the fallback turns on. A server that ran the DDL and then rejected `old.x` has told
    // us the real answer; asking its version afterwards could only overrule a fact with a claim.
    const seen: string[] = []
    const db = fakePg((text) => {
      seen.push(text)
      if (text.includes('old.x')) throw Object.assign(new Error('syntax error at or near "old"'), { code: '42601' })
      return { rows: [] }
    })
    await expect(probeOldNewReturning(db)).resolves.toBe(false)
    expect(seen.some((text) => text.includes('old.x'))).toBe(true) // it got as far as the real question…
    expect(seen.some((text) => text.includes('server_version_num'))).toBe(false) // …and did not second-guess it
  })

  it('permission-denied and the version cannot be read either → unsupported', async () => {
    const db = fakePg((text) => {
      if (text.startsWith('create temp table')) throw denied()
      throw new Error('permission denied')
    })
    await expect(probeOldNewReturning(db)).resolves.toBe(false)
  })

  it('a REAL probe stays proven — the fallback path is not reached when the statement ran', async () => {
    const client = new PGlite()
    const db = pgliteDrizzle({ client })
    await probeOldNewReturning(db)
    expect(oldNewProvenOf(db)).toBe(true)
    await client.close()
  })
})
