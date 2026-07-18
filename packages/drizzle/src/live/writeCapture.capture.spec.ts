// Write-capture CORRECTNESS per dialect (real drivers). Drives captureMutation directly with a custom sink,
// so it asserts BOTH the change(s) emitted AND the caller-visible result (which must equal plain drizzle's).
// Precise via hidden RETURNING (new + old-PK), single OR composite PK (slice 5 lifted composite from coarse);
// fail-closed COARSE for everything outside the contract (PK-changing update, UPSERT, no PK, partial-returning,
// MySQL, unverified driver / sqlite-no-returning).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { integer as sInt, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { and, eq, sql } from 'drizzle-orm'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { captureMutation } from './writeCapture.js'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const composite = pgTable('composite', { a: integer('a'), b: integer('b'), v: text('v') }, (t) => [
  primaryKey({ columns: [t.a, t.b] }),
])
const sqUsers = sqliteTable('users', { id: sInt('id').primaryKey(), name: sText('name') })

let pgClient: PGlite
let pg: ReturnType<typeof pgDrizzle>

/** captureMutation for one op, with a sink that records every emitted batch. `wrapped`/builder are typed
 *  loosely here so the test can drive the real drizzle chain (`.values().returning()` etc.). */
// A loose builder alias so the test can drive the real chained drizzle builder (`.values().returning()`).
type AnyBuilder = (table: unknown) => any
function capturing(db: object, op: 'insert' | 'update' | 'delete', method: (t: never) => unknown) {
  const batches: TableChange[][] = []
  const wrapped = captureMutation(op, method as (...a: unknown[]) => unknown, db, (changes) =>
    batches.push(changes),
  ) as AnyBuilder
  return { wrapped, batches }
}

beforeAll(async () => {
  pgClient = new PGlite()
  pg = pgDrizzle({ client: pgClient })
  await pgClient.exec('create table users (id int primary key, name text)')
  await pgClient.exec('create table composite (a int, b int, v text, primary key (a, b))')
})
afterAll(async () => {
  await pgClient.close()
})

describe('write capture — PG (PGlite) precise via hidden RETURNING', () => {
  it('INSERT (no returning): emits {insert, new: full row}; caller gets the plain result reconstructed', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const result = await wrapped(users).values({ id: 1, name: 'a' })
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 1, name: 'a' } }]])
    // caller-visible result equals plain drizzle's no-returning result — NOT the captured rows
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
  })

  it('DELETE (no returning): emits {delete, key: PK}; result reconstructed', async () => {
    await pg.insert(users).values({ id: 2, name: 'b' })
    const { wrapped, batches } = capturing(pg, 'delete', pg.delete.bind(pg))
    const result = await wrapped(users).where(eq(users.id, 2))
    expect(batches).toEqual([[{ table: 'users', kind: 'delete', key: { id: 2 } }]])
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
  })

  it('UPDATE non-PK-changing (no returning): emits {update, new, key: PK}', async () => {
    await pg.insert(users).values({ id: 3, name: 'c' })
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await wrapped(users).set({ name: 'c2' }).where(eq(users.id, 3))
    expect(batches).toEqual([[{ table: 'users', kind: 'update', new: { id: 3, name: 'c2' }, key: { id: 3 } }]])
  })

  it("caller's own full .returning() is mapped back UNCHANGED; capture is precise", async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const rows = await wrapped(users).values({ id: 4, name: 'd' }).returning()
    expect(rows).toEqual([{ id: 4, name: 'd' }]) // exactly what plain .returning() gives
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 4, name: 'd' } }]])
  })

  it('a failed write emits nothing (a write that did not happen invalidates nothing)', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    await expect(wrapped(users).values({ id: 1, name: 'dup' })).rejects.toThrow() // PK collision
    expect(batches).toEqual([])
  })
})

describe('write capture — fail-closed COARSE (safe over-fire, never a wrong row)', () => {
  it("caller's PARTIAL .returning({id}) → coarse (no full image), but their rows come back exactly", async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const rows = await wrapped(users).values({ id: 10, name: 'p' }).returning({ id: users.id })
    expect(rows).toEqual([{ id: 10 }]) // exactly their projection
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })

  it('PK-changing update (SET touches the PK) → coarse', async () => {
    await pg.insert(users).values({ id: 20, name: 'x' })
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await wrapped(users).set({ id: 21 }).where(eq(users.id, 20))
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })

  it('UPSERT / ON CONFLICT → coarse', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    await wrapped(users).values({ id: 1, name: 'z' }).onConflictDoNothing()
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })

  it('composite PK-changing update (SET touches a PK column) → coarse', async () => {
    await pg.insert(composite).values({ a: 7, b: 8, v: 'p' })
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await wrapped(composite)
      .set({ a: 70 })
      .where(and(eq(composite.a, 7), eq(composite.b, 8)))
    expect(batches).toEqual([[{ table: 'composite', kind: 'coarse' }]]) // old composite key unrecoverable → coarse
  })

  it('raw insert-from-SELECT → coarse', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    await wrapped(users).select(pg.select({ id: users.id, name: users.name }).from(users).where(eq(users.id, 99)))
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })
})

describe('write capture — a capture fault NEVER fails a committed write (isolation)', () => {
  // The DB has already committed by the time the sink runs. A throwing sink/router/transport must not turn a
  // committed write into a caller-visible rejection, and must degrade to a coarse ingest, not half-apply.
  it('a throwing sink does NOT reject the caller; the row is committed and the plain result is returned', async () => {
    const thrown: TableChange[][] = []
    const wrapped = captureMutation('insert', pg.insert.bind(pg) as (...a: unknown[]) => unknown, pg, (changes) => {
      thrown.push(changes)
      throw new Error('sink exploded')
    }) as AnyBuilder
    const result = await wrapped(users).values({ id: 30, name: 'committed' }) // must NOT reject
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 }) // exactly plain drizzle's result
    const persisted = await pg.select().from(users).where(eq(users.id, 30))
    expect(persisted).toEqual([{ id: 30, name: 'committed' }]) // the write really committed
    // degraded: the precise feed threw, so a COARSE marker was attempted for the touched table
    expect(thrown[0]).toEqual([{ table: 'users', kind: 'insert', new: { id: 30, name: 'committed' } }])
    expect(thrown[1]).toEqual([{ table: 'users', kind: 'coarse' }])
  })

  it('a sink that throws on BOTH the precise feed and the coarse fallback still does not reject the caller', async () => {
    let calls = 0
    const wrapped = captureMutation('insert', pg.insert.bind(pg) as (...a: unknown[]) => unknown, pg, () => {
      calls++
      throw new Error('sink always explodes')
    }) as AnyBuilder
    const result = await wrapped(users).values({ id: 31, name: 'still-committed' })
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(calls).toBe(2) // precise attempt + coarse fallback attempt, both contained
  })
})

describe('write capture — composite PK precise (slice 5)', () => {
  it('composite PK INSERT (no returning): emits {insert, new: full row}; result reconstructed', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const result = await wrapped(composite).values({ a: 1, b: 2, v: 'k' })
    expect(batches).toEqual([[{ table: 'composite', kind: 'insert', new: { a: 1, b: 2, v: 'k' } }]])
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
  })

  it('composite PK DELETE (no returning): emits {delete, key: BOTH PK columns}', async () => {
    await pg.insert(composite).values({ a: 3, b: 4, v: 'd' })
    const { wrapped, batches } = capturing(pg, 'delete', pg.delete.bind(pg))
    await wrapped(composite).where(and(eq(composite.a, 3), eq(composite.b, 4)))
    expect(batches).toEqual([[{ table: 'composite', kind: 'delete', key: { a: 3, b: 4 } }]])
  })

  it('composite PK UPDATE non-PK-changing: emits {update, new, key: BOTH PK columns}', async () => {
    await pg.insert(composite).values({ a: 5, b: 6, v: 'u' })
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await wrapped(composite)
      .set({ v: 'u2' })
      .where(and(eq(composite.a, 5), eq(composite.b, 6)))
    expect(batches).toEqual([
      [{ table: 'composite', kind: 'update', new: { a: 5, b: 6, v: 'u2' }, key: { a: 5, b: 6 } }],
    ])
  })
})

describe('write capture — SQLite (node-sqlite)', () => {
  it('no-returning → coarse (lastInsertRowid unreconstructible); caller .returning() → precise', async () => {
    let DatabaseSync: unknown
    try {
      ;({ DatabaseSync } = await import('node:sqlite'))
    } catch {
      return // node:sqlite unavailable — skip
    }
    const { drizzle } = await import('drizzle-orm/node-sqlite')
    const client = new (DatabaseSync as new (p: string) => unknown)(':memory:') as never
    const db = drizzle(client)
    await db.run(sql`create table users (id integer primary key, name text)`)

    const noRet = capturing(db, 'insert', db.insert.bind(db))
    await noRet.wrapped(sqUsers).values({ id: 1, name: 'a' })
    expect(noRet.batches).toEqual([[{ table: 'users', kind: 'coarse' }]]) // reconstruction not provable → coarse

    const withRet = capturing(db, 'insert', db.insert.bind(db))
    const rows = await withRet.wrapped(sqUsers).values({ id: 2, name: 'b' }).returning()
    expect(rows).toEqual([{ id: 2, name: 'b' }])
    expect(withRet.batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 2, name: 'b' } }]])
  })
})
