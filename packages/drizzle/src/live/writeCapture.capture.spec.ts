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
import { captureMutation, captureRawSql } from './writeCapture.js'
import { registryFor } from './dbRuntime.js'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const composite = pgTable('composite', { a: integer('a'), b: integer('b'), v: text('v') }, (t) => [
  primaryKey({ columns: [t.a, t.b] }),
])
const sqUsers = sqliteTable('users', { id: sInt('id').primaryKey(), name: sText('name') })

// The SQLite lane runs only where it CAN run, and says out loud when it cannot.
//
// Module presence is not enough. `node:sqlite` is still stabilising, and drizzle rc.4's node-sqlite driver
// calls APIs that some Node versions' statements do not have — Node 23 lacks `stmt.setReturnArrays`, so CI
// exploded with `TypeError: stmt.setReturnArrays is not a function` on a plain `create table`. A presence
// check reported the lane as available and then failed the suite; the gate has to ask whether the DRIVER
// works here, not whether the module resolves.
//
// So this probes the real path once at module load: build a drizzle db over node:sqlite and run the
// statement that broke. Any failure disables the lane with a reason that names the missing capability, and
// the reason is appended to the test titles so the run summary shows WHY it skipped rather than a bare
// strikethrough. The lane keeps running wherever the driver genuinely works.
type SqliteLane = { ok: true; DatabaseSync: SqliteCtor } | { ok: false; reason: string }
type SqliteCtor = new (path: string) => { close?: () => void }

const sqliteLane: SqliteLane = await probeSqliteLane()
const laneNote = sqliteLane.ok ? '' : ` — SKIPPED: ${sqliteLane.reason}`

async function probeSqliteLane(): Promise<SqliteLane> {
  const mod = await import('node:sqlite').catch(() => null)
  if (!mod) return { ok: false, reason: 'node:sqlite is not available on this runtime' }
  const DatabaseSync = mod.DatabaseSync as unknown as SqliteCtor
  let client: { close?: () => void } | undefined
  try {
    const { drizzle } = await import('drizzle-orm/node-sqlite')
    client = new DatabaseSync(':memory:')
    await drizzle(client as never).run(sql`create table capability_probe (id integer primary key)`)
    return { ok: true, DatabaseSync }
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error)
    return { ok: false, reason: `drizzle's node-sqlite driver cannot run on this runtime (${detail})` }
  } finally {
    client?.close?.()
  }
}

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

describe('write capture — EVERY execution terminal captures (no silent bypass)', () => {
  // Review blocker: only `then`/`execute` were intercepted, so `.catch()`/`.finally()` reached the raw
  // QueryPromise — the row committed and NOTHING was emitted (a systematic missed invalidation).
  it('.catch() executes through the captured run', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const result = await wrapped(users)
      .values({ id: 40, name: 'catch' })
      .catch(() => 'unexpected')
    expect(result).not.toBe('unexpected') // it resolved, not rejected
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 40, name: 'catch' } }]])
  })

  it('.finally() executes through the captured run', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    let ran = false
    await wrapped(users)
      .values({ id: 41, name: 'finally' })
      .finally(() => {
        ran = true
      })
    expect(ran).toBe(true)
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 41, name: 'finally' } }]])
  })

  it('a PREPARED write invalidates on every execution (fails closed to coarse, never uncaptured)', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const prepared = wrapped(users).values({ id: 42, name: 'prepared' }).prepare('cap_prepared')
    await prepared.execute()
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })
})

describe('write capture — RAW SQL fails closed over the db’s watched tables', () => {
  // Review blocker: raw DB SQL was not intercepted at all — `reactive.run(sql`insert …`)` persisted a row and
  // published NOTHING. Owner disposition: coarsen every table with a registered graph on this db.
  it('coarsens exactly the tables that currently have registered graphs', () => {
    const db = {}
    const emitted: TableChange[][] = []
    const graph = {
      tables: ['users', 'notes'],
      apply: () => ({}) as never,
      notifyKey: () => 'g',
      fault() {},
      reseed() {},
      coarsen() {},
    }
    registryFor(db).router.register(graph)
    const run = captureRawSql(
      () => 'driver-result',
      db,
      (changes) => emitted.push(changes),
    )
    expect(run()).toBe('driver-result') // the caller's raw result is untouched
    expect(emitted).toEqual([
      [
        { table: 'users', kind: 'coarse' },
        { table: 'notes', kind: 'coarse' },
      ],
    ])
    registryFor(db).router.unregister(graph)
  })

  it('emits nothing when the db has no registered graphs (nothing to invalidate)', () => {
    const db = {}
    const emitted: TableChange[][] = []
    const run = captureRawSql(
      () => 'ok',
      db,
      (changes) => emitted.push(changes),
    )
    expect(run()).toBe('ok')
    expect(emitted).toEqual([])
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
  /** A drizzle db over a fresh in-memory node:sqlite. Only reached when the lane probe succeeded. */
  async function sqliteDb() {
    const { drizzle } = await import('drizzle-orm/node-sqlite')
    const { DatabaseSync } = sqliteLane as { DatabaseSync: SqliteCtor }
    return drizzle(new DatabaseSync(':memory:') as never)
  }

  // Skipped VISIBLY (with the capability that is missing) rather than silently returning and counting as a
  // PASS — an unsupported runtime must not look like a green SQLite lane.
  it.skipIf(!sqliteLane.ok)(
    `no-returning → coarse (lastInsertRowid unreconstructible); caller .returning() → precise${laneNote}`,
    async () => {
      const db = await sqliteDb()
      await db.run(sql`create table users (id integer primary key, name text)`)

      const noRet = capturing(db, 'insert', db.insert.bind(db))
      await noRet.wrapped(sqUsers).values({ id: 1, name: 'a' })
      expect(noRet.batches).toEqual([[{ table: 'users', kind: 'coarse' }]]) // reconstruction not provable → coarse

      const withRet = capturing(db, 'insert', db.insert.bind(db))
      const rows = await withRet.wrapped(sqUsers).values({ id: 2, name: 'b' }).returning()
      expect(rows).toEqual([{ id: 2, name: 'b' }])
      expect(withRet.batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 2, name: 'b' } }]])
    },
  )

  // `values` names two different things on a write chain and only one executes. Both directions matter: miss
  // the terminal and a real write runs uncaptured; treat the chain method as a terminal and `.values(rows)`
  // fires the statement mid-chain, breaking every insert.
  it.skipIf(!sqliteLane.ok)(`the TERMINAL .values() executes — and is captured, not bypassed${laneNote}`, async () => {
    const db = await sqliteDb()
    await db.run(sql`create table users (id integer primary key, name text)`)
    const rowCount = () =>
      (db.$client as { prepare(s: string): { get(): { c: number } } }).prepare('select count(*) c from users').get().c

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const out = wrapped(sqUsers).values({ id: 1, name: 'a' }).returning().values()

    expect(rowCount()).toBe(1) // it really did execute
    expect(out).toEqual([[1, 'a']]) // caller's result is the driver's own positional rows, unchanged
    // Captured — COARSE, because positional rows carry no column names and mapping them back would mean
    // assuming projection order. Over-invalidating is the contract; guessing is not. The point of the
    // assertion is that SOMETHING was emitted: before this, the write published nothing at all.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })

  it('the proxy does not SYNTHESIZE driver terminals the builder lacks', async () => {
    // The capture proxy must be transparent except for `.live()`. PG write builders have no
    // `run`/`all`/`get`, but the terminal branch returned a callable for those names unconditionally — so
    // `typeof builder.get === 'function'` reported true and calling it died inside the interceptor
    // ("Cannot read properties of undefined") instead of with the driver's own error.
    const { wrapped } = capturing(pg, 'insert', pg.insert.bind(pg))
    const terminals = ['run', 'all', 'get', 'then', 'catch', 'finally', 'execute']
    const shapeOf = (o: unknown) =>
      Object.fromEntries(terminals.map((t) => [t, typeof (o as Record<string, unknown>)[t]]))

    // The INITIAL builder, before `.values()`: plain Drizzle has NONE of these. Synthesizing `then` here
    // made `await db.insert(t)` thenable, so awaiting an unfinished chain would run a write the caller
    // never asked for.
    expect(shapeOf(wrapped(users))).toEqual(shapeOf(pg.insert(users)))

    // And after `.values()`, where PG still has no run/all/get but does have the promise verbs.
    expect(shapeOf(wrapped(users).values({ id: 901, name: 'shape' }))).toEqual(
      shapeOf(pg.insert(users).values({ id: 902, name: 'shape' })),
    )
  })

  it('the CHAIN .values(rows) still only BUILDS — it must not execute mid-chain', async () => {
    // The reason `values` was excluded from the terminal set in the first place. This is the control that
    // fails if the discrimination is dropped and every `values` is treated as a terminal.
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const builder = wrapped(users).values({ id: 900, name: 'chain' }) // built, never awaited
    expect(batches).toEqual([]) // nothing captured, because nothing ran
    const found = await pgClient.query('select * from users where id = 900')
    expect(found.rows).toEqual([]) // and nothing was written
    void builder
  })
})
