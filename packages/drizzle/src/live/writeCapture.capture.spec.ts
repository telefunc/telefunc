// Write-capture CORRECTNESS per dialect (real drivers). Drives captureMutation directly with a custom sink,
// so it asserts BOTH the change(s) emitted AND the caller-visible result (which must equal plain drizzle's).
// Precise via hidden RETURNING (new + old-PK), single OR composite PK (slice 5 lifted composite from coarse),
// and via the caller's own RETURNING — full, or a projection widened internally and projected back (rank 4).
// Fail-closed COARSE for everything outside the contract (PK-changing update, UPSERT, no-PK update/delete, a
// projected SQL expression, MySQL, unverified driver / sqlite-no-returning).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, primaryKey, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { integer as sInt, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { and, eq, sql } from 'drizzle-orm'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { captureMutation, captureRawSql } from './writeCapture.js'
import { oldNewProvenOf, oldNewReturningOf, probeOldNewReturning } from '../binding/database.js'
import { registryFor } from './dbRuntime.js'
import { compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { createLiveGraph } from '../graph/liveGraph.js'
import { QueryBuilder } from 'drizzle-orm/pg-core'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const composite = pgTable('composite', { a: integer('a'), b: integer('b'), v: text('v') }, (t) => [
  primaryKey({ columns: [t.a, t.b] }),
])
const nokey = pgTable('nokey', { a: integer('a'), b: text('b') }) // NO primary key
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
  it('a projected raw SQL expression → coarse; the caller still gets their computed value', async () => {
    // The stated NON-WIN of the widen path: the database computed `id + 1`, and no full row image can
    // produce it back. So the write runs exactly as the caller wrote it and capture fails closed.
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const rows = await wrapped(users).values({ id: 11, name: 'e' }).returning({ id: users.id, next: sql`id + 1` })
    expect(rows).toEqual([{ id: 11, next: 12 }]) // their projection, computed value and all
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
})

// premise audit #4 / H4: a cluster of writes that were coarsened by a rule broader than its own reason.
// Each is taken on its own evidence, and every one of them is asserted against the REAL driver.
describe('write capture — precision the old gates were hiding', () => {
  it('a PARTIAL .returning({id}) is WIDENED: precise capture, and the caller still gets only {id}', async () => {
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const rows = await wrapped(users).values({ id: 10, name: 'p' }).returning({ id: users.id })
    expect(rows).toEqual([{ id: 10 }]) // EXACTLY their projection — the widening is invisible to them
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 10, name: 'p' } }]])
    // and their projection is not a fabrication: it is the same row the database really wrote
    expect(await pg.select().from(users).where(eq(users.id, 10))).toEqual([{ id: 10, name: 'p' }])
  })

  it('a RENAMED projection comes back under the caller’s own aliases, in their own order', async () => {
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    const rows = await wrapped(users)
      .set({ name: 'renamed' })
      .where(eq(users.id, 10))
      .returning({ nm: users.name, ident: users.id })
    expect(rows).toEqual([{ nm: 'renamed', ident: 10 }])
    expect(batches).toEqual([[{ table: 'users', kind: 'update', new: { id: 10, name: 'renamed' }, key: { id: 10 } }]])
  })

  it('a widened DELETE keys its retraction from the full row, not from the caller’s projection', async () => {
    await pg.insert(users).values({ id: 12, name: 'gone' })
    const { wrapped, batches } = capturing(pg, 'delete', pg.delete.bind(pg))
    const rows = await wrapped(users).where(eq(users.id, 12)).returning({ nm: users.name })
    expect(rows).toEqual([{ nm: 'gone' }]) // the caller never asked for the PK…
    expect(batches).toEqual([[{ table: 'users', kind: 'delete', key: { id: 12 } }]]) // …but the retraction has it
  })

  it('an insert-from-SELECT is precise — with the caller’s RETURNING and without it', async () => {
    await pg.insert(users).values({ id: 99, name: 'src' })
    const withReturning = capturing(pg, 'insert', pg.insert.bind(pg))
    const rows = await withReturning
      .wrapped(users)
      .select(
        pg
          .select({ id: sql<number>`${users.id} + 100`, name: users.name })
          .from(users)
          .where(eq(users.id, 99)),
      )
      .returning()
    expect(rows).toEqual([{ id: 199, name: 'src' }]) // the rows that really went in
    expect(withReturning.batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 199, name: 'src' } }]])

    // and via the HIDDEN returning: the plain insert-from-select result shape is an ordinary insert's
    const hidden = capturing(pg, 'insert', pg.insert.bind(pg))
    const plain = await hidden.wrapped(users).select(
      pg
        .select({ id: sql<number>`${users.id} + 200`, name: users.name })
        .from(users)
        .where(eq(users.id, 99)),
    )
    expect(plain).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(hidden.batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 299, name: 'src' } }]])
  })

  it('an insert into a table with NO primary key is precise; its update and delete stay coarse', async () => {
    await pgClient.exec('create table nokey (a int, b text)')
    const inserted = capturing(pg, 'insert', pg.insert.bind(pg))
    await inserted.wrapped(nokey).values({ a: 1, b: 'x' })
    expect(inserted.batches).toEqual([[{ table: 'nokey', kind: 'insert', new: { a: 1, b: 'x' } }]])

    const updated = capturing(pg, 'update', pg.update.bind(pg))
    await updated.wrapped(nokey).set({ b: 'y' })
    expect(updated.batches).toEqual([[{ table: 'nokey', kind: 'coarse' }]]) // no key to retract the old row by

    const deleted = capturing(pg, 'delete', pg.delete.bind(pg))
    await deleted.wrapped(nokey).where(eq(nokey.a, 1))
    expect(deleted.batches).toEqual([[{ table: 'nokey', kind: 'coarse' }]])
  })

  it('a PREPARED write with a full .returning() captures EVERY execution precisely', async () => {
    // `prepare()` freezes the builder, so the plan made for it describes every execution. The old code
    // discarded that plan and coarsened unconditionally.
    const { wrapped, batches } = capturing(pg, 'insert', pg.insert.bind(pg))
    const prepared = wrapped(users)
      .values({ id: sql.placeholder('id'), name: sql.placeholder('name') })
      .returning()
      .prepare('cap_precise_prepared')
    expect(await prepared.execute({ id: 70, name: 'p1' })).toEqual([{ id: 70, name: 'p1' }])
    expect(await prepared.execute({ id: 71, name: 'p2' })).toEqual([{ id: 71, name: 'p2' }])
    expect(batches).toEqual([
      [{ table: 'users', kind: 'insert', new: { id: 70, name: 'p1' } }],
      [{ table: 'users', kind: 'insert', new: { id: 71, name: 'p2' } }],
    ])
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

  it('a PREPARED write with NO returning still fails closed to coarse — never uncaptured', async () => {
    // Capture cannot substitute a hidden RETURNING into a statement the caller already prepared, so this
    // half of the prepared surface stays coarse. (With the caller's own `.returning()` it is precise — see
    // "precision the old gates were hiding".)
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
    // Captured PRECISELY. Naming those positions is not the guess it once looked like: the RETURNING list
    // was BUILT from the builder's own ordered selection, so position i is that selection's i-th column by
    // construction. The next case pins that order against a projection that is deliberately not the table's.
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 1, name: 'a' } }]])
  })

  it.skipIf(!sqliteLane.ok)(
    `positional rows are named from the builder's own ORDER, not the table's${laneNote}`,
    async () => {
      // The control for the order claim. `{ name, id }` reverses the table's column order, so a mapping that
      // assumed table order would emit `{ id: 'a', name: 1 }` — values swapped, and silently precise.
      const db = await sqliteDb()
      await db.run(sql`create table users (id integer primary key, name text)`)
      const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
      const out = wrapped(sqUsers)
        .values({ id: 5, name: 'e' })
        .returning({ name: sqUsers.name, id: sqUsers.id })
        .values()
      expect(out).toEqual([['e', 5]]) // the driver really does return them in the SELECTION's order
      expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 5, name: 'e' } }]])
    },
  )

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

// premise audit #5 / H5. Where the connection can hand back BOTH images of a changed row in the write
// statement itself, two classes stop being coarse at no extra round trip. Driven against the REAL PGlite
// (PostgreSQL 18.3) with the capability actually probed — not stubbed — so these cases are asserting the
// database's behaviour, not a fixture's.
describe('write capture — both images (RETURNING old.*, new.*)', () => {
  let bothClient: PGlite
  let both: ReturnType<typeof pgDrizzle>

  beforeAll(async () => {
    bothClient = new PGlite()
    both = pgDrizzle({ client: bothClient })
    await bothClient.exec('create table users (id int primary key, name text, at timestamptz)')
    const supported = await probeOldNewReturning(both)
    expect(supported).toBe(true) // the lane exists — everything below would be vacuous otherwise
  })
  afterAll(async () => await bothClient.close())

  const imaged = pgTable('users', {
    id: integer('id').primaryKey(),
    name: text('name'),
    at: timestamp('at', { withTimezone: true }),
  })

  it('an UPDATE carries the OLD row alongside the new one', async () => {
    await both.insert(imaged).values({ id: 1, name: 'before' })
    const { wrapped, batches } = capturing(both, 'update', both.update.bind(both))
    const result = await wrapped(imaged).set({ name: 'after' }).where(eq(imaged.id, 1))
    expect(batches).toEqual([
      [
        {
          table: 'users',
          kind: 'update',
          old: { id: 1, name: 'before', at: null },
          new: { id: 1, name: 'after', at: null },
          key: { id: 1 },
        },
      ],
    ])
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 }) // the caller's result is untouched
  })

  it('a PK-CHANGING update is PRECISE — the old key is right there, keyed from the OLD image', async () => {
    // The case fork #2 gave up on: the key a retraction is addressed by is the one the statement destroys.
    await both.insert(imaged).values({ id: 2, name: 'moving' })
    const { wrapped, batches } = capturing(both, 'update', both.update.bind(both))
    await wrapped(imaged).set({ id: 20 }).where(eq(imaged.id, 2))
    expect(batches).toEqual([
      [
        {
          table: 'users',
          kind: 'update',
          old: { id: 2, name: 'moving', at: null },
          new: { id: 20, name: 'moving', at: null },
          key: { id: 2 }, // the OLD key — retracting by the NEW one would retract a row nobody has
        },
      ],
    ])
  })

  it('a DELETE carries the row that was removed, not just its key', async () => {
    await both.insert(imaged).values({ id: 3, name: 'doomed' })
    const { wrapped, batches } = capturing(both, 'delete', both.delete.bind(both))
    await wrapped(imaged).where(eq(imaged.id, 3))
    expect(batches).toEqual([
      [{ table: 'users', kind: 'delete', old: { id: 3, name: 'doomed', at: null }, key: { id: 3 } }],
    ])
  })

  it("the caller's own .returning() still comes back exactly — and a DELETE's is the OLD row", async () => {
    await both.insert(imaged).values({ id: 4, name: 'kept' })
    const updated = capturing(both, 'update', both.update.bind(both))
    expect(await updated.wrapped(imaged).set({ name: 'now' }).where(eq(imaged.id, 4)).returning()).toEqual([
      { id: 4, name: 'now', at: null }, // an UPDATE's plain RETURNING is the NEW row
    ])

    const deleted = capturing(both, 'delete', both.delete.bind(both))
    expect(await deleted.wrapped(imaged).where(eq(imaged.id, 4)).returning({ nm: imaged.name })).toEqual([
      { nm: 'now' }, // a DELETE's is the row that was deleted — the OLD one — projected as they asked
    ])
  })

  it('both images are DECODED by their own columns — a timestamp is a Date, not a string', async () => {
    // The substituted RETURNING is raw SQL (`old."at"`), which drizzle would hand back undecoded unless each
    // expression is mapped through its column. A string here would put a wrong-typed value in the row image.
    const when = new Date('2020-01-02T03:04:05.000Z')
    await both.insert(imaged).values({ id: 5, name: 'timed', at: when })
    const { wrapped, batches } = capturing(both, 'update', both.update.bind(both))
    await wrapped(imaged).set({ name: 'retimed' }).where(eq(imaged.id, 5))
    const change = batches[0]![0]!
    expect(change.old?.at).toBeInstanceOf(Date)
    expect(change.old?.at).toEqual(when)
    expect(change.new?.at).toEqual(when)
  })

  it('a db WITHOUT the capability keeps the old contract — new image only, PK change coarse', async () => {
    // The control that keeps every case above honest: the same writes on a connection whose capability was
    // never probed carry no old image at all, and the PK-changing update is coarse again.
    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await pg.insert(users).values({ id: 80, name: 'plain' })
    await wrapped(users).set({ name: 'plain2' }).where(eq(users.id, 80))
    expect(batches).toEqual([[{ table: 'users', kind: 'update', new: { id: 80, name: 'plain2' }, key: { id: 80 } }]])

    const moved = capturing(pg, 'update', pg.update.bind(pg))
    await moved.wrapped(users).set({ id: 81 }).where(eq(users.id, 80))
    expect(moved.batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
  })
})

// The composite path's teeth. Where a least-privilege role blocks the temp-table probe, the capability is
// taken from `server_version_num` instead — and that is a CLAIM, not a statement that ran. A
// PostgreSQL-compatible fork can report 18 and still reject `RETURNING old.*, new.*`, so the first write
// that depends on the claim has to be the thing that tests it.
//
// Driven against a REAL PGlite behind a client that reproduces exactly that database: it denies CREATE TEMP
// TABLE the way a revoked TEMP privilege does, answers 18 for its version, and rejects OLD/NEW. Everything
// else — the write, the retry, the row that ends up on disk — is the real database.
describe('write capture — a version number is believed only until a statement disagrees', () => {
  const DENIED = () => Object.assign(new Error('permission denied to create temporary tables'), { code: '42501' })
  const REJECTED = () => Object.assign(new Error('syntax error at or near "old"'), { code: '42601' })

  /** Filters the wire by statement text, passing everything else through to the real database. Both the
   *  connection AND the transaction object are wrapped: drizzle's `db.transaction()` calls
   *  `client.transaction(fn)`, so the probe's statements never touch `client.query` (verified). */
  function forkish(real: PGlite): PGlite {
    const screen = (text: unknown) => {
      const sql = String(text)
      if (/create temp table/i.test(sql)) throw DENIED()
      // Matches capture's OLD-image CORRELATION NAME, not the literal `old.`: the images are bound via
      // `RETURNING WITH (OLD AS …)` now, and a fake still matching the old spelling would quietly stop
      // refusing anything and assert the opposite of its own title.
      if (/tf_old__/i.test(sql)) throw REJECTED()
    }
    const wrap = <T extends object>(target: T): T =>
      new Proxy(target, {
        get(object, prop, receiver) {
          const value = Reflect.get(object, prop, receiver)
          if (typeof value !== 'function') return value
          if (prop === 'transaction') {
            return (run: (tx: object) => unknown) =>
              (value as (r: (tx: object) => unknown) => unknown).call(object, (tx) => run(wrap(tx)))
          }
          if (prop !== 'query' && prop !== 'exec') return (value as (...a: unknown[]) => unknown).bind(object)
          return (...args: unknown[]) => {
            screen(args[0])
            return (value as (...a: unknown[]) => unknown).apply(object, args)
          }
        },
      })
    return wrap(real)
  }

  it('probes to SUPPORTED but UNPROVEN, then the first OLD/NEW write demotes it and still lands', async () => {
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    const db = pgDrizzle({ client: forkish(real) })

    // The temp-table probe is refused for privilege, so the version answers instead — supported, unproven.
    await expect(probeOldNewReturning(db)).resolves.toBe(true)
    expect(oldNewProvenOf(db)).toBe(false)

    await db.insert(users).values({ id: 1, name: 'before' })
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(users).set({ name: 'after' }).where(eq(users.id, 1))

    // 1. THE WRITE WAS SAVED. The OLD/NEW statement was refused, but the caller neither saw an error nor
    //    lost their write: they got plain drizzle's result, and the row really changed on disk.
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(await db.select().from(users).where(eq(users.id, 1))).toEqual([{ id: 1, name: 'after' }])
    // 2. THIS write is coarse. The recovery re-runs the caller's statement exactly as written, which carries
    //    no RETURNING at all — so there are no rows to capture, and coarse is the honest answer.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    // 3. THE CLAIM IS RETIRED for this db, so no later write pays for it again.
    expect(oldNewReturningOf(db)).toBe(false)

    const next = capturing(db, 'update', db.update.bind(db))
    await next.wrapped(users).set({ name: 'later' }).where(eq(users.id, 1))
    expect(next.batches).toEqual([[{ table: 'users', kind: 'update', new: { id: 1, name: 'later' }, key: { id: 1 } }]])
    await real.close()
  })

  it('a PROVEN capability does not swallow the caller’s own errors', async () => {
    // The other side of the guard. On a connection that has genuinely DONE an OLD/NEW statement, a failing
    // write is the CALLER's failure — a constraint violation, say — and must propagate untouched rather
    // than be retried and quietly demote the database.
    const real = new PGlite()
    const db = pgDrizzle({ client: real })
    await real.exec('create table users (id int primary key, name text)')
    await expect(probeOldNewReturning(db)).resolves.toBe(true)
    expect(oldNewProvenOf(db)).toBe(true)

    await db.insert(users).values([
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
    ])
    const { wrapped } = capturing(db, 'update', db.update.bind(db))
    // Moving id 2 onto id 1 violates the primary key: a real caller error, on the OLD/NEW path.
    await expect(wrapped(users).set({ id: 1 }).where(eq(users.id, 2))).rejects.toThrow()
    expect(oldNewReturningOf(db)).toBe(true) // …and the capability is untouched
    await real.close()
  })
})

// ONE PRINCIPLE, applying to every statement capture substitutes for the caller's: it must never be what
// fails their write. The OLD/NEW substitution is only the newest one — the HIDDEN full RETURNING has been
// injected into no-returning writes since the original capture engine, and had the same defect.
//
// The realistic cause is not exotic: a role with INSERT/UPDATE but not SELECT on a table commits a plain
// write and is refused `… RETURNING *` with 42501. The caller never asked for RETURNING.
describe('write capture — a statement CAPTURE substituted never fails the caller’s write', () => {
  const REFUSED = () => Object.assign(new Error('permission denied for table users'), { code: '42501' })

  /** A real PGlite that refuses any statement carrying a RETURNING clause — a role that may write the
   *  table but not read it. Everything else is the real database. */
  function writeOnly(real: PGlite): PGlite {
    return new Proxy(real, {
      get(object, prop, receiver) {
        const value = Reflect.get(object, prop, receiver)
        if (typeof value !== 'function') return value
        if (prop !== 'query' && prop !== 'exec') return (value as (...a: unknown[]) => unknown).bind(object)
        return (...args: unknown[]) => {
          if (/\breturning\b/i.test(String(args[0]))) throw REFUSED()
          return (value as (...a: unknown[]) => unknown).apply(object, args)
        }
      },
    })
  }

  it('the injected RETURNING is refused → the write COMMITS, and capture degrades to coarse', async () => {
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    const db = pgDrizzle({ client: writeOnly(real) })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const result = await wrapped(users).values({ id: 1, name: 'committed' })

    // The caller asked for a plain insert and got one: no error, plain drizzle's result, row on disk.
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect((await real.query('select * from users where id = 1')).rows).toEqual([{ id: 1, name: 'committed' }])
    // Capture could not see the row, so it says so rather than staying silent.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await real.close()
  })

  it('every op recovers the same way — update and delete too', async () => {
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    await real.exec("insert into users values (1, 'a'), (2, 'b')")
    const db = pgDrizzle({ client: writeOnly(real) })

    const updated = capturing(db, 'update', db.update.bind(db))
    await updated.wrapped(users).set({ name: 'a2' }).where(eq(users.id, 1))
    expect(updated.batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    expect((await real.query('select name from users where id = 1')).rows).toEqual([{ name: 'a2' }])

    const deleted = capturing(db, 'delete', db.delete.bind(db))
    await deleted.wrapped(users).where(eq(users.id, 2))
    expect(deleted.batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    expect((await real.query('select count(*)::int c from users')).rows).toEqual([{ c: 1 }])
    await real.close()
  })

  it('a WIDENED partial returning recovers too — and the caller still gets their own projection', async () => {
    // Here the caller DID ask for rows, just not these ones: capture widened `.returning({id})` to the whole
    // row. When that is refused, their own statement has to come back — projection and all.
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    let widened = 0
    const db = pgDrizzle({
      client: new Proxy(real, {
        get(object, prop, receiver) {
          const value = Reflect.get(object, prop, receiver)
          if (typeof value !== 'function') return value
          if (prop !== 'query' && prop !== 'exec') return (value as (...a: unknown[]) => unknown).bind(object)
          return (...args: unknown[]) => {
            // Refuse only the WIDENED statement (it returns "name", which the caller did not ask for).
            if (/returning[^;]*"name"/i.test(String(args[0]))) {
              widened++
              throw REFUSED()
            }
            return (value as (...a: unknown[]) => unknown).apply(object, args)
          }
        },
      }),
    })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const rows = await wrapped(users).values({ id: 7, name: 'projected' }).returning({ id: users.id })
    expect(widened).toBe(1) // the widening really was attempted and really was refused
    expect(rows).toEqual([{ id: 7 }]) // …and the caller got EXACTLY the projection they wrote
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    expect((await real.query('select * from users where id = 7')).rows).toEqual([{ id: 7, name: 'projected' }])
    await real.close()
  })

  it('an INTEGRITY violation is the caller’s error and is NOT recovered from', async () => {
    // The line the recovery must not cross. A constraint violation cannot be caused by a RETURNING clause,
    // so it belongs to the caller: re-running it would only put a second failed statement in their log, and
    // inside a transaction would replace their real error with "current transaction is aborted".
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    await real.exec("insert into users values (1, 'taken')")
    let attempts = 0
    const db = pgDrizzle({
      client: new Proxy(real, {
        get(object, prop, receiver) {
          const value = Reflect.get(object, prop, receiver)
          if (typeof value !== 'function') return value
          if (prop !== 'query' && prop !== 'exec') return (value as (...a: unknown[]) => unknown).bind(object)
          return (...args: unknown[]) => {
            if (/insert into "users"/i.test(String(args[0]))) attempts++
            return (value as (...a: unknown[]) => unknown).apply(object, args)
          }
        },
      }),
    })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    await expect(wrapped(users).values({ id: 1, name: 'duplicate' })).rejects.toThrow()
    expect(attempts).toBe(1) // ONE attempt — the caller's error was not retried
    expect(batches).toEqual([]) // a write that did not happen invalidates nothing
    await real.close()
  })
})

// THE ROW-KEY SPACE, end to end. A captured row arrives keyed by drizzle FIELD names; the graph reads
// PHYSICAL column names (`rowSpace.projectRaw` looks up the seed descriptor's columns, which the compiler
// derived from SQL). Where a column is MAPPED — `teamId: integer('team_id')` — those two disagree, and a
// change emitted in field space matches nothing: the write is captured precisely and then invalidates
// NOTHING. Every other case in this file uses columns whose field and column names are identical, which is
// exactly why that could ship unnoticed.
//
// So this asserts the CONSEQUENCE — a real compiled graph fires — rather than the shape of the change.
describe('write capture — an emitted change lands in the space the graphs actually read', () => {
  const members = pgTable('members', {
    id: integer('id').primaryKey(),
    teamId: integer('team_id'), // MAPPED: field `teamId`, column `team_id`
  })

  /** A live stateless graph over `select … where team_id = 10`, registered on the db's router so a captured
   *  write reaches it exactly as it would in production. */
  async function watching(db: object) {
    const graph = createLiveGraph({
      kind: 'stateless',
      instanceKey: 'members-team-10',
      tables: ['members'],
      instantiate: () =>
        compileQuery(
          extractQueryShape(
            new QueryBuilder()
              .select({ id: members.id, teamId: members.teamId })
              .from(members)
              .where(eq(members.teamId, 10)),
            { dialect: 'pg' },
          ),
        ).instantiate() as never,
    })
    await graph.ready()
    // Registered the way the REGISTRY registers one in production (`buildInstance`), rather than handing the
    // router a LiveGraph directly — a LiveGraph is not itself routable, and casting past that would have the
    // test exercise a shape production never builds.
    const routable = {
      tables: ['members'],
      apply: (changes: Parameters<typeof graph.apply>[0]) => graph.apply(changes),
      notifyKey: () => 'members-team-10',
      fault: () => graph.fault(),
      reseed: () => graph.reseed(),
    }
    registryFor(db).router.register(routable)
    return { graph, routable }
  }

  it('a write to a MAPPED column invalidates the live query that selects on it', async () => {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table members (id int primary key, team_id int)')
    const { graph, routable } = await watching(db)
    const before = graph.invalidationSeq()

    // The default sink — ingestWrite → router → graph, the real path.
    const insert = captureMutation('insert', db.insert.bind(db) as (...a: unknown[]) => unknown, db) as AnyBuilder
    await insert(members).values({ id: 1, teamId: 10 })

    expect(graph.invalidationSeq()).toBeGreaterThan(before) // fired: the change was readable
    // …and it is CLASSIFIED, not blanket-fired: a row outside the predicate leaves it alone. Without this
    // the case would also pass for a graph that simply coarsens on everything.
    const after = graph.invalidationSeq()
    await insert(members).values({ id: 2, teamId: 99 })
    expect(graph.invalidationSeq()).toBe(after)

    registryFor(db).router.unregister(routable)
    await client.close()
  })
})

// The two images are bound to CORRELATION NAMES, not written as bare `old.col` / `new.col`. Bare names are
// resolved against the query's own scope first, so on a table literally named `old` they select that
// TABLE's post-update row — a wrong old image, reported as precise.
describe('write capture — the OLD image survives a table named "old"', () => {
  const old = pgTable('old', { id: integer('id').primaryKey(), v: integer('v') })

  it('captures the real previous value, not the table’s own post-update row', async () => {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table old (id int primary key, v int)')
    expect(await probeOldNewReturning(db)).toBe(true)
    await db.insert(old).values({ id: 1, v: 10 })

    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    await wrapped(old).set({ v: 11 }).where(eq(old.id, 1))

    expect(batches).toEqual([
      [{ table: 'old', kind: 'update', old: { id: 1, v: 10 }, new: { id: 1, v: 11 }, key: { id: 1 } }],
    ])
    await client.close()
  })
})
