// EVERY WAY A WRITE CAN EXECUTE goes through capture — and nothing that merely BUILDS executes.
//
// The failure vocabulary here is one word: bypass. A terminal capture does not intercept is a row that
// commits and publishes nothing (a systematic missed invalidation); a chain method mistaken FOR a terminal
// fires the statement mid-chain and breaks every insert. Both directions are pinned below, against the real
// drivers whose terminal surfaces actually differ — PG write builders have no `run`/`all`/`get`, node-sqlite
// builders do.
//
// Raw SQL is the terminal that skips the builder entirely, so it fails closed over the db's watched tables.

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { integer as sInt, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { sql } from 'drizzle-orm'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { captureMutation, captureRawSql } from './writeCapture.js'
import { isBuilderTerminal, isCoarseAllSurface, isDriverTerminal, isPreparedTerminal } from './writeTerminals.js'
import { registryFor } from './dbRuntime.js'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
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
 *  loosely here so the test can drive the real drizzle chain (`.values().returning()` etc.).
 *
 *  DELIBERATELY DUPLICATED across the writeCapture spec files rather than shared — see the note in
 *  `writeCapture.precision.spec.ts`. */
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
})
afterAll(async () => {
  await pgClient.close()
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
  // Observed on the db's OWN router rather than through an injected sink: what the announcement does is
  // `announceCoarse`'s (dbRuntime, spec'd there), and this asks only that a raw statement triggers it.
  it('coarsens exactly the tables that currently have registered graphs', () => {
    const db = {}
    const graph = {
      tables: ['users', 'notes'],
      apply: () => ({}) as never,
      notifyKey: () => 'g',
      fault() {},
      reseed() {},
      coarsen() {},
    }
    registryFor(db).router.register(graph)
    const ingested = vi.spyOn(registryFor(db).router, 'ingest')
    const run = captureRawSql(() => 'driver-result', db)
    expect(run()).toBe('driver-result') // the caller's raw result is untouched
    expect(ingested.mock.calls.map(([batch]) => batch.changes)).toEqual([
      [
        { table: 'users', kind: 'coarse' },
        { table: 'notes', kind: 'coarse' },
      ],
    ])
    ingested.mockRestore()
    registryFor(db).router.unregister(graph)
  })

  it('emits nothing when the db has no registered graphs (nothing to invalidate)', () => {
    const db = {}
    const ingested = vi.spyOn(registryFor(db).router, 'ingest')
    const run = captureRawSql(() => 'ok', db)
    expect(run()).toBe('ok')
    expect(ingested).not.toHaveBeenCalled()
    ingested.mockRestore()
  })
})

// Named for the MECHANISM rather than the dialect, because two of these five cases run on PG. The subject is
// the terminal surface itself, which is not the same on every driver: node-sqlite write builders carry
// `run`/`all`/`get`/`values`, PG builders carry none of them, and capture's proxy has to be transparent about
// that in BOTH directions — intercept every verb the builder really has, and invent none that it does not.
describe('write capture — driver-level terminals: node-sqlite has verbs PG does not', () => {
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

// The classification itself, as a unit. Everything above proves it against real drivers; this pins the
// three classes and their one genuine overlap, so a member cannot be reclassified silently.
describe('write terminals — one classification, three questions', () => {
  const BUILDER = ['then', 'catch', 'finally', 'execute']
  const DRIVER = ['run', 'all', 'get']
  const COARSE_ALL = ['run', 'execute', 'all', 'get', 'values', 'refreshMaterializedView', 'batch']
  /** A receiver that can run what it holds; the chain builder below cannot. */
  const executable = { execute: () => {}, then: () => {} }
  const building = { values: () => {} }

  it('classifies every member of each class, and nothing else', () => {
    for (const prop of BUILDER) expect(isBuilderTerminal(prop)).toBe(true)
    for (const prop of DRIVER) expect(isDriverTerminal(prop, building)).toBe(true)
    for (const prop of COARSE_ALL) expect(isCoarseAllSurface(prop)).toBe(true)
    for (const prop of ['where', 'set', 'returning', 'from', 'prepare', 'toSQL']) {
      expect(isBuilderTerminal(prop)).toBe(false)
      expect(isDriverTerminal(prop, executable)).toBe(false)
      expect(isCoarseAllSurface(prop)).toBe(false)
    }
  })

  it('reads `values` by its RECEIVER — the same word, two meanings', () => {
    expect(isDriverTerminal('values', executable)).toBe(true) // an executable statement: runs it
    expect(isDriverTerminal('values', building)).toBe(false) // a chain builder: supplies rows
    // …and on a PREPARED statement it is unconditional, there being no chain builder left to confuse it with.
    expect(isPreparedTerminal('values')).toBe(true)
  })

  it('a prepared statement executes on every terminal of both other classes', () => {
    for (const prop of [...BUILDER, ...DRIVER]) expect(isPreparedTerminal(prop)).toBe(true)
    for (const prop of ['where', 'toSQL']) expect(isPreparedTerminal(prop)).toBe(false)
  })

  it('every driver terminal is ALSO a coarse-all surface — the overlap that must not drift', () => {
    // The systematic-miss guard. `run`/`all`/`get`/`values` name a driver terminal on a write BUILDER and a
    // raw execution surface on the DB, so a member added to one set and forgotten in the other lets
    // `db.<verb>(sql`...`)` commit with nothing announced. Bounded: this cannot see a name absent from the
    // alphabet below, so a genuinely new verb still needs a line here.
    const alphabet = [...DRIVER, 'values', 'iterate', 'stream', 'exec', 'query']
    for (const prop of alphabet) {
      if (isDriverTerminal(prop, executable)) expect(isCoarseAllSurface(prop)).toBe(true)
    }
  })

  it('a symbol is never a terminal of any class', () => {
    // The proxies dispatch on `string | symbol`, and drizzle's builders carry symbol-keyed internals.
    for (const probe of [Symbol.iterator, Symbol.toStringTag, Symbol('run')]) {
      expect(isBuilderTerminal(probe)).toBe(false)
      expect(isDriverTerminal(probe, executable)).toBe(false)
      expect(isPreparedTerminal(probe)).toBe(false)
      expect(isCoarseAllSurface(probe)).toBe(false)
    }
  })
})
