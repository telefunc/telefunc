// THE RETURNED-IMAGE LANE: what image the connection itself can hand back inside the write statement.
//
// Where a database can return BOTH images of a changed row (`RETURNING old.*, new.*`), two classes stop
// being coarse at no extra round trip — a PK-changing update, whose retraction key is the very value the
// statement destroys, and a DELETE, which can carry the row rather than just its key.
//
// The capability is a fact about the connection, and this file treats it as one: probed against real PGlite
// (PostgreSQL 18.3) rather than stubbed, demoted the moment a statement disagrees with the version number,
// and bound to CORRELATION NAMES so a table literally named `old` cannot shadow it.

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { eq } from 'drizzle-orm'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { captureMutation } from './writeCapture.js'
import { oldNewProvenOf, oldNewReturningOf, probeOldNewReturning } from './writeCapabilities.js'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })

/** captureMutation for one op, with a sink that records every emitted batch. `wrapped`/builder are typed
 *  loosely here so the test can drive the real drizzle chain (`.values().returning()` etc.).
 *
 *  DELIBERATELY DUPLICATED across the writeCapture spec files rather than shared — see the note in
 *  `writeCapture.precision.spec.ts`. */
// A loose builder alias so the test can drive the real chained drizzle builder (`.values().returning()`).
type AnyBuilder = (table: unknown) => any
function capturing(db: object, op: 'insert' | 'update' | 'delete', method: (t: never) => unknown) {
  const batches: TableChange[][] = []
  const wrapped = captureMutation(op, method as (...a: unknown[]) => unknown, {
    sinkMode: 'autocommit',
    identityDb: db,
    sink: (changes) => batches.push(changes),
  }) as AnyBuilder
  return { wrapped, batches }
}

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
    //
    // Its connection is built HERE rather than in a file-scoped `beforeAll`, and closed before the test
    // returns. Every PGlite is a live WASM instance; one held open for a whole file costs that memory for the
    // file's whole run, and this spec needs it for one case.
    const client = new PGlite()
    const pg = pgDrizzle({ client })
    await client.exec('create table users (id int primary key, name text)')

    const { wrapped, batches } = capturing(pg, 'update', pg.update.bind(pg))
    await pg.insert(users).values({ id: 80, name: 'plain' })
    await wrapped(users).set({ name: 'plain2' }).where(eq(users.id, 80))
    expect(batches).toEqual([[{ table: 'users', kind: 'update', new: { id: 80, name: 'plain2' }, key: { id: 80 } }]])

    const moved = capturing(pg, 'update', pg.update.bind(pg))
    await moved.wrapped(users).set({ id: 81 }).where(eq(users.id, 80))
    expect(moved.batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
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
