// THE POST-COMMIT DOMAIN: a decoder that throws INSIDE drizzle's result mapping, after the database has
// already applied the statement.
//
// This is the failure capture creates for itself. When the caller writes without asking for rows, capture
// substitutes a hidden full `RETURNING` — and every column of it is then decoded. A `customType` whose
// `fromDriver` throws therefore explodes over rows the caller never requested, on a write that HAPPENED.
// Rethrowing that reports a committed write as a failure, which is the worst outcome this whole file exists
// to prevent; re-running it would write twice; decoding the rows outside drizzle produces values that are
// subtly not drizzle's (a `timestamptz` arrives as a raw string, not a `Date`).
//
// So capture OBSERVES the statement instead. Everything below is driven against REAL PGlite (and real
// node:sqlite where that lane runs), with the row's presence on disk asserted separately from the caller's
// result — because "the write happened" and "the caller was told it happened" are exactly the two facts this
// defect used to disagree about.

import { PGlite } from '@electric-sql/pglite'
import { customType, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { customType as sqliteCustomType, integer as sInt, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { eq, sql } from 'drizzle-orm'
import { describe, expect, it } from 'vitest'
import { captureMutation } from './writeCapture.js'
import { probeOldNewReturning } from './writeCapabilities.js'
import type { TableChange } from '../router/events.js'

/** A column whose decoder throws on EVERY value. The stored data is ordinary text — nothing about the write
 *  is malformed; only reading it back through this column fails. */
const exploding = customType<{ data: string; driverData: string }>({
  dataType: () => 'text',
  fromDriver: () => {
    throw new Error('decoder exploded')
  },
})

const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
  at: timestamp('at', { withTimezone: true }),
  payload: exploding('payload'),
})

const DDL = 'create table users (id int primary key, name text, at timestamptz, payload text)'

type AnyBuilder = (table: unknown) => any
function capturing(db: object, op: 'insert' | 'update' | 'delete', method: (t: never) => unknown) {
  const batches: TableChange[][] = []
  const wrapped = captureMutation(op, method as (...a: unknown[]) => unknown, db, (changes) =>
    batches.push(changes),
  ) as AnyBuilder
  return { wrapped, batches }
}

async function freshPg() {
  const client = new PGlite()
  const db = pgDrizzle({ client })
  await client.exec(DDL)
  return { client, db }
}

describe('post-commit decode — REACHABILITY of the state every control below depends on', () => {
  // Neither of these asserts capture's behaviour. They prove the fixture can produce the state the controls
  // claim to exercise: a decoder that genuinely throws when it is reached, on a write that genuinely does
  // NOT reach it. Without both, every control below would pass for the wrong reason.
  it('the decoder DOES throw when a statement returns its column', async () => {
    const { client, db } = await freshPg()
    await expect(db.insert(users).values({ id: 1, payload: 'x' }).returning()).rejects.toThrow('decoder exploded')
    await client.close()
  })

  it('...and plain drizzle NEVER reaches it for a write with no returning — the row commits and resolves', async () => {
    const { client, db } = await freshPg()
    // THE ORACLE. This is the exact result capture is obliged to reproduce.
    await expect(db.insert(users).values({ id: 1, payload: 'x' })).resolves.toEqual({
      rows: [],
      fields: [],
      affectedRows: 1,
    })
    expect((await client.query('select id from users')).rows).toEqual([{ id: 1 }])
    await client.close()
  })
})

describe('post-commit decode — a write capture could not READ is still a write that HAPPENED', () => {
  it('no-returning INSERT over a throwing decoder: the caller gets plain drizzle’s result, capture coarsens', async () => {
    const { client, db } = await freshPg()
    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))

    const result = await wrapped(users).values({ id: 1, name: 'a', payload: 'x' })

    // The caller asked for a plain insert and gets exactly one — byte-identical to the oracle above.
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    // The row is on disk. (Read back WITHOUT the exploding column, or the check itself would throw.)
    expect((await client.query('select id, name from users')).rows).toEqual([{ id: 1, name: 'a' }])
    // Capture never saw a row it could trust, so it says so instead of staying silent.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('the recovered count is MEASURED, not assumed: a 3-row delete reports 3', async () => {
    // The count is the whole claim of the observation tap, so it is asserted where a fabricated one would
    // be visibly wrong. `affectedRows: 0` (nothing observed), `1` (a per-statement guess) and `undefined`
    // all fail here; only reading the real returned rows produces 3.
    const { client, db } = await freshPg()
    await client.exec("insert into users values (1,'a',null,'x'), (2,'b',null,'x'), (3,'c',null,'x')")
    const { wrapped, batches } = capturing(db, 'delete', db.delete.bind(db))

    const result = await wrapped(users).where(sql`id > 0`)

    expect(result).toEqual({ rows: [], fields: [], affectedRows: 3 })
    expect((await client.query('select count(*)::int c from users')).rows).toEqual([{ c: 0 }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('UPDATE recovers the same way', async () => {
    const { client, db } = await freshPg()
    await client.exec("insert into users values (1,'a',null,'x'), (2,'b',null,'x')")
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))

    const result = await wrapped(users).set({ name: 'renamed' }).where(eq(users.id, 1))

    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect((await client.query('select name from users where id = 1')).rows).toEqual([{ name: 'renamed' }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })
})

describe('post-commit decode — a WIDENED projection is rebuilt as DRIZZLE would have decoded it', () => {
  // The caller did ask for rows here, just not the exploding one: capture widened `.returning({...})` to the
  // whole row and so introduced the decoder itself. Their own projection has to come back — and come back
  // with drizzle's own value types, which is precisely where hand-rolled decoding fails.
  const WHEN = new Date('2020-03-04T05:06:07.000Z')

  it('their columns come back, TYPED as plain drizzle types them (timestamptz → Date, not a raw string)', async () => {
    const { client, db } = await freshPg()

    // THE ORACLE: plain drizzle, same projection, on a row whose exploding column is never selected.
    const oracle = await db
      .insert(users)
      .values({ id: 1, name: 'a', at: WHEN, payload: 'x' })
      .returning({ myId: users.id, myAt: users.at })
    expect(oracle).toEqual([{ myId: 1, myAt: WHEN }])
    expect(oracle[0]!.myAt).toBeInstanceOf(Date) // the property a re-implemented decoder loses

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const result = await wrapped(users)
      .values({ id: 2, name: 'b', at: WHEN, payload: 'x' })
      .returning({ myId: users.id, myAt: users.at })

    expect(result).toEqual([{ myId: 2, myAt: WHEN }])
    expect(result[0]!.myAt).toBeInstanceOf(Date)
    // The raw driver value here is the local-offset string `2020-03-04 06:06:07+01`. Anything that decoded
    // it outside drizzle would surface that string, or a Date built from the wrong zone.
    expect(result[0]!.myAt.toISOString()).toBe('2020-03-04T05:06:07.000Z')
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    expect((await client.query('select id from users where id = 2')).rows).toEqual([{ id: 2 }])
    await client.close()
  })

  it('a column the CALLER selected themselves still throws — capture must not rescue their own error', async () => {
    // The boundary of the whole mechanism. Capture only owes transparency for decoders IT introduced; a
    // decoder the caller's own statement would have run must fail their write exactly as plain drizzle does.
    const { client, db } = await freshPg()

    await expect(db.insert(users).values({ id: 1, payload: 'x' }).returning({ p: users.payload })).rejects.toThrow(
      'decoder exploded',
    ) // the oracle: plain drizzle rejects

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    await expect(wrapped(users).values({ id: 2, payload: 'x' }).returning({ p: users.payload })).rejects.toThrow(
      'decoder exploded',
    )
    // …and yet the row IS there. What the caller is told and what the database did have come apart, and the
    // graphs follow the DATABASE: a write nobody was told about is exactly how a live query goes stale.
    expect((await client.query('select id from users where id = 2')).rows).toEqual([{ id: 2 }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })
})

describe('post-commit decode — the tap OBSERVES and nothing more', () => {
  it('an ordinary write is untouched: same result, same precise capture, no shadow left on the builder', async () => {
    const plain = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table users (id int primary key, name text)')

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plain).values({ id: 1, name: 'a' })
    const result = await builder

    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 1, name: 'a' } }]])
    // The tap shadows `_prepare` for the duration of one statement. If it leaked, the builder would carry an
    // own `_prepare` afterwards and every later execution would be observed by a stale closure.
    expect(Object.hasOwn(builder, '_prepare')).toBe(false)
    await client.close()
  })

  it('a caller error still reaches them unchanged (the tap does not swallow a rejected statement)', async () => {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table users (id int primary key, name text)')
    await client.exec("insert into users values (1, 'a')")
    const plain = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    await expect(wrapped(plain).values({ id: 1, name: 'dup' })).rejects.toThrow() // PK collision, class 23
    expect(batches).toEqual([])
    await client.close()
  })
})

describe('post-commit decode — where the tap observes NOTHING, the error is loud rather than guessed', () => {
  // The honest bound. A count nobody measured is corruption; an error is merely a failure. So every path
  // that reaches the caller without an observation rethrows, and these pin that it is the ERROR they get and
  // never a fabricated `affectedRows: 0`.

  it('the statement never reached the mapper (the driver itself failed) → the driver’s error, no count', async () => {
    const real = new PGlite()
    await real.exec(DDL)
    const db = pgDrizzle({
      client: new Proxy(real, {
        get(object, prop, receiver) {
          const value = Reflect.get(object, prop, receiver)
          if (typeof value !== 'function') return value
          if (prop !== 'query') return (value as (...a: unknown[]) => unknown).bind(object)
          return (...args: unknown[]) => {
            // No SQLSTATE — a transport-shaped failure, exactly the class that must not be second-guessed.
            if (/\breturning\b/i.test(String(args[0]))) throw new Error('connection lost')
            return (value as (...a: unknown[]) => unknown).apply(object, args)
          }
        },
      }),
    })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    let rejection: unknown
    await wrapped(users)
      .values({ id: 1, payload: 'x' })
      .catch((error: unknown) => {
        rejection = error
      })

    // Drizzle wraps a driver failure, so the caller's own cause is what identifies it.
    expect(rejection).toBeInstanceOf(Error)
    expect((rejection as { cause?: Error }).cause?.message).toBe('connection lost')
    // NOT a count nobody measured: the two shapes the fallback must never produce.
    expect(rejection).not.toMatchObject({ affectedRows: 0 })
    expect(rejection).not.toMatchObject({ affectedRows: 1 })
    expect(batches).toEqual([])
    // Nothing was written either, which is why rethrowing is the whole truth here.
    expect((await real.query('select id from users')).rows).toEqual([])
    await real.close()
  })

  // NOT PINNED, deliberately, and the reason is worth more than the test would be: capture also refuses to
  // answer when the tap cannot be PLACED at all (a builder exposing no `_prepare`). That state is
  // UNREACHABLE on every driver this package drives, because `_prepare` is the same seam the statement
  // EXECUTES through — drizzle's own `execute` is `this._prepare().execute()`. A builder capture cannot
  // observe is therefore a builder that cannot run, so there is no committed write to misreport.
  //
  // Probed rather than reasoned: hiding `_prepare` behind a proxy changed nothing (`.returning()` returns
  // the real instance, so the tap landed on it anyway and the recovery worked), and defining it away on the
  // executing builder stops the statement before it reaches the database. A control asserting that branch
  // would be asserting a state the fixture cannot reach — which is the shape of a test that passes for the
  // wrong reason. The branch stays as a cheap guard against a future driver that executes some other way,
  // and is documented as an honest bound rather than sold as covered.
})

describe('post-commit decode — the BOTH-IMAGES lane (RETURNING old.*, new.*) recovers identically', () => {
  it('an OLD/NEW update whose decoder throws still resolves with the real count', async () => {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec(DDL)
    expect(await probeOldNewReturning(db)).toBe(true) // the lane exists — this would be vacuous otherwise
    await client.exec("insert into users values (1,'a',null,'x'), (2,'b',null,'x')")

    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(users).set({ name: 'renamed' }).where(sql`id > 0`)

    // Two rows updated, through the interleaved `o0,n0,o1,n1,…` layout — the count is the row count, not
    // the count of returned expressions.
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 2 })
    expect((await client.query('select name from users order by id')).rows).toEqual([
      { name: 'renamed' },
      { name: 'renamed' },
    ])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('an OLD/NEW update rebuilds a widened projection from the NEW image — never the stale one', async () => {
    // The two halves of the `o0,n0,o1,n1,…` layout sit one position apart, so reading the wrong one is a
    // silent off-by-one that hands the caller the value their write just replaced. An UPDATE is the only
    // shape where the two differ, which is what makes this the control that can tell them apart.
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec(DDL)
    expect(await probeOldNewReturning(db)).toBe(true)
    await client.exec("insert into users values (1,'before',null,'x')")

    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(users).set({ name: 'after' }).where(eq(users.id, 1)).returning({ n: users.name })

    expect(result).toEqual([{ n: 'after' }]) // 'before' is the stale OLD half — the value plain drizzle never returns
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('an OLD/NEW delete rebuilds a widened projection from the OLD image, as the caller would have got it', async () => {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec(DDL)
    expect(await probeOldNewReturning(db)).toBe(true)
    await client.exec("insert into users values (1,'gone',null,'x')")

    const { wrapped, batches } = capturing(db, 'delete', db.delete.bind(db))
    // PostgreSQL's plain RETURNING on a DELETE is the row that WAS there, so the projection must be taken
    // from the OLD half of the layout. Reading the NEW half would give all-NULLs.
    const result = await wrapped(users).where(eq(users.id, 1)).returning({ was: users.name })

    expect(result).toEqual([{ was: 'gone' }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })
})

// ── the SQLite lane ─────────────────────────────────────────────────
// SQLite prepares differently from PostgreSQL (`SQLiteAsyncPreparedQuery`, with a plural `executors` and an
// `executeMethod`), so the tap deliberately sits on the one thing both shapes share: the `mapper`. This lane
// is what keeps that claim honest rather than asserted.

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

const sExploding = sqliteCustomType<{ data: string; driverData: string }>({
  dataType: () => 'text',
  fromDriver: () => {
    throw new Error('decoder exploded')
  },
})
const sqUsers = sqliteTable('users', {
  id: sInt('id').primaryKey(),
  name: sText('name'),
  payload: sExploding('payload'),
})

describe(`post-commit decode — SQLite${laneNote}`, () => {
  it.skipIf(!sqliteLane.ok)('a widened projection is rebuilt through the same mapper seam', async () => {
    const { DatabaseSync } = sqliteLane as { DatabaseSync: SqliteCtor }
    const { drizzle } = await import('drizzle-orm/node-sqlite')
    const client = new DatabaseSync(':memory:')
    const db = drizzle(client as never)
    await db.run(sql`create table users (id integer primary key, name text, payload text)`)

    // THE ORACLE: plain drizzle, same projection, exploding column never selected.
    const oracle = await db.insert(sqUsers).values({ id: 1, name: 'a', payload: 'x' }).returning({ n: sqUsers.name })
    expect(oracle).toEqual([{ n: 'a' }])

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const result = await wrapped(sqUsers).values({ id: 2, name: 'b', payload: 'x' }).returning({ n: sqUsers.name })

    expect(result).toEqual([{ n: 'b' }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    const rows = await db.all(sql`select id from users order by id`)
    expect(rows).toEqual([{ id: 1 }, { id: 2 }])
    client.close?.()
  })
})
