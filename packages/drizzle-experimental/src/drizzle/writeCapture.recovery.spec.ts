// ONE PRINCIPLE, applying to every statement capture substitutes for the caller's: it must never be what
// fails their write. Capture runs on a database that has ALREADY committed by the time anything here can go
// wrong, so every failure below is a failure of the observer, not of the write — and an observer that turns
// "your write is safe" into "your write failed" is the worst outcome this whole feature has.
//
// Three ways that can happen, and the containment for each:
//   the SINK throws              → the caller still gets plain drizzle's result; capture degrades to coarse
//   the SUBSTITUTED statement is refused → the caller's own statement runs, projection and all
//   capture leaves STATE on the builder → a second await hands back the plain result again, not capture's rows
//
// The realistic cause of the middle one is not exotic: a role with INSERT/UPDATE but not SELECT on a table
// commits a plain write and is refused `… RETURNING *` with 42501. The caller never asked for RETURNING.

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { eq } from 'drizzle-orm'
import { describe, expect, it } from 'vitest'
import { captureMutation } from './writeCapture.js'
import type { TableChange } from '../bus/router/events.js'

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

describe('write capture — a capture fault NEVER fails a committed write (isolation)', () => {
  // The DB has already committed by the time the sink runs. A throwing sink/router/transport must not turn a
  // committed write into a caller-visible rejection, and must degrade to a coarse ingest, not half-apply.
  //
  // Each case builds and closes its own connection rather than sharing a file-scoped one: every PGlite is a
  // live WASM instance, and holding one open for a whole file costs that memory for the file's whole run.
  async function freshPg() {
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table users (id int primary key, name text)')
    return { client, pg: db }
  }

  it('a throwing sink does NOT reject the caller; the row is committed and the plain result is returned', async () => {
    const { client, pg } = await freshPg()
    const thrown: TableChange[][] = []
    const wrapped = captureMutation('insert', pg.insert.bind(pg) as (...a: unknown[]) => unknown, {
      sinkMode: 'autocommit',
      identityDb: pg,
      sink: (changes) => {
        thrown.push(changes)
        throw new Error('sink exploded')
      },
    }) as AnyBuilder
    const result = await wrapped(users).values({ id: 30, name: 'committed' }) // must NOT reject
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 }) // exactly plain drizzle's result
    const persisted = await pg.select().from(users).where(eq(users.id, 30))
    expect(persisted).toEqual([{ id: 30, name: 'committed' }]) // the write really committed
    // degraded: the precise feed threw, so a COARSE marker was attempted for the touched table
    expect(thrown[0]).toEqual([{ table: 'users', kind: 'insert', new: { id: 30, name: 'committed' } }])
    expect(thrown[1]).toEqual([{ table: 'users', kind: 'coarse' }])
    await client.close()
  })

  it('a sink that throws on BOTH the precise feed and the coarse fallback still does not reject the caller', async () => {
    const { client, pg } = await freshPg()
    let calls = 0
    const wrapped = captureMutation('insert', pg.insert.bind(pg) as (...a: unknown[]) => unknown, {
      sinkMode: 'autocommit',
      identityDb: pg,
      sink: () => {
        calls++
        throw new Error('sink always explodes')
      },
    }) as AnyBuilder
    const result = await wrapped(users).values({ id: 31, name: 'still-committed' })
    expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(calls).toBe(2) // precise attempt + coarse fallback attempt, both contained
    await client.close()
  })
})

// The OLD/NEW substitution is only the newest statement capture substitutes. The HIDDEN full RETURNING has
// been injected into no-returning writes since the original capture engine, and had the same defect.
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

// Two properties that only show up on the SECOND look: what the builder is left holding after a capture,
// and what happens when the machinery's own diagnostics misbehave.
describe('write capture — capture leaves nothing of its own behind', () => {
  it('re-awaiting the same builder gives the caller their plain result again, not capture’s rows', async () => {
    // The substitution overwrites the builder's RETURNING IN PLACE. Restoring it only on the failure paths
    // left a SUCCESSFUL capture with capture's own statement still attached, so a second `await` handed the
    // caller capture's row images instead of the plain result they got the first time.
    const client = new PGlite()
    const db = pgDrizzle({ client })
    await client.exec('create table users (id int primary key, name text)')

    await db.insert(users).values({ id: 1, name: 'before' })
    // An UPDATE, deliberately: re-awaiting an insert collides on the primary key, and that rejection would
    // mask the very thing this pins. This one is idempotent, so the SECOND await is a clean observation.
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const builder = wrapped(users).set({ name: 'after' }).where(eq(users.id, 1))

    expect(await builder).toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect(await builder).toEqual({ rows: [], fields: [], affectedRows: 1 }) // …and again, unchanged
    expect(batches).toEqual([
      [{ table: 'users', kind: 'update', new: { id: 1, name: 'after' }, key: { id: 1 } }],
      [{ table: 'users', kind: 'update', new: { id: 1, name: 'after' }, key: { id: 1 } }],
    ])
    await client.close()
  })

  it('a console that THROWS does not turn a recovered write into a failed one', async () => {
    // Every diagnostic sits immediately before a recovery. A host whose console throws would take the
    // recovery down with it and turn "your write is safe" into "your write failed".
    const client = new PGlite()
    await client.exec('create table users (id int primary key, name text)')
    await client.exec('create role writer nologin')
    await client.exec('grant usage on schema public to writer')
    await client.exec('grant insert on users to writer')
    await client.exec('set role writer') // may write, may not SELECT → capture's RETURNING is refused
    const db = pgDrizzle({ client })

    const original = console.error
    console.error = () => {
      throw new Error('console exploded')
    }
    try {
      const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
      const result = await wrapped(users).values({ id: 1, name: 'survives' })
      expect(result).toEqual({ rows: [], fields: [], affectedRows: 1 })
      expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    } finally {
      console.error = original
    }
    await client.exec('reset role')
    expect((await client.query('select name from users where id = 1')).rows).toEqual([{ name: 'survives' }])
    await client.close()
  })
})
