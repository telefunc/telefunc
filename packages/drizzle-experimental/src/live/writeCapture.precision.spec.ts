// WHICH SHAPE IS PRECISE, and which fails closed to coarse. Drives captureMutation directly with a custom
// sink, so every case asserts BOTH the change(s) emitted AND the caller-visible result (which must equal
// plain drizzle's).
//
// Precise via hidden RETURNING (new + old-PK), single OR composite PK, and via the caller's own RETURNING —
// full, or a projection widened internally and projected back. Fail-closed COARSE for everything outside the
// contract: a PK-changing update, UPSERT, a no-PK update/delete, a projected SQL expression.
//
// The last describe is the one that makes the rest worth anything: a change that is precise but keyed in a
// space the graphs do not read invalidates NOTHING.

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, primaryKey, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { and, eq, sql } from 'drizzle-orm'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { captureMutation } from './writeCapture.js'
import { ingestWrite, registryFor } from './dbRuntime.js'
import { compileQuery } from '../engine/compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { createLiveGraph } from '../engine/graph/liveGraph.js'
import { QueryBuilder } from 'drizzle-orm/pg-core'
import type { TableChange } from '../router/events.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const composite = pgTable('composite', { a: integer('a'), b: integer('b'), v: text('v') }, (t) => [
  primaryKey({ columns: [t.a, t.b] }),
])
const nokey = pgTable('nokey', { a: integer('a'), b: text('b') }) // NO primary key

let pgClient: PGlite
let pg: ReturnType<typeof pgDrizzle>

/** captureMutation for one op, with a sink that records every emitted batch. `wrapped`/builder are typed
 *  loosely here so the test can drive the real drizzle chain (`.values().returning()` etc.).
 *
 *  DELIBERATELY DUPLICATED across the writeCapture spec files rather than shared. It is four lines, and each
 *  file drives a different question; a shared harness would couple files that should be free to change the
 *  sink independently. Same call as `writeCapture.postCommit.spec.ts` makes. */
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

// Deliberately NEVER probed for `RETURNING old.*, new.*`. The coarse expectations below are the contract for
// a connection without that capability — on a probed one a PK-changing update is precise instead (which is
// `writeCapture.images.spec.ts`'s subject). Probing this db would silently flip those cases.
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

    // The publishing sink — ingestWrite → router → graph, the real path (what the entry wires for autocommit).
    const insert = captureMutation('insert', db.insert.bind(db) as (...a: unknown[]) => unknown, {
      sinkMode: 'autocommit',
      identityDb: db,
      sink: (changes) => ingestWrite(db, { changes }),
    }) as AnyBuilder
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
